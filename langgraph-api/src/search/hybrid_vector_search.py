"""Milvus 하이브리드 벡터 검색 구현 예시

Dense(의미) + Sparse(키워드) 하이브리드 검색을 통해
정확도와 재현율을 동시에 높이는 검색 전략을 구현합니다.

핵심 구성요소:
1. Dense 검색: BGE-M3 임베딩을 통한 시맨틱 검색
2. Sparse 검색: BM25 스타일 토큰 기반 키워드 검색
3. WeightedRanker: 두 검색 결과를 가중 결합하여 최종 순위 결정
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Union

import requests
from pydantic import BaseModel, Field, field_validator

# Milvus imports
try:
    from pymilvus import (
        AnnSearchRequest,
        Collection,
        WeightedRanker,
        connections,
    )
    from pymilvus.exceptions import MilvusException
    MILVUS_AVAILABLE = True
except ImportError:
    MILVUS_AVAILABLE = False
    AnnSearchRequest = None
    WeightedRanker = None
    MilvusException = Exception

# Trino imports
try:
    from trino.dbapi import connect as trino_connect
except ImportError:
    trino_connect = None


# -----------------------------------------------------------------------------
# 환경 설정
# -----------------------------------------------------------------------------
_OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
_OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "bge-m3")
_MILVUS_HOST = os.environ.get("MILVUS_HOST", "localhost")
_MILVUS_PORT = int(os.environ.get("MILVUS_PORT", "19530"))
_MILVUS_COLLECTION = os.environ.get("MILVUS_COLLECTION", "documents")
_MILVUS_MAX_TOP_K = 50
_SPARSE_HASH_MOD = 1_000_000

# 하이브리드 검색 가중치 (Dense:Sparse = 6:4)
_DENSE_WEIGHT_RATIO = 6.0
_SPARSE_WEIGHT_RATIO = 4.0


# -----------------------------------------------------------------------------
# Pydantic 스키마
# -----------------------------------------------------------------------------
DocTypeLiteral = str  # "ISSUE", "PR", "COMMENT", "PROJECT_ITEM", "COMMIT"


class HybridSearchInput(BaseModel):
    """하이브리드 검색 입력 스키마"""
    query: str = Field(..., description="검색할 문장")
    repo: Optional[str] = Field(
        None,
        description="owner/repo 형식. None이면 모든 레포에서 검색",
    )
    doc_type: Optional[List[str]] = Field(
        default_factory=lambda: ["ISSUE", "PR"],
        description="검색 대상 문서 유형 (ISSUE, PR, COMMENT, PROJECT_ITEM, COMMIT)",
    )
    top_k: Optional[int] = Field(
        5,
        ge=1,
        le=_MILVUS_MAX_TOP_K,
        description="최종 반환할 결과 개수",
    )

    @field_validator("doc_type", mode="before")
    @classmethod
    def normalize_doc_type(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return [value.upper()]
        if isinstance(value, (list, tuple)):
            return [str(v).upper() for v in value if v]
        return None


# -----------------------------------------------------------------------------
# 임베딩 함수
# -----------------------------------------------------------------------------
def call_ollama_embedding(text: str) -> List[float]:
    """Ollama API를 통해 Dense 임베딩 벡터 생성
    
    Args:
        text: 임베딩할 텍스트
        
    Returns:
        1024차원 float 벡터 (BGE-M3 기준)
    """
    endpoint = _OLLAMA_URL.rstrip("/") + "/api/embeddings"
    payload = {"model": _OLLAMA_MODEL, "prompt": text}
    
    for attempt in range(3):
        try:
            response = requests.post(endpoint, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                embedding = data.get("embedding", [])
                return [float(x) for x in embedding]
        except Exception:
            pass
        import time
        time.sleep(0.5 * (attempt + 1))
    
    raise RuntimeError("Ollama embedding request failed after 3 attempts")


def build_sparse_vector(text: str) -> Dict[int, float]:
    """BM25 스타일 Sparse 벡터 생성
    
    토큰 빈도와 IDF를 기반으로 가중치를 계산하고,
    해시 함수를 통해 고정 크기 인덱스에 매핑합니다.
    
    Args:
        text: 토큰화할 텍스트
        
    Returns:
        {인덱스: 가중치} 형태의 sparse 벡터
    """
    # 토큰화: 알파벳, 숫자, 한글, 특수문자 포함
    token_pattern = re.compile(r"[0-9A-Za-z가-힣_#@]+", re.UNICODE)
    tokens = token_pattern.findall(text.lower())
    
    if not tokens:
        return {}
    
    # 토큰 빈도 계산
    token_counts: Dict[str, int] = {}
    for token in tokens:
        token_counts[token] = token_counts.get(token, 0) + 1
    
    # 해시 기반 인덱스 매핑 (충돌 시 가중치 합산)
    sparse: Dict[int, float] = {}
    for token, count in token_counts.items():
        # MD5 해시로 안정적인 인덱스 생성
        hash_bytes = hashlib.md5(token.encode("utf-8")).hexdigest()
        idx = int(hash_bytes, 16) % _SPARSE_HASH_MOD
        sparse[idx] = sparse.get(idx, 0) + float(count)
    
    return sparse


# -----------------------------------------------------------------------------
# Milvus 연결 관리
# -----------------------------------------------------------------------------
_milvus_collection: Optional[Collection] = None


def get_milvus_collection() -> Collection:
    """Milvus 컬렉션 싱글톤 반환"""
    global _milvus_collection
    
    if _milvus_collection is None:
        connections.connect(
            alias="default",
            host=_MILVUS_HOST,
            port=_MILVUS_PORT,
        )
        _milvus_collection = Collection(_MILVUS_COLLECTION)
        _milvus_collection.load()
    
    return _milvus_collection


# -----------------------------------------------------------------------------
# 하이브리드 검색 구현
# -----------------------------------------------------------------------------
def hybrid_vector_search(
    query: str,
    repo: Optional[str] = None,
    doc_type: Optional[List[str]] = None,
    top_k: int = 5,
    dense_weight: float = 0.6,
    sparse_weight: float = 0.4,
) -> Dict[str, Any]:
    """Dense + Sparse 하이브리드 벡터 검색
    
    두 검색 방식의 결과를 WeightedRanker로 결합하여
    의미적 유사성과 키워드 매칭을 동시에 활용합니다.
    
    Args:
        query: 검색 쿼리 (자연어 문장)
        repo: 레포지토리 필터 (owner/repo 형식)
        doc_type: 문서 유형 필터 리스트
        top_k: 반환할 결과 수
        dense_weight: Dense 검색 가중치 (기본 0.6)
        sparse_weight: Sparse 검색 가중치 (기본 0.4)
        
    Returns:
        검색 결과 딕셔너리:
        - query: 검색 쿼리
        - mode: "hybrid"
        - filters: 적용된 필터
        - result_count: 결과 수
        - results: 검색 결과 리스트
        - params: 검색 파라미터
    """
    if not MILVUS_AVAILABLE:
        raise RuntimeError("pymilvus is not installed")
    
    if not query or not query.strip():
        raise ValueError("query is required")
    
    if top_k <= 0 or top_k > _MILVUS_MAX_TOP_K:
        raise ValueError(f"top_k must be between 1 and {_MILVUS_MAX_TOP_K}")
    
    # 필터 표현식 구성
    filter_parts: List[str] = []
    filters: Dict[str, Any] = {}
    
    if repo:
        filter_parts.append(f'repo == "{repo}"')
        filters["repo"] = repo
    
    if doc_type:
        doc_type_upper = [dt.upper() for dt in doc_type]
        type_expr = " or ".join([f'doc_type == "{dt}"' for dt in doc_type_upper])
        filter_parts.append(f"({type_expr})")
        filters["doc_type"] = doc_type_upper
    
    expr = " and ".join(filter_parts) if filter_parts else None
    
    # 벡터 생성
    dense_vector = call_ollama_embedding(query.strip())
    sparse_vector = build_sparse_vector(query.strip())
    
    if not sparse_vector:
        raise ValueError("Query is too short to generate sparse tokens")
    
    # Milvus 컬렉션
    collection = get_milvus_collection()
    
    # 프리페치 수 (후처리를 위해 더 많이 가져옴)
    prefetch = min(_MILVUS_MAX_TOP_K, max(top_k, top_k * 3))
    
    # 검색 파라미터
    dense_params = {"metric_type": "IP", "params": {}}
    sparse_params = {"metric_type": "IP", "params": {}}
    
    # AnnSearchRequest 생성
    dense_req = AnnSearchRequest(
        data=[dense_vector],
        anns_field="dense_vector",
        param=dense_params,
        limit=prefetch,
        expr=expr,
    )
    
    sparse_req = AnnSearchRequest(
        data=[sparse_vector],
        anns_field="sparse_vector",
        param=sparse_params,
        limit=prefetch,
        expr=expr,
    )
    
    # WeightedRanker로 결과 결합
    rerank = WeightedRanker(dense_weight, sparse_weight)
    
    # 하이브리드 검색 실행
    try:
        search_res = collection.hybrid_search(
            [dense_req, sparse_req],
            limit=prefetch,
            output_fields=[
                "chunk_uid",
                "doc_uid",
                "repo",
                "doc_type",
                "chunk_index",
                "chunk_text",
            ],
            rerank=rerank,
        )
    except MilvusException as exc:
        raise RuntimeError(f"Hybrid search failed: {exc}") from exc
    
    # 결과 처리
    hits = search_res[0] if search_res else []
    results: List[Dict[str, Any]] = []
    
    seen_docs: set = set()
    for hit in hits:
        doc_uid = hit.entity.get("doc_uid")
        
        # 문서 단위 중복 제거
        if doc_uid in seen_docs:
            continue
        seen_docs.add(doc_uid)
        
        results.append({
            "chunk_uid": hit.entity.get("chunk_uid"),
            "doc_uid": doc_uid,
            "repo": hit.entity.get("repo"),
            "doc_type": hit.entity.get("doc_type"),
            "chunk_index": hit.entity.get("chunk_index"),
            "chunk_text": hit.entity.get("chunk_text"),
            "score": float(hit.score),
        })
        
        if len(results) >= top_k:
            break
    
    return {
        "query": query.strip(),
        "mode": "hybrid",
        "filters": filters,
        "result_count": len(results),
        "results": results,
        "params": {
            "dense_weight": dense_weight,
            "sparse_weight": sparse_weight,
            "prefetch": prefetch,
        },
    }


def format_search_results_markdown(response: Dict[str, Any]) -> str:
    """검색 결과를 Markdown 형식으로 포맷팅
    
    Args:
        response: hybrid_vector_search 반환값
        
    Returns:
        Markdown 형식의 결과 문자열
    """
    lines: List[str] = []
    
    lines.append(f"query: {response.get('query', '')}")
    lines.append(f"mode: {response.get('mode', 'hybrid')}")
    
    filters = response.get("filters", {})
    if filters:
        filter_str = ", ".join(f"{k}={v}" for k, v in filters.items())
        lines.append(f"filters: {filter_str}")
    
    lines.append(f"result_count: {response.get('result_count', 0)}")
    lines.append("")
    
    for i, result in enumerate(response.get("results", []), 1):
        doc_type = result.get("doc_type", "UNKNOWN")
        repo = result.get("repo", "")
        score = result.get("score", 0)
        chunk_text = result.get("chunk_text", "")
        
        # 텍스트 미리보기 (처음 200자)
        preview = chunk_text[:200].replace("\n", " ").strip()
        if len(chunk_text) > 200:
            preview += "..."
        
        lines.append(f"- {i}) [{doc_type}] {repo} (score {score:.3f})")
        lines.append(f"  {preview}")
        lines.append("")
    
    return "\n".join(lines)


# -----------------------------------------------------------------------------
# LangChain Tool 래퍼
# -----------------------------------------------------------------------------
def vector_search_tool(
    query: str,
    repo: Optional[str] = None,
    doc_type: Optional[List[str]] = None,
    top_k: int = 5,
) -> str:
    """벡터 검색 Tool (LangChain 연동용)
    
    하이브리드 검색을 수행하고 결과를 Markdown으로 반환합니다.
    """
    response = hybrid_vector_search(
        query=query,
        repo=repo,
        doc_type=doc_type,
        top_k=top_k,
    )
    return format_search_results_markdown(response)


# -----------------------------------------------------------------------------
# 사용 예시
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 테스트 쿼리
    test_query = "메일 첨부파일 용량 제한 오류"
    
    try:
        result = hybrid_vector_search(
            query=test_query,
            doc_type=["ISSUE", "PR"],
            top_k=5,
        )
        print(format_search_results_markdown(result))
    except Exception as e:
        print(f"Error: {e}")
