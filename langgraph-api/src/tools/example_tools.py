"""업무 분석용 Tool 모음 예시

Trino(Iceberg 카탈로그) 및 Milvus 벡터 검색을 활용한 도구들입니다.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import List, Literal, Optional, Sequence

from dotenv import load_dotenv
from langchain.tools import tool as lc_tool
from pydantic import BaseModel, Field

# Milvus imports (optional)
try:
    from pymilvus import (
        AnnSearchRequest,
        Collection,
        WeightedRanker,
        connections,
    )
    MILVUS_AVAILABLE = True
except ImportError:
    MILVUS_AVAILABLE = False

# Trino imports
try:
    from trino.dbapi import connect as trino_connect
except ImportError:
    trino_connect = None

load_dotenv()


# -----------------------------------------------------------------------------
# 환경 설정
# -----------------------------------------------------------------------------
_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8082"))
_TRINO_USER = os.environ.get("TRINO_USER", "admin")
_TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
_TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "silver")

_MILVUS_HOST = os.environ.get("MILVUS_HOST", "localhost")
_MILVUS_PORT = os.environ.get("MILVUS_PORT", "19530")
_MILVUS_COLLECTION = os.environ.get("MILVUS_COLLECTION", "documents")

_OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
_OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "bge-m3")


# -----------------------------------------------------------------------------
# 헬퍼 함수
# -----------------------------------------------------------------------------
def _connect_trino():
    """Trino DB-API 연결 생성"""
    if trino_connect is None:
        raise ImportError("trino package is not installed")
    return trino_connect(
        host=_TRINO_HOST,
        port=_TRINO_PORT,
        user=_TRINO_USER,
        catalog=_TRINO_CATALOG,
        schema=_TRINO_SCHEMA,
    )


def _execute_sql(sql: str) -> List[dict]:
    """SQL 실행 후 dict 리스트 반환"""
    conn = _connect_trino()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def _sql_str(v: str) -> str:
    """SQL 문자열 리터럴 이스케이프"""
    return "'" + v.replace("'", "''") + "'"


def _sql_date(v: str | date) -> str:
    """SQL DATE 리터럴"""
    if isinstance(v, date):
        v = v.isoformat()
    return f"DATE '{v}'"


# -----------------------------------------------------------------------------
# Pydantic 스키마 정의
# -----------------------------------------------------------------------------
class ProjectsListInput(BaseModel):
    """프로젝트 목록 조회 입력"""
    start_date: Optional[str] = Field(None, description="조회 시작일 (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="조회 종료일 (YYYY-MM-DD)")
    status: Optional[str] = Field(None, description="상태 필터 (진행중/완료/보류)")
    customer: Optional[str] = Field(None, description="고객사명 필터")
    limit: int = Field(50, description="최대 결과 수")


class IssuesSummaryInput(BaseModel):
    """이슈 통계 조회 입력"""
    group_by: Literal["customer", "type", "assignee", "status"] = Field(
        ..., description="집계 기준 축"
    )
    start_date: Optional[str] = Field(None, description="조회 시작일")
    end_date: Optional[str] = Field(None, description="조회 종료일")


class KpiLeadtimeInput(BaseModel):
    """KPI 리드타임 조회 입력"""
    group_by: Optional[Literal["customer", "type", "assignee"]] = Field(
        None, description="그룹화 기준"
    )
    start_date: Optional[str] = Field(None, description="조회 시작일")
    end_date: Optional[str] = Field(None, description="조회 종료일")


class IssueDetailInput(BaseModel):
    """이슈 상세 조회 입력"""
    issue_number: int = Field(..., description="이슈 번호")
    repo: Optional[str] = Field(None, description="레포지토리명")


class VectorSearchInput(BaseModel):
    """벡터 검색 입력"""
    query: str = Field(..., description="검색 쿼리")
    doc_type: Optional[List[str]] = Field(None, description="문서 유형 필터")
    repo: Optional[str] = Field(None, description="레포지토리 필터")
    top_k: int = Field(10, description="반환할 결과 수")


# -----------------------------------------------------------------------------
# Tool 구현
# -----------------------------------------------------------------------------
@lc_tool(args_schema=ProjectsListInput)
def projects_list_tool(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    customer: Optional[str] = None,
    limit: int = 50,
) -> str:
    """프로젝트 아이템 목록을 조회합니다.
    
    기간, 상태, 고객사 등으로 필터링하여 프로젝트 이슈/PR 목록을 반환합니다.
    """
    conditions = []
    
    if start_date:
        conditions.append(f"created_at >= {_sql_date(start_date)}")
    if end_date:
        conditions.append(f"created_at <= {_sql_date(end_date)}")
    if status:
        conditions.append(f"status = {_sql_str(status)}")
    if customer:
        conditions.append(f"customer_name LIKE {_sql_str(f'%{customer}%')}")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    sql = f"""
    SELECT 
        issue_number,
        title,
        status,
        customer_name,
        assignee,
        created_at,
        closed_at
    FROM project_items
    WHERE {where_clause}
    ORDER BY created_at DESC
    LIMIT {limit}
    """
    
    try:
        rows = _execute_sql(sql)
        if not rows:
            return "조회 결과가 없습니다."
        
        # 간결한 텍스트 형식으로 반환
        lines = [f"총 {len(rows)}건 조회됨\n"]
        for row in rows:
            lines.append(
                f"- #{row['issue_number']} {row['title'][:50]} "
                f"[{row['status']}] {row['customer_name'] or '-'}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"조회 실패: {e}"


@lc_tool(args_schema=IssuesSummaryInput)
def issues_summary_tool(
    group_by: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    """이슈 통계를 축(고객사/유형/담당자/상태)별로 집계합니다."""
    column_map = {
        "customer": "customer_name",
        "type": "issue_type",
        "assignee": "assignee",
        "status": "status",
    }
    group_col = column_map.get(group_by, group_by)
    
    conditions = []
    if start_date:
        conditions.append(f"created_at >= {_sql_date(start_date)}")
    if end_date:
        conditions.append(f"created_at <= {_sql_date(end_date)}")
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    sql = f"""
    SELECT 
        {group_col} as category,
        COUNT(*) as count
    FROM project_items
    WHERE {where_clause}
    GROUP BY {group_col}
    ORDER BY count DESC
    LIMIT 20
    """
    
    try:
        rows = _execute_sql(sql)
        if not rows:
            return "집계 결과가 없습니다."
        
        total = sum(r["count"] for r in rows)
        lines = [f"## {group_by} 별 통계 (총 {total}건)\n"]
        for row in rows:
            pct = row["count"] / total * 100 if total else 0
            lines.append(f"- {row['category'] or '(없음)'}: {row['count']}건 ({pct:.1f}%)")
        return "\n".join(lines)
    except Exception as e:
        return f"집계 실패: {e}"


@lc_tool(args_schema=KpiLeadtimeInput)
def kpi_leadtime_tool(
    group_by: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    """완료된 이슈의 평균 해결 기간(리드타임)을 계산합니다."""
    conditions = ["status = 'CLOSED'", "closed_at IS NOT NULL"]
    if start_date:
        conditions.append(f"closed_at >= {_sql_date(start_date)}")
    if end_date:
        conditions.append(f"closed_at <= {_sql_date(end_date)}")
    
    where_clause = " AND ".join(conditions)
    
    if group_by:
        column_map = {"customer": "customer_name", "type": "issue_type", "assignee": "assignee"}
        group_col = column_map.get(group_by, group_by)
        sql = f"""
        SELECT 
            {group_col} as category,
            COUNT(*) as count,
            AVG(DATE_DIFF('day', created_at, closed_at)) as avg_days,
            MIN(DATE_DIFF('day', created_at, closed_at)) as min_days,
            MAX(DATE_DIFF('day', created_at, closed_at)) as max_days
        FROM project_items
        WHERE {where_clause}
        GROUP BY {group_col}
        ORDER BY avg_days DESC
        """
    else:
        sql = f"""
        SELECT 
            COUNT(*) as count,
            AVG(DATE_DIFF('day', created_at, closed_at)) as avg_days,
            MIN(DATE_DIFF('day', created_at, closed_at)) as min_days,
            MAX(DATE_DIFF('day', created_at, closed_at)) as max_days
        FROM project_items
        WHERE {where_clause}
        """
    
    try:
        rows = _execute_sql(sql)
        if not rows or (len(rows) == 1 and rows[0].get("count", 0) == 0):
            return "완료된 이슈가 없습니다."
        
        if group_by:
            lines = [f"## {group_by} 별 리드타임\n"]
            for row in rows:
                lines.append(
                    f"- {row['category'] or '(없음)'}: "
                    f"평균 {row['avg_days']:.1f}일 (min: {row['min_days']}, max: {row['max_days']}) "
                    f"[{row['count']}건]"
                )
        else:
            row = rows[0]
            lines = [
                f"## 전체 리드타임 KPI\n",
                f"- 완료 건수: {row['count']}건",
                f"- 평균 해결 기간: {row['avg_days']:.1f}일",
                f"- 최소: {row['min_days']}일, 최대: {row['max_days']}일",
            ]
        return "\n".join(lines)
    except Exception as e:
        return f"KPI 계산 실패: {e}"


@lc_tool(args_schema=IssueDetailInput)
def issue_detail_tool(
    issue_number: int,
    repo: Optional[str] = None,
) -> str:
    """이슈/PR의 상세 정보와 타임라인을 조회합니다."""
    conditions = [f"issue_number = {issue_number}"]
    if repo:
        conditions.append(f"repo = {_sql_str(repo)}")
    where_clause = " AND ".join(conditions)
    
    sql = f"""
    SELECT *
    FROM project_items
    WHERE {where_clause}
    LIMIT 1
    """
    
    try:
        rows = _execute_sql(sql)
        if not rows:
            return f"이슈 #{issue_number} 을(를) 찾을 수 없습니다."
        
        row = rows[0]
        lines = [
            f"## #{row.get('issue_number')} {row.get('title', '')}",
            f"- 상태: {row.get('status', '-')}",
            f"- 고객사: {row.get('customer_name', '-')}",
            f"- 담당자: {row.get('assignee', '-')}",
            f"- 생성일: {row.get('created_at', '-')}",
            f"- 종료일: {row.get('closed_at', '-')}",
            f"\n### 설명\n{row.get('body', '(없음)')[:500]}",
        ]
        return "\n".join(lines)
    except Exception as e:
        return f"상세 조회 실패: {e}"


@lc_tool(args_schema=VectorSearchInput)
def vector_search_tool(
    query: str,
    doc_type: Optional[List[str]] = None,
    repo: Optional[str] = None,
    top_k: int = 10,
) -> str:
    """벡터 검색으로 쿼리와 유사한 문서를 찾습니다.
    
    Dense(의미) + Sparse(키워드) 하이브리드 검색을 수행합니다.
    """
    if not MILVUS_AVAILABLE:
        return "벡터 검색을 사용할 수 없습니다 (pymilvus 미설치)"
    
    try:
        # Milvus 연결
        connections.connect(alias="default", host=_MILVUS_HOST, port=_MILVUS_PORT)
        collection = Collection(_MILVUS_COLLECTION)
        collection.load()
        
        # 임베딩 생성 (실제로는 Ollama API 호출)
        # dense_vector = _call_ollama_embedding(query)
        # sparse_vector = _build_sparse_vector(query)
        
        # 하이브리드 검색 (예시)
        # dense_req = AnnSearchRequest(data=[dense_vector], ...)
        # sparse_req = AnnSearchRequest(data=[sparse_vector], ...)
        # results = collection.hybrid_search(reqs=[dense_req, sparse_req], ...)
        
        # 실제 구현 시에는 위 코드 활성화
        return f"'{query}'에 대한 검색 결과 (예시)\n- 유사 문서 1\n- 유사 문서 2\n..."
        
    except Exception as e:
        return f"벡터 검색 실패: {e}"


# -----------------------------------------------------------------------------
# 임베딩 헬퍼 (실제 구현 예시)
# -----------------------------------------------------------------------------
def _call_ollama_embedding(text: str) -> List[float]:
    """Ollama 임베딩 API 호출"""
    import urllib.request
    import json
    
    payload = json.dumps({"model": _OLLAMA_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        f"{_OLLAMA_URL}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
        return data.get("embedding", [])


def _build_sparse_vector(text: str) -> dict:
    """토큰 기반 Sparse 벡터 생성"""
    token_pattern = re.compile(r"[0-9A-Za-z가-힣_#@]+", re.UNICODE)
    tokens = token_pattern.findall(text.lower())
    token_counts = {}
    for t in tokens:
        token_counts[t] = token_counts.get(t, 0) + 1
    
    # 해시 기반 인덱스 매핑
    hash_mod = 1000000
    sparse = {}
    for token, cnt in token_counts.items():
        idx = hash(token) % hash_mod
        sparse[idx] = sparse.get(idx, 0) + cnt
    
    return sparse
