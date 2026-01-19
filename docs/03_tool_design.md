# AI Tool 설계

## 설계 철학

에이전트가 **자연어로 데이터를 조회·분석**할 수 있도록, 전용 Tool 5종을 설계했습니다.

핵심 원칙:

- **정형 데이터 쿼리 + 벡터 검색 하이브리드** 구조
- **Trino 기반 SQL 조회**로 정확한 수치 분석
- **Milvus 벡터 검색**으로 유사 사례 탐색

```text
┌─────────────────────────────────────────────────────────────┐
│                    AI Agent                                  │
│                                                              │
│  "최근 3개월간 장애 이슈 상위 10개 고객사는?"              │
└──────────────────────────┬──────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │ projects │    │ issues   │    │ kpi      │
    │ _list    │    │ _summary │    │ _leadtime│
    └────┬─────┘    └────┬─────┘    └────┬─────┘
         │               │               │
         ▼               ▼               ▼
    ┌─────────────────────────────────────────┐
    │         Trino (Gold Layer)              │
    └─────────────────────────────────────────┘
```

---

## Tool 목록

| Tool명 | 주요 기능 | 데이터 소스 |
| ------ | ------ | ------ |
| `projects_list` | 이슈 목록 조회 | Gold (Trino) |
| `issues_summary_by` | 이슈 집계 | Gold (Trino) |
| `kpi_leadtime` | 처리 리드타임 분석 | Gold (Trino) |
| `issue_detail` | 이슈 상세 조회 | Gold (Trino) |
| `vector_search` | 유사도 검색 | Milvus |

---

## 1. projects_list

**목적**: 다양한 조건으로 프로젝트 이슈 목록을 조회

### projects_list 입력 스키마

```json
{
  "type": "object",
  "properties": {
    "start_date": {"type": "string", "format": "date"},
    "end_date": {"type": "string", "format": "date"},
    "status_in": {
      "type": "array",
      "items": {"enum": ["완료", "진행중", "신규", "보류"]}
    },
    "client_in": {"type": "array", "items": {"type": "string"}},
    "category_in": {
      "type": "array",
      "items": {"enum": ["일반", "개선", "장애", "버그"]}
    },
    "assignee_in": {"type": "array", "items": {"type": "string"}},
    "limit": {"type": "integer", "default": 200}
  }
}
```

### projects_list 출력 예시

```markdown
### 프로젝트 아이템 조회 결과
- 기간: 2025-09-01 ~ 2025-10-16
- 결과 수: 3건

| # | 고객사 | 문의구분 | Status | 접수일자 | 담당자 | 제목 |
|---|--------|----------|--------|----------|--------|------|
| 1 | CustomerA | 개선 | 진행중 | 2025-09-04 | dev-1 | SPF 설정 개선 요청 |
| 2 | CustomerA | 버그 | 진행중 | 2025-09-10 | dev-1 | 메일 첨부 오류 |
| 3 | CustomerB | 일반 | 진행중 | 2025-09-12 | - | SMTP 설정 지원 |
```

---

## 2. issues_summary_by

**목적**: 축별로 이슈 건수를 집계

### issues_summary_by 입력 스키마

```json
{
  "type": "object",
  "properties": {
    "start_date": {"type": "string", "format": "date"},
    "end_date": {"type": "string", "format": "date"},
    "group_dims": {
      "type": "array",
      "items": {"enum": ["고객사", "문의구분", "엔지니어", "Status", "저장소"]}
    },
    "top_n": {"type": "integer", "default": 10},
    "min_count": {"type": "integer", "default": 1}
  },
  "required": ["group_dims"]
}
```

### issues_summary_by 출력 예시

```markdown
### 이슈 집계 결과
- 기간: 2025-01-01 ~ 2025-12-31
- 집계 축: 고객사

| 고객사 | issue_cnt |
|--------|-----------|
| CustomerA | 15 |
| CustomerB | 12 |
| CustomerC | 8 |
```

### 동작 메모

- `엔지니어` 축: 담당자 배열을 UNNEST하여 담당자별 중복 집계
- `저장소` 축: GitHub repo 단위로 집계

---

## 3. kpi_leadtime

**목적**: 완료된 이슈의 평균 해결 기간(리드타임) 계산

### kpi_leadtime 입력 스키마

```json
{
  "type": "object",
  "properties": {
    "start_date": {"type": "string", "format": "date"},
    "end_date": {"type": "string", "format": "date"},
    "by": {"enum": ["문의구분", "고객사", "엔지니어"]}
  },
  "required": ["by"]
}
```

### kpi_leadtime 출력 예시

```markdown
### 평균 해결기간 집계 결과
- 기간: 2025-01-01 ~ 2025-12-31
- 분류 기준: 문의구분
- 대상: Status='완료' & 종료일자 존재

| 문의구분 | avg_days | cnt |
|----------|----------|-----|
| 버그 | 1.8 | 24 |
| 장애 | 2.3 | 15 |
| 개선 | 3.5 | 27 |
```

### 계산 로직

```sql
leadtime_days = DATE_DIFF('day', 접수일자, 종료일자)
```

---

## 4. issue_detail

**목적**: 단일 이슈/PR의 상세 정보 조회

### issue_detail 입력 스키마

```json
{
  "type": "object",
  "properties": {
    "repo": {"type": "string", "default": "mycompany-cloud/service-mail"},
    "number": {"type": "integer"}
  },
  "required": ["number"]
}
```

### issue_detail 출력 예시

```markdown
# 캘린더 오류 수정 PR (#812)
- **Type**: Pull Request
- **Repository**: `mycompany-cloud/service-mail`
- **State**: MERGED
- **Author**: dev-1
- **Created At**: 2025-10-15T00:00:00
- **Merged At**: 2025-10-16T03:12:44
- **Lead Time**: 1.13일

## 라벨
- bugfix

## 프로젝트 정보
- **Client**: CustomerA
- **Category**: 장애

## 본문
PR 상세 설명...

## 최근 코멘트
- 2025-10-15 · **dev-2** — ical4j 의존성 제거로 오류 해결

## 타임라인
- 2025-10-15 · `renamed` — 제목 변경
- 2025-10-16 · `merged` — master에 병합
```

---

## 5. vector_search

**목적**: Dense/Sparse 하이브리드 검색으로 유사 문서 탐색

### vector_search 입력 스키마

```json
{
  "type": "object",
  "properties": {
    "query": {"type": "string"},
    "repo": {"type": "string"},
    "doc_type": {
      "type": "array",
      "items": {"enum": ["ISSUE", "PR", "COMMENT", "PROJECT_ITEM"]},
      "default": ["ISSUE", "PR"]
    },
    "top_k": {"type": "integer", "default": 5, "maximum": 50}
  },
  "required": ["query"]
}
```

### vector_search 출력 예시

```markdown
query: SPF 설정 개선
mode: vector
filters: repo=mycompany-cloud/service-mail
result_count: 3

- 1) [ISSUE] service-mail · SPF 설정 개선 요청 (score 0.912)
  고객사: CustomerA, SPF 레코드 갱신 요청
  created: 2025-09-04

- 2) [PR] service-mail · Fix: SPF policy update (score 0.874)
  SPF TXT 레코드 업데이트 및 테스트 결과
  created: 2025-09-05
```

### 하이브리드 검색 전략

```python
# Dense + Sparse 가중 결합
score = 0.6 * score_dense + 0.4 * score_sparse

# Dense: bge-m3 임베딩 (1024차원)
# Sparse: BM25 유사 토큰 기반 검색
```

---

## Milvus 벡터 컬렉션 스키마

### 필드 정의

| 필드명 | 타입 | 설명 |
| ------ | ------ | ------ |
| `chunk_uid` | VARCHAR(512) | PK, 청크 고유 ID |
| `doc_uid` | VARCHAR(512) | 상위 문서 키 |
| `repo` | VARCHAR(256) | 저장소 |
| `doc_type` | VARCHAR(64) | ISSUE/PR/COMMENT/PROJECT_ITEM |
| `chunk_index` | INT64 | 청크 인덱스 (0부터) |
| `dense_vector` | FLOAT_VECTOR(1024) | Dense 임베딩 벡터 |
| `sparse_vector` | SPARSE_FLOAT_VECTOR | BM25 유사 Sparse 벡터 |

### 인덱스

- **Dense**: HNSW, Metric=IP, `{M: 16, efConstruction: 200}`
- **Sparse**: SPARSE_INVERTED_INDEX, Metric=IP

---

## 환경 변수

```bash
# Trino 연결
TRINO_JDBC_URL=jdbc:trino://host:port/iceberg
TRINO_USER=admin
TRINO_SCHEMA=gold

# Milvus 연결
MILVUS_URI=http://localhost:19530
MILVUS_COLLECTION=gh_chunks_v1

# 임베딩 모델
OLLAMA_MODEL=bge-m3
OLLAMA_EMBED_DIM=1024
```
