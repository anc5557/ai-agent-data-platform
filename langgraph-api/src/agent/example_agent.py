"""LangGraph 기반 업무 분석 에이전트 예시

LLM 모델과 커스텀 Tool들을 연결하여 업무 데이터 분석을 수행하는 에이전트 그래프입니다.
메시지 기반 입력: {"messages": [{"role": "user", "content": "..."}]}
"""
from __future__ import annotations

import os

from langchain.agents import create_agent
from langchain.agents.middleware import TodoListMiddleware
from langchain.agents.middleware.context_editing import (
    ClearToolUsesEdit,
    ContextEditingMiddleware,
)
from langchain_ollama import ChatOllama

# Custom Tool imports
from tools.example_tools import (
    projects_list_tool,
    issues_summary_tool,
    kpi_leadtime_tool,
    issue_detail_tool,
    vector_search_tool,
)


# -----------------------------------------------------------------------------
# 환경 설정
# -----------------------------------------------------------------------------
_OLLAMA_DEFAULT_URL = "http://localhost:11434"
_OLLAMA_URL = os.environ.get("OLLAMA_URL", _OLLAMA_DEFAULT_URL).rstrip("/")
_OLLAMA_CHAT_MODEL = os.environ.get("OLLAMA_CHAT_MODEL", "llama3.1:8b")

# LLM 모델 초기화
model = ChatOllama(
    base_url=_OLLAMA_URL,
    model=_OLLAMA_CHAT_MODEL,
    temperature=0.0,
)

# 에이전트에 연결할 Tool 목록
tools = [
    projects_list_tool,
    issues_summary_tool,
    kpi_leadtime_tool,
    issue_detail_tool,
    vector_search_tool,
]

# -----------------------------------------------------------------------------
# 에이전트 그래프 정의
# -----------------------------------------------------------------------------
graph = create_agent(
    model=model,
    tools=tools,
    middleware=[
        # TodoListMiddleware: 멀티스텝 태스크 추적
        # - 복잡한 요청을 여러 단계로 분해하여 순차 실행
        TodoListMiddleware(),
        
        # ContextEditingMiddleware: 컨텍스트 토큰 관리
        # - 대화가 길어지면 오래된 Tool 호출 결과 정리
        # - trigger: 토큰 수 임계값
        # - keep: 유지할 최근 Tool 호출 수
        ContextEditingMiddleware(
            edits=[
                ClearToolUsesEdit(
                    trigger=7000,  # 7000 토큰 초과 시 정리 시작
                    keep=3,        # 최근 3개 Tool 호출만 유지
                    exclude_tools=["write_todos"],  # 제외할 Tool
                )
            ]
        ),
    ],
    system_prompt=SYSTEM_PROMPT,
)

# -----------------------------------------------------------------------------
# System Prompt
# -----------------------------------------------------------------------------
SYSTEM_PROMPT = """## 역할
업무 데이터 분석가. SQL 데이터베이스와 벡터 검색을 활용하여 프로젝트 현황, 이슈 통계, 
KPI 지표를 분석하고 인사이트를 제공합니다.

## 사용 가능한 도구
- projects_list: 프로젝트 아이템 목록/필터 조회
- issues_summary: 축별 건수 분포 (고객/유형/담당자 등)
- kpi_leadtime: 완료 항목 평균 해결기간 계산
- issue_detail: 단일 이슈 상세 및 타임라인
- vector_search: 문맥 + 키워드 기반 유사 사례 검색

## 분석 플로우 예시

### 1) 운영 현황 스냅샷
projects_list(필터: 상태/채널) → issues_summary(고객/유형) → kpi_leadtime

### 2) 고객/담당자 드릴다운
issues_summary로 상위 항목 파악 → projects_list(해당 필터) → issue_detail

### 3) 유사 사례 탐색
vector_search(query) → 후보 요약 → issue_detail로 상세 확인

## 응답 원칙
- 첫 줄에 **질의 조건 + 핵심 수치** 요약
- 표/불릿으로 간결 정리
- 결과 없으면 **0건** 명시
- 불명확하면 필요한 정보를 1문장으로 요청
- 추정 금지: 도구가 주지 않은 수치는 만들지 않음
"""
