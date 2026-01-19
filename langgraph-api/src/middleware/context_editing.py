"""LangGraph 미들웨어 예시 - 컨텍스트 토큰 관리

대화 길이가 길어지면 컨텍스트 윈도우를 초과하는 문제를 방지하기 위해
오래된 Tool 호출 결과를 정리하는 미들웨어입니다.

핵심 기능:
1. 토큰 수 모니터링 (trigger threshold)
2. 최근 N개 Tool 호출만 유지
3. 특정 Tool은 정리에서 제외 (예: write_todos)
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)


# -----------------------------------------------------------------------------
# 토큰 추정 유틸리티
# -----------------------------------------------------------------------------
def estimate_tokens(text: str) -> int:
    """텍스트의 토큰 수를 추정 (한글 고려)
    
    간단한 휴리스틱:
    - 영문: 약 4 문자 = 1 토큰
    - 한글: 약 2 문자 = 1 토큰
    - 실제로는 tokenizer를 사용해야 정확
    """
    if not text:
        return 0
    
    korean_chars = sum(1 for c in text if "\uac00" <= c <= "\ud7a3")
    other_chars = len(text) - korean_chars
    
    # 한글은 2자당 1토큰, 나머지는 4자당 1토큰으로 추정
    return (korean_chars // 2) + (other_chars // 4) + 1


def count_message_tokens(message: BaseMessage) -> int:
    """메시지 하나의 토큰 수를 추정"""
    content = message.content if isinstance(message.content, str) else ""
    
    # Tool call이 있는 경우 추가 토큰 계산
    additional = 0
    if hasattr(message, "tool_calls"):
        for tc in getattr(message, "tool_calls", []) or []:
            if isinstance(tc, dict):
                additional += estimate_tokens(tc.get("name", ""))
                args = tc.get("args", {})
                if isinstance(args, dict):
                    additional += estimate_tokens(json.dumps(args, ensure_ascii=False))
    
    return estimate_tokens(content) + additional


def count_total_tokens(messages: Sequence[BaseMessage]) -> int:
    """전체 메시지 리스트의 토큰 수를 추정"""
    return sum(count_message_tokens(msg) for msg in messages)


# -----------------------------------------------------------------------------
# Context Editing 미들웨어
# -----------------------------------------------------------------------------
@dataclass
class ClearToolUsesEdit:
    """오래된 Tool 호출 결과를 정리하는 편집 규칙
    
    Attributes:
        trigger: 이 토큰 수를 초과하면 정리 시작
        keep: 유지할 최근 Tool 호출 수
        exclude_tools: 정리 대상에서 제외할 Tool 이름 리스트
    """
    trigger: int = 7000  # 7000 토큰 초과 시 정리 시작
    keep: int = 3        # 최근 3개 Tool 호출만 유지
    exclude_tools: List[str] = field(default_factory=list)
    
    def should_apply(self, messages: Sequence[BaseMessage]) -> bool:
        """정리가 필요한지 확인"""
        return count_total_tokens(messages) > self.trigger
    
    def apply(self, messages: Sequence[BaseMessage]) -> List[BaseMessage]:
        """오래된 Tool 호출 결과 정리
        
        가장 최신 `keep`개의 Tool 호출만 남기고,
        나머지 ToolMessage를 요약 메시지로 대체합니다.
        """
        result: List[BaseMessage] = []
        tool_messages: List[ToolMessage] = []
        
        # Tool 메시지 수집
        for msg in messages:
            if isinstance(msg, ToolMessage):
                # 제외 대상 Tool인지 확인
                tool_name = getattr(msg, "name", "") or ""
                if tool_name not in self.exclude_tools:
                    tool_messages.append(msg)
        
        # 유지할 Tool 메시지 ID 수집 (최신 N개)
        keep_ids: set = set()
        for tm in tool_messages[-self.keep:]:
            keep_ids.add(tm.tool_call_id)
        
        # 메시지 필터링
        removed_count = 0
        for msg in messages:
            if isinstance(msg, ToolMessage):
                tool_name = getattr(msg, "name", "") or ""
                
                # 제외 대상이거나 유지 대상이면 그대로 유지
                if tool_name in self.exclude_tools or msg.tool_call_id in keep_ids:
                    result.append(msg)
                else:
                    # 오래된 Tool 결과는 요약으로 대체
                    removed_count += 1
                    summary = ToolMessage(
                        content="[이전 호출 결과 생략됨]",
                        tool_call_id=msg.tool_call_id,
                        name=tool_name,
                    )
                    result.append(summary)
            elif isinstance(msg, AIMessage):
                # AI 메시지의 tool_calls 중 삭제된 것 처리
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    # Tool call 자체는 유지 (응답 매칭을 위해)
                    result.append(msg)
                else:
                    result.append(msg)
            else:
                result.append(msg)
        
        return result


@dataclass
class ContextEditingMiddleware:
    """컨텍스트 편집 미들웨어
    
    여러 편집 규칙을 순차적으로 적용하여
    대화 컨텍스트를 관리합니다.
    """
    edits: List[ClearToolUsesEdit] = field(default_factory=list)
    
    def __call__(self, messages: Sequence[BaseMessage]) -> List[BaseMessage]:
        """미들웨어 실행"""
        result = list(messages)
        
        for edit in self.edits:
            if edit.should_apply(result):
                result = edit.apply(result)
        
        return result


@dataclass
class TodoListMiddleware:
    """멀티스텝 태스크 추적 미들웨어 (Placeholder)
    
    복잡한 요청을 여러 단계로 분해하여 순차 실행합니다.
    """
    
    def __call__(self, messages: Sequence[BaseMessage]) -> List[BaseMessage]:
        """미들웨어 실행 (현재는 pass-through)"""
        # TODO: 태스크 분해 및 추적 로직 구현
        return list(messages)


# -----------------------------------------------------------------------------
# 에이전트에 미들웨어 적용 예시
# -----------------------------------------------------------------------------
def create_agent_with_middleware(
    model,
    tools: List[Any],
    system_prompt: str,
) -> Any:
    """미들웨어가 적용된 에이전트 생성
    
    Args:
        model: LLM 모델 (ChatOllama 등)
        tools: 에이전트에 연결할 Tool 리스트
        system_prompt: 시스템 프롬프트
        
    Returns:
        미들웨어가 적용된 에이전트 그래프
    """
    from langchain.agents import create_agent
    
    # 미들웨어 설정
    middleware = [
        # 멀티스텝 태스크 추적
        TodoListMiddleware(),
        
        # 컨텍스트 토큰 관리
        ContextEditingMiddleware(
            edits=[
                ClearToolUsesEdit(
                    trigger=7000,       # 7000 토큰 초과 시 정리
                    keep=3,             # 최근 3개 Tool 호출만 유지
                    exclude_tools=["write_todos"],  # 이 Tool은 정리 제외
                )
            ]
        ),
    ]
    
    # 에이전트 생성
    graph = create_agent(
        model=model,
        tools=tools,
        middleware=middleware,
        system_prompt=system_prompt,
    )
    
    return graph


# -----------------------------------------------------------------------------
# 사용 예시
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 테스트용 메시지 생성
    test_messages: List[BaseMessage] = [
        SystemMessage(content="시스템 프롬프트"),
        HumanMessage(content="이슈 목록 조회해줘"),
        AIMessage(
            content="",
            tool_calls=[{"id": "tc1", "name": "projects_list", "args": {}}],
        ),
        ToolMessage(content="이슈 1, 이슈 2, ..." * 100, tool_call_id="tc1", name="projects_list"),
        HumanMessage(content="상세 조회해줘"),
        AIMessage(
            content="",
            tool_calls=[{"id": "tc2", "name": "issue_detail", "args": {"number": 1}}],
        ),
        ToolMessage(content="이슈 상세 내용..." * 50, tool_call_id="tc2", name="issue_detail"),
    ]
    
    print(f"Original token count: {count_total_tokens(test_messages)}")
    
    # 미들웨어 적용
    middleware = ContextEditingMiddleware(
        edits=[
            ClearToolUsesEdit(trigger=500, keep=1)
        ]
    )
    
    result = middleware(test_messages)
    print(f"After middleware: {count_total_tokens(result)}")
    
    for msg in result:
        if isinstance(msg, ToolMessage):
            print(f"  ToolMessage({msg.name}): {msg.content[:50]}...")
