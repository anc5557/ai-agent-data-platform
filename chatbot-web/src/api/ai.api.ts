/**
 * AI API 연동 모듈
 * 
 * LangGraph API 서버와 통신하여 AI 에이전트 대화를 처리합니다.
 * SSE (Server-Sent Events) 기반 실시간 스트리밍을 지원합니다.
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

/**
 * 메시지 타입 정의
 */
export interface Message {
    id: string;
    type: 'human' | 'ai' | 'tool' | 'system';
    content: ContentBlock[];
    tool_calls?: ToolCall[];
}

export interface ContentBlock {
    type: 'text' | 'output_text' | 'tool_call' | 'tool_output' | 'image';
    text?: string;
    id?: string;
    callId?: string;
    name?: string;
    args?: Record<string, unknown>;
}

export interface ToolCall {
    id: string;
    name: string;
    args: Record<string, unknown>;
}

export interface Conversation {
    id: string;
    title?: string;
    createdAt: string;
    updatedAt: string;
}

/**
 * 대화 목록 조회
 */
export async function getConversations(): Promise<Conversation[]> {
    const response = await fetch(`${API_BASE_URL}/v1/ai/conversations`);

    if (!response.ok) {
        throw new Error(`Failed to fetch conversations: ${response.status}`);
    }

    return response.json();
}

/**
 * 대화 생성
 */
export async function createConversation(): Promise<Conversation> {
    const response = await fetch(`${API_BASE_URL}/v1/ai/conversations`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });

    if (!response.ok) {
        throw new Error(`Failed to create conversation: ${response.status}`);
    }

    return response.json();
}

/**
 * 대화 메시지 조회
 */
export async function getMessages(conversationId: string): Promise<Message[]> {
    const response = await fetch(`${API_BASE_URL}/v1/ai/conversations/${conversationId}/messages`);

    if (!response.ok) {
        throw new Error(`Failed to fetch messages: ${response.status}`);
    }

    return response.json();
}

/**
 * 메시지 전송 (SSE 스트리밍)
 * 
 * @param conversationId - 대화 ID
 * @param message - 사용자 메시지
 * @param onChunk - 청크 수신 콜백
 * @param onComplete - 완료 콜백
 * @param onError - 에러 콜백
 */
export async function sendMessage(
    conversationId: string,
    message: string,
    onChunk: (chunk: StreamChunk) => void,
    onComplete: () => void,
    onError: (error: Error) => void,
): Promise<void> {
    const response = await fetch(`${API_BASE_URL}/v1/ai/conversations/${conversationId}/messages`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'text/event-stream',
        },
        body: JSON.stringify({ content: message }),
    });

    if (!response.ok) {
        onError(new Error(`Failed to send message: ${response.status}`));
        return;
    }

    const reader = response.body?.getReader();
    if (!reader) {
        onError(new Error('No response body'));
        return;
    }

    const decoder = new TextDecoder();
    let buffer = '';

    try {
        while (true) {
            const { done, value } = await reader.read();

            if (done) {
                onComplete();
                break;
            }

            buffer += decoder.decode(value, { stream: true });

            // SSE 이벤트 파싱
            const lines = buffer.split('\n');
            buffer = lines.pop() || ''; // 마지막 불완전한 라인은 버퍼에 유지

            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    const data = line.slice(6);

                    if (data === '[DONE]') {
                        onComplete();
                        return;
                    }

                    try {
                        const chunk = JSON.parse(data) as StreamChunk;
                        onChunk(chunk);
                    } catch (e) {
                        console.warn('Failed to parse SSE data:', data);
                    }
                }
            }
        }
    } catch (error) {
        onError(error instanceof Error ? error : new Error(String(error)));
    }
}

/**
 * SSE 스트림 청크 타입
 */
export interface StreamChunk {
    type: 'content' | 'tool_call' | 'tool_output' | 'error' | 'done';
    content?: string;
    tool_call?: {
        id: string;
        name: string;
        args: Record<string, unknown>;
    };
    tool_output?: {
        call_id: string;
        content: string;
    };
    error?: string;
}

/**
 * 메시지 수정
 */
export async function editMessage(
    conversationId: string,
    messageId: string,
    newContent: string,
): Promise<void> {
    const response = await fetch(
        `${API_BASE_URL}/v1/ai/conversations/${conversationId}/messages/${messageId}`,
        {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ content: newContent }),
        },
    );

    if (!response.ok) {
        throw new Error(`Failed to edit message: ${response.status}`);
    }
}

/**
 * 대화 삭제
 */
export async function deleteConversation(conversationId: string): Promise<void> {
    const response = await fetch(`${API_BASE_URL}/v1/ai/conversations/${conversationId}`, {
        method: 'DELETE',
    });

    if (!response.ok) {
        throw new Error(`Failed to delete conversation: ${response.status}`);
    }
}
