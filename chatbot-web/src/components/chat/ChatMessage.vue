<!-- eslint-disable vue/no-v-html -->
<template>
  <div
    v-if="shouldRenderMessage"
    ref="messageRoot"
    :class="outerClass"
    :data-message-id="message.id"
  >
    <div :class="containerClass">
      <!-- 사용자 메시지 -->
      <template v-if="isHuman">
        <div
          class="group flex items-end gap-2"
          @mouseenter="isHovering = true"
          @mouseleave="isHovering = false"
        >
          <!-- 복사 버튼 (왼쪽) -->
          <div class="flex h-full items-end gap-1 pb-2">
            <button
              class="rounded-lg p-2 text-muted-foreground transition-all hover:bg-muted hover:text-foreground"
              :class="isHovering ? 'opacity-100' : 'opacity-0'"
              :title="copied ? '복사됨!' : '복사'"
              @click="copyMessageContent"
            >
              <Check v-if="copied" class="h-4 w-4" />
              <Copy v-else class="h-4 w-4" />
            </button>
          </div>
          
          <!-- 메시지 버블 -->
          <div :class="humanBubbleClass">
            <div
              v-for="(block, index) in textBlocks"
              :key="`human-${index}`"
              :class="getTextBlockClass(true)"
              v-html="md.render(block.text || '')"
            />
          </div>
        </div>
      </template>

      <!-- AI 메시지 -->
      <template v-else>
        <div
          class="group flex items-end gap-1"
          @mouseenter="isHovering = true"
          @mouseleave="isHovering = false"
        >
          <!-- 메시지 버블 -->
          <div :class="assistantBubbleClass">
            <div class="space-y-3">
              <template
                v-for="(block, index) in textBlocks"
                :key="`assistant-${index}`"
              >
                <!-- eslint-disable-next-line vue/no-v-html -->
                <div
                  :class="getTextBlockClass(false)"
                  v-html="md.render(block.text || '')"
                />
              </template>
            </div>
          </div>
          
          <!-- 복사 버튼 (오른쪽) -->
          <div class="flex h-full items-end pb-2">
            <button
              class="rounded-lg p-2 text-muted-foreground transition-all hover:bg-muted hover:text-foreground"
              :class="isHovering ? 'opacity-100' : 'opacity-0'"
              :title="copied ? '복사됨!' : '복사'"
              @click="copyMessageContent"
            >
              <Check v-if="copied" class="h-4 w-4" />
              <Copy v-else class="h-4 w-4" />
            </button>
          </div>
        </div>

        <!-- 로딩 상태 -->
        <div
          v-if="isLoading && textBlocks.length === 0"
          class="flex items-center gap-1 text-sm text-muted-foreground"
        >
          <span class="loading-dot">•</span>
          <span class="loading-dot animation-delay-200">•</span>
          <span class="loading-dot animation-delay-400">•</span>
        </div>
      </template>

      <slot name="footer" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import MarkdownIt from "markdown-it";
import { Copy, Check } from "lucide-vue-next";
import { cn } from "@/lib/utils";

interface ContentBlock {
  type: "text" | "output_text" | "tool_call" | "tool_output" | "image" | "artifact";
  text?: string;
  data?: string;
  mimeType?: string;
  title?: string;
  id?: string;
  callId?: string;
}

interface Message {
  id: string;
  type: "human" | "ai" | "tool" | "system";
  content?: ContentBlock[];
}

const props = defineProps<{
  message: Message;
  isLoading?: boolean;
}>();

const messageRoot = ref<HTMLElement | null>(null);
const isHovering = ref(false);
const copied = ref(false);

// Markdown 파서 설정 (GFM 테이블, 링크 지원)
const md = new MarkdownIt({
  linkify: true,
  breaks: true,
  typographer: true,
});

// 외부 링크는 새 탭에서 열기
const defaultLinkRenderer =
  md.renderer.rules.link_open ??
  ((tokens, idx, options, env, self) => self.renderToken(tokens, idx, options));

md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  const token = tokens[idx];
  const href = token.attrGet("href") ?? "";

  if (!href.startsWith("#")) {
    token.attrSet("target", "_blank");
    token.attrSet("rel", "noopener noreferrer");
  }

  return defaultLinkRenderer(tokens, idx, options, env, self);
};

const isHuman = computed(() => props.message.type === "human");

const outerClass = computed(() =>
  cn(
    "flex w-full",
    isHuman.value ? "justify-end" : "justify-start",
  ),
);

const containerClass = computed(() =>
  cn(
    "flex w-full max-w-3xl flex-col gap-3",
    isHuman.value ? "items-end" : "items-start",
  ),
);

const humanBubbleClass = "max-w-full rounded-3xl bg-primary px-4 py-3 text-sm text-primary-foreground shadow-sm";
const assistantBubbleClass = "max-w-full rounded-2xl border border-border/60 bg-card px-4 py-3 text-sm text-card-foreground shadow-sm";

// 텍스트 블록만 필터링
const textBlocks = computed(() =>
  (props.message.content ?? []).filter(
    (block): block is ContentBlock & { text: string } =>
      (block.type === "text" || block.type === "output_text") && !!block.text,
  ),
);

const shouldRenderMessage = computed(() => textBlocks.value.length > 0);

function getTextBlockClass(invert = false) {
  return cn(
    "chat-markdown prose prose-sm dark:prose-invert max-w-none",
    invert ? "chat-markdown-invert" : "",
  );
}

// 메시지 복사
async function copyMessageContent() {
  try {
    const textContent = textBlocks.value
      .map((block) => block.text || "")
      .join("\n\n");

    if (textContent) {
      await navigator.clipboard.writeText(textContent);
      copied.value = true;
      setTimeout(() => {
        copied.value = false;
      }, 2000);
    }
  } catch (err) {
    console.error("복사 실패:", err);
  }
}
</script>

<style scoped>
@keyframes loading-pulse {
  0%, 100% {
    opacity: 0.3;
  }
  50% {
    opacity: 1;
  }
}

.loading-dot {
  animation: loading-pulse 1.4s ease-in-out infinite;
}

.animation-delay-200 {
  animation-delay: 0.2s;
}

.animation-delay-400 {
  animation-delay: 0.4s;
}

/* 마크다운 스타일 */
:deep(.chat-markdown) {
  word-break: break-word;
}

:deep(.chat-markdown table) {
  border-collapse: collapse;
  width: 100%;
  margin: 0.5rem 0;
}

:deep(.chat-markdown th),
:deep(.chat-markdown td) {
  border: 1px solid hsl(var(--border));
  padding: 0.5rem;
  text-align: left;
}

:deep(.chat-markdown th) {
  background-color: hsl(var(--muted));
  font-weight: 600;
}

:deep(.chat-markdown code) {
  background-color: hsl(var(--muted));
  padding: 0.125rem 0.25rem;
  border-radius: 0.25rem;
  font-size: 0.875em;
}

:deep(.chat-markdown pre) {
  background-color: hsl(var(--muted));
  padding: 1rem;
  border-radius: 0.5rem;
  overflow-x: auto;
}

:deep(.chat-markdown pre code) {
  background-color: transparent;
  padding: 0;
}

/* 반전 스타일 (사용자 메시지) */
:deep(.chat-markdown-invert) {
  color: hsl(var(--primary-foreground));
}

:deep(.chat-markdown-invert a) {
  color: hsl(var(--primary-foreground));
  text-decoration: underline;
}
</style>
