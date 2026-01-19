<template>
  <form
    :class="formClasses"
    @submit.prevent="handleSubmit"
  >
    <Textarea
      ref="textareaRef"
      v-model="text"
      :placeholder="placeholder"
      :rows="textareaRows"
      :class="textareaClass"
      :disabled="isBusy"
      @keydown="handleKeydown"
      @input="adjustTextareaHeight"
    />
    <div class="flex items-center justify-between gap-4 pl-2">
      <div class="flex items-center gap-2">
        <slot name="left" />
      </div>
      <Button
        type="submit"
        size="icon"
        :disabled="isSubmitDisabled"
        :aria-busy="isBusy || undefined"
        :aria-label="isBusy ? '응답 중단' : '메시지 전송'"
        class="relative h-8 w-8 overflow-visible rounded-full p-0 transition-all duration-200"
      >
        <span
          v-if="isBusy"
          class="pointer-events-none absolute -inset-1.5 flex items-center justify-center"
          aria-hidden="true"
        >
          <span
            class="h-full w-full rounded-full border-2 border-border/40 border-t-primary animate-spin"
          />
        </span>
        <ArrowUp
          v-if="!isBusy"
          class="relative z-10 h-5 w-5"
        />
        <Square
          v-else
          class="relative z-10 h-5 w-5"
        />
      </Button>
    </div>
  </form>
</template>

<script setup lang="ts">
import { computed, ref, nextTick, watch, onMounted } from "vue";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { ArrowUp, Square } from "lucide-vue-next";

const props = defineProps<{
  disabled?: boolean;
  stopDisabled?: boolean;
  placeholder?: string;
  compact?: boolean;
}>();

const emit = defineEmits<{
  submit: [payload: { text: string }];
  stop: [];
}>();

const text = ref("");
const textareaRef = ref<InstanceType<typeof Textarea>>();

const isBusy = computed(() => !!props.disabled);
const isStopDisabled = computed(() => !!props.stopDisabled);
const isCompact = computed(() => !!props.compact);
const hasText = computed(() => text.value.trim().length > 0);

const isSubmitDisabled = computed(() => {
  if (isBusy.value) {
    return isStopDisabled.value;
  }
  return !hasText.value;
});

const placeholder = computed(
  () => props.placeholder ?? "메시지를 입력하세요...",
);

const formClasses = computed(() => [
  "flex flex-col rounded-3xl border border-border/60 bg-card shadow-sm",
  isCompact.value ? "gap-2 p-3" : "gap-3 p-4",
]);

const textareaClass = computed(() => {
  const classes = [
    "resize-none border-0 shadow-none focus-visible:ring-0 overflow-y-auto",
    isCompact.value ? "min-h-[60px] max-h-[240px]" : "min-h-[80px] max-h-[300px]",
  ];
  return classes.join(" ");
});

const textareaRows = computed(() => (isCompact.value ? 2 : 3));

function adjustTextareaHeight() {
  nextTick(() => {
    const textarea = textareaRef.value?.$el as HTMLTextAreaElement;
    if (textarea) {
      // 높이를 auto로 설정하여 scrollHeight 측정
      textarea.style.height = 'auto';
      textarea.style.height = `${textarea.scrollHeight}px`;
    }
  });
}

function handleKeydown(event: KeyboardEvent) {
  // Enter 키만 누르면 제출, Shift + Enter는 줄바꿈
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    handleSubmit();
  }
}

function handleSubmit() {
  if (isBusy.value) {
    if (!isStopDisabled.value) {
      emit("stop");
    }
    return;
  }

  if (isSubmitDisabled.value) return;
  emit("submit", { text: text.value });
  text.value = "";
  
  // 텍스트 제출 후 높이 리셋
  nextTick(() => {
    const textarea = textareaRef.value?.$el as HTMLTextAreaElement;
    if (textarea) {
      textarea.style.height = 'auto';
    }
  });
}

// 컴포넌트 마운트 시 초기 높이 설정
onMounted(() => {
  adjustTextareaHeight();
});

// text 값 변경 시 높이 조정
watch(text, () => {
  adjustTextareaHeight();
});
</script>
