export const DEFAULT_OPENROUTER_MODEL = "anthropic/claude-3.5-sonnet";
export const DEFAULT_OPENROUTER_FALLBACK_MODELS = Object.freeze([
  "openai/gpt-4o",
  "google/gemini-2.0-flash-thinking-exp:free",
  "qwen/qwen-2.5-72b-instruct:free",
]);

const normalizeModelId = (value) => String(value || "").trim();

export const parseOpenRouterModelList = (value) => {
  if (Array.isArray(value)) {
    return value.map(normalizeModelId).filter(Boolean);
  }
  return String(value || "")
    .split(",")
    .map(normalizeModelId)
    .filter(Boolean);
};

export const buildOpenRouterModelCandidates = ({
  primaryModel,
  fallbackModels,
  defaultModel = DEFAULT_OPENROUTER_MODEL,
  defaultFallbackModels = DEFAULT_OPENROUTER_FALLBACK_MODELS,
  maxModels = 4,
} = {}) => {
  const seen = new Set();
  const candidates = [];
  const addCandidate = (value) => {
    const model = normalizeModelId(value);
    if (!model || seen.has(model)) return;
    seen.add(model);
    candidates.push(model);
  };

  addCandidate(primaryModel);
  addCandidate(defaultModel);
  parseOpenRouterModelList(fallbackModels).forEach(addCandidate);
  parseOpenRouterModelList(defaultFallbackModels).forEach(addCandidate);

  return candidates.slice(0, Math.max(Number(maxModels) || 0, 1));
};

export const extractOpenRouterContentText = (content) => {
  if (typeof content === "string") {
    return content.trim();
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((part) => {
      if (typeof part === "string") return part;
      if (typeof part?.text === "string") return part.text;
      return "";
    })
    .join("\n")
    .trim();
};

const truncateForLog = (value, maxLength = 240) => {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  return text.length <= maxLength ? text : `${text.slice(0, maxLength)}...`;
};

export const requestOpenRouterText = async ({
  apiKey,
  endpoint,
  siteUrl = "",
  siteName = "",
  primaryModel,
  fallbackModels,
  prompt,
  temperature = 0.65,
  maxOutputTokens = 240,
  timeoutMs = 12_000,
  maxModels = 4,
  fetchImpl = globalThis.fetch,
} = {}) => {
  if (!apiKey || typeof fetchImpl !== "function") {
    return { text: null, model: null, attempts: [] };
  }

  const candidates = buildOpenRouterModelCandidates({
    primaryModel,
    fallbackModels,
    maxModels,
  });
  const attempts = [];

  for (const model of candidates) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          ...(siteUrl ? { "HTTP-Referer": siteUrl } : {}),
          ...(siteName ? { "X-OpenRouter-Title": siteName } : {}),
        },
        body: JSON.stringify({
          model,
          messages: [{ role: "user", content: prompt }],
          temperature,
          max_tokens: maxOutputTokens,
          stream: false,
        }),
        signal: controller.signal,
      });

      if (!response.ok) {
        attempts.push({
          model,
          ok: false,
          status: response.status,
          error: truncateForLog(await response.text().catch(() => "")) || "request_failed",
        });
        continue;
      }

      const data = await response.json();
      const rawText = extractOpenRouterContentText(data?.choices?.[0]?.message?.content);
      if (!rawText) {
        attempts.push({
          model,
          ok: false,
          status: response.status,
          error: "empty_content",
        });
        continue;
      }

      attempts.push({
        model,
        ok: true,
        status: response.status,
      });
      return {
        text: rawText,
        model,
        attempts,
      };
    } catch (error) {
      attempts.push({
        model,
        ok: false,
        status: null,
        error:
          error?.name === "AbortError"
            ? "timeout"
            : truncateForLog(error?.message || String(error)) || "request_failed",
      });
    } finally {
      clearTimeout(timeout);
    }
  }

  return {
    text: null,
    model: null,
    attempts,
  };
};
