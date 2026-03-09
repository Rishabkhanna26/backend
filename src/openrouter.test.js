import { describe, expect, it, vi } from "vitest";
import {
  DEFAULT_OPENROUTER_MODEL,
  buildOpenRouterModelCandidates,
  extractOpenRouterContentText,
  requestOpenRouterText,
} from "./openrouter.js";

describe("buildOpenRouterModelCandidates", () => {
  it("keeps the preferred model first and de-duplicates fallbacks", () => {
    expect(
      buildOpenRouterModelCandidates({
        primaryModel: "stepfun/step-3.5-flash:free",
        fallbackModels:
          "arcee-ai/trinity-large-preview:free, stepfun/step-3.5-flash:free, liquid/lfm-2.5-1.2b-instruct:free",
        defaultFallbackModels: ["arcee-ai/trinity-large-preview:free"],
      })
    ).toEqual([
      "stepfun/step-3.5-flash:free",
      DEFAULT_OPENROUTER_MODEL,
      "arcee-ai/trinity-large-preview:free",
      "liquid/lfm-2.5-1.2b-instruct:free",
    ]);
  });
});

describe("extractOpenRouterContentText", () => {
  it("joins multipart content blocks", () => {
    expect(
      extractOpenRouterContentText([
        { text: "Hello" },
        "there",
        { text: "World" },
      ])
    ).toBe("Hello\nthere\nWorld");
  });
});

describe("requestOpenRouterText", () => {
  it("falls back when the preferred model returns empty content", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          choices: [
            {
              message: {
                content: null,
                reasoning: "Used completion budget before user-visible text.",
              },
            },
          ],
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          choices: [
            {
              message: {
                content: "Working reply",
              },
            },
          ],
        }),
      });

    const result = await requestOpenRouterText({
      apiKey: "test-key",
      endpoint: "https://openrouter.example/api/v1/chat/completions",
      primaryModel: "stepfun/step-3.5-flash:free",
      fallbackModels: "arcee-ai/trinity-large-preview:free",
      prompt: "Reply with exactly: ok",
      fetchImpl,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(JSON.parse(fetchImpl.mock.calls[0][1].body).model).toBe(
      "stepfun/step-3.5-flash:free"
    );
    expect(JSON.parse(fetchImpl.mock.calls[1][1].body).model).toBe(
      DEFAULT_OPENROUTER_MODEL
    );
    expect(result).toEqual({
      text: "Working reply",
      model: DEFAULT_OPENROUTER_MODEL,
      attempts: [
        {
          model: "stepfun/step-3.5-flash:free",
          ok: false,
          status: 200,
          error: "empty_content",
        },
        {
          model: DEFAULT_OPENROUTER_MODEL,
          ok: true,
          status: 200,
        },
      ],
    });
  });
});
