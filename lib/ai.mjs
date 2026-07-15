function marketPrompt(snapshot, userPrompt) {
  return `You are a risk-focused crypto futures research assistant. Analyze the supplied market snapshot without promising returns and without issuing an order. Separate observations from uncertainty. Keep the answer concise.\n\nMarket snapshot:\n${JSON.stringify(snapshot, null, 2)}\n\nUser request:\n${userPrompt}`;
}

async function fetchJson(url, options) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(60_000) });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error?.message ?? payload.message ?? `AI provider returned ${response.status}`);
  }
  return payload;
}

function openAIText(payload) {
  for (const item of payload.output ?? []) {
    for (const content of item.content ?? []) {
      if (content.type === "output_text" && content.text) return content.text;
    }
  }
  return payload.output_text ?? "";
}

export async function analyzeWithProvider({ provider, model, prompt, snapshot, secretStore }) {
  const input = marketPrompt(snapshot, prompt);
  if (provider === "openai") {
    const apiKey = secretStore.getSecret("openai_api_key");
    if (!apiKey) throw new Error("OpenAI API Key 未配置");
    const payload = await fetchJson("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: model || "gpt-5.6-luna", input, reasoning: { effort: "low" } }),
    });
    return { provider, model: payload.model ?? model, text: openAIText(payload) };
  }

  if (provider === "anthropic") {
    const apiKey = secretStore.getSecret("anthropic_api_key");
    if (!apiKey) throw new Error("Claude API Key 未配置");
    const selectedModel = model || process.env.ANTHROPIC_MODEL || "claude-sonnet-4-5";
    const payload = await fetchJson("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ model: selectedModel, max_tokens: 900, messages: [{ role: "user", content: input }] }),
    });
    const text = (payload.content ?? []).filter((item) => item.type === "text").map((item) => item.text).join("\n");
    return { provider, model: payload.model ?? selectedModel, text };
  }

  if (provider === "deepseek") {
    const apiKey = secretStore.getSecret("deepseek_api_key");
    if (!apiKey) throw new Error("DeepSeek API Key 未配置");
    const selectedModel = model || "deepseek-chat";
    const payload = await fetchJson("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: selectedModel, temperature: 0.2, messages: [{ role: "user", content: input }] }),
    });
    return { provider, model: payload.model ?? selectedModel, text: payload.choices?.[0]?.message?.content ?? "" };
  }

  throw new Error("不支持的 AI 提供商");
}
