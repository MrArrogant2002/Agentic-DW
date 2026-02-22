import json
import os
import re
from dataclasses import dataclass
from urllib import error, request

from utils.env_loader import load_environments


@dataclass
class Plan:
    question: str
    requires_mining: bool
    intent: str
    planner_source: str


_VALID_INTENTS = {
    "country_revenue",
    "top_customers",
    "top_products",
    "monthly_revenue",
    "trend_analysis",
    "customer_segmentation",
    "generic_sales_summary",
}


def _extract_json_blob(text: str) -> dict:
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1))

    inline = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if inline:
        return json.loads(inline.group(0))

    return json.loads(text)


def build_plan(question: str) -> Plan:
    load_environments()
    model = os.getenv("OLLAMA_MODEL")
    base_url = os.getenv("OLLAMA_BASE_URL")
    timeout_raw = os.getenv("OLLAMA_TIMEOUT_SEC")
    planner_enabled = os.getenv("OLLAMA_PLANNER_ENABLED")

    if not planner_enabled:
        raise RuntimeError("OLLAMA_PLANNER_ENABLED is required")
    if planner_enabled.strip().lower() in {"0", "false", "no"}:
        raise RuntimeError("OLLAMA planner is disabled via OLLAMA_PLANNER_ENABLED")
    if not model:
        raise RuntimeError("OLLAMA_MODEL is required")
    if not base_url:
        raise RuntimeError("OLLAMA_BASE_URL is required")
    if not timeout_raw:
        raise RuntimeError("OLLAMA_TIMEOUT_SEC is required")

    timeout_sec = float(timeout_raw)

    prompt = (
        "You are a planner for a retail SQL analytics system.\n"
        "Return JSON only with keys: intent, requires_mining.\n"
        "Allowed intent values: country_revenue, top_customers, top_products, monthly_revenue, trend_analysis, customer_segmentation, generic_sales_summary.\n"
        "requires_mining must be true if question asks about trend/segment/cluster/rfm/anomaly, else false.\n"
        f"Question: {question}"
    )

    payload = json.dumps(
        {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0},
        }
    ).encode("utf-8")

    http_request = request.Request(
        url=f"{base_url.rstrip('/')}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=timeout_sec) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Ollama planner request failed: {exc}") from exc

    text = body.get("response", "").strip()
    if not text:
        raise RuntimeError("Ollama returned empty planner response")

    parsed = _extract_json_blob(text)
    intent = str(parsed.get("intent", "")).strip()
    requires_mining = bool(parsed.get("requires_mining", False))

    if intent not in _VALID_INTENTS:
        raise RuntimeError(f"Ollama returned invalid intent: {intent}")

    return Plan(
        question=question,
        requires_mining=requires_mining,
        intent=intent,
        planner_source="ollama",
    )

