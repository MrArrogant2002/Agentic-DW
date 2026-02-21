import json
import os
import re
from urllib import error, request
from dataclasses import dataclass

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
    "generic_sales_summary",
}


def _heuristic_plan(question: str) -> Plan:
    text = question.strip().lower()
    requires_mining = any(token in text for token in ("trend", "segment", "cluster", "rfm", "anomaly"))

    if any(token in text for token in ("country", "countries", "nation", "nations")):
        intent = "country_revenue"
    elif "customer" in text and ("top" in text or "best" in text):
        intent = "top_customers"
    elif "product" in text and ("top" in text or "best" in text):
        intent = "top_products"
    elif "month" in text or "monthly" in text:
        intent = "monthly_revenue"
    else:
        intent = "generic_sales_summary"

    return Plan(question=question, requires_mining=requires_mining, intent=intent, planner_source="fallback")


def _extract_json_blob(text: str) -> dict:
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1))

    inline = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if inline:
        return json.loads(inline.group(0))

    return json.loads(text)


def _ollama_plan(question: str) -> Plan:
    load_environments()
    model = os.getenv("OLLAMA_MODEL", "mistral:latest")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    timeout_sec = float(os.getenv("OLLAMA_TIMEOUT_SEC", "20"))

    prompt = (
        "You are a planner for a retail SQL analytics system.\n"
        "Return JSON only with keys: intent, requires_mining.\n"
        "Allowed intent values: country_revenue, top_customers, top_products, monthly_revenue, generic_sales_summary.\n"
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

    return Plan(question=question, requires_mining=requires_mining, intent=intent, planner_source="ollama")


def build_plan(question: str) -> Plan:
    load_environments()
    if os.getenv("OLLAMA_PLANNER_ENABLED", "1").strip().lower() in {"0", "false", "no"}:
        return _heuristic_plan(question)

    try:
        return _ollama_plan(question)
    except Exception:
        return _heuristic_plan(question)
