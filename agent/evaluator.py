from typing import Any, Dict, List


def evaluate_result(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"status": "retry", "reason": "query_returned_no_rows"}
    return {"status": "ok", "reason": None}

