from fastapi.testclient import TestClient

from api.main import app


def test_evaluation_failures_endpoint(monkeypatch):
    client = TestClient(app)
    traces = [
        {"trace_id": "a1", "dataset_id": "d1", "question": "q1", "evaluation_status": "ok", "retries_used": 0},
        {"trace_id": "a2", "dataset_id": "d1", "question": "q2", "evaluation_status": "retry", "evaluation_reason": "empty_rows", "retries_used": 1},
        {"trace_id": "a3", "dataset_id": "d2", "question": "q3", "error": "syntax_error::bad sql", "retries_used": 2},
    ]
    monkeypatch.setattr("api.routes.load_query_traces", lambda limit=5000: traces)

    response = client.get("/evaluation/failures?limit=10")
    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["total_analyze_requests"] == 3
    assert body["summary"]["total_failed_requests"] == 2
    assert body["error_taxonomy"]["syntax_error"] == 1
