from fastapi.testclient import TestClient

from api.main import app


def test_evaluation_metrics_endpoint(monkeypatch):
    client = TestClient(app)

    traces = [
        {
            "trace_id": "t1",
            "evaluation_status": "ok",
            "retries_used": 0,
            "cache_hit": True,
            "timing_ms": {"total": 10},
        },
        {
            "trace_id": "t2",
            "evaluation_status": "ok",
            "retries_used": 1,
            "cache_hit": False,
            "timing_ms": {"total": 30},
        },
        {
            "trace_id": "t2",
            "stage": "insight_generation",
            "enabled": True,
        },
    ]

    monkeypatch.setattr("api.routes.load_query_traces", lambda limit=1000: traces)

    response = client.get("/evaluation/metrics?limit=20")
    assert response.status_code == 200
    body = response.json()
    assert body["totals"]["analyze_requests"] == 2
    assert body["rates"]["execution_success_rate"] == 1.0
    assert body["rates"]["retry_rate"] == 0.5
    assert body["rates"]["cache_hit_rate"] == 0.5
    assert body["latency_ms"]["avg_total"] == 20.0
