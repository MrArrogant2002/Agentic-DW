from typing import Any, Dict, List, Tuple


def _base_report_fields(analysis: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str], List[str], float]:
    findings: List[Dict[str, Any]] = []
    traceability: List[Dict[str, Any]] = []
    risks: List[str] = []
    actions: List[str] = []
    confidence = 0.8

    if analysis.get("evaluator_status") != "ok":
        risks.append(f"Evaluator status is {analysis.get('evaluator_status')}")
        confidence = 0.4

    if analysis.get("retries_used", 0) > 0:
        risks.append("Query required retry; review generated SQL quality.")
        confidence = min(confidence, 0.7)

    return findings, traceability, risks, actions, confidence


def _add_sql_findings(analysis: Dict[str, Any], findings: List[Dict[str, Any]], traceability: List[Dict[str, Any]], actions: List[str]) -> None:
    rows = analysis.get("rows", [])
    if not rows:
        actions.append("No rows returned. Validate query intent and filters.")
        return

    first = rows[0]
    if "revenue" in first:
        findings.append({"finding": "Top record revenue", "value": first["revenue"], "unit": "currency"})
        traceability.append(
            {
                "claim": "Top record revenue",
                "source_path": "rows[0].revenue",
                "source_value": first["revenue"],
            }
        )
    if "country" in first:
        findings.append({"finding": "Top country by revenue", "value": first["country"], "unit": None})
        traceability.append(
            {
                "claim": "Top country by revenue",
                "source_path": "rows[0].country",
                "source_value": first["country"],
            }
        )

    actions.append("Use breakdown-level analysis to inspect drivers for the top result.")


def _add_trend_findings(snapshot_data: Dict[str, Any], findings: List[Dict[str, Any]], traceability: List[Dict[str, Any]], risks: List[str], actions: List[str]) -> None:
    trend = snapshot_data.get("trend", {})
    direction = trend.get("direction")
    slope = trend.get("slope_per_month")
    r2 = trend.get("r2")

    findings.append({"finding": "Revenue trend direction", "value": direction, "unit": None})
    findings.append({"finding": "Revenue trend slope per month", "value": slope, "unit": "currency/month"})
    findings.append({"finding": "Trend fit quality (R2)", "value": r2, "unit": None})

    traceability.append({"claim": "Revenue trend direction", "source_path": "rows[0].data.trend.direction", "source_value": direction})
    traceability.append({"claim": "Revenue trend slope per month", "source_path": "rows[0].data.trend.slope_per_month", "source_value": slope})
    traceability.append({"claim": "Trend fit quality (R2)", "source_path": "rows[0].data.trend.r2", "source_value": r2})

    if isinstance(r2, (float, int)) and r2 < 0.4:
        risks.append("Trend fit is weak-to-moderate; avoid overconfident long-term projections.")
    actions.append("Combine trend with seasonality and promotion calendar before forecasting.")


def _add_segmentation_findings(snapshot_data: Dict[str, Any], findings: List[Dict[str, Any]], traceability: List[Dict[str, Any]], risks: List[str], actions: List[str]) -> None:
    clustering = snapshot_data.get("clustering", {})
    score = clustering.get("silhouette_score")
    k = clustering.get("k")
    clusters = clustering.get("clusters", [])

    findings.append({"finding": "Cluster count", "value": k, "unit": None})
    findings.append({"finding": "Silhouette score", "value": score, "unit": None})
    traceability.append({"claim": "Cluster count", "source_path": "rows[0].data.clustering.k", "source_value": k})
    traceability.append({"claim": "Silhouette score", "source_path": "rows[0].data.clustering.silhouette_score", "source_value": score})

    if clusters:
        top = max(clusters, key=lambda c: c.get("size", 0))
        findings.append({"finding": "Largest segment label", "value": top.get("label"), "unit": None})
        findings.append({"finding": "Largest segment size", "value": top.get("size"), "unit": "customers"})
        traceability.append({"claim": "Largest segment label", "source_path": "rows[0].data.clustering.clusters[*].label", "source_value": top.get("label")})
        traceability.append({"claim": "Largest segment size", "source_path": "rows[0].data.clustering.clusters[*].size", "source_value": top.get("size")})

    if isinstance(score, (float, int)) and score < 0.5:
        risks.append("Segmentation separation is moderate; validate clusters with business review.")
    actions.append("Activate segment-specific campaigns and monitor conversion lift.")


def generate_structured_report(analysis: Dict[str, Any]) -> Dict[str, Any]:
    findings, traceability, risks, actions, confidence = _base_report_fields(analysis)
    intent = analysis.get("intent", "")
    rows = analysis.get("rows", [])

    mode = "sql_live"
    snapshot_meta = None
    if rows and isinstance(rows[0], dict) and "snapshot_type" in rows[0]:
        mode = "mining_snapshot"
        snapshot_meta = {
            "snapshot_type": rows[0].get("snapshot_type"),
            "generated_at": rows[0].get("generated_at"),
            "source_max_date": rows[0].get("source_max_date"),
            "snapshot_version": rows[0].get("snapshot_version"),
            "run_id": rows[0].get("run_id"),
            "refreshed": rows[0].get("refreshed"),
        }

    if mode == "sql_live":
        _add_sql_findings(analysis, findings, traceability, actions)
    elif intent == "trend_analysis" and rows:
        _add_trend_findings(rows[0].get("data", {}), findings, traceability, risks, actions)
    elif intent == "customer_segmentation" and rows:
        _add_segmentation_findings(rows[0].get("data", {}), findings, traceability, risks, actions)

    if not findings:
        risks.append("No structured findings were produced for this request.")
        confidence = min(confidence, 0.5)
    if not actions:
        actions.append("Review source data coverage and rerun analysis with tighter scope.")

    assumptions = [
        "All findings are derived from current warehouse data and cached mining snapshots.",
        "Currency values are interpreted in source dataset units.",
    ]

    return {
        "query_context": {
            "question": analysis.get("question"),
            "intent": analysis.get("intent"),
            "planner_source": analysis.get("planner_source"),
            "evaluator_status": analysis.get("evaluator_status"),
            "evaluator_reason": analysis.get("evaluator_reason"),
        },
        "execution_evidence": {
            "mode": mode,
            "sql": analysis.get("sql"),
            "row_count": len(rows),
            "retries_used": analysis.get("retries_used", 0),
            "snapshot_meta": snapshot_meta,
        },
        "key_findings": findings,
        "risk_flags": risks,
        "recommended_actions": actions,
        "traceability": traceability,
        "confidence": round(float(confidence), 2),
        "assumptions": assumptions,
    }

