import argparse
import json
from typing import Any, Dict, List

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from mining.rfm import fetch_rfm


def _cluster_label(recency: float, frequency: float, monetary: float, medians: Dict[str, float]) -> str:
    if frequency >= medians["frequency"] and monetary >= medians["monetary"] and recency <= medians["recency"]:
        return "high_value_active"
    if frequency < medians["frequency"] and monetary < medians["monetary"] and recency > medians["recency"]:
        return "low_value_at_risk"
    if recency <= medians["recency"]:
        return "active_mid_value"
    return "dormant_mid_value"


def run_kmeans(k: int = 4, random_state: int = 42, n_init: int = 20) -> Dict[str, Any]:
    rfm_rows = fetch_rfm()
    if len(rfm_rows) < max(k, 3):
        return {
            "status": "insufficient_data",
            "reason": f"Need at least {max(k, 3)} customers for clustering",
            "customers": len(rfm_rows),
        }

    X = np.array(
        [[row["recency_days"], row["frequency"], row["monetary"]] for row in rfm_rows],
        dtype=float,
    )
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = KMeans(n_clusters=k, random_state=random_state, n_init=n_init)
    labels = model.fit_predict(X_scaled)
    score = float(silhouette_score(X_scaled, labels))

    raw_recency = X[:, 0]
    raw_frequency = X[:, 1]
    raw_monetary = X[:, 2]
    medians = {
        "recency": float(np.median(raw_recency)),
        "frequency": float(np.median(raw_frequency)),
        "monetary": float(np.median(raw_monetary)),
    }

    clusters: List[Dict[str, Any]] = []
    for cluster_id in range(k):
        idx = np.where(labels == cluster_id)[0]
        cluster_recency = raw_recency[idx]
        cluster_frequency = raw_frequency[idx]
        cluster_monetary = raw_monetary[idx]

        label = _cluster_label(
            recency=float(np.mean(cluster_recency)),
            frequency=float(np.mean(cluster_frequency)),
            monetary=float(np.mean(cluster_monetary)),
            medians=medians,
        )

        clusters.append(
            {
                "cluster_id": cluster_id,
                "label": label,
                "size": int(len(idx)),
                "avg_recency_days": round(float(np.mean(cluster_recency)), 4),
                "avg_frequency": round(float(np.mean(cluster_frequency)), 4),
                "avg_monetary": round(float(np.mean(cluster_monetary)), 4),
            }
        )

    return {
        "status": "ok",
        "customers": len(rfm_rows),
        "k": k,
        "silhouette_score": round(score, 4),
        "clusters": sorted(clusters, key=lambda c: c["cluster_id"]),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run customer segmentation with KMeans on RFM features.")
    parser.add_argument("--k", type=int, default=4, help="Number of clusters.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed.")
    parser.add_argument("--n-init", type=int, default=20, help="KMeans n_init value.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    result = run_kmeans(k=args.k, random_state=args.random_state, n_init=args.n_init)
    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result))

