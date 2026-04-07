"""
export.py – Eksporterer data til CSV og JSON via pandas.
"""

import json
import math
import sqlite3
import pandas as pd
from config import OUTPUT_DIR


class _NaNEncoder(json.JSONEncoder):
    """Konverterer float NaN/Inf til None i JSON."""
    def default(self, obj):
        return super().default(obj)

    def iterencode(self, obj, _one_shot=False):
        return super().iterencode(self._clean(obj), _one_shot)

    def _clean(self, obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: self._clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean(v) for v in obj]
        return obj


def export_all(conn: sqlite3.Connection) -> None:
    """Eksporterer ryttere og resultater fra SQLite."""
    # Ryttere
    df = pd.read_sql_query("SELECT * FROM cyclists ORDER BY name", conn)
    csv_path = f"{OUTPUT_DIR}/danish_cyclists.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"CSV gemt: {csv_path} ({len(df)} ryttere)")

    # JSON med resultater nested under hver rytter
    cyclists = df.to_dict(orient="records")
    results_df = pd.read_sql_query("SELECT * FROM results", conn)
    results_by_cyclist = results_df.groupby("cyclist_id").apply(
        lambda g: g.to_dict(orient="records"),
        include_groups=False,
    ).to_dict() if not results_df.empty else {}

    for c in cyclists:
        c["results"] = results_by_cyclist.get(c["id"], [])

    json_path = f"{OUTPUT_DIR}/danish_cyclists.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(cyclists, f, ensure_ascii=False, indent=2, cls=_NaNEncoder)
    print(f"JSON gemt: {json_path} ({len(cyclists)} ryttere)")
