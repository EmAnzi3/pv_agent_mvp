from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any


def as_number(value: Any) -> float:
    if value is None or value == "":
        return 0.0

    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return float(str(value).replace(".", "").replace(",", "."))
        except (TypeError, ValueError):
            return 0.0


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def is_terna(record: dict) -> bool:
    source = str(record.get("source") or "").strip().lower()
    label = str(record.get("source_label") or "").strip().lower()
    url = str(record.get("url") or "").strip().lower()

    return (
        bool(record.get("is_terna"))
        or source == "terna_econnextion"
        or label == "terna econnextion"
        or "terna_econnextion" in url
    )


def is_punctual(record: dict) -> bool:
    return not is_terna(record)


def preferred_label(labels: Counter, fallback: str) -> str:
    if not labels:
        return fallback

    non_empty = Counter({
        label: count
        for label, count in labels.items()
        if str(label).strip()
    })

    if not non_empty:
        return fallback

    return non_empty.most_common(1)[0][0]


def rebuild_source_counts(
    records: list[dict],
) -> list[dict]:
    counts = Counter()
    labels: dict[str, Counter] = defaultdict(Counter)

    for record in records:
        source_group = str(
            record.get("source_group")
            or record.get("source")
            or "n/d"
        ).strip().lower()

        label = str(
            record.get("source_label")
            or ""
        ).strip()

        counts[source_group] += 1

        if label:
            labels[source_group][label] += 1

    rows = [
        {
            "source": source_group,
            "label": preferred_label(
                labels[source_group],
                source_group,
            ),
            "count": count,
        }
        for source_group, count in counts.items()
    ]

    rows.sort(
        key=lambda row: (
            -row["count"],
            row["source"].lower(),
        )
    )

    return rows

def rebuild_regions(records: list[dict]) -> list[dict]:
    stats: dict[str, dict] = defaultdict(
        lambda: {
            "punctual_count": 0,
            "punctual_mw": 0.0,
            "terna_count": 0,
            "terna_mw": 0.0,
            "terna_practices": 0,
        }
    )

    for record in records:
        region = str(record.get("region") or "").strip() or "n/d"
        mw = as_number(record.get("power_mw"))

        if is_terna(record):
            stats[region]["terna_count"] += 1
            stats[region]["terna_mw"] += mw
            stats[region]["terna_practices"] += int(
                as_number(record.get("numero_pratiche"))
            )
        else:
            stats[region]["punctual_count"] += 1
            stats[region]["punctual_mw"] += mw

    rows = []

    for region, values in stats.items():
        punctual_mw = round(values["punctual_mw"], 3)
        terna_mw = round(values["terna_mw"], 3)

        # Formula già utilizzata dalla dashboard.
        priority_score = round(
            values["punctual_count"] * 0.05
            + punctual_mw * 0.001
            + terna_mw * 0.0002,
            1,
        )

        rows.append({
            "region": region,
            "punctual_count": values["punctual_count"],
            "punctual_mw": punctual_mw,
            "terna_count": values["terna_count"],
            "terna_mw": terna_mw,
            "terna_practices": values["terna_practices"],
            "total_mw": round(punctual_mw + terna_mw, 3),
            "priority_score": priority_score,
        })

    rows.sort(
        key=lambda row: (
            -row["priority_score"],
            -row["punctual_mw"],
            row["region"].lower(),
        )
    )

    return rows


def rebuild_top_projects(
    records: list[dict],
    limit: int,
) -> list[dict]:
    candidates = [
        record
        for record in records
        if is_punctual(record) and as_number(record.get("power_mw")) > 0
    ]

    candidates.sort(
        key=lambda record: (
            -as_number(record.get("power_mw")),
            str(record.get("title") or "").lower(),
            str(record.get("url") or "").lower(),
        )
    )

    return deepcopy(candidates[:limit])


def rebuild_quality(records: list[dict]) -> tuple[dict, list[dict]]:
    punctual = [record for record in records if is_punctual(record)]

    quality = {
        "punctual_records": len(punctual),
        "missing_mw": sum(
            is_missing(record.get("power_mw")) for record in punctual
        ),
        "missing_region": sum(
            is_missing(record.get("region")) for record in punctual
        ),
        "missing_province": sum(
            is_missing(record.get("province")) for record in punctual
        ),
        "missing_municipality": sum(
            is_missing(record.get("municipalities")) for record in punctual
        ),
        "missing_url": sum(
            is_missing(record.get("url")) for record in punctual
        ),
        "province_deduced": sum(
            bool(record.get("province_deduced")) for record in punctual
        ),
        "municipalities_deduced": sum(
            bool(record.get("municipalities_deduced")) for record in punctual
        ),
    }

    grouped: dict[str, list[dict]] = defaultdict(list)
    labels: dict[str, Counter] = defaultdict(Counter)

    for record in records:
        source_group = str(
            record.get("source_group")
            or record.get("source")
            or "n/d"
        ).strip().lower()

        grouped[source_group].append(record)

        label = str(
            record.get("source_label") or ""
        ).strip()

        if label:
            labels[source_group][label] += 1

    quality_by_source = []

    for source, source_records in grouped.items():
        count = len(source_records)

        missing_mw = sum(
            is_missing(record.get("power_mw"))
            for record in source_records
        )
        missing_province = sum(
            is_missing(record.get("province"))
            for record in source_records
        )
        missing_municipality = sum(
            is_missing(record.get("municipalities"))
            for record in source_records
        )

        completeness = 0.0
        if count:
            completeness = round(
                100
                * (
                    count * 3
                    - missing_mw
                    - missing_province
                    - missing_municipality
                )
                / (count * 3),
                1,
            )

        quality_by_source.append({
            "source": source,
            "source_label": preferred_label(labels[source], source),
            "count": count,
            "missing_mw": missing_mw,
            "missing_province": missing_province,
            "missing_municipality": missing_municipality,
            "province_deduced": sum(
                bool(record.get("province_deduced"))
                for record in source_records
            ),
            "municipalities_deduced": sum(
                bool(record.get("municipalities_deduced"))
                for record in source_records
            ),
            "completeness_pct": completeness,
        })

    quality_by_source.sort(
        key=lambda row: (-row["count"], row["source"].lower())
    )

    return quality, quality_by_source


def rebuild_terna_summary(records: list[dict]) -> dict:
    grouped: dict[str, dict] = defaultdict(
        lambda: {"mw": 0.0, "count": 0, "practices": 0}
    )

    for record in records:
        if not is_terna(record):
            continue

        status = str(record.get("status") or "").strip() or "ND"

        grouped[status]["mw"] += as_number(record.get("power_mw"))
        grouped[status]["count"] += 1
        grouped[status]["practices"] += int(
            as_number(record.get("numero_pratiche"))
        )

    rows = [
        {
            "status": status,
            "mw": round(values["mw"], 3),
            "count": values["count"],
            "practices": values["practices"],
        }
        for status, values in grouped.items()
    ]

    rows.sort(key=lambda row: (-row["mw"], row["status"].lower()))
    return {"status_rows": rows}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    args = parser.parse_args()

    data_path = Path(args.data)

    if not data_path.exists():
        raise SystemExit(f"ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = data.get("records")

    if not isinstance(records, list):
        raise SystemExit("ERRORE: records non trovato nel JSON")

    summary = data.setdefault("summary", {})

    punctual = [record for record in records if is_punctual(record)]
    terna = [record for record in records if is_terna(record)]

    current_top_limit = max(
        len(data.get("top_projects") or []),
        len(summary.get("top_projects") or []),
        20,
    )

    top_projects = rebuild_top_projects(records, current_top_limit)
    quality, quality_by_source = rebuild_quality(records)

    summary["total_records"] = len(records)
    summary["punctual_records"] = len(punctual)
    summary["terna_records"] = len(terna)
    summary["total_mw_punctual"] = round(
        sum(as_number(record.get("power_mw")) for record in punctual),
        3,
    )
    summary["total_mw_terna"] = round(
        sum(as_number(record.get("power_mw")) for record in terna),
        3,
    )
    summary["source_counts"] = rebuild_source_counts(records)
    summary["regions"] = rebuild_regions(records)
    summary["top_projects"] = deepcopy(top_projects)
    summary["terna_summary"] = rebuild_terna_summary(records)
    summary["quality"] = quality
    summary["quality_by_source"] = quality_by_source

    data["top_projects"] = deepcopy(top_projects)

    data_quality = data.setdefault("data_quality", {})
    data_quality["derived_fields_rebuilt_at"] = (
        datetime.now().isoformat(timespec="seconds")
    )

    data_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("[rebuild-derived] records:", len(records))
    print("[rebuild-derived] punctual:", len(punctual))
    print("[rebuild-derived] Terna:", len(terna))
    print(
        "[rebuild-derived] MW puntuali:",
        summary["total_mw_punctual"],
    )
    print(
        "[rebuild-derived] MW Terna:",
        summary["total_mw_terna"],
    )
    print("[rebuild-derived] top_projects:", len(top_projects))
    print("[rebuild-derived] file:", data_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
