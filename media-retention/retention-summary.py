#!/usr/bin/env python3
"""
Retention Summary — quick rollups + pattern-based counts for the
candidates CSV.

INPUT:  the CSV produced by media-retention-report.py (split-CSV pipeline)
        headers: policy, entry_id, entry_name, media_type, created_on,
                 last_updated, duration_seconds, plays, status, owner,
                 lastPlayedAt, reason

OUTPUT: a wide summary CSV with columns:
        metric, 2-year, 4-year, non-ready, total
"""

import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
from dotenv import load_dotenv


# Timezone helpers for consistent timestamping
def _get_tz() -> ZoneInfo:
    tzname = (
        os.getenv("TIMEZONE") or "America/Los_Angeles"
    ).strip() or "America/Los_Angeles"
    try:
        return ZoneInfo(tzname)
    except Exception:
        return ZoneInfo("America/Los_Angeles")


def ts() -> str:
    tz = _get_tz()
    return datetime.now(
        tz=timezone.utc
        ).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def stamp() -> str:
    tz = _get_tz()
    return datetime.now(
        tz=timezone.utc
        ).astimezone(tz).strftime("%Y-%m-%d-%H%M")


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=True)
    # Coerce numerics we need
    df["plays"] = pd.to_numeric(
        df.get("plays"), errors="coerce"
    ).fillna(0).astype(int)
    df["duration_seconds"] = pd.to_numeric(
        df.get("duration_seconds"), errors="coerce"
    ).fillna(0).astype(int)
    # If present (from include-flavor-calculations.py),
    # coerce bytes_saved to int
    if "bytes_saved" in df.columns:
        df["bytes_saved"] = pd.to_numeric(
            df.get("bytes_saved"), errors="coerce"
        ).fillna(0).astype(int)

    # Ensure required text columns exist (create empty if missing)
    required_text_cols = [
        "owner", "entry_name", "media_type", "policy", "status", "reason",
    ]
    for col in required_text_cols:
        if col not in df.columns:
            df[col] = ""

    # Normalize key text columns for case-insensitive matching
    for col in required_text_cols:
        df[col] = df[col].astype(str).fillna("").str.strip()

    return df


def split_list(env_val: str | None) -> list[str]:
    if not env_val:
        return []
    return [p.strip() for p in env_val.split(",") if p.strip()]


def split_ints(env_val: str | None) -> list[int]:
    vals: list[int] = []
    if not env_val:
        return vals
    for part in env_val.split(","):
        s = part.strip()
        if not s:
            continue
        try:
            vals.append(int(s))
        except Exception:
            # ignore non‑integer tokens silently
            continue
    return vals


def ci_contains(series: pd.Series, needle: str) -> pd.Series:
    return series.str.lower().str.contains(needle.lower(), na=False)


def ci_startswith(series: pd.Series, needle: str) -> pd.Series:
    return series.str.lower().str.startswith(needle.lower(), na=False)


def ci_endswith(series: pd.Series, needle: str) -> pd.Series:
    return series.str.lower().str.endswith(needle.lower(), na=False)


def add_row(
    rows: list[dict], metric: str, scope: str, label: str, df: pd.DataFrame,
    value=None
):
    rows.append({
        "metric": metric,
        "scope": scope,       # e.g., all / filter name
        "label": label,       # e.g., 'UCSD_' or '-transitional'
        "entries": int(len(df)),
        "unique_owners": int(
            df["owner"].nunique() if "owner" in df.columns else 0
            ),
        "value": value if value is not None else ""
    })


def _nunique_safe(series: pd.Series) -> int:
    try:
        return int(series.nunique())
    except Exception:
        return 0


def build_wide_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a compact, legible summary with per-policy columns:
    columns: metric, 2-year, 4-year, non-ready, total
    """
    # Detect presence of bytes_saved
    has_bytes = "bytes_saved" in df.columns
    # Normalize policy and media_type
    pol = df.get("policy", "").astype(str).str.lower().fillna("")
    df2 = df[pol == "2year"].copy()
    df4 = df[pol == "4year"].copy()
    dfnr = df[pol == "nonready"].copy()

    def block(dfx: pd.DataFrame) -> dict:
        mt_x = dfx.get("media_type", "").astype(str).str.lower().fillna("")
        plays_x = pd.to_numeric(
            dfx.get("plays"), errors="coerce"
            ).fillna(0).astype(int)
        dur_x = pd.to_numeric(
            dfx.get("duration_seconds"), errors="coerce"
            ).fillna(0).astype(int)
        owners = dfx.get("owner", "").astype(str)
        # Optional: bytes saved (from include-flavor-calculations.py)
        bytes_sum = (
            int(
                pd.to_numeric(dfx.get("bytes_saved"), errors="coerce")
                .fillna(0)
                .astype(int)
                .sum()
            ) if has_bytes else 0
        )
        mb = round(bytes_sum / float(1024 ** 2), 2) if has_bytes else 0.0
        gb = round(bytes_sum / float(1024 ** 3), 2) if has_bytes else 0.0
        tb = round(bytes_sum / float(1024 ** 4), 2) if has_bytes else 0.0
        data = {
            "Candidates": int(len(dfx)),
            "Unique users": _nunique_safe(owners),
            "Unique users (0 plays)": _nunique_safe(
                owners[plays_x == 0]
                ) if not dfx.empty else 0,
            "Media type: video": int((mt_x == "video").sum()),
            "Media type: audio": int((mt_x == "audio").sum()),
            "Media type: image": int((mt_x == "image").sum()),
            "Media subtype: YouTube": int((mt_x == "youtube").sum()),
            "Media subtype: Quiz": int((mt_x == "quiz").sum()),
            "Media subtype: YouTube Quiz": int((mt_x == "youtube quiz").sum()),
            # Exclude image entries from duration totals
            "Duration of entries (seconds)": int(dur_x[mt_x != "image"].sum()),
            "Duration of entries (hours)": round(
                float(dur_x[mt_x != "image"].sum()) / 3600.0, 2
                ),
        }
        if has_bytes:
            data.update({
                "Bytes saved": bytes_sum,
                "MB saved": mb,
                "GB saved": gb,
                "TB saved": tb,
            })
        return data

    b2 = block(df2)
    b4 = block(df4)
    bnr = block(dfnr)
    ball = block(df)  # total across all

    # Assemble rows in the requested order
    metrics_order = [
        "Candidates",
        "Unique users",
        "Unique users (0 plays)",
        "Media type: video",
        "Media type: audio",
        "Media type: image",
        "Media subtype: YouTube",
        "Media subtype: Quiz",
        "Media subtype: YouTube Quiz",
        "Duration of entries (seconds)",
        "Duration of entries (hours)",
    ]
    if has_bytes:
        metrics_order += [
            "Bytes saved",
            "MB saved",
            "GB saved",
            "TB saved",
        ]

    rows = []
    for m in metrics_order:
        rows.append({
            "metric": m,
            "2-year": b2.get(m, ""),
            "4-year": b4.get(m, ""),
            "non-ready": bnr.get(m, ""),
            "total": ball.get(m, ""),
        })
    return pd.DataFrame(
        rows,
        columns=["metric", "2-year", "4-year", "non-ready", "total"],
    )


def summarize_basics(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []

    # Overall unique users
    add_row(
        rows, "unique_users_all", "all", "", df,
        value=int(df["owner"].nunique())
        )

    # Unique users with 0 plays
    zero_df = df[df["plays"] == 0]
    add_row(
        rows, "unique_users_zero_plays", "plays==0", "", zero_df,
        value=int(zero_df["owner"].nunique()),
    )

    # Media type counts
    mt = df["media_type"].str.lower()
    add_row(
        rows, "media_type_video", "all", "video",
        df[mt == "video"], value=int((mt == "video").sum()),
    )
    add_row(
        rows, "media_type_audio", "all", "audio",
        df[mt == "audio"], value=int((mt == "audio").sum()),
    )
    add_row(
        rows, "media_type_image", "all", "image",
        df[mt == "image"], value=int((mt == "image").sum()),
    )

    # Policy counts
    pol = df["policy"].str.lower()
    add_row(rows, "policy_2year", "all", "2year", df[pol == "2year"],
            value=int((pol == "2year").sum()))
    add_row(rows, "policy_4year", "all", "4year", df[pol == "4year"],
            value=int((pol == "4year").sum()))
    add_row(rows, "policy_nonready", "all", "nonready", df[pol == "nonready"],
            value=int((pol == "nonready").sum()))

    # Total possible time affected
    total_sec = int(df["duration_seconds"].sum())
    total_hours = total_sec / 3600.0
    add_row(rows, "total_duration_seconds", "all", "", df, value=total_sec)
    add_row(
        rows, "total_duration_hours", "all", "", df,
        value=round(total_hours, 2)
        )

    return rows


def summarize_patterns(df: pd.DataFrame, rows: list[dict]):
    # Helper to process a family of env vars against a given column
    def do_family(col: str, base: str):
        # If the column is missing for any reason, skip gracefully
        if col not in df.columns:
            return
        # Prefer SUMMARY_* to avoid collisions with other scripts; fall back
        # to legacy names
        legacy_alias = (
            f"{base.replace('OWNER', 'USERNAME')}" if "OWNER" in base else None
        )
        env_contains = (
            os.getenv(f"SUMMARY_{base}")
            or os.getenv(base)
            or (os.getenv(legacy_alias) if legacy_alias else None)
        )
        env_starts = (
            os.getenv(f"SUMMARY_{base}_BEGINS_WITH")
            or os.getenv(f"{base}_BEGINS_WITH")
        )
        env_ends = (
            os.getenv(f"SUMMARY_{base}_ENDS_WITH")
            or os.getenv(f"{base}_ENDS_WITH")
        )

        col_series = df[col].fillna("")

        # Exact matches (OWNER only): SUMMARY_OWNER (comma-delimited),
        # fallback OWNER_EXACT
        if col == "owner":
            env_exact = os.getenv("SUMMARY_OWNER") or os.getenv("OWNER_EXACT")
            for pat in split_list(env_exact):
                # case-insensitive exact match
                mask = col_series.str.lower() == pat.lower()
                sub = df[mask]
                add_row(rows, f"{col}_exact", "summary_owner", pat, sub,
                        value=int(sub[col].nunique()))

        for pat in split_list(env_contains):
            mask = ci_contains(col_series, pat)
            sub = df[mask]
            count = int(
                sub[col].nunique()
                ) if col == "owner" else int(len(sub))
            add_row(
                rows,
                f"{col}_contains",
                base.lower(),
                pat,
                sub,
                value=count,
            )

        for pat in split_list(env_starts):
            mask = ci_startswith(col_series, pat)
            sub = df[mask]
            count = int(
                sub[col].nunique()
                ) if col == "owner" else int(len(sub))
            add_row(
                rows,
                f"{col}_starts_with",
                f"{base}_BEGINS_WITH".lower(),
                pat,
                sub,
                value=count,
            )

        for pat in split_list(env_ends):
            mask = ci_endswith(col_series, pat)
            sub = df[mask]
            count = int(
                sub[col].nunique()
                ) if col == "owner" else int(len(sub))
            add_row(
                rows,
                f"{col}_ends_with",
                f"{base}_ENDS_WITH".lower(),
                pat,
                sub,
                value=count,
            )

    # Owners (a.k.a. usernames)
    do_family("owner", "OWNER")
    # Creators
    # do_family("creator", "CREATOR")
    # Entry names
    do_family("entry_name", "ENTRY_NAME")


def _policy_slices(df: pd.DataFrame):
    pol = df.get("policy", "").astype(str).str.lower().fillna("")
    return (
        df[pol == "2year"],
        df[pol == "4year"],
        df[pol == "nonready"],
        df,  # total
    )


def _counts_by_policy(
        dfx2: pd.DataFrame, dfx4: pd.DataFrame, dfxnr: pd.DataFrame,
        dft: pd.DataFrame, unique_owners: bool
        ) -> dict:
    if unique_owners:
        c2 = int(dfx2.get("owner", pd.Series([], dtype=str)).nunique())
        c4 = int(dfx4.get("owner", pd.Series([], dtype=str)).nunique())
        cn = int(dfxnr.get("owner", pd.Series([], dtype=str)).nunique())
        ct = int(dft.get("owner", pd.Series([], dtype=str)).nunique())
    else:
        c2 = int(len(dfx2))
        c4 = int(len(dfx4))
        cn = int(len(dfxnr))
        ct = int(len(dft))
    return {"2-year": c2, "4-year": c4, "non-ready": cn, "total": ct}


def build_env_summary_rows(df: pd.DataFrame) -> list[dict]:
    """
    Build additional rows driven by SUMMARY_* env vars.
    For OWNER filters we count *unique owners* matching the pattern.
    For ENTRY_NAME filters we count *entries* matching the pattern.
    """
    rows: list[dict] = []
    owner_series = df.get(
        "owner", pd.Series([], dtype=str)
        ).astype(str).fillna("")
    name_series = df.get(
        "entry_name", pd.Series([], dtype=str)
        ).astype(str).fillna("")
    duration_series = pd.to_numeric(
        df.get("duration_seconds", pd.Series([], dtype=str)), errors="coerce"
        ).fillna(0).astype(int)
    media_series = df.get(
        "media_type", pd.Series([], dtype=str)
        ).astype(str).str.lower().fillna("")
    df2, df4, dfnr, dft = _policy_slices(df)

    def add_env_row(metric_label: str, mask: pd.Series, owners_unique: bool):
        sub_all = df[mask]
        sub2, sub4, subnr, subt = _policy_slices(sub_all)
        counts = _counts_by_policy(sub2, sub4, subnr, sub_all, owners_unique)
        rows.append({"metric": metric_label, **counts})

    # SUMMARY_OWNER: exact (comma-delimited)
    exacts = split_list(os.getenv("SUMMARY_OWNER"))
    for pat in exacts:
        mask = owner_series.str.lower() == pat.lower()
        add_env_row(f"Owner == {pat}", mask, owners_unique=True)

    # SUMMARY_OWNER_BEGINS_WITH
    starts = split_list(os.getenv("SUMMARY_OWNER_BEGINS_WITH"))
    for pat in starts:
        mask = ci_startswith(owner_series, pat)
        add_env_row(f"Owner begins with '{pat}'", mask, owners_unique=True)

    # SUMMARY_OWNER_ENDS_WITH
    ends = split_list(os.getenv("SUMMARY_OWNER_ENDS_WITH"))
    for pat in ends:
        mask = ci_endswith(owner_series, pat)
        add_env_row(f"Owner ends with '{pat}'", mask, owners_unique=True)

    # SUMMARY_ENTRY_NAME (contains)
    name_contains = split_list(os.getenv("SUMMARY_ENTRY_NAME"))
    for pat in name_contains:
        mask = ci_contains(name_series, pat)
        add_env_row(f"Entry name contains '{pat}'", mask, owners_unique=False)

    # SUMMARY_ENTRY_NAME_ENDS_WITH
    name_ends = split_list(os.getenv("SUMMARY_ENTRY_NAME_ENDS_WITH"))
    for pat in name_ends:
        mask = ci_endswith(name_series, pat)
        add_env_row(f"Entry name ends with '{pat}'", mask, owners_unique=False)

    # LENGTH_EQUALS: comma‑delimited integer seconds (default to 0 if unset)
    eq_values = split_ints(os.getenv("LENGTH_EQUALS") or "0")
    for n in eq_values:
        mask = (media_series != "image") & (duration_series == n)
        add_env_row(f"Duration == {n} sec", mask, owners_unique=False)

    # LENGTH_LESS_THAN: comma‑delimited integer seconds (exclude images;
    # require >=1 sec)
    lt_values = split_ints(os.getenv("LENGTH_LESS_THAN"))
    for n in lt_values:
        mask = (
            (media_series != "image")
            & (duration_series >= 1)
            & (duration_series < n)
        )
        add_env_row(f"Duration < {n} sec (>=1)", mask, owners_unique=False)

    # LENGTH_GREATER_THAN: comma‑delimited integer seconds (exclude images)
    gt_values = split_ints(os.getenv("LENGTH_GREATER_THAN"))
    for n in gt_values:
        mask = (media_series != "image") & (duration_series > n)
        add_env_row(f"Duration > {n} sec", mask, owners_unique=False)

    return rows


def main():
    load_dotenv()
    in_path = (
        os.getenv("SUMMARY_INPUT_FILENAME")
        or os.getenv("INPUT_FILENAME")
        or ""
        ).strip()
    if not in_path:
        print(
            "ERROR: Set SUMMARY_INPUT_FILENAME (or INPUT_FILENAME) in your "
            ".env (path to candidates CSV).", file=sys.stderr
            )
        sys.exit(2)
    if not os.path.exists(in_path):
        print(
            f"ERROR: SUMMARY_INPUT_FILENAME path not found: {in_path}",
            file=sys.stderr
            )
        sys.exit(2)

    out_path = (
        os.getenv("SUMMARY_OUTPUT_FILENAME")
        or os.getenv("OUTPUT_SUMMARY_CSV")
        or ""
    ).strip()

    # Default: append timestamp suffix unless explicitly disabled
    append_ts = os.getenv(
        "SUMMARY_OUTPUT_TIMESTAMP", "1"
        ).strip().lower() not in (
        "0", "false", "no", "n", ""
    )

    if out_path:
        if append_ts:
            base, ext = os.path.splitext(out_path)
            out_path = f"{base}_{stamp()}{ext or '.csv'}"
    else:
        out_path = f"summary_{stamp()}.csv" if append_ts else "summary.csv"

    print(f"[{ts()}] Loading {in_path} …")
    df = load_csv(in_path)

    expected = [
        "policy", "entry_id", "entry_name", "media_type", "created_on",
        "last_updated", "duration_seconds", "plays", "status", "owner",
        "lastPlayedAt", "reason",
    ]
    present = [c for c in expected if c in df.columns]
    missing = [c for c in expected if c not in df.columns]
    print(f"[{ts()}] Columns present: {', '.join(present)}")
    if missing:
        print(
            f"[{ts()}] (Info) Columns missing and treated as empty: "
            f"{', '.join(missing)}"
        )

    # Build the wide summary table
    wide = build_wide_summary(df)

    # Append environment-driven pattern rows (if any)
    env_rows = build_env_summary_rows(df)
    if env_rows:
        # Optional visual separator row
        sep = {
            "metric": "---", "2-year": "", "4-year": "",
            "non-ready": "", "total": "",
        }
        wide = pd.concat(
            [
                wide,
                pd.DataFrame([sep]),
                pd.DataFrame(
                    env_rows,
                    columns=[
                        "metric", "2-year", "4-year", "non-ready", "total"
                        ],
                ),
            ],
            ignore_index=True,
        )

    # Write CSV
    print(f"[{ts()}] Writing {out_path} …")
    wide.to_csv(out_path, index=False)

    # Console preview
    print("\nSummary")
    print("-------")
    for _, r in wide.iterrows():
        parts = [
            f"{r['metric']}: ",
            f"2-year={r['2-year']}, ",
            f"4-year={r['4-year']}, ",
            f"non-ready={r['non-ready']}, ",
            f"total={r['total']}",
        ]
        print("".join(parts))

    print(f"\n[{ts()}] Done. → {out_path}")


if __name__ == "__main__":
    main()
