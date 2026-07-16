"""FieldMaps admin-1 matching for storm exposure.

Maps each external source's native admin-1 units onto the canonical FieldMaps
(FM) pcode, so GDACS / ADAM / CHD exposure can be compared per subnational
unit. Pure pandas + sqlalchemy.text — no geopandas/spatial join, since the
GDACS/ADAM -> FM crosswalks are precomputed lookup tables
(storms.gdacs_fm_lookup / storms.adam_fm_lookup), not live geometry matching.

CHD/NHC needs no matching: its `pcode` already IS the FM pcode at adm1.

VENDORED from ds-storms-alerts's src/fm_matching.py (itself vendored from
ds-storm-impact-harmonisation) for the hdx-scraper-storms per-storm exposure
CSV. Keep in sync with the upstream if the matching logic changes.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

ADMIN_LEVEL = 1

_OUT_COLS = [
    "atcf_id",
    "iso3",
    "fm_pcode",
    "wind_speed_kt",
    "pop_exposed",
    "n_src_admins",
    "src_admins",
    "caveat_kind",
    "caveat_note",
]

_REQ_GDACS = (
    "atcf_id",
    "iso3",
    "gdacs_admin_code",
    "admin_name",
    "wind_speed_kt",
    "pop_exposed",
)
_REQ_ADAM = ("atcf_id", "iso3", "admin_name", "wind_speed_kt", "pop_exposed")


def _require(df: pd.DataFrame, cols, who: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"{who}: missing required column(s) {missing}; got {list(df.columns)}"
        )


def load_gdacs_lookup(engine, admin_level: int = ADMIN_LEVEL) -> pd.DataFrame:
    """Static GDACS->FM crosswalk at `admin_level`.

    Columns: iso3, gmi_admin, fm_pcode, fm_name, caveat_kind, caveat_note.
    Country-only-coverage countries carry a gmi_admin IS NULL row (used to
    exclude them from adm1).
    """
    return pd.read_sql(
        text("""
            SELECT iso3, gmi_admin, fm_pcode, fm_name, caveat_kind, caveat_note
            FROM storms.gdacs_fm_lookup WHERE admin_level = :lvl
        """),
        engine,
        params={"lvl": admin_level},
    )


def load_adam_lookup(engine, admin_level: int = ADMIN_LEVEL) -> pd.DataFrame:
    """Static ADAM->FM crosswalk at `admin_level`.

    Columns: iso3, adam_admin_name, fm_pcode, fm_name, caveat_kind, caveat_note.
    """
    return pd.read_sql(
        text("""
            SELECT iso3, adam_admin_name, fm_pcode, fm_name, caveat_kind, caveat_note
            FROM storms.adam_fm_lookup WHERE admin_level = :lvl
        """),
        engine,
        params={"lvl": admin_level},
    )


def match_gdacs(gdacs_rows: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Map GDACS adm1 rows onto FM pcodes, SUM-aggregated per FM unit.

    gdacs_rows: one row per (atcf_id, iso3, gdacs_admin_code, wind_speed_kt)
      with `admin_name` and `pop_exposed`, already time-resolved by the caller.
    lookup: load_gdacs_lookup(engine).

    Countries GDACS only covers nationally (a gmi_admin IS NULL row) are
    excluded from adm1. GDACS admins with no FM match are returned as orphan
    rows (fm_pcode = NA) for the caller to log/drop.
    """
    if gdacs_rows.empty:
        return pd.DataFrame(columns=_OUT_COLS)
    _require(gdacs_rows, _REQ_GDACS, "match_gdacs")
    country_only = set(lookup.loc[lookup["gmi_admin"].isna(), "iso3"])
    rows = gdacs_rows[~gdacs_rows["iso3"].isin(country_only)]
    lk = lookup.loc[
        lookup["gmi_admin"].notna(),
        ["iso3", "gmi_admin", "fm_pcode", "caveat_kind", "caveat_note"],
    ]
    merged = rows.merge(
        lk,
        how="left",
        left_on=["iso3", "gdacs_admin_code"],
        right_on=["iso3", "gmi_admin"],
    )
    return _aggregate_to_fm(merged.assign(_src_id=merged["gdacs_admin_code"]))


def match_adam(adam_rows: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Map ADAM adm1 rows onto FM pcodes, SUM-aggregated per FM unit.

    ADAM matches FM by case-insensitive admin name. ADAM admins with no FM
    match are returned as orphan rows (fm_pcode = NA) for the caller to decide
    whether to drop.
    """
    if adam_rows.empty:
        return pd.DataFrame(columns=_OUT_COLS)
    _require(adam_rows, _REQ_ADAM, "match_adam")
    rows = adam_rows.assign(_nm=adam_rows["admin_name"].str.lower())
    lk = lookup.assign(_nm=lookup["adam_admin_name"].str.lower())[
        ["iso3", "_nm", "fm_pcode", "caveat_kind", "caveat_note"]
    ]
    merged = rows.merge(lk, how="left", on=["iso3", "_nm"])
    return _aggregate_to_fm(merged.assign(_src_id=merged["admin_name"]))


def _aggregate_to_fm(merged: pd.DataFrame) -> pd.DataFrame:
    """Shared tail of both matchers: SUM matched rows per FM unit, keep
    unmatched (orphan) rows as-is with fm_pcode = NA."""
    out = []
    matched = merged[merged["fm_pcode"].notna()]
    if not matched.empty:
        out.append(
            matched.groupby(
                ["atcf_id", "iso3", "fm_pcode", "wind_speed_kt"], as_index=False
            ).agg(
                pop_exposed=("pop_exposed", "sum"),
                n_src_admins=("_src_id", "nunique"),
                src_admins=("admin_name", _join_names),
                caveat_kind=("caveat_kind", _agg_first),
                caveat_note=("caveat_note", _agg_join),
            )
        )
    orphan = merged[merged["fm_pcode"].isna()]
    if not orphan.empty:
        out.append(
            orphan.assign(
                fm_pcode=pd.NA,
                n_src_admins=1,
                src_admins=orphan["admin_name"],
                caveat_kind=pd.NA,
                caveat_note=pd.NA,
            )[_OUT_COLS]
        )
    if not out:
        return pd.DataFrame(columns=_OUT_COLS)
    return pd.concat(out, ignore_index=True)[_OUT_COLS]


def _join_names(s):
    return " | ".join(sorted({x for x in s if pd.notna(x)}))


def _agg_first(s):
    vals = sorted({x for x in s if pd.notna(x)})
    return vals[-1] if vals else pd.NA


def _agg_join(s):
    vals = sorted({x for x in s if pd.notna(x)})
    return " | ".join(vals) if vals else pd.NA
