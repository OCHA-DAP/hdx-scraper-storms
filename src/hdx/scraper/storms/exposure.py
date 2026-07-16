"""Per-storm population-exposure computation.

Adapted from ds-storms-alerts's generate_exposure_csv (blends NHC forecast/
observed tracks with GDACS and ADAM sources, at admin0 and admin1 level).
The original computes exposure for every storm active at one alert's
issued_time; here each storm is evaluated independently at its own latest
available issued_time (see pipeline.get_latest_issued_time), since this
pipeline publishes one dataset per storm for the whole season rather than
one alert email per advisory.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from hdx.location.country import Country
from sqlalchemy import Engine, bindparam, text

from hdx.scraper.storms import fm_matching as fm

_ADMIN_LEVEL = 0
_ADMIN_LEVEL_1 = 1
_WIND_SPEEDS_KT = (34, 50, 64)
_ISSUED_OFFSET_HOURS = 3

_SRC_LABELS = {"our": "CHD", "ADAM": "ADAM", "GDACS": "GDACS"}

_CSV_COLS = [
    "admin_level",
    "country",
    "iso3",
    "pcode",
    "adm1_name",
    "is_final_alert",
    "pop_exposed_34kt",
    "pop_exposed_50kt",
    "pop_exposed_64kt",
    "sources_34kt",
    "sources_50kt",
    "sources_64kt",
    "caveat_34kt",
    "caveat_50kt",
    "caveat_64kt",
]


# ── admin-0 fetchers ─────────────────────────────────────────────────────


def fetch_fcast_exposure(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """Forecast-only exposure for one storm, all wind speeds, at issued_time."""
    sql = text("""
        SELECT e.atcf_id, e.iso3, e.wind_speed_kt, e.pop_exposed
        FROM storms.nhc_tracks_fcastonly_exposure e
        WHERE e.atcf_id = :atcf_id
          AND e.issued_time = :issued_time
          AND e.admin_level = :admin_level
          AND e.pop_exposed > 0
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "issued_time": issued_time,
                "admin_level": _ADMIN_LEVEL,
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def fetch_current_obsv_exposure(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """Latest cumulative observed exposure per (iso3, wind_speed_kt) for one storm."""
    sql = text("""
        SELECT DISTINCT ON (e.iso3, e.wind_speed_kt)
          e.atcf_id, e.iso3, e.wind_speed_kt, e.pop_exposed
        FROM storms.nhc_tracks_obsv_exposure e
        WHERE e.atcf_id = :atcf_id
          AND e.admin_level = :admin_level
          AND e.valid_time <= :issued_time
        ORDER BY e.iso3, e.wind_speed_kt, e.valid_time DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "issued_time": issued_time,
                "admin_level": _ADMIN_LEVEL,
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def fetch_gdacs_current_exposure(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """GDACS exposure per (iso3, wind_speed_kt) for one storm's advisory window."""
    sql = text("""
        SELECT DISTINCT ON (g.iso3, g.wind_speed_kt)
            lk.atcf_id, g.iso3, g.wind_speed_kt, g.pop_exposed
        FROM storms.gdacs_exposure g
        JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
        WHERE lk.atcf_id = :atcf_id
          AND g.admin_level = :admin_level
          AND g.pop_exposed > 0
          AND g.valid_time IN (:t_exact, :t_prev)
        ORDER BY g.iso3, g.wind_speed_kt, g.valid_time DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "admin_level": _ADMIN_LEVEL,
                "t_exact": issued_time,
                "t_prev": issued_time - timedelta(hours=_ISSUED_OFFSET_HOURS),
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def fetch_adam_current_exposure(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """ADAM exposure per (iso3, wind_speed_kt) for one storm's advisory window."""
    sql = text("""
        SELECT DISTINCT ON (a.iso3, a.wind_speed_kt)
            lk.atcf_id, a.iso3, a.wind_speed_kt, a.pop_exposed
        FROM storms.adam_exposure a
        JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
        WHERE lk.atcf_id = :atcf_id
          AND a.admin_level = :admin_level
          AND a.pop_exposed > 0
          AND a.valid_time IN (:t_exact, :t_prev)
        ORDER BY a.iso3, a.wind_speed_kt, a.valid_time DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "admin_level": _ADMIN_LEVEL,
                "t_exact": issued_time,
                "t_prev": issued_time - timedelta(hours=_ISSUED_OFFSET_HOURS),
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


# ── admin-1 fetchers ─────────────────────────────────────────────────────


def fetch_fcast_exposure_adm1(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """Forecast-only adm1 exposure for one storm. NHC `pcode` IS the FM pcode."""
    sql = text("""
        SELECT e.atcf_id, e.iso3, e.pcode AS fm_pcode, e.wind_speed_kt, e.pop_exposed
        FROM storms.nhc_tracks_fcastonly_exposure e
        WHERE e.atcf_id = :atcf_id
          AND e.issued_time = :issued_time
          AND e.admin_level = :admin_level
          AND e.pop_exposed > 0
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "issued_time": issued_time,
                "admin_level": _ADMIN_LEVEL_1,
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def fetch_current_obsv_exposure_adm1(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """Latest cumulative observed adm1 exposure for one storm."""
    sql = text("""
        SELECT DISTINCT ON (e.iso3, e.pcode, e.wind_speed_kt)
          e.atcf_id, e.iso3, e.pcode AS fm_pcode, e.wind_speed_kt, e.pop_exposed
        FROM storms.nhc_tracks_obsv_exposure e
        WHERE e.atcf_id = :atcf_id
          AND e.admin_level = :admin_level
          AND e.valid_time <= :issued_time
        ORDER BY e.iso3, e.pcode, e.wind_speed_kt, e.valid_time DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "issued_time": issued_time,
                "admin_level": _ADMIN_LEVEL_1,
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def _fetch_gdacs_adm1_window_rows(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    sql = text("""
        SELECT DISTINCT ON (g.gdacs_admin_code, g.wind_speed_kt)
            lk0.atcf_id, g.iso3, g.gdacs_admin_code,
            g.gdacs_admin_code AS admin_name, g.wind_speed_kt, g.pop_exposed
        FROM storms.gdacs_exposure g
        JOIN storms.storm_id_lookup lk0 ON lk0.gdacs_eventid = g.gdacs_eventid
        WHERE lk0.atcf_id = :atcf_id
          AND g.admin_level = :admin_level
          AND g.pop_exposed > 0
          AND g.valid_time IN (:t_exact, :t_prev)
        ORDER BY g.gdacs_admin_code, g.wind_speed_kt, g.valid_time DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "admin_level": _ADMIN_LEVEL_1,
                "t_exact": issued_time,
                "t_prev": issued_time - timedelta(hours=_ISSUED_OFFSET_HOURS),
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def fetch_gdacs_current_exposure_adm1(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """GDACS adm1 exposure aggregated to FieldMaps pcode for one storm."""
    cols = [
        "atcf_id",
        "iso3",
        "fm_pcode",
        "wind_speed_kt",
        "pop_exposed",
        "n_gdacs_admins",
        "gdacs_admins",
        "caveat_note",
    ]
    raw = _fetch_gdacs_adm1_window_rows(engine, atcf_id, issued_time)
    if raw.empty:
        return pd.DataFrame(columns=cols)
    matched = fm.match_gdacs(raw, fm.load_gdacs_lookup(engine))
    return matched.rename(
        columns={"n_src_admins": "n_gdacs_admins", "src_admins": "gdacs_admins"}
    )[cols]


def _fetch_adam_adm1_window_rows(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    sql = text("""
        SELECT DISTINCT ON (a.iso3, lower(a.admin_name), a.wind_speed_kt)
            lk0.atcf_id, a.iso3, a.admin_name, a.wind_speed_kt, a.pop_exposed
        FROM storms.adam_exposure a
        JOIN storms.storm_id_lookup lk0 ON lk0.adam_eventid = a.adam_eventid
        WHERE lk0.atcf_id = :atcf_id
          AND a.admin_level = :admin_level
          AND a.pop_exposed > 0
          AND a.valid_time IN (:t_exact, :t_prev)
        ORDER BY a.iso3, lower(a.admin_name), a.wind_speed_kt, a.valid_time DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "admin_level": _ADMIN_LEVEL_1,
                "t_exact": issued_time,
                "t_prev": issued_time - timedelta(hours=_ISSUED_OFFSET_HOURS),
            },
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def fetch_adam_current_exposure_adm1(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> pd.DataFrame:
    """ADAM adm1 exposure aggregated to FieldMaps pcode for one storm."""
    cols = [
        "atcf_id",
        "iso3",
        "fm_pcode",
        "wind_speed_kt",
        "pop_exposed",
        "n_adam_admins",
        "adam_admins",
        "caveat_note",
    ]
    raw = _fetch_adam_adm1_window_rows(engine, atcf_id, issued_time)
    if raw.empty:
        return pd.DataFrame(columns=cols)
    matched = fm.match_adam(raw, fm.load_adam_lookup(engine))
    matched = matched[matched["fm_pcode"].notna()].copy()
    return matched.rename(
        columns={"n_src_admins": "n_adam_admins", "src_admins": "adam_admins"}
    )[cols]


def fetch_fm_names(engine: Engine, iso3s: list[str]) -> dict[str, str]:
    """{fm_pcode: fm_name} at admin_level=1, UNIONed across GDACS/ADAM lookups."""
    if not iso3s:
        return {}
    sql = text("""
        SELECT fm_pcode, MAX(fm_name) AS fm_name FROM (
            SELECT fm_pcode, fm_name FROM storms.gdacs_fm_lookup
            WHERE admin_level = :admin_level
              AND iso3 IN :iso3s AND fm_pcode IS NOT NULL
            UNION ALL
            SELECT fm_pcode, fm_name FROM storms.adam_fm_lookup
            WHERE admin_level = :admin_level
              AND iso3 IN :iso3s AND fm_pcode IS NOT NULL
        ) z
        GROUP BY fm_pcode
    """).bindparams(bindparam("iso3s", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(
            sql, {"admin_level": _ADMIN_LEVEL_1, "iso3s": iso3s}
        ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def fetch_storm_countries(
    engine: Engine, atcf_id: str, issued_time: datetime
) -> list[str]:
    """iso3s with any admin0 exposure (any source) for this storm's issued_time."""
    sql = text("""
        SELECT DISTINCT iso3 FROM (
            SELECT iso3 FROM storms.nhc_tracks_fcastonly_exposure
              WHERE atcf_id = :atcf_id AND admin_level = :admin_level
                AND issued_time = :issued_time AND pop_exposed > 0
            UNION
            SELECT iso3 FROM storms.nhc_tracks_obsv_exposure
              WHERE atcf_id = :atcf_id AND admin_level = :admin_level
                AND valid_time <= :issued_time AND pop_exposed > 0
        ) z
    """)
    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "atcf_id": atcf_id,
                "admin_level": _ADMIN_LEVEL,
                "issued_time": issued_time,
            },
        ).fetchall()
    return sorted(r[0] for r in rows)


# ── row building ─────────────────────────────────────────────────────────


def _build_adm1_rows(
    atcf_id: str,
    storm_iso3s: set[str],
    iso3_to_name: dict[str, str],
    fm_name_by_pcode: dict[str, str],
    fcast_adm1_df: pd.DataFrame,
    obsv_adm1_df: pd.DataFrame,
    gdacs_adm1_df: pd.DataFrame,
    adam_adm1_df: pd.DataFrame,
) -> list[dict]:
    """Build admin-1 CSV rows for one storm, MAX-combining sources per FM unit.

    The set of sources used is held consistent across all admin-1 units within
    a storm-country (per wind speed) so units are directly comparable — see
    the original ds-storms-alerts._build_adm1_rows docstring for rationale.
    """
    f1, o1, g1, a1 = fcast_adm1_df, obsv_adm1_df, gdacs_adm1_df, adam_adm1_df

    def _match(df, iso3, pcode, wsp):
        if df.empty:
            return None
        sub = df[
            (df["iso3"] == iso3)
            & (df["fm_pcode"] == pcode)
            & (df["wind_speed_kt"] == wsp)
        ]
        return sub if not sub.empty else None

    def _num(df, iso3, pcode, wsp):
        sub = _match(df, iso3, pcode, wsp)
        return int(sub["pop_exposed"].iloc[0]) if sub is not None else 0

    def _caveat(df, iso3, pcode, wsp):
        sub = _match(df, iso3, pcode, wsp)
        if sub is None:
            return None
        val = sub["caveat_note"].iloc[0]
        return val if pd.notna(val) else None

    adm1_keys: set[tuple[str, str]] = set()
    for src in (f1, o1, g1, a1):
        sub = src[src["iso3"].isin(storm_iso3s)]
        adm1_keys |= {
            (r.iso3, r.fm_pcode) for r in sub.itertuples() if pd.notna(r.fm_pcode)
        }

    unit_vals: dict[tuple[str, str, int], dict[str, int]] = {}
    sources_used: dict[tuple[str, int], set[str]] = {}
    for iso3, pcode in adm1_keys:
        for wsp in _WIND_SPEEDS_KT:
            vals = {
                "our": _num(f1, iso3, pcode, wsp) + _num(o1, iso3, pcode, wsp),
                "ADAM": _num(a1, iso3, pcode, wsp),
                "GDACS": _num(g1, iso3, pcode, wsp),
            }
            unit_vals[(iso3, pcode, wsp)] = vals
            used = sources_used.setdefault((iso3, wsp), set())
            used |= {k for k, v in vals.items() if v > 0}

    out: list[dict] = []
    for iso3, pcode in sorted(adm1_keys):
        row: dict = {
            "admin_level": 1,
            "country": iso3_to_name.get(iso3, iso3),
            "iso3": iso3,
            "pcode": pcode,
            "adm1_name": fm_name_by_pcode.get(pcode, pcode),
            "is_final_alert": False,
        }
        any_value = False
        for wsp in _WIND_SPEEDS_KT:
            vals = unit_vals[(iso3, pcode, wsp)]
            used = sources_used.get((iso3, wsp), set())
            ordered = [k for k in ("our", "ADAM", "GDACS") if k in used]
            unit_val = max(vals[k] for k in ordered) if ordered else 0
            row[f"pop_exposed_{wsp}kt"] = unit_val
            row[f"sources_{wsp}kt"] = (
                ",".join(_SRC_LABELS[k] for k in ordered) if unit_val > 0 else ""
            )
            cavs = [
                c
                for c in (_caveat(g1, iso3, pcode, wsp), _caveat(a1, iso3, pcode, wsp))
                if c
            ]
            row[f"caveat_{wsp}kt"] = " | ".join(dict.fromkeys(cavs))
            any_value = any_value or any(v > 0 for v in vals.values())
        if any_value:
            out.append(row)
    return out


def build_storm_rows(engine: Engine, atcf_id: str, issued_time: datetime) -> list[dict]:
    """Admin0 + admin1 population-exposure rows for one storm at its latest
    available issued_time. Returns [] if the storm has no positive exposure
    anywhere (caller should skip publishing a dataset in that case)."""
    storm_iso3s = fetch_storm_countries(engine, atcf_id, issued_time)
    if not storm_iso3s:
        return []
    iso3_to_name = {
        iso3: Country.get_country_name_from_iso3(iso3) or iso3 for iso3 in storm_iso3s
    }

    fcast_df = fetch_fcast_exposure(engine, atcf_id, issued_time)
    obsv_df = fetch_current_obsv_exposure(engine, atcf_id, issued_time)
    gdacs_df = fetch_gdacs_current_exposure(engine, atcf_id, issued_time)
    adam_df = fetch_adam_current_exposure(engine, atcf_id, issued_time)

    def _obsv(iso3: str, wsp: int) -> int:
        sub = obsv_df[(obsv_df["iso3"] == iso3) & (obsv_df["wind_speed_kt"] == wsp)]
        return int(sub["pop_exposed"].sum()) if not sub.empty else 0

    rows: list[dict] = []
    for iso3 in storm_iso3s:
        row: dict = {
            "admin_level": 0,
            "country": iso3_to_name.get(iso3, iso3),
            "iso3": iso3,
            "pcode": "",
            "adm1_name": "",
            "is_final_alert": False,
        }
        for wsp in _WIND_SPEEDS_KT:
            tr = fcast_df[
                (fcast_df["iso3"] == iso3) & (fcast_df["wind_speed_kt"] == wsp)
            ]
            fcast_val = int(tr["pop_exposed"].iloc[0]) if not tr.empty else 0
            our_val = fcast_val + _obsv(iso3, wsp)

            gr = gdacs_df[
                (gdacs_df["iso3"] == iso3) & (gdacs_df["wind_speed_kt"] == wsp)
            ]
            gdacs_val = int(gr["pop_exposed"].iloc[0]) if not gr.empty else 0

            ar = adam_df[(adam_df["iso3"] == iso3) & (adam_df["wind_speed_kt"] == wsp)]
            adam_val = int(ar["pop_exposed"].iloc[0]) if not ar.empty else 0

            active = {
                k: v
                for k, v in {
                    "our": our_val,
                    "ADAM": adam_val,
                    "GDACS": gdacs_val,
                }.items()
                if v > 0
            }
            row[f"pop_exposed_{wsp}kt"] = max(active.values()) if active else 0
            row[f"sources_{wsp}kt"] = (
                ",".join(_SRC_LABELS.get(k, k) for k in active) if active else ""
            )
            row[f"caveat_{wsp}kt"] = ""
        rows.append(row)

    fcast_adm1_df = fetch_fcast_exposure_adm1(engine, atcf_id, issued_time)
    obsv_adm1_df = fetch_current_obsv_exposure_adm1(engine, atcf_id, issued_time)
    gdacs_adm1_df = fetch_gdacs_current_exposure_adm1(engine, atcf_id, issued_time)
    adam_adm1_df = fetch_adam_current_exposure_adm1(engine, atcf_id, issued_time)
    fm_name_by_pcode = fetch_fm_names(engine, storm_iso3s)

    rows.extend(
        _build_adm1_rows(
            atcf_id,
            set(storm_iso3s),
            iso3_to_name,
            fm_name_by_pcode,
            fcast_adm1_df,
            obsv_adm1_df,
            gdacs_adm1_df,
            adam_adm1_df,
        )
    )

    df_out = pd.DataFrame(rows).reindex(columns=_CSV_COLS)
    df_out = df_out.sort_values(["iso3", "admin_level", "pcode"], na_position="first")
    return df_out.to_dict("records")
