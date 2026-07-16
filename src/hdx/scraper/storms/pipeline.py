#!/usr/bin/python
"""Storms scraper — one HDX dataset per storm, population exposure by admin0/admin1."""

import logging
from datetime import datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve
from slugify import slugify
from sqlalchemy import Engine, bindparam, text

from hdx.scraper.storms.exposure import _CSV_COLS, build_storm_rows

logger = logging.getLogger(__name__)


def get_storms(engine: Engine, season: int) -> list[dict]:
    """Storms in `season` from the canonical NHC storm registry.

    Returns [{atcf_id, name, season}, ...]. If a current-season storm exists
    only in the historical ibtracs_storms fallback (not yet in nhc_storms),
    it won't appear here — nhc_storms is expected to be populated in near
    real time for active storms.
    """
    sql = text("""
        SELECT atcf_id, name, season
        FROM storms.nhc_storms
        WHERE season = :season
        ORDER BY atcf_id
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"season": season}).fetchall()
    return [{"atcf_id": r[0], "name": r[1], "season": r[2]} for r in rows]


def get_latest_issued_time(engine: Engine, atcf_ids: list[str]) -> dict[str, datetime]:
    """{atcf_id: MAX(issued_time)} across all forecast advisories on record."""
    if not atcf_ids:
        return {}
    sql = text("""
        SELECT atcf_id, MAX(issued_time) AS issued_time
        FROM storms.nhc_tracks_geo
        WHERE atcf_id IN :atcf_ids
        GROUP BY atcf_id
    """).bindparams(bindparam("atcf_ids", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(sql, {"atcf_ids": atcf_ids}).fetchall()
    return {r[0]: r[1] for r in rows}


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
        engine: Engine,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._engine = engine

    def generate_dataset(self, atcf_id: str, name: str, season: int) -> Dataset | None:
        issued_times = get_latest_issued_time(self._engine, [atcf_id])
        issued_time = issued_times.get(atcf_id)
        if issued_time is None:
            logger.warning(f"No track data for storm {atcf_id}, skipping")
            return None

        storm_label = (name or atcf_id).strip().title()
        rows = build_storm_rows(self._engine, atcf_id, issued_time)
        if not rows:
            logger.info(
                f"No positive exposure for storm {atcf_id} ({storm_label}), skipping"
            )
            return None

        dataset_name = slugify(f"storm-{storm_label}-{season}-{atcf_id}")
        dataset_title = f"{storm_label} ({season}) - Storm Population Exposure"
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        dataset["notes"] = (
            f"Population exposure estimates for {storm_label} ({season}), blending NHC "
            f"forecast/observed tracks with GDACS and ADAM sources, at admin0 and admin1 "
            f"level. Reflects the storm's most recent available advisory "
            f"({issued_time.isoformat()})."
        )
        dataset.set_time_period(issued_time)
        dataset.add_tags(self._configuration["tags"])

        iso3s = sorted({row["iso3"] for row in rows if row["iso3"]})
        dataset.set_subnational(True)
        try:
            dataset.add_country_locations(iso3s)
        except HDXError:
            logger.error(
                f"Could not find countries {iso3s} for storm {atcf_id}, skipping"
            )
            return None

        resource_name = f"{dataset_name}.csv"
        resourcedata = {
            "name": resource_name,
            "description": (
                "Admin0 and admin1 population exposure by wind speed band, sourced from "
                "NHC/CHD, GDACS, and ADAM (MAX across sources per unit)."
            ),
        }
        dataset.generate_resource(
            self._tempdir,
            resource_name,
            rows,
            resourcedata,
            _CSV_COLS,
        )
        return dataset
