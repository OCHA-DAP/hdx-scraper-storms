#!/usr/bin/python
"""Storms scraper — one HDX dataset per storm, population exposure by admin0/admin1."""

import logging
from datetime import datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify
from sqlalchemy import Engine, bindparam, text

from hdx.scraper.storms.exposure import _CSV_COLS, build_storm_rows

logger = logging.getLogger(__name__)

BASIN_NAMES = {
    "al": "North Atlantic",
    "ep": "Eastern Pacific",
    "cp": "Central Pacific",
    "wp": "Western Pacific",
    "io": "North Indian Ocean",
    "sh": "Southern Hemisphere",
}


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
        basin_name = BASIN_NAMES.get(atcf_id[:2].lower())
        if basin_name:
            storm_descriptor = f"{storm_label} ({season}, {basin_name})"
        else:
            storm_descriptor = f"{storm_label} ({season})"

        rows = build_storm_rows(self._engine, atcf_id, issued_time)
        if not rows:
            logger.info(
                f"No positive exposure for storm {atcf_id} ({storm_label}), skipping"
            )
            return None

        iso3s = sorted({row["iso3"] for row in rows if row["iso3"]})
        country_names = [Country.get_country_name_from_iso3(iso3) or iso3 for iso3 in iso3s]

        dataset_name = slugify(f"storm-{storm_label}-{season}-{atcf_id}")
        dataset_title = f"{', '.join(country_names)} - Storm Population Exposure, {storm_descriptor}"
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        dataset["notes"] = (
            f"This dataset contains population exposure estimates for {storm_descriptor}, "
            f"blending National Hurricane Center (NHC) forecast/observed "
            f"track buffers with Global Disaster Alert and Coordination System (GDACS) and "
            f"Advanced Geospatial Data Management (ADAM) population exposure estimates, at admin0 and admin1 level. Reflects the storm's most "
            f"recent available advisory."
        )
        dataset.set_time_period(issued_time)
        dataset.add_tags(self._configuration["tags"])
        is_final_alert = rows[0]["is_final_alert"]
        dataset.set_expected_update_frequency(-1 if is_final_alert else 1)

        dataset.set_subnational(True)
        try:
            dataset.add_country_locations(iso3s)
        except HDXError:
            logger.error(
                f"Could not find countries {iso3s} for storm {atcf_id}, skipping"
            )
            return None

        storm_slug = slugify(storm_label, separator="_")
        resource_name = f"storm_exposure_{storm_slug}_{atcf_id.lower()}.csv"
        resourcedata = {
            "name": resource_name,
            "description": (
                "Admin0 and admin1 population exposure by wind speed band, sourced from "
                "the National Hurricane Center (NHC)/Centre for Humanitarian Data (CHD), "
                "the Global Disaster Alert and Coordination System (GDACS), and Advanced Geospatial Data Management (ADAM) "
                "(MAX across sources per unit)."
            ),
        }
        dataset.generate_resource(
            self._tempdir,
            resource_name,
            rows,
            resourcedata,
            _CSV_COLS,
            encoding="utf-8-sig",
        )
        return dataset
