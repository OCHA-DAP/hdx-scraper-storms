#!/usr/bin/python
"""Storms scraper — one HDX dataset per storm, population exposure by admin0/admin1."""

import json
import logging
from datetime import UTC, datetime

import ocha_stratus as stratus
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)

BASIN_NAMES = {
    "al": "North Atlantic",
    "ep": "Eastern Pacific",
    "cp": "Central Pacific",
    "wp": "Western Pacific",
    "io": "North Indian Ocean",
    "sh": "Southern Hemisphere",
}

# Written by the hdx_export task of the "Storm Alert" Databricks job
# (OCHA-DAP/ds-storms-alerts, src/hdx_export.py) on the DEV storage account,
# readable here with the DSCI_AZ_BLOB_DEV_SAS org secret. The manifest is
# written last and lists one CSV per storm, so it always describes a complete
# set.
BLOB_STAGE = "dev"
MANIFEST_BLOB = "ds-storms-alerts/processed/hdx/latest/manifest.json"

# Column order of the per-storm CSV (and of the HDX resource). The export
# writes the same list into the manifest; read_manifest refuses a mismatch.
_CSV_COLS = [
    "atcf_id",
    "admin_level",
    "iso3",
    "country_name",
    "admin_pcode",
    "admin_name",
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


def read_manifest() -> dict:
    """The latest export manifest: generated_at, as_of, season, and one entry
    per storm with positive exposure: {atcf_id, name, season, issued_time,
    blob, n_rows, iso3s, all_final}."""
    manifest = json.loads(stratus.load_blob_data(MANIFEST_BLOB, stage=BLOB_STAGE))
    if manifest["columns"] != _CSV_COLS:
        raise ValueError(
            f"Export columns {manifest['columns']} do not match this pipeline's "
            f"{_CSV_COLS}; update _CSV_COLS together with the export."
        )
    return manifest


def manifest_age_hours(manifest: dict, now: datetime | None = None) -> float:
    """Hours since the export wrote `manifest`."""
    now = now or datetime.now(UTC)
    generated_at = datetime.fromisoformat(manifest["generated_at"])
    return (now - generated_at).total_seconds() / 3600


def load_storm_rows(blob: str) -> list[dict]:
    """One storm's CSV rows, every value kept as the export wrote it (strings;
    no NaN for the empty admin_pcode/sources/caveat cells)."""
    df = stratus.load_csv_from_blob(
        blob, stage=BLOB_STAGE, dtype=str, keep_default_na=False
    )
    return df[_CSV_COLS].to_dict("records")


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def generate_dataset(self, storm: dict, rows: list[dict]) -> Dataset | None:
        """Dataset for one manifest entry `storm` and its CSV `rows`."""
        atcf_id = storm["atcf_id"]
        name = storm["name"]
        season = storm["season"]
        issued_time = datetime.fromisoformat(storm["issued_time"])

        storm_label = (name or atcf_id).strip().title()
        basin_name = BASIN_NAMES.get(atcf_id[:2].lower())
        if basin_name:
            storm_descriptor = f"{storm_label} ({season}, {basin_name})"
        else:
            storm_descriptor = f"{storm_label} ({season})"

        if not rows:
            logger.info(
                f"No positive exposure for storm {atcf_id} ({storm_label}), skipping"
            )
            return None

        iso3s = sorted({row["iso3"] for row in rows if row["iso3"]})
        iso3_to_name = {
            iso3: Country.get_country_name_from_iso3(iso3) or iso3 for iso3 in iso3s
        }
        # The export names countries as the alert emails do (FieldMaps, e.g.
        # "Saint Pierre and Miquelon (Fr.)"); use HDX's names on HDX, matching
        # the title and the dataset's locations.
        for row in rows:
            country_name = iso3_to_name.get(row["iso3"], row["country_name"])
            row["country_name"] = country_name
            if str(row["admin_level"]) == "0":
                row["admin_name"] = country_name
        country_names = [iso3_to_name[iso3] for iso3 in iso3s]

        dataset_name = slugify(f"storm-{storm_label}-{season}-{atcf_id}")
        # HDX title convention: a single-country dataset starts with the country,
        # otherwise with the broadest meaningful region (here the basin). The
        # season stays in because storm names are reused every few years.
        scope = (
            country_names[0] if len(country_names) == 1 else (basin_name or "Global")
        )
        dataset_title = f"{scope} - Storm {storm_label} ({season}) Population Exposure"
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
        # all_final: every country's is_final_alert is TRUE, i.e. the storm no
        # longer threatens any of them (judged by the export against the
        # current advisory), so the figures will not change.
        dataset.set_expected_update_frequency(-1 if storm["all_final"] else 1)

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
