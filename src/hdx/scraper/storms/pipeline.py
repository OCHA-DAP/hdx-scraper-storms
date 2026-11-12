#!/usr/bin/python
"""Storms scraper — one HDX dataset per storm, population exposure by admin0/admin1."""

import json
import logging
from datetime import UTC, datetime

import ocha_stratus as stratus
from azure.core.exceptions import ResourceNotFoundError
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)

# Basin of an NHC storm, from the first two letters of its ATCF id. Other
# exports name their basin in the configuration (`basin_name`).
BASIN_NAMES = {
    "al": "North Atlantic",
    "ep": "Eastern Pacific",
    "cp": "Central Pacific",
    "wp": "Western Pacific",
    "io": "North Indian Ocean",
    "sh": "Southern Hemisphere",
}

# The exports are written by the hdx_export tasks of the "Storm Alert"
# Databricks job (OCHA-DAP/ds-storms-alerts, src/hdx_export.py) on the DEV
# storage account, readable here with the DSCI_AZ_BLOB_DEV_SAS org secret:
# one per basin entry of the project configuration. A manifest is written
# last and lists one CSV per storm, so it always describes a complete set.
BLOB_STAGE = "dev"

# Columns of the per-storm CSV (and of the HDX resource) after the storm id,
# which is named per basin (`id_column`): atcf_id for NHC storms, storm_id
# where there is no ATCF id. The export writes its full list into the
# manifest; read_manifest refuses a mismatch.
_COMMON_COLS = [
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


class ExportNotReady(Exception):
    """A basin's export cannot be published in this run."""


def csv_columns(basin: dict) -> list[str]:
    """Column order of `basin`'s per-storm CSV."""
    return [basin["id_column"], *_COMMON_COLS]


def read_manifest(basin: dict) -> dict:
    """The latest export manifest of `basin` (an entry of the configuration's
    `basins`): generated_at, as_of, season, and one entry per storm with
    positive exposure: {<id_column>, name, season, issued_time, blob, n_rows,
    iso3s, all_final}."""
    manifest = json.loads(stratus.load_blob_data(basin["manifest"], stage=BLOB_STAGE))
    columns = csv_columns(basin)
    if manifest["columns"] != columns:
        raise ValueError(
            f"Export columns {manifest['columns']} of {basin['manifest']} do not "
            f"match this pipeline's {columns}; update them together with the export."
        )
    return manifest


def manifest_age_hours(manifest: dict, now: datetime | None = None) -> float:
    """Hours since the export wrote `manifest`."""
    now = now or datetime.now(UTC)
    generated_at = datetime.fromisoformat(manifest["generated_at"])
    return (now - generated_at).total_seconds() / 3600


def load_export(
    key: str,
    basin: dict,
    max_age_hours: float,
    force: bool = False,
    now: datetime | None = None,
) -> dict | None:
    """The manifest of basin `key` if it can be published now.

    None if the basin has no export yet and is not `required` (a basin whose
    export is not live): it is skipped with a warning. Raises ExportNotReady
    if a required manifest is missing, if its columns are not this pipeline's,
    or if it is more than `max_age_hours` old and `force` is not set: the
    export runs four times a day, all year, so an older manifest means it
    missed a cycle, and old figures are not republished.
    """
    try:
        manifest = read_manifest(basin)
    except ResourceNotFoundError:
        if basin.get("required", True):
            raise ExportNotReady(
                f"{key}: no export manifest at {basin['manifest']}"
            ) from None
        logger.warning(
            f"{key}: no export manifest at {basin['manifest']} yet, skipping"
        )
        return None
    except ValueError as err:
        raise ExportNotReady(f"{key}: {err}") from None
    age_hours = manifest_age_hours(manifest, now)
    logger.info(
        f"{key}: export manifest generated {manifest['generated_at']} "
        f"({age_hours:.1f}h ago) as of {manifest['as_of']}: "
        f"{len(manifest['storms'])} storm(s)"
    )
    if age_hours > max_age_hours:
        if not force:
            raise ExportNotReady(
                f"{key}: export manifest is {age_hours:.1f}h old (limit "
                f"{max_age_hours}h): the Databricks export has not run for the "
                "current cycle. Nothing published for it; check the Storm Alert "
                "job's hdx_export tasks."
            )
        logger.warning(f"{key}: manifest is stale but FORCE_PUBLISH is set.")
    return manifest


def load_storm_rows(blob: str, columns: list[str]) -> list[dict]:
    """One storm's CSV rows in `columns` order, every value kept as the
    export wrote it (strings; no NaN for the empty admin_pcode/sources/caveat
    cells)."""
    df = stratus.load_csv_from_blob(
        blob, stage=BLOB_STAGE, dtype=str, keep_default_na=False
    )
    return df[columns].to_dict("records")


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

    def generate_dataset(
        self, storm: dict, rows: list[dict], basin: dict
    ) -> Dataset | None:
        """Dataset for one manifest entry `storm` of `basin` (an entry of
        the configuration's `basins`) and its CSV `rows`."""
        storm_id = storm[basin["id_column"]]
        name = storm["name"]
        season = storm["season"]
        if basin.get("split_year_season"):
            # Southern Hemisphere seasons run July to June; the export names
            # them by the year they end.
            season = f"{season - 1}/{season % 100:02d}"
        issued_time = datetime.fromisoformat(storm["issued_time"])

        storm_label = (name or storm_id).strip().title()
        basin_name = basin.get("basin_name") or BASIN_NAMES.get(storm_id[:2].lower())
        if basin_name:
            storm_descriptor = f"{storm_label} ({season}, {basin_name})"
        else:
            storm_descriptor = f"{storm_label} ({season})"

        if not rows:
            logger.info(
                f"No positive exposure for storm {storm_id} ({storm_label}), skipping"
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

        if basin.get("name_by_id"):
            # The storm may be named after its first forecasts: leave the
            # name out, or the named storm would become a second dataset.
            dataset_name = slugify(f"storm-{basin_name}-{season}-{storm_id}")
        else:
            dataset_name = slugify(f"storm-{storm_label}-{season}-{storm_id}")
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
        dataset["notes"] = basin["notes"].format(storm=storm_descriptor)
        dataset["methodology_other"] = basin["methodology_other"]
        dataset["caveats"] = basin["caveats"]
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
                f"Could not find countries {iso3s} for storm {storm_id}, skipping"
            )
            return None

        id_slug = slugify(storm_id, separator="_")
        if basin.get("name_by_id"):
            # As for the dataset name: HDX matches resources by name, so a
            # renamed file would replace the resource and break its links.
            resource_name = f"storm_exposure_{id_slug}.csv"
        else:
            storm_slug = slugify(storm_label, separator="_")
            resource_name = f"storm_exposure_{storm_slug}_{id_slug}.csv"
        resourcedata = {
            "name": resource_name,
            "description": basin["resource_description"],
        }
        dataset.generate_resource(
            self._tempdir,
            resource_name,
            rows,
            resourcedata,
            csv_columns(basin),
            encoding="utf-8-sig",
        )
        return dataset
