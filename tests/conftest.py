import json
from datetime import datetime
from os.path import join

import pandas as pd
import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def config_dir(fixtures_dir):
    return join("src", "hdx", "scraper", "storms", "config")


@pytest.fixture(scope="session")
def configuration(config_dir):
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=join(config_dir, "project_configuration.yaml"),
    )
    Locations.set_validlocations(
        [
            {"name": "usa", "title": "United States of America"},
            {"name": "nic", "title": "Nicaragua"},
        ]
    )
    Country.countriesdata(False)
    Vocabulary._approved_vocabulary = {
        "tags": [
            {"name": tag}
            for tag in (
                "climate-weather",
                "cyclones-hurricanes-typhoons",
            )
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return Configuration.read()


@pytest.fixture(scope="session")
def storm_fixtures(input_dir):
    """Real captured Postgres query results (see tests/fixtures/input/storm_exposure.json),
    one entry per real storm from the dev DB, keyed by atcf_id."""
    with open(join(input_dir, "storm_exposure.json")) as f:
        return json.load(f)


@pytest.fixture
def mock_exposure_fetchers(monkeypatch, storm_fixtures):
    """Monkeypatch exposure.py's DB fetch functions to replay storm_fixtures instead
    of hitting Postgres, so build_storm_rows' real aggregation/harmonization logic
    still runs against real captured data."""
    from hdx.scraper.storms import exposure as ex

    def _df(atcf_id, key, columns):
        records = storm_fixtures[atcf_id][key]
        return (
            pd.DataFrame(records, columns=columns)
            if records
            else pd.DataFrame(columns=columns)
        )

    monkeypatch.setattr(
        ex,
        "fetch_storm_countries",
        lambda engine, atcf_id, issued_time: storm_fixtures[atcf_id]["storm_iso3s"],
    )
    monkeypatch.setattr(
        ex,
        "fetch_fcast_exposure",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "fcast_exposure",
            ["atcf_id", "iso3", "wind_speed_kt", "pop_exposed"],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_current_obsv_exposure",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "obsv_exposure",
            ["atcf_id", "iso3", "wind_speed_kt", "pop_exposed"],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_gdacs_current_exposure",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "gdacs_exposure",
            ["atcf_id", "iso3", "wind_speed_kt", "pop_exposed"],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_adam_current_exposure",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "adam_exposure",
            ["atcf_id", "iso3", "wind_speed_kt", "pop_exposed"],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_fcast_exposure_adm1",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "fcast_exposure_adm1",
            ["atcf_id", "iso3", "fm_pcode", "wind_speed_kt", "pop_exposed"],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_current_obsv_exposure_adm1",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "obsv_exposure_adm1",
            ["atcf_id", "iso3", "fm_pcode", "wind_speed_kt", "pop_exposed"],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_gdacs_current_exposure_adm1",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "gdacs_exposure_adm1",
            [
                "atcf_id",
                "iso3",
                "fm_pcode",
                "wind_speed_kt",
                "pop_exposed",
                "n_gdacs_admins",
                "gdacs_admins",
                "caveat_note",
            ],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_adam_current_exposure_adm1",
        lambda engine, atcf_id, issued_time: _df(
            atcf_id,
            "adam_exposure_adm1",
            [
                "atcf_id",
                "iso3",
                "fm_pcode",
                "wind_speed_kt",
                "pop_exposed",
                "n_adam_admins",
                "adam_admins",
                "caveat_note",
            ],
        ),
    )
    monkeypatch.setattr(
        ex,
        "fetch_fm_names",
        lambda engine, iso3s: _fm_names_for(storm_fixtures, iso3s),
    )
    monkeypatch.setattr(
        ex,
        "fetch_prev_fcast_iso3s",
        lambda engine, atcf_id, issued_time: set(
            storm_fixtures[atcf_id].get("prev_fcast_iso3s", [])
        ),
    )

    def _get_latest_issued_time(engine, atcf_ids):
        return {
            aid: datetime.fromisoformat(storm_fixtures[aid]["issued_time"])
            for aid in atcf_ids
            if aid in storm_fixtures
        }

    from hdx.scraper.storms import pipeline as pl

    monkeypatch.setattr(pl, "get_latest_issued_time", _get_latest_issued_time)


def _fm_names_for(storm_fixtures, iso3s):
    for data in storm_fixtures.values():
        if set(data["storm_iso3s"]) & set(iso3s):
            return data["fm_names"]
    return {}
