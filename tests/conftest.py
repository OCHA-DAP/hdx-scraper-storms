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


@pytest.fixture
def blob_fixtures(monkeypatch, input_dir):
    """Serve the export's blobs from tests/fixtures/input: the manifest
    (manifest.json) and each storm's CSV (<atcf_id>.csv, from a real export of
    the prod storms database), through the same stratus calls the pipeline
    makes against the dev storage account."""
    import ocha_stratus as stratus

    def _path(blob):
        return join(input_dir, blob.rsplit("/", 1)[-1])

    def _load_blob_data(blob, stage="dev", container_name="projects"):
        with open(_path(blob), "rb") as f:
            return f.read()

    def _load_csv_from_blob(blob, stage="dev", container_name="projects", **kwargs):
        return pd.read_csv(_path(blob), **kwargs)

    monkeypatch.setattr(stratus, "load_blob_data", _load_blob_data)
    monkeypatch.setattr(stratus, "load_csv_from_blob", _load_csv_from_blob)
