from datetime import UTC, datetime

import pytest
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.storms.pipeline import (
    _CSV_COLS,
    Pipeline,
    load_storm_rows,
    manifest_age_hours,
    read_manifest,
)


class TestPipeline:
    def test_generate_dataset_with_exposure(
        self, configuration, input_dir, config_dir, blob_fixtures
    ):
        """ARTHUR (AL012026): real exported exposure, admin0 + admin1 rows."""
        manifest = read_manifest()
        storm = manifest["storms"][0]
        rows = load_storm_rows(storm["blob"])
        with temp_dir(
            "TestStormsArthur", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                dataset = pipeline.generate_dataset(storm, rows)

        assert dataset is not None
        assert dataset["name"] == "storm-arthur-2026-al012026"
        assert (
            dataset["title"]
            == "United States of America - Storm Arthur (2026) Population Exposure"
        )
        assert {t["name"] for t in dataset["tags"]} == {
            "climate-weather",
            "cyclones-hurricanes-typhoons",
        }
        assert dataset["data_update_frequency"] == "-1"
        resources = dataset.get_resources()
        assert len(resources) == 1
        assert resources[0]["name"] == "storm_exposure_arthur_al012026.csv"

    def test_load_storm_rows_keeps_export_values(self, blob_fixtures):
        rows = load_storm_rows("ds-storms-alerts/processed/hdx/x/al012026.csv")
        assert [list(row) for row in rows] == [_CSV_COLS] * 3
        adm0 = rows[0]
        assert adm0["admin_level"] == "0"
        assert adm0["admin_pcode"] == ""
        assert adm0["is_final_alert"] == "True"
        assert adm0["pop_exposed_34kt"] == "16996703"
        assert adm0["sources_34kt"] == "CHD|ADAM|GDACS"

    def test_read_manifest_rejects_other_columns(self, blob_fixtures, monkeypatch):
        from hdx.scraper.storms import pipeline as pl

        monkeypatch.setattr(pl, "_CSV_COLS", _CSV_COLS[:-1])
        with pytest.raises(ValueError, match="do not match"):
            read_manifest()

    def test_manifest_age_hours(self, blob_fixtures):
        manifest = read_manifest()
        now = datetime(2026, 10, 22, 13, 22, 31, 412093, tzinfo=UTC)
        assert manifest_age_hours(manifest, now) == pytest.approx(3.0)

    def test_generate_dataset_skips_zero_exposure(self, configuration):
        storm = {
            "atcf_id": "EP012026",
            "name": "AMANDA",
            "season": 2026,
            "issued_time": "2026-05-30T03:00:00",
            "all_final": True,
        }
        pipeline = Pipeline(configuration, None, None)
        assert pipeline.generate_dataset(storm, []) is None

    def test_generate_dataset_unknown_country(
        self, configuration, blob_fixtures, monkeypatch
    ):
        from hdx.data.dataset import Dataset
        from hdx.data.hdxobject import HDXError

        def _raise(self, iso3s):
            raise HDXError("bad location")

        monkeypatch.setattr(Dataset, "add_country_locations", _raise)
        storm = read_manifest()["storms"][0]
        rows = load_storm_rows(storm["blob"])
        with temp_dir(
            "TestStormsBadLoc", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir)
            assert pipeline.generate_dataset(storm, rows) is None

    def test_generate_dataset_unknown_basin_and_active(self, configuration):
        row = dict.fromkeys(_CSV_COLS, "")
        row.update(
            atcf_id="XX012026",
            admin_level="0",
            iso3="NIC",
            # Export (FieldMaps) naming, replaced by HDX's on the way out.
            country_name="Nicaragua (FieldMaps)",
            admin_name="Nicaragua (FieldMaps)",
            is_final_alert="False",
            pop_exposed_34kt="10",
        )
        storm = {
            "atcf_id": "XX012026",
            "name": None,
            "season": 2026,
            "issued_time": "2026-06-01T00:00:00",
            "all_final": False,
        }
        with temp_dir(
            "TestStormsBasin", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir)
            dataset = pipeline.generate_dataset(storm, [row])

        assert dataset["name"] == "storm-xx012026-2026-xx012026"
        assert (
            dataset["title"] == "Nicaragua - Storm Xx012026 (2026) Population Exposure"
        )
        assert dataset["data_update_frequency"] == "1"
        assert row["country_name"] == "Nicaragua"
        assert row["admin_name"] == "Nicaragua"

    @pytest.mark.parametrize(
        "atcf_id, scope", [("AL102026", "North Atlantic"), ("XX102026", "Global")]
    )
    def test_generate_dataset_multi_country_title(self, configuration, atcf_id, scope):
        rows = []
        for iso3 in ("USA", "NIC"):
            row = dict.fromkeys(_CSV_COLS, "")
            row.update(
                atcf_id=atcf_id,
                admin_level="0",
                iso3=iso3,
                is_final_alert="True",
                pop_exposed_34kt="10",
            )
            rows.append(row)
        storm = {
            "atcf_id": atcf_id,
            "name": "JOSEPHINE",
            "season": 2026,
            "issued_time": "2026-10-22T00:00:00",
            "all_final": True,
        }
        with temp_dir(
            "TestStormsMulti", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir)
            dataset = pipeline.generate_dataset(storm, rows)

        assert dataset["title"] == (
            f"{scope} - Storm Josephine (2026) Population Exposure"
        )
