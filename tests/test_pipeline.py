from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.storms.pipeline import Pipeline


class TestPipeline:
    def test_generate_dataset_with_exposure(
        self, configuration, input_dir, config_dir, mock_exposure_fetchers
    ):
        """ARTHUR (AL012026): real captured exposure, admin0 + admin1 rows."""
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
                pipeline = Pipeline(configuration, retriever, tempdir, engine=None)
                dataset = pipeline.generate_dataset("AL012026", "ARTHUR", 2026)

        assert dataset is not None
        assert dataset["name"] == "storm-arthur-2026-al012026"
        assert (
            dataset["title"] == "United States of America - Storm Population Exposure, "
            "Arthur (2026, North Atlantic)"
        )
        assert {t["name"] for t in dataset["tags"]} == {
            "climate-weather",
            "cyclones-hurricanes-typhoons",
        }
        resources = dataset.get_resources()
        assert len(resources) == 1
        assert resources[0]["name"] == "storm_exposure_arthur_al012026.csv"

    def test_generate_dataset_skips_zero_exposure(
        self, configuration, input_dir, config_dir, mock_exposure_fetchers
    ):
        """AMANDA (EP012026): no positive exposure anywhere -> no dataset published."""
        with temp_dir(
            "TestStormsAmanda", delete_on_success=True, delete_on_failure=False
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
                pipeline = Pipeline(configuration, retriever, tempdir, engine=None)
                dataset = pipeline.generate_dataset("EP012026", "AMANDA", 2026)

        assert dataset is None

    def test_generate_dataset_no_track_data(self, configuration, monkeypatch):
        from hdx.scraper.storms import pipeline as pl

        monkeypatch.setattr(pl, "get_latest_issued_time", lambda engine, ids: {})
        pipeline = Pipeline(configuration, None, None, engine=None)
        assert pipeline.generate_dataset("AL012026", "ARTHUR", 2026) is None

    def test_generate_dataset_unknown_country(
        self, configuration, mock_exposure_fetchers, monkeypatch
    ):
        from hdx.data.dataset import Dataset
        from hdx.data.hdxobject import HDXError

        def _raise(self, iso3s):
            raise HDXError("bad location")

        monkeypatch.setattr(Dataset, "add_country_locations", _raise)
        with temp_dir(
            "TestStormsBadLoc", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir, engine=None)
            assert pipeline.generate_dataset("AL012026", "ARTHUR", 2026) is None

    def test_generate_dataset_unknown_basin_and_final_alert(
        self, configuration, monkeypatch
    ):
        from datetime import datetime

        from hdx.scraper.storms import pipeline as pl
        from hdx.scraper.storms.exposure import _CSV_COLS

        row = dict.fromkeys(_CSV_COLS, "")
        row.update(
            atcf_id="XX012026",
            admin_level=0,
            iso3="NIC",
            country_name="Nicaragua",
            admin_name="Nicaragua",
            is_final_alert=True,
            pop_exposed_34kt=10,
        )
        monkeypatch.setattr(
            pl,
            "get_latest_issued_time",
            lambda engine, ids: {"XX012026": datetime(2026, 6, 1)},
        )
        monkeypatch.setattr(pl, "build_storm_rows", lambda engine, aid, t: [row])
        with temp_dir(
            "TestStormsBasin", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir, engine=None)
            dataset = pipeline.generate_dataset("XX012026", None, 2026)

        assert dataset["name"] == "storm-xx012026-2026-xx012026"
        assert dataset["title"] == (
            "Nicaragua - Storm Population Exposure, Xx012026 (2026)"
        )
        assert dataset["data_update_frequency"] == "-1"
