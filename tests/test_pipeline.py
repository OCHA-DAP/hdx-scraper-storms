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
            dataset["title"]
            == "United States of America - Storm Population Exposure, "
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
