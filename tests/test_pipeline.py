from datetime import UTC, datetime

import pytest
from azure.core.exceptions import ResourceNotFoundError
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.storms.pipeline import (
    ExportNotReady,
    Pipeline,
    csv_columns,
    load_export,
    load_storm_rows,
    manifest_age_hours,
    read_manifest,
)

# Just after the fixture manifests were generated (see their generated_at).
_NHC_NOW = datetime(2026, 10, 22, 13, 22, 31, 412093, tzinfo=UTC)
_SP_NOW = datetime(2027, 1, 14, 4, 46, 40, 503112, tzinfo=UTC)


class TestPipeline:
    def test_generate_dataset_with_exposure(
        self, configuration, input_dir, config_dir, blob_fixtures, nhc
    ):
        """ARTHUR (AL012026): real exported exposure, admin0 + admin1 rows."""
        manifest = read_manifest(nhc)
        storm = manifest["storms"][0]
        rows = load_storm_rows(storm["blob"], csv_columns(nhc))
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
                dataset = pipeline.generate_dataset(storm, rows, nhc)

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
        # The NHC texts, word for word as before they moved to the
        # configuration.
        assert dataset["notes"] == (
            "This dataset contains population exposure estimates for Arthur "
            "(2026, North Atlantic), blending National Hurricane Center (NHC) "
            "forecast/observed track buffers with Global Disaster Alert and "
            "Coordination System (GDACS) and Advanced Geospatial Data Management "
            "(ADAM) population exposure estimates, at admin0 and admin1 level. "
            "Reflects the storm's most recent available advisory."
        )
        assert dataset["methodology_other"] == (
            "Modeled population exposure combining NHC storm forecast/observed "
            "track buffers with GDACS/ADAM exposure data"
        )
        assert dataset["caveats"] == (
            "Admin1 units are identified using FieldMaps p-codes"
        )
        assert resources[0]["description"] == (
            "Admin0 and admin1 population exposure by wind speed band, sourced "
            "from the National Hurricane Center (NHC)/Centre for Humanitarian "
            "Data (CHD), the Global Disaster Alert and Coordination System "
            "(GDACS), and Advanced Geospatial Data Management (ADAM) (MAX across "
            "sources per unit)."
        )

    def test_generate_dataset_south_pacific(self, configuration, blob_fixtures, sp):
        """A South Pacific (FMS) storm: storm_id column, July-June season,
        basin and texts from the configuration."""
        manifest = read_manifest(sp)
        assert manifest["basin"] == "sp"
        storm = manifest["storms"][0]
        rows = load_storm_rows(storm["blob"], csv_columns(sp))
        assert list(rows[0])[0] == "storm_id"
        with temp_dir(
            "TestStormsSouthPacific", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir)
            dataset = pipeline.generate_dataset(storm, rows, sp)
            resource = dataset.get_resources()[0]
            with open(resource.get_file_to_upload(), encoding="utf-8-sig") as f:
                header = f.readline().strip().split(",")

        assert dataset["name"] == "storm-south-pacific-2026-27-2627-03f"
        assert dataset["title"] == (
            "South Pacific - Storm Example (2026/27) Population Exposure"
        )
        assert dataset["notes"].startswith(
            "This dataset contains population exposure estimates for Example "
            "(2026/27, South Pacific): "
        )
        assert "Fiji Meteorological Service" in dataset["notes"]
        assert "NHC" not in dataset["notes"] + dataset["methodology_other"]
        assert "Kiribati" in dataset["caveats"]
        assert dataset["data_update_frequency"] == "1"
        assert {g["name"] for g in dataset["groups"]} == {"fji", "ton"}
        assert resource["name"] == "storm_exposure_example_2627_03f.csv"
        assert "Fiji Meteorological Service" in resource["description"]
        assert header == csv_columns(sp)

    def test_load_storm_rows_keeps_export_values(self, blob_fixtures, nhc):
        rows = load_storm_rows(
            "ds-storms-alerts/processed/hdx/x/al012026.csv", csv_columns(nhc)
        )
        assert [list(row) for row in rows] == [csv_columns(nhc)] * 3
        adm0 = rows[0]
        assert adm0["admin_level"] == "0"
        assert adm0["admin_pcode"] == ""
        assert adm0["is_final_alert"] == "True"
        assert adm0["pop_exposed_34kt"] == "16996703"
        assert adm0["sources_34kt"] == "CHD|ADAM|GDACS"

    def test_read_manifest_rejects_other_columns(self, blob_fixtures, nhc, sp):
        # The NHC export read as if it were the South Pacific one.
        with pytest.raises(ValueError, match="do not match"):
            read_manifest({**sp, "manifest": nhc["manifest"]})

    def test_manifest_age_hours(self, blob_fixtures, nhc):
        manifest = read_manifest(nhc)
        assert manifest_age_hours(manifest, _NHC_NOW) == pytest.approx(3.0)

    def test_load_export_fresh(self, blob_fixtures, nhc, sp):
        assert load_export("nhc", nhc, 6, now=_NHC_NOW)["season"] == 2026
        assert load_export("sp", sp, 6, now=_SP_NOW)["season"] == 2027

    def test_load_export_stale(self, blob_fixtures, nhc):
        with pytest.raises(ExportNotReady, match="nhc: export manifest is 3.0h old"):
            load_export("nhc", nhc, 2, now=_NHC_NOW)
        assert load_export("nhc", nhc, 2, force=True, now=_NHC_NOW) is not None

    def test_load_export_other_columns(self, blob_fixtures, nhc, sp):
        with pytest.raises(ExportNotReady, match="sp: Export columns"):
            load_export("sp", {**sp, "manifest": nhc["manifest"]}, 6, now=_NHC_NOW)

    def test_load_export_missing(self, monkeypatch, nhc, sp):
        """No manifest: skipped for a basin whose export is not live yet,
        a failure for a required one."""
        import ocha_stratus as stratus

        def _missing(blob, stage="dev", container_name="projects"):
            raise ResourceNotFoundError("BlobNotFound")

        monkeypatch.setattr(stratus, "load_blob_data", _missing)
        assert sp["required"] is False
        assert load_export("sp", sp, 6) is None
        with pytest.raises(ExportNotReady, match="nhc: no export manifest"):
            load_export("nhc", nhc, 6)
        with pytest.raises(ExportNotReady, match="sp: no export manifest"):
            load_export("sp", {**sp, "required": True}, 6)

    def test_generate_dataset_skips_zero_exposure(self, configuration, nhc):
        storm = {
            "atcf_id": "EP012026",
            "name": "AMANDA",
            "season": 2026,
            "issued_time": "2026-05-30T03:00:00",
            "all_final": True,
        }
        pipeline = Pipeline(configuration, None, None)
        assert pipeline.generate_dataset(storm, [], nhc) is None

    def test_generate_dataset_unknown_country(
        self, configuration, blob_fixtures, monkeypatch, nhc
    ):
        from hdx.data.dataset import Dataset
        from hdx.data.hdxobject import HDXError

        def _raise(self, iso3s):
            raise HDXError("bad location")

        monkeypatch.setattr(Dataset, "add_country_locations", _raise)
        storm = read_manifest(nhc)["storms"][0]
        rows = load_storm_rows(storm["blob"], csv_columns(nhc))
        with temp_dir(
            "TestStormsBadLoc", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir)
            assert pipeline.generate_dataset(storm, rows, nhc) is None

    def test_generate_dataset_unknown_basin_and_active(self, configuration, nhc):
        row = dict.fromkeys(csv_columns(nhc), "")
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
            dataset = pipeline.generate_dataset(storm, [row], nhc)

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
    def test_generate_dataset_multi_country_title(
        self, configuration, nhc, atcf_id, scope
    ):
        rows = []
        for iso3 in ("USA", "NIC"):
            row = dict.fromkeys(csv_columns(nhc), "")
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
            dataset = pipeline.generate_dataset(storm, rows, nhc)

        assert dataset["title"] == (
            f"{scope} - Storm Josephine (2026) Population Exposure"
        )

    def test_generate_dataset_south_pacific_unnamed_single_country(
        self, configuration, sp
    ):
        """An unnamed disturbance over one country: the FMS id stands in for
        the name and the country leads the title."""
        row = dict.fromkeys(csv_columns(sp), "")
        row.update(
            storm_id="2627-05F",
            admin_level="0",
            iso3="FJI",
            is_final_alert="True",
            pop_exposed_34kt="10",
        )
        storm = {
            "storm_id": "2627-05F",
            "name": None,
            "season": 2027,
            "issued_time": "2027-02-01T06:00:00",
            "all_final": True,
        }
        with temp_dir(
            "TestStormsUnnamed", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            pipeline = Pipeline(configuration, None, tempdir)
            dataset = pipeline.generate_dataset(storm, [row], sp)
            named = pipeline.generate_dataset({**storm, "name": "EXAMPLE"}, [row], sp)

        # The same name it will have once the storm is named.
        assert dataset["name"] == "storm-south-pacific-2026-27-2627-05f"
        assert named["name"] == dataset["name"]
        assert named["title"] == "Fiji - Storm Example (2026/27) Population Exposure"
        assert dataset["title"] == (
            "Fiji - Storm 2627-05F (2026/27) Population Exposure"
        )
        assert dataset["data_update_frequency"] == "-1"
        assert dataset.get_resources()[0]["name"] == (
            "storm_exposure_2627_05f_2627_05f.csv"
        )
