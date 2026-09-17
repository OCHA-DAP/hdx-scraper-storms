import pandas as pd
import pytest

from hdx.scraper.storms import fm_matching as fm


def _gdacs_rows(*rows):
    return pd.DataFrame(
        list(rows),
        columns=[
            "atcf_id",
            "iso3",
            "gdacs_admin_code",
            "admin_name",
            "wind_speed_kt",
            "pop_exposed",
        ],
    )


def _gdacs_lookup(*rows):
    return pd.DataFrame(
        list(rows),
        columns=[
            "iso3",
            "gmi_admin",
            "fm_pcode",
            "fm_name",
            "caveat_kind",
            "caveat_note",
        ],
    )


def _adam_rows(*rows):
    return pd.DataFrame(
        list(rows),
        columns=["atcf_id", "iso3", "admin_name", "wind_speed_kt", "pop_exposed"],
    )


def _adam_lookup(*rows):
    return pd.DataFrame(
        list(rows),
        columns=[
            "iso3",
            "adam_admin_name",
            "fm_pcode",
            "fm_name",
            "caveat_kind",
            "caveat_note",
        ],
    )


class TestMatchGdacs:
    def test_empty_input(self):
        out = fm.match_gdacs(_gdacs_rows(), _gdacs_lookup())
        assert out.empty
        assert list(out.columns) == fm._OUT_COLS

    def test_missing_columns_raises(self):
        with pytest.raises(ValueError, match="match_gdacs: missing required column"):
            fm.match_gdacs(pd.DataFrame({"atcf_id": ["AL012026"]}), _gdacs_lookup())

    def test_sums_multiple_gdacs_admins_into_one_fm_unit(self):
        rows = _gdacs_rows(
            ("AL012026", "USA", "G1", "Dade", 34, 100),
            ("AL012026", "USA", "G2", "Broward", 34, 250),
        )
        lookup = _gdacs_lookup(
            ("USA", "G1", "US12", "Florida", "many_to_one", "note-a"),
            ("USA", "G2", "US12", "Florida", "many_to_one", "note-b"),
        )
        out = fm.match_gdacs(rows, lookup)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["fm_pcode"] == "US12"
        assert row["pop_exposed"] == 350
        assert row["n_src_admins"] == 2
        assert row["src_admins"] == "Broward | Dade"
        assert row["caveat_kind"] == "many_to_one"
        assert row["caveat_note"] == "note-a | note-b"

    def test_orphan_kept_with_null_pcode(self):
        rows = _gdacs_rows(
            ("AL012026", "USA", "G1", "Dade", 34, 100),
            ("AL012026", "USA", "G9", "Nowhere", 34, 7),
        )
        lookup = _gdacs_lookup(("USA", "G1", "US12", "Florida", None, None))
        out = fm.match_gdacs(rows, lookup)
        assert len(out) == 2
        orphan = out[out["fm_pcode"].isna()].iloc[0]
        assert orphan["src_admins"] == "Nowhere"
        assert orphan["n_src_admins"] == 1
        assert orphan["pop_exposed"] == 7
        matched = out[out["fm_pcode"].notna()].iloc[0]
        assert pd.isna(matched["caveat_kind"])
        assert pd.isna(matched["caveat_note"])

    def test_country_only_coverage_excluded(self):
        rows = _gdacs_rows(
            ("AL012026", "NIC", "N1", "Managua", 34, 500),
            ("AL012026", "USA", "G1", "Dade", 34, 100),
        )
        lookup = _gdacs_lookup(
            ("NIC", None, None, None, None, None),
            ("USA", "G1", "US12", "Florida", None, None),
        )
        out = fm.match_gdacs(rows, lookup)
        assert set(out["iso3"]) == {"USA"}

    def test_all_orphans(self):
        rows = _gdacs_rows(("AL012026", "USA", "G9", "Nowhere", 34, 7))
        out = fm.match_gdacs(rows, _gdacs_lookup())
        assert len(out) == 1
        assert out["fm_pcode"].isna().all()


class TestMatchAdam:
    def test_empty_input(self):
        out = fm.match_adam(_adam_rows(), _adam_lookup())
        assert out.empty
        assert list(out.columns) == fm._OUT_COLS

    def test_missing_columns_raises(self):
        with pytest.raises(ValueError, match="match_adam: missing required column"):
            fm.match_adam(pd.DataFrame({"atcf_id": ["AL012026"]}), _adam_lookup())

    def test_case_insensitive_name_match(self):
        rows = _adam_rows(("AL012026", "USA", "FLORIDA", 50, 300))
        lookup = _adam_lookup(("USA", "Florida", "US12", "Florida", None, None))
        out = fm.match_adam(rows, lookup)
        assert len(out) == 1
        assert out.iloc[0]["fm_pcode"] == "US12"
        assert out.iloc[0]["pop_exposed"] == 300
        assert out.iloc[0]["src_admins"] == "FLORIDA"

    def test_orphan_on_no_name_match(self):
        rows = _adam_rows(("AL012026", "USA", "Atlantis", 34, 9))
        lookup = _adam_lookup(("USA", "Florida", "US12", "Florida", None, None))
        out = fm.match_adam(rows, lookup)
        assert len(out) == 1
        assert pd.isna(out.iloc[0]["fm_pcode"])
        assert out.iloc[0]["src_admins"] == "Atlantis"
