# Collector for Storms Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-storms/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-storms/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-storms/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-storms?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline publishes one HDX dataset per tropical storm in the current
season, containing modeled population exposure at admin0 (country) and admin1
level.

The exposure figures are computed by the OCHA Data Science storms database's
own pipelines on Databricks: the `hdx_export` task of the "Storm Alert" job
([OCHA-DAP/ds-storms-alerts](https://github.com/OCHA-DAP/ds-storms-alerts),
`src/hdx_export.py`). The database is reachable only from inside the OCHA
network (private endpoint) since 30 September 2026, so this pipeline no longer
queries it. After every NHC advisory that task:

1. Takes the season's storms (`storms.nhc_storms`) and each storm's most recent
   NHC advisory.
2. Pulls population exposure from three sources at that advisory time: CHD's
   own NHC forecast/observed track buffers, GDACS, and ADAM, mapping GDACS and
   ADAM admin1 units onto FieldMaps p-codes so the sources can be compared per
   subnational unit.
3. For each admin unit and wind-speed band (34, 50, 64 kt), reports the
   maximum exposed population across sources, along with which sources
   contributed and any matching caveats. A country is flagged `is_final_alert`
   once the storm no longer threatens it at the current advisory (no track
   forecast and no wind speed probability exposure), so every country of a
   storm that has ended is final.
4. Writes one CSV per storm that has had positive exposure to the DSci dev
   blob, `ds-storms-alerts/processed/hdx/<advisory>/<atcf_id>.csv`, and then a
   manifest listing them, `ds-storms-alerts/processed/hdx/latest/manifest.json`.
   A storm whose forecast moved off land without ever reaching it stays in,
   reporting the countries it last threatened at the current figures (zero
   from CHD's own tracks), so its dataset is still marked final.

This pipeline reads that manifest and, for each storm in it, writes the CSV as
the dataset's single resource (`storm_exposure_<name>_<atcf_id>.csv`, with HDX
country names) and creates or updates the dataset in HDX.

### Scheduling

The pipeline is meant to run 30 minutes after each Databricks export (the cron
in `.github/workflows/run-python-script.yaml`, commented out until go-live) and
overwrites every dataset in the manifest. If the manifest is more than 6 hours
old (the export missed an advisory), the run fails without publishing; set
`FORCE_PUBLISH=true` (the workflow's `force` input) to publish it anyway.

### Expected update frequency

The HDX `expected_update_frequency` is set per dataset from the
`is_final_alert` flag: once every affected country's admin0 row is flagged
final (the storm has dropped out of the forecast for all of them), the
frequency is set to `-1` (never); otherwise it is set to `1`
(every day) while the storm is still active. The manifest carries this as
`all_final` per storm.

Reading the export requires the `DSCI_AZ_BLOB_DEV_SAS` environment variable
(the OCHA-DAP organisation secret of the same name), used by `ocha-stratus`.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-storms** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.storms
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you’ve introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
