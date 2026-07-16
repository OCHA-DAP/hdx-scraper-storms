#!/usr/bin/python
"""
Top level script. Loops over every storm in the current season and creates
one HDX dataset per storm.

"""

import logging
from datetime import UTC, datetime
from os.path import expanduser, join

import ocha_stratus as stratus
from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.storms._version import __version__
from hdx.scraper.storms.pipeline import Pipeline, get_storms

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-storms"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Storms"

# Only dev-stage Azure Postgres access is available for this pipeline so far.
# Promoting to stage="prod" is a follow-up once prod DB credentials exist.
_DB_STAGE = "dev"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate one dataset per current-season storm and create them in HDX

    Args:
        save: Save downloaded data. Defaults to False.
        use_saved: Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")
    season = datetime.now(UTC).year
    engine = stratus.get_engine(stage=_DB_STAGE)

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, tempdir, engine)

            storms = get_storms(engine, season)
            logger.info(f"Found {len(storms)} storm(s) for season {season}")
            for storm in storms:
                atcf_id = storm["atcf_id"]
                try:
                    dataset = pipeline.generate_dataset(
                        atcf_id, storm["name"], storm["season"]
                    )
                except Exception:
                    logger.exception(
                        f"Failed to generate dataset for storm {atcf_id}, skipping"
                    )
                    continue
                if not dataset:
                    continue
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )


if __name__ == "__main__":
    facade(
        main,
        #        hdx_site="dev",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
