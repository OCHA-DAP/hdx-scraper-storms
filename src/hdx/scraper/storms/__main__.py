#!/usr/bin/python
"""
Top level script. Loops over every storm in the latest export manifest and
creates one HDX dataset per storm.

"""

import logging
from os import getenv
from os.path import expanduser, join

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
from hdx.scraper.storms.pipeline import (
    Pipeline,
    load_storm_rows,
    manifest_age_hours,
    read_manifest,
)

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-storms"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Storms"

# The export runs with every NHC advisory (four times a day). A manifest older
# than this means the export has not run for the current advisory, so there
# is nothing new to publish. Set FORCE_PUBLISH=true to publish it anyway.
_MAX_AGE_HOURS = 6


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
    User.check_current_user_write_access("05547d2c-8430-4ce8-8e48-e3ea7e52a9a2")
    manifest = read_manifest()
    age_hours = manifest_age_hours(manifest)
    logger.info(
        f"Export manifest generated {manifest['generated_at']} ({age_hours:.1f}h ago) "
        f"for advisory {manifest['as_of']}: {len(manifest['storms'])} storm(s)"
    )
    if age_hours > _MAX_AGE_HOURS:
        if getenv("FORCE_PUBLISH", "false").lower() != "true":
            logger.warning(
                f"Manifest is older than {_MAX_AGE_HOURS}h: the export has not run "
                "for the current advisory, so nothing is published."
            )
            return
        logger.warning("Manifest is stale but FORCE_PUBLISH is set: publishing.")

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
            pipeline = Pipeline(configuration, retriever, tempdir)

            for storm in manifest["storms"]:
                atcf_id = storm["atcf_id"]
                try:
                    rows = load_storm_rows(storm["blob"])
                    dataset = pipeline.generate_dataset(storm, rows)
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
        # hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
