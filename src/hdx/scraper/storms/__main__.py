#!/usr/bin/python
"""
Top level script. For each basin's export, loops over every storm in its
latest manifest and creates one HDX dataset per storm.

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
    ExportNotReady,
    Pipeline,
    csv_columns,
    load_export,
    load_storm_rows,
)

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-storms"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Storms"

# Every basin's export runs with each NHC advisory (four times a day), all
# year. A manifest older than this means it missed the current cycle: that
# basin is not published and the run fails (so the failure mail goes out)
# rather than republishing old figures. Set FORCE_PUBLISH=true to publish it
# anyway.
_MAX_AGE_HOURS = 6


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate one dataset per current-season storm of every basin and
    create them in HDX

    Args:
        save: Save downloaded data. Defaults to False.
        use_saved: Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("05547d2c-8430-4ce8-8e48-e3ea7e52a9a2")
    force = getenv("FORCE_PUBLISH", "false").lower() == "true"
    # A basin whose export is missing or stale is left as it is on HDX; the
    # others are still published, then the run fails.
    not_ready = []

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

            for key, basin in configuration["basins"].items():
                try:
                    manifest = load_export(key, basin, _MAX_AGE_HOURS, force)
                except ExportNotReady as err:
                    logger.error(str(err))
                    not_ready.append(str(err))
                    continue
                if manifest is None:
                    continue
                columns = csv_columns(basin)
                for storm in manifest["storms"]:
                    storm_id = storm[basin["id_column"]]
                    try:
                        rows = load_storm_rows(storm["blob"], columns)
                        dataset = pipeline.generate_dataset(storm, rows, basin)
                    except Exception:
                        logger.exception(
                            f"Failed to generate dataset for storm {storm_id}, skipping"
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

    if not_ready:
        raise RuntimeError("; ".join(not_ready))


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
