"""Run main script."""

import snowflake.snowpark as snowpark

from zurich_bdx_accelerator.utils.file_handling import extract_staged_files as fx
from zurich_bdx_accelerator.utils.load_config import load_config
from zurich_bdx_accelerator.utils.snowpark_connector import snowpark_session_create

config = load_config()

# Create a Snowflake session
session = snowpark_session_create()


def main(session: snowpark.Session):
    """Run scripts.

        Args:
        session (snowpark.Session): Snowflake session

    Returns:
        Message upon completion.
    """
    file_list = fx.extract_files_from_stage(session, config["stage_dir"])

    fx.file_to_table(session, file_list)

    return "Complete"


main(session)
