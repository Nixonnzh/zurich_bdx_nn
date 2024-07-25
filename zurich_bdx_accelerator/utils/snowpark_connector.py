"""Create Snowpark session."""
import logging
import os

from dotenv import load_dotenv
from snowflake.snowpark import Session

# Load environment variables from a .env file
load_dotenv(override=True)


def snowpark_session_create():
    """
    Create a Snowflake session using environment variables for configuration.

    Returns:
        snowflake.snowpark.Session: A Snowflake session object.
    """
    # Retrieve environment variables
    env_vars = [
        "ACCOUNT",
        "USERNAME",
        "PASSWORD",
        "ROLE",
        "WAREHOUSE",
        "DATABASE",
        "SCHEMA",
    ]
    config = {}

    for var in env_vars:
        value = os.getenv(var)
        if not value:
            raise ValueError(f"Environment variable {var} is not set or empty.")
        config[var.lower()] = value

    # Map the environment variables to the connection parameters
    connection_params = {
        "account": config["account"],
        "user": config["username"],
        "password": config["password"],
        "role": config["role"],
        "warehouse": config["warehouse"],
        "database": config["database"],
        "schema": config["schema"],
    }

    # Create a Snowflake session
    try:
        session = Session.builder.configs(connection_params).create()
        logging.info("Snowflake session created successfully.")
        return session
    except Exception as e:
        logging.error(f"Failed to create Snowflake session: {e}")
        raise


# Example usage:
# session = snowpark_session_create()
