"""This is an example to demonstrate dotenv. To be deleted later."""

import os

from dotenv import load_dotenv

load_dotenv()

username = os.getenv("USERNAME")
