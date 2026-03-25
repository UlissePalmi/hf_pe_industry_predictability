"""WRDS authentication and connection management."""
import getpass
import os
from pathlib import Path
from typing import Optional

import wrds
from dotenv import load_dotenv


def get_connection() -> "wrds.Connection":
    """
    Get a WRDS database connection.

    Loads credentials from .env file if available, otherwise prompts interactively.

    Returns:
        WRDS Connection object.
    """
    # Try to load from .env file first
    load_dotenv()

    username = os.getenv("WRDS_USERNAME")
    if username is None:
        username = input("Enter WRDS username: ")
    
    password = os.getenv("WRDS_PASSWORD")
    if password is None:
        password = getpass.getpass("Enter WRDS password: ")
    
    try:
        conn = wrds.Connection(username=username, password=password)
        print(f"✓ Connected to WRDS as {username}")
        return conn
    except Exception as e:
        raise ConnectionError(f"Failed to connect to WRDS: {e}") from e
