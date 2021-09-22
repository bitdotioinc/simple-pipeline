"""This is a script for running ad hoc SQL on bit.io.

This example omits many best practices (e.g. logging, error handling,
parameterizatin + config files, etc.) for the sake of a brief, minimal example.
"""
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine


# Load credentials from .env file, if needed
if not os.getenv('PG_CONN_STRING'):
    load_dotenv()
PG_CONN_STRING = os.getenv('PG_CONN_STRING')


def main(script_path, owner, repo):
    engine = create_engine(PG_CONN_STRING)

    with open(script_path, 'r') as f:
        sql = f.read()
        sql = sql.replace('$OWNER', owner)
        sql = sql.replace('$REPO', repo)
        commands = sql.split(';')

    with engine.connect() as conn:
        conn.execute("SET statement_timeout = 600000;")
        for command in commands:
            if len(command) > 0:
                conn.execute(command)

if __name__ == '__main__':
    # Parse command line arguments
    args = [arg for arg in sys.argv[1:]]
    # Validation
    if len(args) != 3:
        raise ValueError("Exactly three arguments (SQL script path, owner, and reponame) are required.")
    script_path, owner, repo = args

    # Execute script
    main(script_path, owner, repo)