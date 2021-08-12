"""This is a script for running ad hoc SQL on bit.io.

This example omits many best practices (e.g. logging, error handling,
parameterizatin + config files, etc.) for the sake of a brief, minimal example.
"""
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine


# Load credentials from ENV
load_dotenv()
PG_CONN_STRING = os.getenv('PG_CONN_STRING')


def main(script_path):
    engine = create_engine(PG_CONN_STRING)

    with open(script_path, 'r') as f:
        sql = f.read()
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
    if len(args) != 1:
        raise ValueError("Only one argument (SQL script path) is permitted.")
    script_path = args[0]

    # Execute script
    main(script_path)