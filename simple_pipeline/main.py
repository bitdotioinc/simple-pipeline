"""This is an example of a simple ETL pipeline for loading data into bit.io.

This example omits many best practices (e.g. logging, error handling,
parameterizatin + config files, etc.) for the sake of a brief, minimal example.
"""
import os
import sys

from dotenv import load_dotenv

import extract
import transform
import load


# Load credentials from .env file, if needed
if not os.getenv('PG_CONN_STRING'):
    load_dotenv()
PG_CONN_STRING = os.getenv('PG_CONN_STRING')


def main(src, dest, local_src, options):
    """
    Executes ETL pipeline for a single table.

    Extracts source data, (optionally) transforms the data, and loads the data
    to a Postgres database on bit.io.

    Parameters
    ----------
    src : str
        URL for source data extraction.
    dest : str
        Fully-qualified table for load into bit.io.
    local_src : str
        True if src is path to a local csv file.
    options : dict
        Option - argument map from the user command.
    """
    # EXTRACT data
    if local_src:
        df = extract.csv_from_local(src)
    else:
        df = extract.csv_from_get_request(src)
    # TRANSFORM data
    if 'name' in options:
        if hasattr(transform, options['name']):
            df = getattr(transform, options['name'])(df)
        else:
            raise ValueError("Specified transformation name not found.")
    # LOAD data
    load.to_table(df, dest, PG_CONN_STRING)


if __name__ == '__main__':
    # Parse command line options and arguments
    opts = [opt[1:] for opt in sys.argv[1:] if opt.startswith("-")]
    local_source = 'local_source' in opts
    opts = [opt for opt in opts if opt != 'local_source']
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]
    # Validation
    if len(args) != len(opts) + 2:
        raise ValueError("At least one argument is missing, check the README.")
    # Set up local variables
    source, destination = args[-2:]
    opt_args = args[:-2]
    option_args = dict(zip(opts, opt_args))

    # Execute ETL
    main(source, destination, local_source, option_args)
