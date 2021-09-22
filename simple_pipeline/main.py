"""This is an example of a simple ETL pipeline for loading data into bit.io.

This example omits many best practices (e.g. logging, error handling,
parameterizatin + config files, etc.) for the sake of a brief, minimal example.
"""

import logging
import os
import sys

from dotenv import load_dotenv

import extract
import transform
import validate
import load


# Set up logging
logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# Load credentials from .env file, if needed
if not os.getenv('PG_CONN_STRING'):
    load_dotenv()
PG_CONN_STRING = os.getenv('PG_CONN_STRING')


def main(src, dest, local_src, validate_data, options):
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
    local_src : boolean
        True if src is path to a local csv file.
    validate_data : boolean
        True if data validation should be run.
    options : dict
        Option - argument map from the user command.
    """
    # EXTRACT data
    logger.info('Starting extract...')
    if local_src:
        df = extract.csv_from_local(src)
    else:
        df = extract.csv_from_get_request(src)
    # TRANSFORM data
    if 'name' in options:
        if hasattr(transform, options['name']):
            logger.info(f"Starting transform with {options['name']}...")
            df = getattr(transform, options['name'])(df)
        else:
            raise ValueError("Specified transformation name not found.")
    else:
        logger.info(f"No transformation specified, skipping to validation step.")
    # VALIDATE data
    if ('name' in options) and validate_data:
        if hasattr(validate, options['name']):
            logger.info(f"Starting data validation with {options['name']}...")
            tests = getattr(validate, options['name'])
            assert validate.test_data(df, tests), 'Data validation failed, terminating ETL.'
        else:
            raise ValueError("Specified test suite not found.")
    else:
        logger.info(f"No data validation specified, skipping to load step.")
    # LOAD data
    logger.info(f"Loading data to bit.io...")
    load.to_table(df, dest, PG_CONN_STRING)
    logger.info(f"Data loaded to bit.io.")


if __name__ == '__main__':
    # Parse command line options and arguments
    logger.info('Parsing command...')
    opts = [opt[1:] for opt in sys.argv[1:] if opt.startswith("-")]
    local_source = 'local_source' in opts
    validate_data = 'validate_data' in opts
    opts = [opt for opt in opts if opt not in ['local_source', 'validate_data']]
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]
    # Validation
    if len(args) != len(opts) + 2:
        raise ValueError("At least one argument is missing, check the README.")
    # Set up local variables
    source, destination = args[-2:]
    opt_args = args[:-2]
    option_args = dict(zip(opts, opt_args))

    # Execute ETL
    logger.info('Starting ETL...')
    main(source, destination, local_source, validate_data, option_args)
