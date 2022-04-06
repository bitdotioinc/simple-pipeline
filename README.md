# bit.io simple pipeline

A simple bit.io pipeline example using scripts and the UNIX cron scheduler.

## Scope

This repo is intended to provide a simple pipeline example for getting started with programmtic data ingestion and updates in bit.io. To keep the repo simple, many best practices such as logging, configuration files, and a more robust orchestration/scheduling framework are omitted.

## Setup

- Add a .env file at the root with your own bit.io Postgres connection string as `PG_CONN_STRING`
- Create environment
    - `python3 -m venv venv`<br>
    - `source venv/bin/activate`<br>
    - `python3 -m pip install --upgrade pip -r requirements.txt`<br>
- Create a repo on bit.io, we named ours `simple_pipeline` for this demo

## Contents

- simple_pipeline
    - main.py # command line script for ETL jobs
    - extract.py # Handles extraction of data into a pandas DataFrame
    - transform.py # Transforms data using pandas
    - load.py # Loads data from pandas to bit.io
    - sql_executor.py # Runs arbitrary SQL scripts on bit.io
    - ca_covid_data.sql # Example SQL script for bit.io
    - acs_5yr_population_data.csv # Population data, this changes annually
- README.md
- requirements.txt
- scheduled_run.sh # This shows how to batch calls to the python scripts together for a simple pipeline
- LICENSE

## Usage

As a demo piece, this simple pipeline contains two main data processing scripts:
1. `simple_pipeline/main.py` extracts, transforms (optional), and loads a csv from a URL or local file into bit.io
2. `simple_pipeline/sql_executor.py` executes SQL scripts on bit.io, such as for creating joined, de-normalized tables

In addition, a shell script `scheduled_run.sh` is included to show how the two scripts can be composed to form a simple pipeline. Utility programs like `cron` can then be used to run the shell script on a schedule for automated updates in bit.io. Here is an example `crontab` job that I created on my local system for this pipeline:

`45 09 * * * cd ~/Documents/simple_pipeline && ./scheduled_run.sh`

The `45 09 * * *` defines a schedule of once daily, at 9:45. You can learn more about cron syntax at [crontab.guru](https://crontab.guru/).

## Using simple_pipeline/main.py

This is a simple extract, transform, load script. The main script `main.py` can be run from the command line as follows:

`python simple_pipeline/main.py <SOURCE_URL_OR_FILE_PATH> <DESTINATION_FULLY_QUALIFIED_TABLE>`

The script also takes a `-local_source` option that indicates the source is a local file path (default is a URL) and a `-name` option with an argument for a transformation function to run. Here is an example command for a URL source with a transformation function called "nyt_cases_counties":

`python main.py -name nyt_cases_counties https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv bitdotio/simple_pipeline.cases_counties`

Here is an example command that uses a local file and skips the transformation step (note that no `-name` specified):
`python main.py -local_source -name acs_population_counties acs_5yr_population_data.csv bitdotio/simple_pipeline.population_counties`

The transformation functions are defined in `transform.py`. If you want to run these examples, make sure to update the destination with your own username in place of `bitdotio` and your own repo name if it is different from `simple_pipeline`.

## Using simple_pipeline/sql_executor.py

Once data has been extracted, transformed, and loaded, we sometimes want to create derived tables within the database. This script takes one argument, a path to a SQL script to run on bit.io. For example, to create the derived California COVID data table, the script is called as follows:

`python sql_executor.py ca_covid_data.sql bitdotio simple_pipeline`
