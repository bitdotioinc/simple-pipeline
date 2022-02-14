#!/bin/bash
source venv/bin/activate
cd simple_pipeline
# Uncomment the line below if you would like to re-run the population data pipeline
# The population data is updated annually by the Census Bureau and does not need to re-run daily
# python main.py -local_source -name acs_population_counties acs_5yr_population_data.csv bitdotio/simple_pipeline.population_counties
python main.py -name nyt_cases_counties 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv' bitdotio/simple_pipeline.cases_counties
python main.py -name cdc_vaccines_counties 'https://data.cdc.gov/api/views/8xkx-amqh/rows.csv?accessType=DOWNLOAD' bitdotio/simple_pipeline.vaccinations_counties
python sql_executor.py ca_covid_data.sql bitdotio simple_pipeline
