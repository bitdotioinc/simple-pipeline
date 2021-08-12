"""Provides extraction functions, currently only supports GET from URL."""

import pandas as pd


def nyt_cases_counties(df):
    """Transforms NYT county-level COVID data"""
    # Cast date as datetime
    df['date'] = pd.to_datetime(df['date'])
    # Store FIPS codes as standard 5 digit strings
    df['fips'] = df['fips'].astype(str).str.extract('(.*)\.', expand=False).str.zfill(5)
    # Drop Puerto Rico due to missing deaths data, cast deaths to int
    df = df.loc[df['state'] != 'Puerto Rico'].copy()
    df['deaths'] = df['deaths'].astype(int)
    return df


def cdc_vaccines_counties(df):
    """Transforms CDC county-level vaccination data"""
    # Lowercase column names
    df.columns = [col.lower() for col in df.columns]
    # Filter columns
    keep_columns = [
        'date',
        'fips',
        'series_complete_pop_pct',
        'series_complete_yes',
        'series_complete_18plus',
        'series_complete_18pluspop_pct',
        'series_complete_65plus',
        'series_complete_65pluspop_pct',
        'completeness_pct']
    df = df[keep_columns].copy()
    # Cast date as datetime
    df['date'] = pd.to_datetime(df['date'])
    # Store FIPS codes as standard 5 digit strings
    df['fips'] = df['fips'].str.zfill(5)
    return df


def acs_population_counties(df):
    """Transforms 5-Year ACS population data"""
    # Drop extra header row
    df = df.iloc[1:,:].copy()
    # Transform and filter columns of interest
    df['fips'] = df['GEO_ID'].str.slice(-5,)
    df['total_population'] = df['S0101_C01_001E'].astype(int)
    df['population_16plus'] = df['S0101_C01_025E'].astype(int)
    df['population_18plus'] = df['S0101_C01_026E'].astype(int)
    df['population_65plus'] = df['S0101_C01_030E'].astype(int)
    keep_columns = [
        'fips',
        'total_population',
        'population_16plus',
        'population_18plus',
        'population_65plus']
    df = df[keep_columns].copy()
    return df



