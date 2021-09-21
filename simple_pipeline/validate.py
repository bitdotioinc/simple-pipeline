import logging

logger = logging.getLogger(__name__)


def test_data(df, tests):
    """Run provided data tests on provided data.

    Parameters
    ----------
    df : pandas.DataFrame object
      The dataset to test.
    tests : dict
      A mapping from tests to failure messages.
    
    Returns
    ----------
    boolean
    """
    results = []
    for test_func, failure_message in tests:
        results.append(test_func(df.copy()))
        if results[-1]:
            logger.info(f'Data test {test_func.__name__} passed.')
        else:
            logger.error(f'Data test {test_func.__name__} failed. {failure_message}')
    logger.info(f'{sum(results)}/{len(results)} passed.')
    return sum(results) == len(results)


def cases_vs_deaths(df):
    """Checks that death count is no more than case count."""
    return (df['deaths'] <= df['cases']).all()


def unique_records(df):
    """Checks that each date and FIPs combination is unique."""
    return df[['date', 'fips']].drop_duplicates().shape[0] == df.shape[0]


def range_test(series, min, max):
    """Checks that all values in a series are within a range, inclusive"""
    return (series >= min).all() and (series <= max).all() 


def cases_range_test(df):
    """Checks that all cases are non-negative and <= 10M"""
    return range_test(df['cases'], 0, 10e6)


def deaths_range_test(df):
    """Checks that all deaths are non-negative and <= 100K"""
    return range_test(df['deaths'], 0, 1e5)  


# Data test for NYT covid cases and deaths
nyt_cases_counties = [
    (cases_vs_deaths, "Death counts cannot exceed case counts."),
    (unique_records, "Only one record per FIPs, per date allowed."),
    (cases_range_test, "Cases must be non-negative and <= 10M"),
    (deaths_range_test, "Deaths must be non-negative and <= 100K")
]
