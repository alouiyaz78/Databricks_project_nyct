from dateutil.relativedelta import relativedelta
from datetime import date

def get_target_yyyy_mm(months_ago: int):
    """
    Returns the year-month string in 'yyyy-MM' format for a date that is a specified number of months prior to the current month.

    Parameters:
    months_ago (int): The number of months to go back from the current month.

    Returns:
    str: A string representing the target year and month in 'yyyy-MM' format.
    """
    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime("%Y-%m")

def get_mont_start_n_moth_ago(months_ago: int):
    """
    Returns the first day of the month for a date that is a specified number of months prior to the current month.

    Parameters:
    months_ago (int): The number of months to go back from the current month.

    Returns:
    date: A date object representing the first day of the target month.
    """
    return (date.today().replace(day=1) - relativedelta(months=months_ago))
