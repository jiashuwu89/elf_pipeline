"""General utility functions"""
import datetime as dt


def convert_date_to_datetime(date: dt.date) -> dt.datetime:
    """Converts a date to the equivalent datetime at midnight."""
    return dt.datetime.combine(date, dt.datetime.min.time())
