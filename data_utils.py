"""
Data processing utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pandas as pd
import pyodbc
from typing import Optional, Tuple, Dict
from datetime import datetime, timedelta


def pyodbc_rows_to_dataframe(cursor: pyodbc.Cursor) -> pd.DataFrame:
    """
    Convert pyodbc cursor results to a pandas DataFrame
    
    Args:
        cursor: pyodbc cursor with executed query
        
    Returns:
        pd.DataFrame: Query results as DataFrame
    """
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()
    
    columns = [col[0] for col in cursor.description]
    data = [tuple(row) for row in rows]  # Critical conversion
    return pd.DataFrame(data, columns=columns)


class DateRangeCalculator:
    """Calculate report date ranges based on a reference date"""
    
    def __init__(self, run_date: datetime = None):
        self.run_date = run_date or datetime.now()
        # Base date is usually "Yesterday" relative to run date
        self.base_date = self.run_date - timedelta(days=1)
        self.base_date = self.base_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
    def get_all_ranges(self) -> Dict[str, Tuple[datetime, datetime]]:
        """Get all 6 required date ranges"""
        return {
            "For The Day (Actual)": self.for_the_day_actual(),
            "For The Day (Prior Year)": self.for_the_day_prior_year(),
            "For The Week Ending (Actual)": self.week_ending_actual(),
            "For The Week Ending (Prior Year)": self.week_ending_prior_year(),
            "For Winter Ending (Actual)": self.winter_ending_actual(),
            "For Winter Ending (Prior Year)": self.winter_ending_prior_year()
        }

    def for_the_day_actual(self) -> Tuple[datetime, datetime]:
        """Yesterday start to end"""
        start = self.base_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = self.base_date
        return start, end

    def for_the_day_prior_year(self) -> Tuple[datetime, datetime]:
        """Same day of week last year"""
        # Go back 52 weeks to align day of week
        prior_date = self.base_date - timedelta(weeks=52)
        start = prior_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = prior_date
        return start, end

    def week_ending_actual(self) -> Tuple[datetime, datetime]:
        """Monday of current week to For The Day"""
        # Monday is weekday 0
        days_since_monday = self.base_date.weekday()
        start = (self.base_date - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = self.base_date
        return start, end

    def week_ending_prior_year(self) -> Tuple[datetime, datetime]:
        """Monday of prior year week to For The Day Prior Year"""
        # Get the prior year "For The Day" (DOW aligned)
        prior_end_date = (self.base_date - timedelta(weeks=52))
        
        # Calculate Monday of that week
        days_since_monday = prior_end_date.weekday()
        start = (prior_end_date - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, prior_end_date

    def winter_ending_actual(self) -> Tuple[datetime, datetime]:
        """Nov 1 of current season to For The Day"""
        # Determine season start year
        # If month is Nov(11) or Dec(12), season start is current year Nov 1
        # If month is Jan-Oct, season start is previous year Nov 1
        current_month = self.base_date.month
        current_year = self.base_date.year
        
        if current_month >= 11:
            season_start_year = current_year
        else:
            season_start_year = current_year - 1
            
        start = datetime(season_start_year, 11, 1, 0, 0, 0)
        end = self.base_date
        return start, end

    def winter_ending_prior_year(self) -> Tuple[datetime, datetime]:
        """Nov 1 of prior season to Same Date last year (Date aligned, not DOW)"""
        # First, determine the "same date" last year
        # e.g. Nov 19, 2025 -> Nov 19, 2024
        prior_date_year = self.base_date.year - 1
        
        # Handle leap year feb 29 if necessary, though typically Nov-Apr season
        try:
            end = self.base_date.replace(year=prior_date_year)
        except ValueError: # Feb 29 on non-leap year
            end = self.base_date.replace(year=prior_date_year, day=28)
            
        # Determine prior season start
        # Logic matches actual season logic but shifted back 1 year
        current_month = self.base_date.month
        
        if current_month >= 11:
            season_start_year = prior_date_year
        else:
            season_start_year = prior_date_year - 1
            
        start = datetime(season_start_year, 11, 1, 0, 0, 0)
        return start, end
