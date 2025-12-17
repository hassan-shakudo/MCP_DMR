"""
Data processing utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pandas as pd
import pyodbc
from typing import Tuple, Dict
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
    
    columns = [column_info[0] for column_info in cursor.description]
    data = [tuple(row) for row in rows]  # Critical conversion
    return pd.DataFrame(data, columns=columns)


class DateRangeCalculator:
    """Calculate report date ranges based on a reference date"""
    
    def __init__(self, run_date: datetime = None, is_current_date: bool = False, use_exact_date: bool = False):
        """
        Initialize date range calculator
        
        Args:
            run_date: Reference date for calculations (defaults to now)
            is_current_date: If True, uses run_date as base (today) with current time as end.
                           If False and use_exact_date=False, uses run_date - 1 day (yesterday) as base.
                           If False and use_exact_date=True, uses run_date exactly as base.
            use_exact_date: If True, uses run_date exactly without subtracting 1 day (for past dates)
        """
        self.run_date = run_date or datetime.now()
        self.is_current_date = is_current_date
        self.current_time = datetime.now() if is_current_date else None
        
        if is_current_date:
            # For current date, use run_date (today) as base date for calculations
            # The actual end time will be current time, not end of day
            self.base_date = self.run_date
        elif use_exact_date:
            # Use the exact date provided (for past dates where we want that specific date)
            self.base_date = self.run_date.replace(hour=23, minute=59, second=59, microsecond=0)
        else:
            # Base date is usually "Yesterday" relative to run date
            self.base_date = self.run_date - timedelta(days=1)
            self.base_date = self.base_date.replace(hour=23, minute=59, second=59, microsecond=0)
        
    def get_all_ranges(self) -> Dict[str, Tuple[datetime, datetime]]:
        """Get all 9 required date ranges"""
        return {
            "For The Day (Actual)": self.for_the_day_actual(),
            "For The Day (Prior Year)": self.for_the_day_prior_year(),
            "For The Week Ending (Actual)": self.week_ending_actual(),
            "For The Week Ending (Prior Year)": self.week_ending_prior_year(),
            "Week Total (Prior Year)": self.week_total_prior_year(),
            "Month to Date (Actual)": self.month_to_date_actual(),
            "Month to Date (Prior Year)": self.month_to_date_prior_year(),
            "For Winter Ending (Actual)": self.winter_ending_actual(),
            "For Winter Ending (Prior Year)": self.winter_ending_prior_year()
        }

    def for_the_day_actual(self) -> Tuple[datetime, datetime]:
        """Yesterday start to end, or today start to current time if current date"""
        start = self.base_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if self.is_current_date and self.current_time:
            end = self.current_time  # Current time for today
        else:
            end = self.base_date  # End of day for yesterday
        return start, end

    def for_the_day_prior_year(self) -> Tuple[datetime, datetime]:
        """Same day of week last year"""
        # Go back 52 weeks to align day of week
        prior_date = self.base_date - timedelta(weeks=52)
        start = prior_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = prior_date
        return start, end

    def week_ending_actual(self) -> Tuple[datetime, datetime]:
        """Monday of current week to For The Day (or current time if current date)"""
        # Monday is weekday 0
        days_since_monday = self.base_date.weekday()
        start = (self.base_date - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        if self.is_current_date and self.current_time:
            end = self.current_time  # Current time for today
        else:
            end = self.base_date  # End of day for yesterday
        return start, end

    def week_ending_prior_year(self) -> Tuple[datetime, datetime]:
        """Monday of prior year week to For The Day Prior Year"""
        # Get the prior year "For The Day" (DOW aligned)
        prior_end_date = (self.base_date - timedelta(weeks=52))
        
        # Calculate Monday of that week
        days_since_monday = prior_end_date.weekday()
        start = (prior_end_date - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, prior_end_date

    def week_total_prior_year(self) -> Tuple[datetime, datetime]:
        """Monday 00:00:00 to Sunday 23:59:59 of prior year week"""
        # Get the prior year "For The Day" (DOW aligned)
        prior_date = (self.base_date - timedelta(weeks=52))
        
        # Calculate Monday of that week
        days_since_monday = prior_date.weekday()
        start = (prior_date - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate Sunday (end of week) - 6 days after Monday
        end = (start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
        
        return start, end

    def week_total_actual(self) -> Tuple[datetime, datetime]:
        """Monday 00:00:00 to Sunday 23:59:59 of current week"""
        # Monday is weekday 0
        days_since_monday = self.base_date.weekday()
        start = (self.base_date - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate Sunday (end of week) - 6 days after Monday
        end = (start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
        
        return start, end

    def month_to_date_actual(self) -> Tuple[datetime, datetime]:
        """First day of current month to For The Day (or current time if current date)"""
        # Get the first day of the current month
        start = self.base_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if self.is_current_date and self.current_time:
            end = self.current_time  # Current time for today
        else:
            end = self.base_date  # End of day for yesterday
        return start, end

    def month_to_date_prior_year(self) -> Tuple[datetime, datetime]:
        """First day of same month prior year to same date prior year"""
        # Calculate prior year date (same month, same day, previous year)
        prior_year = self.base_date.year - 1
        try:
            prior_end_date = self.base_date.replace(year=prior_year)
        except ValueError:  # Feb 29 on non-leap year
            prior_end_date = self.base_date.replace(year=prior_year, day=28)
        
        # First day of that month
        start = prior_end_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = prior_end_date
        return start, end

    def winter_ending_actual(self) -> Tuple[datetime, datetime]:
        """Nov 1 of current season to For The Day (or current time if current date)"""
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
        if self.is_current_date and self.current_time:
            end = self.current_time  # Current time for today
        else:
            end = self.base_date  # End of day for yesterday
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
