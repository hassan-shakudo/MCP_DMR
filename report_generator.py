"""
Report Generator for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
import pandas as pd
import xlsxwriter
from datetime import datetime, timedelta
from typing import Dict, Any, Union

from db_connection import DatabaseConnection
from stored_procedures import StoredProcedures
from data_utils import DateRangeCalculator
from config import CandidateColumns


class ReportGenerator:
    """Generate comprehensive ski resort reports"""
    
    def __init__(self, output_dir: str = "reports"):
        """
        Initialize report generator
        
        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✓ Created output directory: {output_dir}")
    
    def _sanitize_filename(self, name: str) -> str:
        """Sanitize a string to be used as a filename"""
        # Replace invalid characters with underscores
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        # Remove leading/trailing spaces and dots
        name = name.strip('. ')
        return name
    
    def _export_sp_result(self, 
                         dataframe: pd.DataFrame, 
                         range_name: str, 
                         sp_name: str, 
                         resort_name: str) -> str:
        """
        Export a stored procedure result to an Excel file
        
        Args:
            dataframe: DataFrame to export
            range_name: Name of the date range (e.g., "For The Day (Actual)")
            sp_name: Name of the stored procedure (e.g., "Revenue", "Payroll")
            resort_name: Name of the resort
            
        Returns:
            Path to saved Excel file
        """
        # Sanitize range name and SP name for filename
        sanitized_range = self._sanitize_filename(range_name)
        sanitized_sp = self._sanitize_filename(sp_name)
        
        # Create filename: RangeName_SPname.xlsx
        filename = f"{sanitized_range}_{sanitized_sp}.xlsx"
        filepath = os.path.join(self.output_dir, filename)
        
        # Sort by department/department code for Revenue and Payroll
        if sp_name in ['Revenue', 'Payroll']:
            # Find department column (case-insensitive search)
            dept_col = None
            dataframe_columns_lower = [col.lower() for col in dataframe.columns]
            
            for candidate in CandidateColumns.department:
                # Try exact match first
                if candidate in dataframe.columns:
                    dept_col = candidate
                    break
                # Try case-insensitive match
                candidate_lower = candidate.lower()
                for idx, col_lower in enumerate(dataframe_columns_lower):
                    if col_lower == candidate_lower:
                        dept_col = dataframe.columns[idx]
                        break
                if dept_col:
                    break
            
            # If still no department column found, try department title as fallback
            if not dept_col:
                for candidate in CandidateColumns.departmentTitle:
                    if candidate in dataframe.columns:
                        dept_col = candidate
                        break
                    # Try case-insensitive match
                    candidate_lower = candidate.lower()
                    for idx, col_lower in enumerate(dataframe_columns_lower):
                        if col_lower == candidate_lower:
                            dept_col = dataframe.columns[idx]
                            break
                    if dept_col:
                        break
            
            if dept_col:
                # Sort by department code/title (convert to string for consistent sorting)
                dataframe_sorted = dataframe.copy()
                dataframe_sorted['_sort_key'] = dataframe_sorted[dept_col].astype(str).str.strip()
                dataframe_sorted = dataframe_sorted.sort_values(by='_sort_key', na_position='last')
                dataframe_sorted = dataframe_sorted.drop(columns=['_sort_key'])
            else:
                dataframe_sorted = dataframe
        else:
            dataframe_sorted = dataframe
        
        # Write to Excel using xlsxwriter
        workbook = xlsxwriter.Workbook(filepath)
        worksheet = workbook.add_worksheet('Data')
        
        # Write header
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        for col_idx, col_name in enumerate(dataframe_sorted.columns):
            worksheet.write(0, col_idx, col_name, header_format)
        
        # Write data
        data_format = workbook.add_format({'border': 1})
        for row_idx, (_, row) in enumerate(dataframe_sorted.iterrows(), start=1):
            for col_idx, value in enumerate(row):
                # Handle NaT (Not a Time) values - convert to None/empty string
                if pd.isna(value):
                    worksheet.write(row_idx, col_idx, None, data_format)
                else:
                    worksheet.write(row_idx, col_idx, value, data_format)
        
        # Auto-adjust column widths
        for col_idx, col_name in enumerate(dataframe_sorted.columns):
            # Get max width of column
            max_width = len(str(col_name))
            for _, row in dataframe_sorted.iterrows():
                max_width = max(max_width, len(str(row[col_name])))
            worksheet.set_column(col_idx, col_idx, min(max_width + 2, 50))
        
        workbook.close()
        
        return filepath
            
    def generate_comprehensive_report(self, 
                                    resort_config: Dict[str, Any], 
                                    run_date: Union[str, datetime, None] = None,
                                    debug: Any = False) -> str:
        """
        Generate the comprehensive Excel report for a resort.
        
        Args:
            resort_config: Dictionary containing resort details (dbName, resortName, groupNum)
            run_date: Date the report is being run. Can be:
                     - None: Uses current date and generates report for previous day
                     - String in MM/DD/YYYY format: Parses and generates report for that date (start to end of day)
                     - datetime object: Uses the provided datetime
                     If the date is current date (or None), generates report for today 
                     (start of day to current time) and skips payroll.
                     If past date, generates report for that day (start to end of day) normally.
            debug: Debug mode - False (no debug), 'simple' (top 5 rows), or 'verbose' (complete datasets)
            
        Returns:
            Path to saved Excel file
        """
        current_date = datetime.now()
        
        # Parse run_date if it's a string (MM/DD/YYYY format)
        if run_date is None:
            # No date provided: use current date (will be treated as current date)
            run_date = current_date
            is_current_date = True
        elif isinstance(run_date, str):
            try:
                # Parse MM/DD/YYYY format
                run_date = datetime.strptime(run_date, "%m/%d/%Y")
            except ValueError:
                raise ValueError(f"Invalid date format. Expected MM/DD/YYYY, got: {run_date}")
            # Check if parsed date is current date
            is_current_date = (run_date.date() == current_date.date())
        else:
            # datetime object provided
            is_current_date = (run_date.date() == current_date.date())
        
        # If current date, treat same as no date case (use current date logic)
        if is_current_date:
            run_date = current_date
            is_current_date = True
            
        resort_name = resort_config.get('resortName', 'Unknown')
        db_name = resort_config.get('dbName', resort_name)
        group_num = resort_config.get('groupNum', -1)
        
        print(f"\n📊 Generating Comprehensive Report for {resort_name}...")
        if is_current_date:
            print(f"📅 Report Date: {run_date.strftime('%Y-%m-%d')} (current date - start of day to now)")
            print(f"   ℹ️  Generating report for today - payroll will be set to 0")
        else:
            print(f"📅 Report Date: {run_date.strftime('%Y-%m-%d')} (start to end of day)")
            print(f"   ℹ️  Generating report for {run_date.strftime('%B %d, %Y')}")

        # 1. Calculate Date Ranges
        # For past dates, use exact date (use_exact_date=True)
        # For current date, use is_current_date=True
        use_exact_date = not is_current_date
        date_calc = DateRangeCalculator(run_date, is_current_date=is_current_date, use_exact_date=use_exact_date)
        ranges = date_calc.get_all_ranges()
        range_names = [
            "For The Day (Actual)",
            "For The Day (Prior Year)",
            "For The Week Ending (Actual)", 
            "For The Week Ending (Prior Year)",
            "Week Total (Prior Year)",
            "Month to Date (Actual)", 
            "Month to Date (Prior Year)",
            "For Winter Ending (Actual)", 
            "For Winter Ending (Prior Year)"
        ]
        
        # 2. Fetch Data for all ranges
        data_store = {name: {} for name in range_names}
        
        # Fetch salary payroll data once (rate_per_day per department)
        salary_payroll_data = None
        
        with DatabaseConnection() as conn:
            stored_procedures = StoredProcedures(conn)
            
            # Fetch salary payroll once per resort
            # Skip if current date (payroll will be set to 0)
            if not is_current_date:
                print(f"   ⏳ Fetching salary payroll data for {resort_name}...")
                salary_payroll_data = stored_procedures.execute_payroll_salary(resort_name)
                # Export Salary Payroll SP result (not range-specific, so use a generic range name)
                if not salary_payroll_data.empty:
                    export_path = self._export_sp_result(salary_payroll_data, "SalaryPayroll", "PayrollSalary", resort_name)
                    print(f"      💾 Exported Salary Payroll data: {os.path.basename(export_path)}")
                if debug == 'verbose':
                    print(f"      [DEBUG VERBOSE] Salary payroll data (complete):")
                    print(f"      {salary_payroll_data}")
                elif debug == 'simple':
                    print(f"      [DEBUG SIMPLE] Salary payroll data (top 5 rows):")
                    print(f"      {salary_payroll_data.head(5) if not salary_payroll_data.empty else 'Empty DataFrame'}")
            else:
                print(f"   ⏭️  Skipping salary payroll data fetch (current date - payroll will be 0)")
                salary_payroll_data = None
            
            for range_name in range_names:
                start, end = ranges[range_name]
                print(f"   ⏳ Fetching data for {range_name} ({start.date()} - {end.date()})...")
                
                # Revenue
                revenue_dataframe = stored_procedures.execute_revenue(db_name, group_num, start, end)
                data_store[range_name]['revenue'] = revenue_dataframe
                # Export Revenue SP result
                if not revenue_dataframe.empty:
                    export_path = self._export_sp_result(revenue_dataframe, range_name, "Revenue", resort_name)
                    print(f"      💾 Exported Revenue data: {os.path.basename(export_path)}")
                if debug == 'verbose':
                    print(f"      [DEBUG VERBOSE] Revenue data for {range_name} (complete):")
                    print(f"      {revenue_dataframe}")
                elif debug == 'simple':
                    print(f"      [DEBUG SIMPLE] Revenue data for {range_name} (top 5 rows):")
                    print(f"      {revenue_dataframe.head(5) if not revenue_dataframe.empty else 'Empty DataFrame'}")
                
                # Payroll - skip if current date
                if not is_current_date:
                    # For Actual ranges: fetch payroll for the whole range (no splitting)
                    if range_name in ["For The Day (Actual)", "For The Week Ending (Actual)", 
                                     "Month to Date (Actual)", "For Winter Ending (Actual)"]:
                        payroll_dataframe = stored_procedures.execute_payroll(resort_name, start, end)
                        data_store[range_name]['payroll'] = payroll_dataframe
                        
                        # Export Payroll SP result
                        if not payroll_dataframe.empty:
                            export_path = self._export_sp_result(payroll_dataframe, range_name, "Payroll", resort_name)
                            print(f"      💾 Exported Payroll data: {os.path.basename(export_path)}")
                        if debug == 'verbose':
                            print(f"      [DEBUG VERBOSE] Payroll data for {range_name} (complete):")
                            print(f"      {payroll_dataframe}")
                        elif debug == 'simple':
                            print(f"      [DEBUG SIMPLE] Payroll data for {range_name} (top 5 rows):")
                            print(f"      {payroll_dataframe.head(5) if not payroll_dataframe.empty else 'Empty DataFrame'}")
                    else:
                        # Prior Year ranges - no payroll data needed (will use history only)
                        data_store[range_name]['payroll'] = pd.DataFrame()
                        if debug in ['simple', 'verbose']:
                            print(f"      [DEBUG] Skipping payroll fetch for {range_name} (Prior Year - using history only)")
                else:
                    # Set empty DataFrame for payroll when current date
                    data_store[range_name]['payroll'] = pd.DataFrame()
                    if debug in ['simple', 'verbose']:
                        print(f"      [DEBUG] Skipping payroll fetch for {range_name} (current date - payroll will be 0)")
                
                # Visits
                visits_dataframe = stored_procedures.execute_visits(resort_name, start, end)
                data_store[range_name]['visits'] = visits_dataframe
                # Export Visits SP result
                if not visits_dataframe.empty:
                    export_path = self._export_sp_result(visits_dataframe, range_name, "Visits", resort_name)
                    print(f"      💾 Exported Visits data: {os.path.basename(export_path)}")
                if debug == 'verbose':
                    print(f"      [DEBUG VERBOSE] Visits data for {range_name} (complete):")
                    print(f"      {visits_dataframe}")
                elif debug == 'simple':
                    print(f"      [DEBUG SIMPLE] Visits data for {range_name} (top 5 rows):")
                    print(f"      {visits_dataframe.head(5) if not visits_dataframe.empty else 'Empty DataFrame'}")
                
                # Weather/Snow
                snow_dataframe = stored_procedures.execute_weather(resort_name, start, end)
                data_store[range_name]['snow'] = snow_dataframe
                # Export Weather/Snow SP result
                if not snow_dataframe.empty:
                    export_path = self._export_sp_result(snow_dataframe, range_name, "Weather", resort_name)
                    print(f"      💾 Exported Weather data: {os.path.basename(export_path)}")
                if debug == 'verbose':
                    print(f"      [DEBUG VERBOSE] Snow data for {range_name} (complete):")
                    print(f"      {snow_dataframe}")
                elif debug == 'simple':
                    print(f"      [DEBUG SIMPLE] Snow data for {range_name} (top 5 rows):")
                    print(f"      {snow_dataframe.head(5) if not snow_dataframe.empty else 'Empty DataFrame'}")
                
                # Payroll History - only for Prior Year ranges, skip if current date
                if not is_current_date:
                    # Only fetch history for Prior Year ranges
                    if range_name not in ["For The Day (Actual)", "For The Week Ending (Actual)", 
                                         "Month to Date (Actual)", "For Winter Ending (Actual)"]:
                        # Prior Year ranges - fetch history for full range
                        history_payroll_dataframe = stored_procedures.execute_payroll_history(resort_name, start, end)
                        data_store[range_name]['payroll_history'] = history_payroll_dataframe
                        
                        if not history_payroll_dataframe.empty:
                            export_path = self._export_sp_result(history_payroll_dataframe, range_name, "PayrollHistory", resort_name)
                            print(f"      💾 Exported Payroll History data: {os.path.basename(export_path)}")
                        if debug == 'verbose':
                            print(f"      [DEBUG VERBOSE] Payroll history data for {range_name} ({start.date()} - {end.date()}) (complete):")
                            print(f"      {history_payroll_dataframe}")
                        elif debug == 'simple':
                            print(f"      [DEBUG SIMPLE] Payroll history data for {range_name} ({start.date()} - {end.date()}) (top 5 rows):")
                            print(f"      {history_payroll_dataframe.head(5) if not history_payroll_dataframe.empty else 'Empty DataFrame'}")
                    else:
                        # Actual ranges - no history needed
                        data_store[range_name]['payroll_history'] = pd.DataFrame()
                        if debug in ['simple', 'verbose']:
                            print(f"      [DEBUG] Skipping payroll history for {range_name} (Actual range - using payroll + salary)")
                else:
                    # No history needed for current date
                    data_store[range_name]['payroll_history'] = pd.DataFrame()
                    if debug in ['simple', 'verbose']:
                        print(f"      [DEBUG] Skipping payroll history for {range_name} (current date - payroll will be 0)")

        # 3. Process Data and Collect Row Headers
        all_locations = set()
        all_departments = set()
        department_code_to_title = {}  # Map department codes to titles
        
        # Processed data structure: category -> range -> key -> value
        processed_snow = {range_name: {'snow_24hrs': 0.0, 'base_depth': 0.0} for range_name in range_names}
        processed_visits = {range_name: {} for range_name in range_names} # location -> sum
        processed_revenue = {range_name: {} for range_name in range_names} # department -> sum
        processed_payroll = {range_name: {} for range_name in range_names} # department -> sum
        
        # Helper to guess column names if they vary
        def get_col(dataframe, candidates):
            for candidate_column in candidates:
                if candidate_column in dataframe.columns:
                    return candidate_column
            return None
        
        # Helper to safely convert numeric values (handles Decimal, None, etc.)
        def normalize_value(value):
            if value is None:
                return 0.0
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0
        
        # Helper to trim and normalize department codes for matching
        def trim_dept_code(code):
            """Trim whitespace from department code for consistent matching"""
            if code is None:
                return ""
            return str(code).strip()
        
        # Process salary payroll data into a dictionary: deptcode -> rate_per_day
        salary_payroll_rates = {}
        if salary_payroll_data is not None and not salary_payroll_data.empty:
            deptcode_column = get_col(salary_payroll_data, CandidateColumns.salaryDeptcode)
            rate_column = get_col(salary_payroll_data, CandidateColumns.salaryRatePerDay)
            title_column = get_col(salary_payroll_data, CandidateColumns.departmentTitle)
            
            if deptcode_column and rate_column:
                for _, row in salary_payroll_data.iterrows():
                    dept_code = trim_dept_code(row[deptcode_column])
                    rate_per_day = normalize_value(row[rate_column])
                    
                    if dept_code:
                        salary_payroll_rates[dept_code] = rate_per_day
                        
                        # Also update department_code_to_title if available
                        if title_column and title_column in row:
                            title = str(row[title_column]).strip() if pd.notna(row[title_column]) else ""
                            if dept_code:
                                if dept_code not in department_code_to_title:
                                    if title:
                                        department_code_to_title[dept_code] = title
                                    else:
                                        # Warning: Empty/null title found
                                        print(f"    ⚠️  [WARN] Empty/null title for department code '{dept_code}' in salary payroll data")
                                        print(f"       Salary payroll row: {row.to_dict()}")
                                elif not title:
                                    # Warning: Title exists in mapping but current row has empty title
                                    print(f"    ⚠️  [WARN] Empty/null title for department code '{dept_code}' in salary payroll data (mapping already exists)")
                                    print(f"       Salary payroll row: {row.to_dict()}")
                
                if debug in ['simple', 'verbose']:
                    print(f"      [DEBUG] Salary payroll rates: {salary_payroll_rates}")
                    print(f"      [DEBUG] Department code to title mapping (from salary payroll): {department_code_to_title}")
        
        # Helper function to calculate days in a date range
        def calculate_days_in_range(start_date: datetime, end_date: datetime) -> int:
            """Calculate the number of days in a date range (inclusive)"""
            if start_date > end_date:
                return 0
            delta = end_date - start_date
            # Add 1 to make it inclusive (e.g., same day = 1 day)
            return delta.days + 1

        for range_name in range_names:
            # --- Snow ---
            snow_dataframe = data_store[range_name]['snow']
            if not snow_dataframe.empty:
                # Sum snow_24hrs
                snow_column = get_col(snow_dataframe, CandidateColumns.snow)
                base_column = get_col(snow_dataframe, CandidateColumns.baseDepth)
                
                if snow_column:
                    processed_snow[range_name]['snow_24hrs'] = normalize_value(snow_dataframe[snow_column].sum())
                if base_column:
                    processed_snow[range_name]['base_depth'] = normalize_value(snow_dataframe[base_column].sum()) # Instruction: "sum up"

            # --- Visits ---
            visits_dataframe = data_store[range_name]['visits']
            if not visits_dataframe.empty:
                location_column = get_col(visits_dataframe, CandidateColumns.location)
                value_column = get_col(visits_dataframe, CandidateColumns.visits)
                
                # If no explicit value column, maybe count rows? 
                # User said "sum up the visits". 
                # If DataFrame has one row per visit, we count. If it has aggregated 'Visits' col, we sum.
                # Assuming 'Visits' column exists or we sum rows if no obvious numeric column found?
                # Let's look for numeric columns.
                if not value_column:
                    # Fallback: look for any numeric column that isn't an ID
                    numeric_columns = visits_dataframe.select_dtypes(include=['number']).columns
                    if len(numeric_columns) > 0:
                        value_column = numeric_columns[-1] # Pick last numeric? risky.
                
                if location_column:
                    # Group and sum
                    if value_column:
                        grouped = visits_dataframe.groupby(location_column)[value_column].sum()
                    else:
                        # Count rows per location
                        grouped = visits_dataframe.groupby(location_column).size()
                        
                    for location, value in grouped.items():
                        location_string = str(location)
                        processed_visits[range_name][location_string] = normalize_value(value)
                        all_locations.add(location_string)

            # --- Revenue ---
            revenue_dataframe = data_store[range_name]['revenue']
            if not revenue_dataframe.empty:
                # Find department code and title columns
                department_code_column = get_col(revenue_dataframe, CandidateColumns.department) or 'department'
                department_title_column = get_col(revenue_dataframe, CandidateColumns.departmentTitle) or 'DepartmentTitle'
                revenue_column = get_col(revenue_dataframe, CandidateColumns.revenue) or 'revenue'
                
                # Find likely revenue column if not explicit
                if not revenue_column:
                     numeric_columns = revenue_dataframe.select_dtypes(include=['number']).columns
                     # Usually the last numeric column is the amount
                     if len(numeric_columns) > 0:
                         revenue_column = numeric_columns[-1]

                if department_code_column and revenue_column:
                    # Build mapping from code to title (with whitespace trimming)
                    if department_title_column and department_title_column != department_code_column:
                        for _, row in revenue_dataframe.iterrows():
                            code = trim_dept_code(row[department_code_column])
                            title = str(row[department_title_column]).strip() if pd.notna(row[department_title_column]) else ""
                            if code:
                                if code not in department_code_to_title:
                                    if title:
                                        department_code_to_title[code] = title
                                    else:
                                        # Warning: Empty/null title found
                                        print(f"    ⚠️  [WARN] Empty/null title for department code '{code}' in revenue data")
                                        print(f"       Revenue row: {row.to_dict()}")
                                elif not title:
                                    # Warning: Title exists in mapping but current row has empty title
                                    print(f"    ⚠️  [WARN] Empty/null title for department code '{code}' in revenue data (mapping already exists)")
                                    print(f"       Revenue row: {row.to_dict()}")
                    if debug in ['simple', 'verbose']:
                        print(f'    [DEBUG] Department code to title mapping (after revenue processing for {range_name}): {department_code_to_title}')
                    grouped = revenue_dataframe.groupby(department_code_column)[revenue_column].sum()
                    for department, value in grouped.items():
                        department_string = trim_dept_code(department)
                        processed_revenue[range_name][department_string] = normalize_value(value)
                        all_departments.add(department_string)
                        # If no title mapping yet, use the code as title
                        if department_string and department_string not in department_code_to_title:
                            # Warning: Fallback triggered - find matching rows
                            print(f"    ⚠️  [WARN] FALLBACK: No title found for department code '{department_string}' - using code as title")
                            
                            # Find matching rows in revenue dataframe
                            revenue_matches = revenue_dataframe[
                                revenue_dataframe[department_code_column].apply(lambda x: trim_dept_code(x) == department_string)
                            ]
                            if not revenue_matches.empty:
                                print(f"       Matching revenue rows ({len(revenue_matches)}):")
                                for idx, match_row in revenue_matches.head(3).iterrows():
                                    print(f"         Row {idx}: {match_row.to_dict()}")
                                if len(revenue_matches) > 3:
                                    print(f"         ... and {len(revenue_matches) - 3} more rows")
                            
                            # Find matching rows in payroll dataframe (if available)
                            if not payroll_dataframe.empty:
                                payroll_dept_col = get_col(payroll_dataframe, CandidateColumns.department)
                                if payroll_dept_col:
                                    payroll_matches = payroll_dataframe[
                                        payroll_dataframe[payroll_dept_col].apply(lambda x: trim_dept_code(x) == department_string)
                                    ]
                                    if not payroll_matches.empty:
                                        print(f"       Matching payroll rows ({len(payroll_matches)}):")
                                        for idx, match_row in payroll_matches.head(3).iterrows():
                                            print(f"         Row {idx}: {match_row.to_dict()}")
                                        if len(payroll_matches) > 3:
                                            print(f"         ... and {len(payroll_matches) - 3} more rows")
                            
                            department_code_to_title[department_string] = department_string

            # --- Payroll ---
            # Initialize tracking variables
            calculated_payroll = {}  # department -> calculated wages
            contract_payroll_rows = {}  # department -> list of employee rows
            salary_totals_by_dept = {}  # dept_code -> salary_total_for_range
            history_payroll = {}  # department -> total from history
            
            # Get date range info (needed for logging)
            start_date, end_date = ranges[range_name]
            days_in_range = calculate_days_in_range(start_date, end_date)
            
            # Skip payroll processing if current date (payroll will be set to 0)
            if is_current_date:
                # Set all payroll values to 0 for all departments found in revenue
                for dept_code in processed_revenue[range_name].keys():
                    processed_payroll[range_name][dept_code] = 0.0
                    all_departments.add(dept_code)
                    # Initialize tracking for logging
                    calculated_payroll[dept_code] = 0.0
                    salary_totals_by_dept[dept_code] = 0.0
                    history_payroll[dept_code] = 0.0
            else:
                # Step 1: Calculate regular payroll (contract-based employees)
                payroll_dataframe = data_store[range_name]['payroll']
                
                # Store column names for payroll processing
                start_column = None
                end_column = None
                rate_column = None
                department_column = None
                valid_rows = pd.DataFrame()
                
                if not payroll_dataframe.empty:
                    # Need columns: Department, start_punchtime, end_punchtime, rate, hours, dollaramount
                    department_column = get_col(payroll_dataframe, CandidateColumns.department) or 'department'
                    department_title_column = get_col(payroll_dataframe, CandidateColumns.departmentTitle)
                    start_column = get_col(payroll_dataframe, CandidateColumns.payrollStartTime)
                    end_column = get_col(payroll_dataframe, CandidateColumns.payrollEndTime)
                    rate_column = get_col(payroll_dataframe, CandidateColumns.payrollRate)
                    hours_column = get_col(payroll_dataframe, CandidateColumns.payrollHours)
                    dollaramount_column = get_col(payroll_dataframe, CandidateColumns.payrollDollarAmount)
                    
                    if department_column and start_column and end_column and rate_column:
                        # Build mapping from code to title (with whitespace trimming)
                        # Check if payroll has a title column and build mapping from it
                        if department_title_column and department_title_column != department_column:
                            for _, row in payroll_dataframe.iterrows():
                                code = trim_dept_code(row[department_column])
                                title = str(row[department_title_column]).strip() if pd.notna(row[department_title_column]) else ""
                                if code:
                                    if code not in department_code_to_title:
                                        if title:
                                            department_code_to_title[code] = title
                                        else:
                                            # Warning: Empty/null title found
                                            print(f"    ⚠️  [WARN] Empty/null title for department code '{code}' in payroll data")
                                            print(f"       Payroll row: {row.to_dict()}")
                                    elif not title:
                                        # Warning: Title exists in mapping but current row has empty title
                                        print(f"    ⚠️  [WARN] Empty/null title for department code '{code}' in payroll data (mapping already exists)")
                                        print(f"       Payroll row: {row.to_dict()}")
                        
                        # Ensure datetime and numeric types
                        payroll_dataframe[start_column] = pd.to_datetime(payroll_dataframe[start_column], errors='coerce')
                        payroll_dataframe[end_column] = pd.to_datetime(payroll_dataframe[end_column], errors='coerce')
                        payroll_dataframe[rate_column] = pd.to_numeric(payroll_dataframe[rate_column], errors='coerce').fillna(0)
                        
                        # Convert hours and dollaramount columns if they exist
                        if hours_column:
                            payroll_dataframe[hours_column] = pd.to_numeric(payroll_dataframe[hours_column], errors='coerce').fillna(0)
                        if dollaramount_column:
                            payroll_dataframe[dollaramount_column] = pd.to_numeric(payroll_dataframe[dollaramount_column], errors='coerce').fillna(0)
                        
                        # Process all rows (we'll handle nulls in the calculation)
                        for _, row in payroll_dataframe.iterrows():
                            start_time = row[start_column]
                            end_time = row[end_column]
                            rate = normalize_value(row[rate_column])
                            department = trim_dept_code(row[department_column])
                            
                            # Skip if department is missing
                            if not department:
                                continue
                                
                            all_departments.add(department) # Add to departments if not in revenue
                            
                            # If no title mapping yet, use the code as title
                            if department and department not in department_code_to_title:
                                # Warning: Fallback triggered - find matching rows
                                print(f"    ⚠️  [WARN] FALLBACK: No title found for department code '{department}' - using code as title")
                                
                                # Find matching rows in payroll dataframe
                                payroll_matches = payroll_dataframe[
                                    payroll_dataframe[department_column].apply(lambda x: trim_dept_code(x) == department)
                                ]
                                if not payroll_matches.empty:
                                    print(f"       Matching payroll rows ({len(payroll_matches)}):")
                                    for idx, match_row in payroll_matches.head(3).iterrows():
                                        print(f"         Row {idx}: {match_row.to_dict()}")
                                    if len(payroll_matches) > 3:
                                        print(f"         ... and {len(payroll_matches) - 3} more rows")
                                
                                # Find matching rows in revenue dataframe (if available)
                                if not revenue_dataframe.empty:
                                    revenue_dept_col = get_col(revenue_dataframe, CandidateColumns.department)
                                    if revenue_dept_col:
                                        revenue_matches = revenue_dataframe[
                                            revenue_dataframe[revenue_dept_col].apply(lambda x: trim_dept_code(x) == department)
                                        ]
                                        if not revenue_matches.empty:
                                            print(f"       Matching revenue rows ({len(revenue_matches)}):")
                                            for idx, match_row in revenue_matches.head(3).iterrows():
                                                print(f"         Row {idx}: {match_row.to_dict()}")
                                            if len(revenue_matches) > 3:
                                                print(f"         ... and {len(revenue_matches) - 3} more rows")
                                
                                department_code_to_title[department] = department
                            
                            # Calculate working_hours from punch in/out (or 0 if any is null)
                            if pd.notna(start_time) and pd.notna(end_time):
                                working_hours = (end_time - start_time).total_seconds() / 3600.0
                                if working_hours < 0:
                                    working_hours = 0
                            else:
                                working_hours = 0
                            
                            # Get hours and dollaramount from columns (default to 0 if not present or null)
                            hours_value = normalize_value(row[hours_column]) if hours_column and hours_column in payroll_dataframe.columns else 0
                            dollaramount_value = normalize_value(row[dollaramount_column]) if dollaramount_column and dollaramount_column in payroll_dataframe.columns else 0
                            
                            # Calculate wages using new formula:
                            # if hours > 0: wage = (hours × rate) + dollaramount
                            # else: wage = (working_hours × rate) + dollaramount
                            if hours_value > 0:
                                wages = (hours_value * rate) + dollaramount_value
                            else:
                                wages = (working_hours * rate) + dollaramount_value
                            
                            # Track employee row details for logging
                            if department not in contract_payroll_rows:
                                contract_payroll_rows[department] = []
                            contract_payroll_rows[department].append({
                                'start_time': start_time,
                                'end_time': end_time,
                                'working_hours': working_hours,
                                'hours_column': hours_value,
                                'rate': rate,
                                'dollaramount': dollaramount_value,
                                'wages': wages
                            })
                            
                            current_wages = normalize_value(calculated_payroll.get(department, 0))
                            calculated_payroll[department] = current_wages + normalize_value(wages)
                
                # Step 2: Process history payroll data
                history_payroll_dataframe = data_store[range_name]['payroll_history']
                if history_payroll_dataframe is not None and not history_payroll_dataframe.empty:
                    history_dept_column = get_col(history_payroll_dataframe, CandidateColumns.historyDepartment) or 'department'
                    history_total_column = get_col(history_payroll_dataframe, CandidateColumns.historyTotal)
                    
                    if history_dept_column and history_total_column:
                        for _, row in history_payroll_dataframe.iterrows():
                            dept_code = trim_dept_code(row[history_dept_column])
                            total = normalize_value(row[history_total_column])
                            if dept_code:
                                history_payroll[dept_code] = total
                
                # Step 3: Apply simplified payroll logic based on range type
                if range_name in ["For The Day (Actual)", "For The Week Ending (Actual)", 
                                 "Month to Date (Actual)", "For Winter Ending (Actual)"]:
                    # For all Actual ranges: (Salary × number of days) + Payroll data for that whole range
                    for dept_code, calculated_wages in calculated_payroll.items():
                        salary_rate = salary_payroll_rates.get(dept_code, 0)
                        salary_total = normalize_value(salary_rate) * days_in_range
                        salary_totals_by_dept[dept_code] = salary_total
                        total_payroll = normalize_value(calculated_wages) + salary_total
                        processed_payroll[range_name][dept_code] = total_payroll
                    
                    # Add departments that only have salary payroll
                    for dept_code, salary_rate in salary_payroll_rates.items():
                        if dept_code not in processed_payroll[range_name]:
                            salary_total = normalize_value(salary_rate) * days_in_range
                            salary_totals_by_dept[dept_code] = salary_total
                            processed_payroll[range_name][dept_code] = salary_total
                            all_departments.add(dept_code)
                
                else:
                    # All Prior Year ranges: ONLY use history payroll
                    # Prior Year ranges don't use salary payroll or contract payroll
                    for dept_code, history_total in history_payroll.items():
                        salary_totals_by_dept[dept_code] = 0.0  # No salary for prior year
                        # Only use history payroll, ignore calculated_payroll (contract employees)
                        processed_payroll[range_name][dept_code] = normalize_value(history_total)
                        all_departments.add(dept_code)
            
            # Step 5: Log detailed payroll breakdown for each department (always execute)
            print(f"\n{'='*80}")
            print(f"  📊 PAYROLL CALCULATION BREAKDOWN - {range_name}")
            if is_current_date:
                print(f"  ⚠️  NOTE: Current date - payroll set to 0 for all departments")
            
            print(f"{'='*80}")
            
            # Get all departments that have payroll data
            all_payroll_depts = set(processed_payroll[range_name].keys())
            
            # Check if this is a Prior Year range
            is_prior_year = range_name not in ["For The Day (Actual)", "For The Week Ending (Actual)", 
                                               "Month to Date (Actual)", "For Winter Ending (Actual)"]
            
            if not all_payroll_depts:
                print(f"    No payroll data found for this range.")
            else:
                for dept_code in sorted(all_payroll_depts):
                    dept_title = department_code_to_title.get(dept_code, dept_code)
                    print(f"\n  📁 Department: {dept_code} ({dept_title})")
                    print(f"     {'─'*76}")
                    
                    # Contract Payroll Details
                    contract_rows = contract_payroll_rows.get(dept_code, [])
                    contract_total = normalize_value(calculated_payroll.get(dept_code, 0))
                    
                    print(f"     📋 Contract Payroll (Hourly Employees):")
                    if is_prior_year:
                        print(f"        • Prior Year Range - Contract Payroll NOT USED (ignored)")
                        print(f"        • Only History Payroll is used for Prior Year ranges")
                    elif contract_rows:
                        print(f"        • Employee rows received: {len(contract_rows)}")
                        for idx, row_data in enumerate(contract_rows, 1):
                            working_hours = row_data.get('working_hours', 0)
                            hours_column = row_data.get('hours_column', 0)
                            dollaramount = row_data.get('dollaramount', 0)
                            print(f"          Row {idx}: Start={row_data['start_time']}, End={row_data['end_time']}, "
                                  f"WorkingHours={working_hours:.2f}, HoursColumn={hours_column:.2f}, "
                                  f"Rate=${row_data['rate']:.2f}, DollarAmount=${dollaramount:,.2f}, "
                                  f"Wages=${row_data['wages']:.2f}")
                        print(f"        • Aggregated Contract Payroll Total: ${contract_total:,.2f}")
                    else:
                        print(f"        • No contract payroll rows found")
                        print(f"        • Aggregated Contract Payroll Total: $0.00")
                    
                    # Salary Payroll Details
                    salary_rate = salary_payroll_rates.get(dept_code, 0)
                    salary_total = salary_totals_by_dept.get(dept_code, 0)
                    
                    print(f"\n     💰 Salary Payroll:")
                    print(f"        • Daily Salary Rate: ${salary_rate:,.2f}")
                    
                    # Show salary total based on range type
                    if is_current_date:
                        print(f"        • Salary for Range: $0.00 (Current date - not calculated)")
                    elif is_prior_year:
                        # Prior Year ranges don't use salary payroll
                        print(f"        • Salary for Range: $0.00 (Prior Year - not applicable)")
                    else:
                        print(f"        • Salary for Range ({days_in_range} days): ${salary_total:,.2f}")
                    
                    # History Payroll Details
                    history_total = normalize_value(history_payroll.get(dept_code, 0))
                    print(f"\n     📜 History Payroll:")
                    if is_prior_year:
                        if history_total > 0:
                            print(f"        • Historical Payroll Total: ${history_total:,.2f}")
                        else:
                            print(f"        • No history payroll data found")
                    else:
                        print(f"        • Not used for Actual ranges")
                    
                    # Final Total
                    final_total = normalize_value(processed_payroll[range_name].get(dept_code, 0))
                    print(f"\n     ✅ FINAL PAYROLL TOTAL: ${final_total:,.2f}")
                    
                    # Show breakdown based on range type
                    if is_prior_year:
                        print(f"        Breakdown: History Only (${history_total:,.2f}) - Prior Year ranges use only history payroll")
                    else:
                        print(f"        Breakdown: Contract Payroll (${contract_total:,.2f}) + (Salary Rate × {days_in_range} days = ${salary_total:,.2f}) = ${final_total:,.2f}")
            
            print(f"\n{'='*80}\n")

        # Debug: Print final department code to title mapping (shown in both simple and verbose modes)
        if debug in ['simple', 'verbose']:
            print(f"\n{'='*70}")
            print(f"  [DEBUG] Final Department Code to Title Mapping")
            print(f"{'='*70}")
            if department_code_to_title:
                for dept_code, dept_title in sorted(department_code_to_title.items()):
                    print(f"    {dept_code} -> {dept_title}")
            else:
                print("    (No mappings found)")
            print(f"{'='*70}\n")

        # 4. Write to Excel
        filename = f"{resort_name}_Report_{self.timestamp}.xlsx"
        filepath = os.path.join(self.output_dir, filename)
        
        workbook = xlsxwriter.Workbook(filepath)
        worksheet = workbook.add_worksheet("Report")
        
        # Formats
        header_fmt = workbook.add_format({'bold': True, 'align': 'center', 'bg_color': '#D3D3D3', 'border': 1, 'text_wrap': True})
        row_header_fmt = workbook.add_format({'bold': True, 'border': 1})
        data_fmt = workbook.add_format({'border': 1, 'num_format': '#,##0.00'})
        snow_fmt = workbook.add_format({'border': 1, 'num_format': '0.0'})
        percent_fmt = workbook.add_format({'border': 1, 'num_format': '0"%"'})
        
        # Create top-left cell with resort info and "For the day Actual" date
        day_actual_start, day_actual_end = ranges["For The Day (Actual)"]
        day_name = day_actual_start.strftime('%A')  # e.g., "Wednesday"
        day_date = day_actual_start.strftime('%d %B, %Y')  # e.g., "19 November, 2025"
        
        # Format: remove leading zero from day if present
        if day_date.startswith('0'):
            day_date = day_date[1:]
        
        top_left_text = f"{resort_name} Resort\nDaily Management Report\nAs of {day_name} - {day_date}"
        worksheet.write(0, 0, top_left_text, header_fmt)
        
        # Write Column Headers
        for column_index, range_name in enumerate(range_names):
            start, end = ranges[range_name]
            header_text = f"{range_name}\n{start.strftime('%b %d')} - {end.strftime('%b %d')}"
            worksheet.write(0, column_index + 1, header_text, header_fmt)
            worksheet.set_column(column_index + 1, column_index + 1, 18) # Set width

        worksheet.set_column(0, 0, 30) # Set Row Header width
        
        # Freeze first row and first column
        worksheet.freeze_panes(1, 1)
        
        current_row = 1
        
        # --- Snow Section ---
        worksheet.write(current_row, 0, "Snow 24hrs", row_header_fmt)
        for column_index, range_name in enumerate(range_names):
            value = normalize_value(processed_snow[range_name]['snow_24hrs'])
            worksheet.write(current_row, column_index + 1, value, snow_fmt)
        current_row += 1
        
        worksheet.write(current_row, 0, "Base Depth", row_header_fmt)
        for column_index, range_name in enumerate(range_names):
            value = normalize_value(processed_snow[range_name]['base_depth'])
            worksheet.write(current_row, column_index + 1, value, snow_fmt)
        current_row += 2 # Spacer
        
        # --- Visits Section ---
        worksheet.write(current_row, 0, "VISITS", header_fmt)
        current_row += 1
        
        sorted_locations = sorted(list(all_locations))
        
        for location in sorted_locations:
            worksheet.write(current_row, 0, location, row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                value = normalize_value(processed_visits[range_name].get(location, 0))
                worksheet.write(current_row, column_index + 1, value, data_fmt)
            current_row += 1
            
        # Total Visits
        worksheet.write(current_row, 0, "Total Tickets", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total = normalize_value(sum(processed_visits[range_name].values()))
            worksheet.write(current_row, column_index + 1, total, data_fmt)
        current_row += 2
        
        # --- Financials Section ---
        worksheet.write(current_row, 0, "FINANCIALS", header_fmt)
        current_row += 1
        
        # Get all departments from payroll processed data (these are the ones we want to match)
        payroll_departments = set()
        for range_name in range_names:
            payroll_departments.update(processed_payroll[range_name].keys())
        
        # Sort departments for consistent display
        sorted_payroll_departments = sorted(list(payroll_departments))
        
        # For each department in payroll, match with revenue and display together
        for department_code in sorted_payroll_departments:
            # Get department title for display (use code as fallback)
            # Trim whitespace from code before lookup
            trimmed_code = trim_dept_code(department_code)
            department_title = department_code_to_title.get(trimmed_code, trimmed_code)
            
            # Revenue Row - show revenue for this department (0 if not in revenue)
            worksheet.write(current_row, 0, f"{department_title} - Revenue", row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                value = normalize_value(processed_revenue[range_name].get(trimmed_code, 0))
                worksheet.write(current_row, column_index + 1, value, data_fmt)
            current_row += 1
            
            # Payroll Row - show payroll for this department
            worksheet.write(current_row, 0, f"{department_title} - Payroll", row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                value = normalize_value(processed_payroll[range_name].get(trimmed_code, 0))
                worksheet.write(current_row, column_index + 1, value, data_fmt)
            current_row += 1
            
            # PR% Row: (Revenue / Payroll) × 100, ignoring negative signs
            worksheet.write(current_row, 0, f"PR % of {department_title}", row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                revenue = abs(normalize_value(processed_revenue[range_name].get(trimmed_code, 0)))
                payroll = abs(normalize_value(processed_payroll[range_name].get(trimmed_code, 0)))
                
                # If either revenue or payroll is 0, show 0%
                if revenue == 0 or payroll == 0:
                    percentage = 0
                else:
                    percentage = abs((revenue / payroll) * 100)  # Ensure non-negative
                
                worksheet.write(current_row, column_index + 1, percentage, percent_fmt)
            current_row += 1
        
        # Totals
        current_row += 1
        worksheet.write(current_row, 0, "Total Revenue", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total = normalize_value(sum(processed_revenue[range_name].values()))
            worksheet.write(current_row, column_index + 1, total, data_fmt)
        current_row += 1
        
        worksheet.write(current_row, 0, "Total Payroll", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total = normalize_value(sum(processed_payroll[range_name].values()))
            worksheet.write(current_row, column_index + 1, total, data_fmt)
        current_row += 1
        
        # PR % of Total Revenue
        worksheet.write(current_row, 0, "PR % of Total Revenue", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total_revenue = abs(normalize_value(sum(processed_revenue[range_name].values())))
            total_payroll = abs(normalize_value(sum(processed_payroll[range_name].values())))
            
            # If either revenue or payroll is 0, show 0%
            if total_revenue == 0 or total_payroll == 0:
                percentage = 0
            else:
                percentage = abs((total_revenue / total_payroll) * 100)  # Ensure non-negative
            
            worksheet.write(current_row, column_index + 1, percentage, percent_fmt)
        current_row += 1
        
        # Net Total Revenue
        worksheet.write(current_row, 0, "Net Total Revenue", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total_revenue = normalize_value(sum(processed_revenue[range_name].values()))
            total_payroll = normalize_value(sum(processed_payroll[range_name].values()))
            net_total = total_revenue - total_payroll
            worksheet.write(current_row, column_index + 1, net_total, data_fmt)
            
        workbook.close()
        print(f"✓ Report saved: {filepath}")
        return filepath

