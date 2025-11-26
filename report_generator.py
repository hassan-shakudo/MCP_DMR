"""
Report Generator for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
import pandas as pd
import xlsxwriter
from datetime import datetime
from typing import List, Dict, Any, Tuple

from db_connection import DatabaseConnection
from stored_procedures import StoredProcedures
from data_utils import DateRangeCalculator


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
            
    def generate_comprehensive_report(self, 
                                    resort_config: Dict[str, Any], 
                                    run_date: datetime = None,
                                    debug: bool = False) -> str:
        """
        Generate the comprehensive Excel report for a resort.
        
        Args:
            resort_config: Dictionary containing resort details (dbName, resortName, groupNum)
            run_date: Date the report is being run (default now)
            debug: If True, print data from stored procedures for debugging (default False)
            
        Returns:
            Path to saved Excel file
        """
        if run_date is None:
            run_date = datetime.now()
            
        resort_name = resort_config.get('resortName', 'Unknown')
        db_name = resort_config.get('dbName', resort_name)
        group_num = resort_config.get('groupNum', -1)
        
        print(f"\n📊 Generating Comprehensive Report for {resort_name}...")
        print(f"📅 Run Date: {run_date.strftime('%Y-%m-%d')}")

        # 1. Calculate Date Ranges
        date_calc = DateRangeCalculator(run_date)
        ranges = date_calc.get_all_ranges()
        range_names = [
            "For The Day (Actual)", "For The Day (Prior Year)",
            "For The Week Ending (Actual)", "For The Week Ending (Prior Year)",
            "Week Total (Prior Year)",
            "Month to Date (Actual)", "Month to Date (Prior Year)",
            "For Winter Ending (Actual)", "For Winter Ending (Prior Year)"
        ]
        
        # 2. Fetch Data for all ranges
        data_store = {name: {} for name in range_names}
        
        with DatabaseConnection() as conn:
            stored_procedures = StoredProcedures(conn)
            
            for range_name in range_names:
                start, end = ranges[range_name]
                print(f"   ⏳ Fetching data for {range_name} ({start.date()} - {end.date()})...")
                
                # Revenue
                revenue_dataframe = stored_procedures.execute_revenue(db_name, group_num, start, end)
                data_store[range_name]['revenue'] = revenue_dataframe
                if debug:
                    print(f"      [DEBUG] Revenue data for {range_name}:")
                    print(f"      {revenue_dataframe}")
                
                # Payroll
                payroll_dataframe = stored_procedures.execute_payroll(resort_name, start, end)
                data_store[range_name]['payroll'] = payroll_dataframe
                if debug:
                    print(f"      [DEBUG] Payroll data for {range_name}:")
                    print(f"      {payroll_dataframe}")
                
                # Visits
                visits_dataframe = stored_procedures.execute_visits(resort_name, start, end)
                data_store[range_name]['visits'] = visits_dataframe
                if debug:
                    print(f"      [DEBUG] Visits data for {range_name}:")
                    print(f"      {visits_dataframe}")
                
                # Weather/Snow
                snow_dataframe = stored_procedures.execute_weather(resort_name, start, end)
                data_store[range_name]['snow'] = snow_dataframe
                if debug:
                    print(f"      [DEBUG] Snow data for {range_name}:")
                    print(f"      {snow_dataframe}")

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
                    print(f"      [DEBUG] Found column: {candidate_column}, dataframe_columns: {dataframe.columns}, dataframe_head: {dataframe.head()}")
                    return candidate_column
            print(f"      [DEBUG] No column found for {candidates} in dataframe_columns: {dataframe.columns}, dataframe_head: {dataframe.head()}")
            return None
        
        # Helper to safely convert numeric values (handles Decimal, None, etc.)
        def normalize_value(value):
            if value is None:
                return 0.0
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        for range_name in range_names:
            # --- Snow ---
            snow_dataframe = data_store[range_name]['snow']
            if not snow_dataframe.empty:
                # Sum snow_24hrs
                snow_column = get_col(snow_dataframe, ['snow_24hrs', 'Snow24Hrs', 'Snow_24hrs'])
                base_column = get_col(snow_dataframe, ['base_depth', 'BaseDepth', 'Base_Depth'])
                
                if snow_column:
                    processed_snow[range_name]['snow_24hrs'] = snow_dataframe[snow_column].sum()
                if base_column:
                    processed_snow[range_name]['base_depth'] = snow_dataframe[base_column].sum() # Instruction: "sum up"

            # --- Visits ---
            visits_dataframe = data_store[range_name]['visits']
            if not visits_dataframe.empty:
                location_column = get_col(visits_dataframe, ['Location', 'location', 'Resort', 'resort'])
                value_column = get_col(visits_dataframe, ['Visits', 'visits', 'Count', 'count']) # Guessing value column
                
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
                        processed_visits[range_name][location_string] = value
                        all_locations.add(location_string)

            # --- Revenue ---
            revenue_dataframe = data_store[range_name]['revenue']
            if not revenue_dataframe.empty:
                # Find department code and title columns
                department_code_column = get_col(revenue_dataframe, ['Department', 'department', 'DepartmentCode', 'department_code', 'deptCode', 'DeptCode', 'dept_code'])
                department_title_column = get_col(revenue_dataframe, ['DepartmentTitle', 'department_title', 'departmentTitle', 'DeptTitle', 'dept_title'])
                revenue_column = get_col(revenue_dataframe, ['Revenue', 'revenue', 'Amount', 'amount']) # Guessing
                
                # If we can't find both department columns, try using any department-like column
                if not department_code_column:
                    print(f"      [DEBUG] No department code column found for {range_name}. dataframe_columns:: {revenue_dataframe.columns}, df_head: {revenue_dataframe.head()}")
                    department_code_column = department_title_column
                if not department_title_column:
                    print(f"      [DEBUG] No department title column found for {range_name}. dataframe_columns:: {revenue_dataframe.columns}, df_head: {revenue_dataframe.head()}")
                    department_title_column = department_code_column
                
                # Find likely revenue column if not explicit
                if not revenue_column:
                     numeric_columns = revenue_dataframe.select_dtypes(include=['number']).columns
                     # Usually the last numeric column is the amount
                     if len(numeric_columns) > 0:
                         revenue_column = numeric_columns[-1]

                if department_code_column and revenue_column:
                    # Build mapping from code to title
                    if department_title_column and department_title_column != department_code_column:
                        for _, row in revenue_dataframe.iterrows():
                            code = str(row[department_code_column])
                            title = str(row[department_title_column])
                            if code not in department_code_to_title:
                                department_code_to_title[code] = title
                    
                    grouped = revenue_dataframe.groupby(department_code_column)[revenue_column].sum()
                    for department, value in grouped.items():
                        department_string = str(department)
                        processed_revenue[range_name][department_string] = value
                        all_departments.add(department_string)
                        # If no title mapping yet, use the code as title
                        if department_string not in department_code_to_title:
                            department_code_to_title[department_string] = department_string

            # --- Payroll ---
            payroll_dataframe = data_store[range_name]['payroll']
            if not payroll_dataframe.empty:
                # Need columns: Department, start_punchtime, end_punchtime, rate
                department_column = get_col(payroll_dataframe, ['Department', 'department', 'Dept', 'dept'])
                start_column = get_col(payroll_dataframe, ['start_punchtime', 'StartPunchTime', 'StartTime'])
                end_column = get_col(payroll_dataframe, ['end_punchtime', 'EndPunchTime', 'EndTime'])
                rate_column = get_col(payroll_dataframe, ['rate', 'Rate', 'HourlyRate'])
                
                if department_column and start_column and end_column and rate_column:
                    # Vectorized calculation is faster but let's iterate for safety with OT logic
                    # Group by department first to avoid huge DataFrame operations if needed
                    # But row-based calc is needed for OT
                    
                    # Ensure datetime
                    payroll_dataframe[start_column] = pd.to_datetime(payroll_dataframe[start_column], errors='coerce')
                    payroll_dataframe[end_column] = pd.to_datetime(payroll_dataframe[end_column], errors='coerce')
                    payroll_dataframe[rate_column] = pd.to_numeric(payroll_dataframe[rate_column], errors='coerce').fillna(0)
                    
                    # Remove invalid times
                    valid_rows = payroll_dataframe.dropna(subset=[start_column, end_column])
                    
                    for _, row in valid_rows.iterrows():
                        start_time = row[start_column]
                        end_time = row[end_column]
                        rate = row[rate_column]
                        department = str(row[department_column])
                        all_departments.add(department) # Add to departments if not in revenue
                        
                        # Calculate hours
                        hours_worked = (end_time - start_time).total_seconds() / 3600.0
                        if hours_worked < 0: hours_worked = 0 # Should not happen but safety
                        
                        # OT Logic
                        # <= 8 hrs: hours * rate
                        # > 8 hrs: (8 * rate) + ((hours - 8) * rate * 1.5)
                        if hours_worked <= 8:
                            wages = hours_worked * rate
                        else:
                            wages = (8 * rate) + ((hours_worked - 8) * rate * 1.5)
                            
                        processed_payroll[range_name][department] = processed_payroll[range_name].get(department, 0) + wages


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
            worksheet.write(current_row, column_index + 1, processed_snow[range_name]['snow_24hrs'], snow_fmt)
        current_row += 1
        
        worksheet.write(current_row, 0, "Base Depth", row_header_fmt)
        for column_index, range_name in enumerate(range_names):
            worksheet.write(current_row, column_index + 1, processed_snow[range_name]['base_depth'], snow_fmt)
        current_row += 2 # Spacer
        
        # --- Visits Section ---
        worksheet.write(current_row, 0, "VISITS", header_fmt)
        current_row += 1
        
        sorted_locations = sorted(list(all_locations))
        
        for location in sorted_locations:
            worksheet.write(current_row, 0, location, row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                value = processed_visits[range_name].get(location, 0)
                worksheet.write(current_row, column_index + 1, value, data_fmt)
            current_row += 1
            
        # Total Visits
        worksheet.write(current_row, 0, "Total Tickets", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total = sum(processed_visits[range_name].values())
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
            department_title = department_code_to_title.get(department_code, department_code)
            
            # Revenue Row - show revenue for this department (0 if not in revenue)
            worksheet.write(current_row, 0, f"{department_title} - Revenue", row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                value = processed_revenue[range_name].get(department_code, 0)
                worksheet.write(current_row, column_index + 1, value, data_fmt)
            current_row += 1
            
            # Payroll Row - show payroll for this department
            worksheet.write(current_row, 0, f"{department_title} - Payroll", row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                value = processed_payroll[range_name].get(department_code, 0)
                worksheet.write(current_row, column_index + 1, value, data_fmt)
            current_row += 1
            
            # PR% Row: (Revenue / Payroll) × 100, ignoring negative signs
            worksheet.write(current_row, 0, f"{department_title} %", row_header_fmt)
            for column_index, range_name in enumerate(range_names):
                revenue = abs(normalize_value(processed_revenue[range_name].get(department_code, 0)))
                payroll = abs(normalize_value(processed_payroll[range_name].get(department_code, 0)))
                
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
            total = sum(processed_revenue[range_name].values())
            worksheet.write(current_row, column_index + 1, total, data_fmt)
        current_row += 1
        
        worksheet.write(current_row, 0, "Total Payroll", header_fmt)
        for column_index, range_name in enumerate(range_names):
            total = sum(processed_payroll[range_name].values())
            worksheet.write(current_row, column_index + 1, total, data_fmt)
            
        workbook.close()
        print(f"✓ Report saved: {filepath}")
        return filepath

