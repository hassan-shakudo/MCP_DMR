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
                                    run_date: datetime = None) -> str:
        """
        Generate the comprehensive Excel report for a resort.
        
        Args:
            resort_config: Dictionary containing resort details (dbName, resortName, groupNum)
            run_date: Date the report is being run (default now)
            
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
            "Month to Date (Actual)", "Month to Date (Prior Year)",
            "For Winter Ending (Actual)", "For Winter Ending (Prior Year)"
        ]
        
        # 2. Fetch Data for all ranges
        data_store = {name: {} for name in range_names}
        
        with DatabaseConnection() as conn:
            sp = StoredProcedures(conn)
            
            for r_name in range_names:
                start, end = ranges[r_name]
                print(f"   ⏳ Fetching data for {r_name} ({start.date()} - {end.date()})...")
                
                # Revenue
                rev_df = sp.execute_revenue(db_name, group_num, start, end)
                data_store[r_name]['revenue'] = rev_df
                
                # Payroll
                pay_df = sp.execute_payroll(resort_name, start, end)
                data_store[r_name]['payroll'] = pay_df
                
                # Visits
                vis_df = sp.execute_visits(resort_name, start, end)
                data_store[r_name]['visits'] = vis_df
                
                # Weather/Snow
                snow_df = sp.execute_weather(resort_name, start, end)
                data_store[r_name]['snow'] = snow_df

        # 3. Process Data and Collect Row Headers
        all_locations = set()
        all_depts = set()
        dept_code_to_title = {}  # Map department codes to titles
        
        # Processed data structure: category -> range -> key -> value
        processed_snow = {r: {'snow_24hrs': 0.0, 'base_depth': 0.0} for r in range_names}
        processed_visits = {r: {} for r in range_names} # loc -> sum
        processed_revenue = {r: {} for r in range_names} # dept -> sum
        processed_payroll = {r: {} for r in range_names} # dept -> sum
        
        # Helper to guess column names if they vary
        def get_col(df, candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            return None

        for r_name in range_names:
            # --- Snow ---
            df = data_store[r_name]['snow']
            if not df.empty:
                # Sum snow_24hrs
                snow_col = get_col(df, ['snow_24hrs', 'Snow24Hrs', 'Snow_24hrs'])
                base_col = get_col(df, ['base_depth', 'BaseDepth', 'Base_Depth'])
                
                if snow_col:
                    processed_snow[r_name]['snow_24hrs'] = df[snow_col].sum()
                if base_col:
                    processed_snow[r_name]['base_depth'] = df[base_col].sum() # Instruction: "sum up"

            # --- Visits ---
            df = data_store[r_name]['visits']
            if not df.empty:
                loc_col = get_col(df, ['Location', 'location', 'Resort', 'resort'])
                val_col = get_col(df, ['Visits', 'visits', 'Count', 'count']) # Guessing value column
                
                # If no explicit value column, maybe count rows? 
                # User said "sum up the visits". 
                # If DataFrame has one row per visit, we count. If it has aggregated 'Visits' col, we sum.
                # Assuming 'Visits' column exists or we sum rows if no obvious numeric column found?
                # Let's look for numeric columns.
                if not val_col:
                    # Fallback: look for any numeric column that isn't an ID
                    numerics = df.select_dtypes(include=['number']).columns
                    if len(numerics) > 0:
                        val_col = numerics[-1] # Pick last numeric? risky.
                
                if loc_col:
                    # Group and sum
                    if val_col:
                        grouped = df.groupby(loc_col)[val_col].sum()
                    else:
                        # Count rows per location
                        grouped = df.groupby(loc_col).size()
                        
                    for loc, val in grouped.items():
                        loc_str = str(loc)
                        processed_visits[r_name][loc_str] = val
                        all_locations.add(loc_str)

            # --- Revenue ---
            df = data_store[r_name]['revenue']
            if not df.empty:
                # Find department code and title columns
                dept_code_col = get_col(df, ['Department', 'department', 'DepartmentCode', 'department_code'])
                dept_title_col = get_col(df, ['DepartmentTitle', 'department_title', 'DeptTitle', 'dept_title'])
                rev_col = get_col(df, ['Revenue', 'revenue', 'Amount', 'amount']) # Guessing
                
                # If we can't find both dept columns, try using any dept-like column
                if not dept_code_col:
                    dept_code_col = dept_title_col
                if not dept_title_col:
                    dept_title_col = dept_code_col
                
                # Find likely revenue column if not explicit
                if not rev_col:
                     numerics = df.select_dtypes(include=['number']).columns
                     # Usually the last numeric column is the amount
                     if len(numerics) > 0:
                         rev_col = numerics[-1]

                if dept_code_col and rev_col:
                    # Build mapping from code to title
                    if dept_title_col and dept_title_col != dept_code_col:
                        for _, row in df.iterrows():
                            code = str(row[dept_code_col])
                            title = str(row[dept_title_col])
                            if code not in dept_code_to_title:
                                dept_code_to_title[code] = title
                    
                    grouped = df.groupby(dept_code_col)[rev_col].sum()
                    for dept, val in grouped.items():
                        dept_str = str(dept)
                        processed_revenue[r_name][dept_str] = val
                        all_depts.add(dept_str)
                        # If no title mapping yet, use the code as title
                        if dept_str not in dept_code_to_title:
                            dept_code_to_title[dept_str] = dept_str

            # --- Payroll ---
            df = data_store[r_name]['payroll']
            if not df.empty:
                # Need columns: Department, start_punchtime, end_punchtime, rate
                dept_col = get_col(df, ['Department', 'department', 'Dept', 'dept'])
                start_col = get_col(df, ['start_punchtime', 'StartPunchTime', 'StartTime'])
                end_col = get_col(df, ['end_punchtime', 'EndPunchTime', 'EndTime'])
                rate_col = get_col(df, ['rate', 'Rate', 'HourlyRate'])
                
                if dept_col and start_col and end_col and rate_col:
                    # Vectorized calculation is faster but let's iterate for safety with OT logic
                    # Group by department first to avoid huge DF operations if needed
                    # But row-based calc is needed for OT
                    
                    # Ensure datetime
                    df[start_col] = pd.to_datetime(df[start_col], errors='coerce')
                    df[end_col] = pd.to_datetime(df[end_col], errors='coerce')
                    df[rate_col] = pd.to_numeric(df[rate_col], errors='coerce').fillna(0)
                    
                    # Remove invalid times
                    valid_rows = df.dropna(subset=[start_col, end_col])
                    
                    for _, row in valid_rows.iterrows():
                        start_t = row[start_col]
                        end_t = row[end_col]
                        rate = row[rate_col]
                        dept = str(row[dept_col])
                        all_depts.add(dept) # Add to depts if not in revenue
                        
                        # Calculate hours
                        diff = (end_t - start_t).total_seconds() / 3600.0
                        if diff < 0: diff = 0 # Should not happen but safety
                        
                        # OT Logic
                        # <= 8 hrs: hours * rate
                        # > 8 hrs: (8 * rate) + ((hours - 8) * rate * 1.5)
                        if diff <= 8:
                            wages = diff * rate
                        else:
                            wages = (8 * rate) + ((diff - 8) * rate * 1.5)
                            
                        processed_payroll[r_name][dept] = processed_payroll[r_name].get(dept, 0) + wages


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
        for i, r_name in enumerate(range_names):
            start, end = ranges[r_name]
            header_text = f"{r_name}\n{start.strftime('%b %d')} - {end.strftime('%b %d')}"
            worksheet.write(0, i + 1, header_text, header_fmt)
            worksheet.set_column(i + 1, i + 1, 18) # Set width

        worksheet.set_column(0, 0, 30) # Set Row Header width
        
        # Freeze first row and first column
        worksheet.freeze_panes(1, 1)
        
        current_row = 1
        
        # --- Snow Section ---
        worksheet.write(current_row, 0, "Snow 24hrs", row_header_fmt)
        for i, r_name in enumerate(range_names):
            worksheet.write(current_row, i + 1, processed_snow[r_name]['snow_24hrs'], snow_fmt)
        current_row += 1
        
        worksheet.write(current_row, 0, "Base Depth", row_header_fmt)
        for i, r_name in enumerate(range_names):
            worksheet.write(current_row, i + 1, processed_snow[r_name]['base_depth'], snow_fmt)
        current_row += 2 # Spacer
        
        # --- Visits Section ---
        worksheet.write(current_row, 0, "VISITS", header_fmt)
        current_row += 1
        
        sorted_locs = sorted(list(all_locations))
        
        for loc in sorted_locs:
            worksheet.write(current_row, 0, loc, row_header_fmt)
            for i, r_name in enumerate(range_names):
                val = processed_visits[r_name].get(loc, 0)
                worksheet.write(current_row, i + 1, val, data_fmt)
            current_row += 1
            
        # Total Visits
        worksheet.write(current_row, 0, "Total Tickets", header_fmt)
        for i, r_name in enumerate(range_names):
            total = sum(processed_visits[r_name].values())
            worksheet.write(current_row, i + 1, total, data_fmt)
        current_row += 2
        
        # --- Financials Section ---
        worksheet.write(current_row, 0, "FINANCIALS", header_fmt)
        current_row += 1
        
        sorted_depts = sorted(list(all_depts))
        
        for dept in sorted_depts:
            # Get department title for display (use code as fallback)
            dept_title = dept_code_to_title.get(dept, dept)
            
            # Revenue Row
            worksheet.write(current_row, 0, f"{dept_title} - Revenue", row_header_fmt)
            for i, r_name in enumerate(range_names):
                val = processed_revenue[r_name].get(dept, 0)
                worksheet.write(current_row, i + 1, val, data_fmt)
            current_row += 1
            
            # Payroll Row (only if payroll exists for this dept? User says: "If we have the matching dept in the payroll then we will display it.")
            # But also says: "right below this will be the matching payroll... display it."
            # If no payroll for dept ever, maybe skip payroll row? Or show 0?
            # "If we have the matching dept... then we will display it" implies conditional.
            # Check if any payroll exists for this dept across any range.
            
            has_payroll = any(dept in processed_payroll[r] for r in range_names)
            
            if has_payroll:
                worksheet.write(current_row, 0, f"{dept_title} - Payroll", row_header_fmt)
                for i, r_name in enumerate(range_names):
                    val = processed_payroll[r_name].get(dept, 0)
                    worksheet.write(current_row, i + 1, val, data_fmt)
                current_row += 1
                
                # PR% Row: (Revenue / Payroll) × 100, ignoring negative signs
                worksheet.write(current_row, 0, f"PR % of {dept_title}", row_header_fmt)
                percent_fmt = workbook.add_format({'border': 1, 'num_format': '0.00'})
                for i, r_name in enumerate(range_names):
                    revenue = abs(processed_revenue[r_name].get(dept, 0))
                    payroll = abs(processed_payroll[r_name].get(dept, 0))
                    
                    if payroll != 0:
                        percentage = (revenue / payroll) * 100
                    else:
                        percentage = 0  # Avoid division by zero
                    
                    worksheet.write(current_row, i + 1, percentage, percent_fmt)
                current_row += 1
        
        # Totals
        current_row += 1
        worksheet.write(current_row, 0, "Total Revenue", header_fmt)
        for i, r_name in enumerate(range_names):
            total = sum(processed_revenue[r_name].values())
            worksheet.write(current_row, i + 1, total, data_fmt)
        current_row += 1
        
        worksheet.write(current_row, 0, "Total Payroll", header_fmt)
        for i, r_name in enumerate(range_names):
            total = sum(processed_payroll[r_name].values())
            worksheet.write(current_row, i + 1, total, data_fmt)
            
        workbook.close()
        print(f"✓ Report saved: {filepath}")
        return filepath

