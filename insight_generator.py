"""
Insight Generator for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
Compares performance between two single-day inputs (Comparison Date and Anchor Date)
"""

import os
import pandas as pd
import xlsxwriter
from datetime import datetime, timedelta
from typing import Dict, Any, Union, Tuple, Set, Optional

from db_connection import DatabaseConnection
from stored_procedures import StoredProcedures
from utils import DateRangeCalculator, DataUtils
from config import CandidateColumns, VISITS_DEPT_CODE_MAPPING


class InsightGenerator:
    """Generate comparative insights between two single-day dates"""
    
    def __init__(self, output_dir: str = "reports"):
        """
        Initialize the insight generator
        
        Args:
            output_dir: Base output directory (insights folder will be created at same level)
        """
        self.output_dir = output_dir
        # Create insights directory at same level as reports folder
        # If output_dir is "reports", insights will be at the same level
        output_path = os.path.abspath(output_dir)
        parent_dir = os.path.dirname(output_path)
        self.insights_dir = os.path.join(parent_dir, "insights")
        if not os.path.exists(self.insights_dir):
            os.makedirs(self.insights_dir)
            print(f"✓ Created insights directory: {self.insights_dir}")

    def _is_within_one_year(self, date: datetime) -> bool:
        """
        Check if a date falls within the last one-year window from today
        
        Args:
            date: Date to check
            
        Returns:
            bool: True if date is within last year, False otherwise
        """
        one_year_ago = datetime.now() - timedelta(days=365)
        return date >= one_year_ago

    def _process_single_day_visits(self, dataframe: pd.DataFrame) -> Dict[str, float]:
        """
        Process visits data for a single day
        
        Args:
            dataframe: Visits dataframe from stored procedure
            
        Returns:
            Dict mapping location names to visit counts
        """
        processed_visits = {}
        if dataframe.empty:
            return processed_visits
        
        location_col = DataUtils.get_col(dataframe, CandidateColumns.location)
        visits_col = DataUtils.get_col(dataframe, CandidateColumns.visits)
        
        if location_col:
            if visits_col:
                grouped = dataframe.groupby(location_col)[visits_col].sum()
            else:
                grouped = dataframe.groupby(location_col).size()
            
            for location, value in grouped.items():
                location_str = str(location)
                processed_visits[location_str] = DataUtils.normalize_value(value)
        
        return processed_visits

    def _process_single_day_revenue(self, dataframe: pd.DataFrame, 
                                    department_to_title: Dict[str, str]) -> Dict[str, float]:
        """
        Process revenue data for a single day
        
        Args:
            dataframe: Revenue dataframe from stored procedure
            department_to_title: Dictionary to update with department titles
            
        Returns:
            Dict mapping department codes to revenue amounts
        """
        processed_revenue = {}
        if dataframe.empty:
            return processed_revenue
        
        code_col = DataUtils.get_col(dataframe, CandidateColumns.departmentCode) or 'department'
        title_col = DataUtils.get_col(dataframe, CandidateColumns.departmentTitle) or 'DepartmentTitle'
        revenue_col = DataUtils.get_col(dataframe, CandidateColumns.revenue) or 'revenue'
        
        if not revenue_col:
            numeric_cols = dataframe.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                revenue_col = numeric_cols[-1]
        
        if code_col and revenue_col:
            # Update department title mapping
            for _, row in dataframe.iterrows():
                dept_code = DataUtils.trim_dept_code(row[code_col])
                if dept_code and title_col in dataframe.columns and pd.notna(row[title_col]):
                    if dept_code not in department_to_title:
                        department_to_title[dept_code] = str(row[title_col]).strip()
            
            # Group by department code and sum revenue
            grouped = dataframe.groupby(code_col)[revenue_col].sum()
            for dept, value in grouped.items():
                dept_str = DataUtils.trim_dept_code(dept)
                processed_revenue[dept_str] = DataUtils.normalize_value(value)
                if dept_str not in department_to_title:
                    department_to_title[dept_str] = dept_str
        
        return processed_revenue

    def _process_single_day_payroll_actual(self, 
                                           payroll_df: pd.DataFrame,
                                           salary_df: pd.DataFrame,
                                           department_to_title: Dict[str, str],
                                           date_label: str = "",
                                           debug_log_file: Any = None) -> Dict[str, float]:
        """
        Process payroll data for a single day using actual ranges logic
        (contract payroll + salary payroll)
        
        Args:
            payroll_df: Contract payroll dataframe
            salary_df: Salary payroll dataframe
            department_to_title: Dictionary to update with department titles
            date_label: Label for logging (e.g., "Comparison Date" or "Anchor Date")
            debug_log_file: Optional file handle for debug logging
            
        Returns:
            Dict mapping department codes to payroll amounts
        """
        log_message = f"\n{'='*80}\n  📊 PAYROLL CALCULATION BREAKDOWN - {date_label}\n"
        log_message += f"  Method: Actual Ranges (Contract + Salary)\n"
        log_message += f"{'='*80}\n"
        
        processed_payroll = {}
        calculated_wages = {}
        contract_rows_by_dept = {}
        
        # Process contract payroll
        if not payroll_df.empty:
            code_col = DataUtils.get_col(payroll_df, CandidateColumns.departmentCode) or 'department'
            title_col = DataUtils.get_col(payroll_df, CandidateColumns.departmentTitle)
            start_col = DataUtils.get_col(payroll_df, CandidateColumns.payrollStartTime)
            end_col = DataUtils.get_col(payroll_df, CandidateColumns.payrollEndTime)
            rate_col = DataUtils.get_col(payroll_df, CandidateColumns.payrollRate)
            hours_col = DataUtils.get_col(payroll_df, CandidateColumns.payrollHours)
            dollar_col = DataUtils.get_col(payroll_df, CandidateColumns.payrollDollarAmount)
            
            for _, row in payroll_df.iterrows():
                dept_code = DataUtils.trim_dept_code(row[code_col])
                if not dept_code:
                    continue
                
                # Update title mapping if present
                if title_col and pd.notna(row[title_col]) and dept_code not in department_to_title:
                    department_to_title[dept_code] = str(row[title_col]).strip()
                
                rate = DataUtils.normalize_value(row[rate_col])
                hours_from_col = DataUtils.normalize_value(row[hours_col]) if hours_col else 0
                dollar_amt = DataUtils.normalize_value(row[dollar_col]) if dollar_col else 0
                
                # Calculate working hours from punch times
                working_hours = 0.0
                if pd.notna(row[start_col]) and pd.notna(row[end_col]):
                    try:
                        start_time = pd.to_datetime(row[start_col])
                        end_time = pd.to_datetime(row[end_col])
                        if pd.notna(start_time) and pd.notna(end_time):
                            seconds_diff = (end_time - start_time).total_seconds()
                            working_hours = max(0.0, seconds_diff / 3600.0)  # Ensure non-negative
                            # Normalize to handle any edge cases
                            working_hours = DataUtils.normalize_value(working_hours)
                    except (ValueError, TypeError, OverflowError):
                        working_hours = 0.0
                
                # Apply business logic for wage calculation
                try:
                    if hours_from_col > 0:
                        wage = (hours_from_col * rate) + dollar_amt
                    else:
                        wage = (working_hours * rate) + dollar_amt
                    
                    # Normalize wage to handle any edge cases (NaN, Inf, etc.)
                    wage = DataUtils.normalize_value(wage)
                except (OverflowError, ValueError, TypeError):
                    wage = 0.0
                
                calculated_wages[dept_code] = DataUtils.normalize_value(
                    calculated_wages.get(dept_code, 0.0) + wage
                )
                
                # Track for logging
                if dept_code not in contract_rows_by_dept:
                    contract_rows_by_dept[dept_code] = []
                contract_rows_by_dept[dept_code].append({
                    'start': row[start_col], 'end': row[end_col], 'rate': rate,
                    'w_hrs': working_hours, 'h_col': hours_from_col, 'd_amt': dollar_amt, 'wage': wage
                })
        
        # Process salary payroll
        salary_totals = {}
        if not salary_df.empty:
            salary_code_column = DataUtils.get_col(salary_df, CandidateColumns.departmentCode)
            salary_total_column = DataUtils.get_col(salary_df, CandidateColumns.salaryTotal)
            salary_title_column = DataUtils.get_col(salary_df, CandidateColumns.departmentTitle)
            
            for _, row in salary_df.iterrows():
                dept = DataUtils.trim_dept_code(row[salary_code_column])
                if dept:
                    salary_totals[dept] = DataUtils.normalize_value(row[salary_total_column])
                    if salary_title_column and pd.notna(row[salary_title_column]) and dept not in department_to_title:
                        department_to_title[dept] = str(row[salary_title_column]).strip()
        
        # Combine contract and salary payroll
        relevant_depts = set(calculated_wages.keys()) | set(salary_totals.keys())
        for dept_code in sorted(list(relevant_depts)):
            dept_title = department_to_title.get(dept_code, dept_code)
            log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
            
            # Log Contract Details
            log_message += "     📋 Contract Payroll (Hourly):\n"
            rows = contract_rows_by_dept.get(dept_code, [])
            for idx, r in enumerate(rows, 1):
                log_message += f"          Row {idx}: Start={r['start']}, End={r['end']}, WHrs={r['w_hrs']:.2f}, HCol={r['h_col']:.2f}, Rate=${r['rate']:.2f}, Dlr=${r['d_amt']:.2f}, Wage=${r['wage']:.2f}\n"
            
            contract_total = DataUtils.normalize_value(calculated_wages.get(dept_code, 0.0))
            salary_total = DataUtils.normalize_value(salary_totals.get(dept_code, 0.0))
            
            log_message += f"        • Aggregated Contract: ${contract_total:,.2f}\n"
            log_message += f"        • Salary for Range: ${salary_total:,.2f}\n"
            
            # Normalize final wage to handle any edge cases
            try:
                final_wage = DataUtils.normalize_value(contract_total + salary_total)
            except (OverflowError, ValueError, TypeError):
                final_wage = 0.0
            processed_payroll[dept_code] = final_wage
            log_message += f"     ✅ FINAL PAYROLL TOTAL: ${final_wage:,.2f}\n"
        
        log_message += f"\n{'='*80}\n"
        print(log_message, end='')
        if debug_log_file:
            debug_log_file.write(log_message)
            debug_log_file.flush()
        
        return processed_payroll

    def _process_single_day_payroll_prior_year(self,
                                               history_df: pd.DataFrame,
                                               department_to_title: Dict[str, str],
                                               date_label: str = "",
                                               debug_log_file: Any = None) -> Dict[str, float]:
        """
        Process payroll data for a single day using prior-year ranges logic
        (history payroll only)
        
        Args:
            history_df: Payroll history dataframe
            department_to_title: Dictionary to update with department titles
            date_label: Label for logging (e.g., "Comparison Date" or "Anchor Date")
            debug_log_file: Optional file handle for debug logging
            
        Returns:
            Dict mapping department codes to payroll amounts
        """
        log_message = f"\n{'='*80}\n  📊 PAYROLL CALCULATION BREAKDOWN - {date_label}\n"
        log_message += f"  Method: Prior Year Ranges (History Only)\n"
        log_message += f"{'='*80}\n"
        
        processed_payroll = {}
        if history_df.empty:
            log_message += "  ⚠️  No payroll history data available\n"
            log_message += f"\n{'='*80}\n"
            print(log_message, end='')
            if debug_log_file:
                debug_log_file.write(log_message)
                debug_log_file.flush()
            return processed_payroll
        
        history_code_column = DataUtils.get_col(history_df, CandidateColumns.departmentCode) or 'department'
        history_total_column = DataUtils.get_col(history_df, CandidateColumns.historyTotal)
        
        for _, row in history_df.iterrows():
            dept = DataUtils.trim_dept_code(row[history_code_column])
            if dept:
                processed_payroll[dept] = DataUtils.normalize_value(row[history_total_column])
        
        # Log results
        for dept_code in sorted(list(processed_payroll.keys())):
            dept_title = department_to_title.get(dept_code, dept_code)
            history_total = processed_payroll[dept_code]
            log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
            log_message += f"        • Contract: (Not used for Prior Year)\n"
            log_message += f"        • Salary: (Not used for Prior Year)\n"
            log_message += f"        • Historical Total: ${history_total:,.2f}\n"
            log_message += f"     ✅ FINAL PAYROLL TOTAL: ${history_total:,.2f}\n"
        
        log_message += f"\n{'='*80}\n"
        print(log_message, end='')
        if debug_log_file:
            debug_log_file.write(log_message)
            debug_log_file.flush()
        
        return processed_payroll

    def _process_single_day_budget(self, dataframe: pd.DataFrame,
                                   department_to_title: Dict[str, str]) -> Dict[str, Dict[str, float]]:
        """
        Process budget data for a single day
        
        Args:
            dataframe: Budget dataframe from stored procedure
            department_to_title: Dictionary to update with department titles
            
        Returns:
            Dict mapping department codes to budget dict with 'Revenue' and 'Payroll' keys
        """
        processed_budget = {}
        if dataframe.empty:
            return processed_budget
        
        code_col = DataUtils.get_col(dataframe, CandidateColumns.departmentCode)
        type_col = DataUtils.get_col(dataframe, CandidateColumns.budgetType)
        amount_col = DataUtils.get_col(dataframe, CandidateColumns.budgetAmount)
        title_col = DataUtils.get_col(dataframe, CandidateColumns.departmentTitle)
        
        if code_col and type_col and amount_col:
            for _, row in dataframe.iterrows():
                dept_code = DataUtils.trim_dept_code(row[code_col])
                amount = DataUtils.normalize_value(row[amount_col])
                budget_type = str(row[type_col]).strip().lower() if pd.notna(row[type_col]) else ""
                
                if not dept_code:
                    continue
                
                # Skip visits budget (handled separately)
                if 'visits' in budget_type:
                    continue
                
                if dept_code not in processed_budget:
                    processed_budget[dept_code] = {'Payroll': 0.0, 'Revenue': 0.0}
                
                if 'payroll' in budget_type:
                    processed_budget[dept_code]['Payroll'] = amount
                elif 'revenue' in budget_type:
                    processed_budget[dept_code]['Revenue'] = amount
                
                if title_col and title_col in row and pd.notna(row[title_col]) and dept_code not in department_to_title:
                    department_to_title[dept_code] = str(row[title_col]).strip()
        
        return processed_budget

    def _fetch_single_day_data(self,
                               resort_config: Dict[str, Any],
                               target_date: datetime,
                               is_within_year: bool,
                               is_current_date: bool = False,
                               debug: bool = False,
                               debug_directory: str = None,
                               date_label: str = "") -> Dict[str, Any]:
        """
        Fetch all data for a single day
        
        Args:
            resort_config: Resort configuration dict with 'resortName', 'dbName', 'groupNum'
            target_date: The date to fetch data for
            is_within_year: Whether the date is within the last year
            is_current_date: Whether the target date is the current date
            debug: Whether debug mode is enabled
            debug_directory: Directory for debug exports
            date_label: Label for debug exports (e.g., "Comparison" or "Anchor")
            
        Returns:
            Dict containing all fetched dataframes
        """
        resort_name = resort_config['resortName']
        db_name = resort_config.get('dbName', resort_name)
        group_num = resort_config.get('groupNum', -1)
        
        # Calculate date range for "For The Day"
        # If current date, use start of day to current time, otherwise start to end of day
        date_calculator = DateRangeCalculator(target_date, is_current_date=is_current_date, use_exact_date=not is_current_date)
        day_range = date_calculator.for_the_day_actual()
        start, end = day_range
        
        data = {
            'revenue': pd.DataFrame(),
            'visits': pd.DataFrame(),
            'budget': pd.DataFrame(),
            'payroll': pd.DataFrame(),
            'salary_payroll': pd.DataFrame(),
            'payroll_history': pd.DataFrame()
        }
        
        with DatabaseConnection() as conn:
            stored_procedures_handler = StoredProcedures(conn)
            
            print(f"   ⏳ Fetching {date_label} data ({start.date()} to {end.date()})...")
            
            # Always fetch revenue, visits, and budget
            data['revenue'] = stored_procedures_handler.execute_revenue(db_name, group_num, start, end)
            data['visits'] = stored_procedures_handler.execute_visits(resort_name, start, end)
            data['budget'] = stored_procedures_handler.execute_budget(resort_name, start, end)
            
            # Fetch payroll based on date age
            if is_within_year:
                # Use actual ranges logic
                data['payroll'] = stored_procedures_handler.execute_payroll(resort_name, start, end)
                data['salary_payroll'] = stored_procedures_handler.execute_payroll_salary(resort_name, start, end)
            else:
                # Use prior-year ranges logic
                data['payroll_history'] = stored_procedures_handler.execute_payroll_history(resort_name, start, end)
            
            # Export datasets in debug mode
            if debug and debug_directory:
                for key in ['revenue', 'visits', 'budget', 'payroll', 'salary_payroll', 'payroll_history']:
                    if not data[key].empty:
                        self._export_sp_result(data[key], date_label, key.capitalize(), resort_name, debug_directory)
        
        return data

    def _calculate_variance_percentage(self, comparison_value: float, anchor_value: float) -> float:
        """
        Calculate variance percentage using Anchor Date as baseline
        
        Formula: ((comparison_value - anchor_value) * 100) / anchor_value
        
        Args:
            comparison_value: Value from Comparison Date
            anchor_value: Value from Anchor Date (baseline)
            
        Returns:
            Variance percentage (float), normalized to handle edge cases
        """
        # Normalize inputs to handle None, NaN, Inf
        comparison_value = DataUtils.normalize_value(comparison_value)
        anchor_value = DataUtils.normalize_value(anchor_value)
        
        # Handle division by zero (including very small values close to zero)
        if abs(anchor_value) < 1e-10:
            return 0.0
        
        try:
            result = ((comparison_value - anchor_value) * 100) / anchor_value
            # Normalize result to handle NaN/Inf
            return DataUtils.normalize_value(result)
        except (ZeroDivisionError, OverflowError, ValueError):
            return 0.0

    def _export_sp_result(self, dataframe: pd.DataFrame, date_label: str, stored_procedure_name: str, 
                         resort_name: str, export_directory: str = None) -> str:
        """
        Export stored procedure result to Excel file
        
        Args:
            dataframe: DataFrame to export
            date_label: Label for the date (e.g., "Comparison" or "Anchor")
            stored_procedure_name: Name of the stored procedure
            resort_name: Name of the resort
            export_directory: Directory to export to
            
        Returns:
            Path to exported file
        """
        sanitized_date = DataUtils.sanitize_filename(date_label)
        sanitized_sp = DataUtils.sanitize_filename(stored_procedure_name)
        file_path = os.path.join(export_directory or self.insights_dir, f"{sanitized_date}_{sanitized_sp}.xlsx")
        
        # Sort logic
        dataframe_to_write = dataframe
        if stored_procedure_name in ['Revenue', 'Payroll']:
            dept_column = DataUtils.get_col(dataframe, CandidateColumns.departmentCode + CandidateColumns.departmentTitle)
            if dept_column:
                dataframe_to_write = dataframe.copy()
                dataframe_to_write['_sort_key'] = dataframe_to_write[dept_column].astype(str).str.strip()
                dataframe_to_write = dataframe_to_write.sort_values(by='_sort_key', na_position='last').drop(columns=['_sort_key'])
        
        # Write dataset
        workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})
        worksheet = workbook.add_worksheet('Data')
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        data_format = workbook.add_format({'border': 1})
        
        for col_index, column_name in enumerate(dataframe_to_write.columns):
            worksheet.write(0, col_index, column_name, header_format)
            max_column_width = len(str(column_name))
            for row_index, (_, row_data) in enumerate(dataframe_to_write.iterrows(), start=1):
                cell_value = row_data[column_name]
                worksheet.write(row_index, col_index, None if pd.isna(cell_value) else cell_value, data_format)
                max_column_width = max(max_column_width, len(str(cell_value)))
            worksheet.set_column(col_index, col_index, min(max_column_width + 2, 50))
        
        workbook.close()
        return file_path

    def generate_insights(self,
                         resort_config: Dict[str, Any],
                         comparison_date: Union[str, datetime],
                         anchor_date: Union[str, datetime],
                         debug: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Generate comparative insights between two single-day dates
        
        Args:
            resort_config: Resort configuration dict with 'resortName', 'dbName', 'groupNum'
            comparison_date: Comparison Date (string format: "MM/DD/YYYY" or datetime)
            anchor_date: Anchor Date (string format: "MM/DD/YYYY" or datetime)
            debug: If True, export datasets and create debug logs
            
        Returns:
            Dict with two keys:
            - 'department_analytics': DataFrame with department-level metrics
            - 'visit_analytics': DataFrame with visit-level metrics
        """
        # Parse dates
        current_now = datetime.now()
        if isinstance(comparison_date, str):
            comparison_date = datetime.strptime(comparison_date, "%m/%d/%Y")
        if isinstance(anchor_date, str):
            anchor_date = datetime.strptime(anchor_date, "%m/%d/%Y")
        
        # Check if comparison date is current date
        comparison_is_current = (comparison_date.date() == current_now.date())
        
        # If comparison date is current date, use current time for both dates
        # Otherwise, use start to end of day for both
        if comparison_is_current:
            anchor_is_current = True  # Use current time for anchor too
        else:
            anchor_is_current = False
        
        # Determine payroll calculation method for each date
        comparison_is_within_year = self._is_within_one_year(comparison_date)
        anchor_is_within_year = self._is_within_one_year(anchor_date)
        
        # Setup debug directory and logging
        debug_directory = None
        debug_log_handle = None
        if debug:
            resort_name = resort_config['resortName']
            sanitized_resort = DataUtils.sanitize_filename(resort_name).lower()
            comparison_str = comparison_date.strftime("%Y%m%d")
            anchor_str = anchor_date.strftime("%Y%m%d")
            debug_directory = os.path.join(self.insights_dir, f"{comparison_str}-{anchor_str}-insights")
            if not os.path.exists(debug_directory):
                os.makedirs(debug_directory)
            debug_log_handle = open(os.path.join(debug_directory, "debugLog.txt"), 'w', encoding='utf-8')
            
            # Write header to debug log
            header = f"""
{'='*80}
INSIGHT GENERATION DEBUG LOG
{'='*80}
Resort: {resort_name}
Comparison Date: {comparison_date.strftime('%Y-%m-%d')} {'(Current Date - using current time)' if comparison_is_current else '(Past Date - using full day)'}
Anchor Date: {anchor_date.strftime('%Y-%m-%d')} {'(Using current time)' if anchor_is_current else '(Using full day)'}
Comparison Date Payroll Method: {'Actual Ranges' if comparison_is_within_year else 'Prior Year Ranges'}
Anchor Date Payroll Method: {'Actual Ranges' if anchor_is_within_year else 'Prior Year Ranges'}
{'='*80}

"""
            debug_log_handle.write(header)
            debug_log_handle.flush()
            print(header, end='')
        
        # Fetch data for both dates
        print(f"Fetching data for Comparison Date: {comparison_date.strftime('%Y-%m-%d')}")
        comparison_data = self._fetch_single_day_data(
            resort_config, comparison_date, comparison_is_within_year,
            is_current_date=comparison_is_current, debug=debug,
            debug_directory=debug_directory, date_label="Comparison"
        )
        
        print(f"Fetching data for Anchor Date: {anchor_date.strftime('%Y-%m-%d')}")
        anchor_data = self._fetch_single_day_data(
            resort_config, anchor_date, anchor_is_within_year,
            is_current_date=anchor_is_current, debug=debug,
            debug_directory=debug_directory, date_label="Anchor"
        )
        
        # Process data for both dates
        department_to_title = {}
        
        # Process Comparison Date (skip if dataset is empty)
        comparison_visits = {}
        comparison_revenue = {}
        comparison_budget = {}
        comparison_payroll = {}
        
        if not comparison_data['visits'].empty:
            comparison_visits = self._process_single_day_visits(comparison_data['visits'])
        if not comparison_data['revenue'].empty:
            comparison_revenue = self._process_single_day_revenue(comparison_data['revenue'], department_to_title)
        if not comparison_data['budget'].empty:
            comparison_budget = self._process_single_day_budget(comparison_data['budget'], department_to_title)
        
        if comparison_is_within_year:
            if not comparison_data['payroll'].empty or not comparison_data['salary_payroll'].empty:
                comparison_payroll = self._process_single_day_payroll_actual(
                    comparison_data['payroll'],
                    comparison_data['salary_payroll'],
                    department_to_title,
                    date_label="Comparison Date",
                    debug_log_file=debug_log_handle
                )
        else:
            if not comparison_data['payroll_history'].empty:
                comparison_payroll = self._process_single_day_payroll_prior_year(
                    comparison_data['payroll_history'],
                    department_to_title,
                    date_label="Comparison Date",
                    debug_log_file=debug_log_handle
                )
        
        # Process Anchor Date (skip if dataset is empty)
        anchor_visits = {}
        anchor_revenue = {}
        anchor_budget = {}
        anchor_payroll = {}
        
        if not anchor_data['visits'].empty:
            anchor_visits = self._process_single_day_visits(anchor_data['visits'])
        if not anchor_data['revenue'].empty:
            anchor_revenue = self._process_single_day_revenue(anchor_data['revenue'], department_to_title)
        if not anchor_data['budget'].empty:
            anchor_budget = self._process_single_day_budget(anchor_data['budget'], department_to_title)
        
        if anchor_is_within_year:
            if not anchor_data['payroll'].empty or not anchor_data['salary_payroll'].empty:
                anchor_payroll = self._process_single_day_payroll_actual(
                    anchor_data['payroll'],
                    anchor_data['salary_payroll'],
                    department_to_title,
                    date_label="Anchor Date",
                    debug_log_file=debug_log_handle
                )
        else:
            if not anchor_data['payroll_history'].empty:
                anchor_payroll = self._process_single_day_payroll_prior_year(
                    anchor_data['payroll_history'],
                    department_to_title,
                    date_label="Anchor Date",
                    debug_log_file=debug_log_handle
                )
        
        # Build department-level analytics dataframe
        department_analytics = self._build_department_analytics(
            comparison_revenue, comparison_payroll, comparison_budget,
            anchor_revenue, anchor_payroll, anchor_budget,
            department_to_title
        )
        
        # Build visit-level analytics dataframe
        visit_analytics = self._build_visit_analytics(
            comparison_visits, anchor_visits
        )
        
        # Export insight dataframes in debug mode
        if debug and debug_directory:
            # Export department analytics
            dept_file = os.path.join(debug_directory, "department_analytics.xlsx")
            with pd.ExcelWriter(dept_file, engine='xlsxwriter') as writer:
                department_analytics.to_excel(writer, sheet_name='Department Analytics', index=False)
            print(f"✓ Department analytics exported: {dept_file}")
            
            # Export visit analytics
            visit_file = os.path.join(debug_directory, "visit_analytics.xlsx")
            with pd.ExcelWriter(visit_file, engine='xlsxwriter') as writer:
                visit_analytics.to_excel(writer, sheet_name='Visit Analytics', index=False)
            print(f"✓ Visit analytics exported: {visit_file}")
            
            # Close debug log
            if debug_log_handle:
                debug_log_handle.write(f"\n{'='*80}\nInsight generation complete!\n{'='*80}\n")
                debug_log_handle.close()
                print(f"✓ Debug log saved: {os.path.join(debug_directory, 'debugLog.txt')}")
        
        return {
            'department_analytics': department_analytics,
            'visit_analytics': visit_analytics
        }

    def _build_department_analytics(self,
                                   comparison_revenue: Dict[str, float],
                                   comparison_payroll: Dict[str, float],
                                   comparison_budget: Dict[str, Dict[str, float]],
                                   anchor_revenue: Dict[str, float],
                                   anchor_payroll: Dict[str, float],
                                   anchor_budget: Dict[str, Dict[str, float]],
                                   department_to_title: Dict[str, str]) -> pd.DataFrame:
        """
        Build department-level analytics dataframe
        
        Returns:
            DataFrame with columns:
            - Department Code
            - Department Title
            - Comparison Revenue
            - Comparison Payroll
            - Comparison Budget
            - Anchor Revenue
            - Anchor Payroll
            - Anchor Budget
            - Revenue Variance %
            - Payroll Variance %
            - Budget Variance %
            - Revenue-to-Payroll % (Comparison)
            - Budget-to-Payroll % (Comparison)
        """
        # Collect all department codes
        all_depts = set(comparison_revenue.keys()) | set(comparison_payroll.keys()) | \
                   set(comparison_budget.keys()) | set(anchor_revenue.keys()) | \
                   set(anchor_payroll.keys()) | set(anchor_budget.keys())
        
        # If no departments found, return empty dataframe with proper columns
        if not all_depts:
            return pd.DataFrame(columns=[
                'Department Code', 'Department Title',
                'Comparison Revenue', 'Comparison Payroll', 'Comparison Budget',
                'Anchor Revenue', 'Anchor Payroll', 'Anchor Budget',
                'Revenue Variance %', 'Payroll Variance %', 'Budget Variance %',
                'Revenue-to-Payroll %', 'Budget-to-Payroll %'
            ])
        
        rows = []
        for dept_code in sorted(all_depts):
            dept_title = department_to_title.get(dept_code, dept_code)
            
            # Normalize all values to handle None, NaN, Inf
            comp_rev = DataUtils.normalize_value(comparison_revenue.get(dept_code, 0.0))
            comp_pay = DataUtils.normalize_value(comparison_payroll.get(dept_code, 0.0))
            comp_bud = DataUtils.normalize_value(comparison_budget.get(dept_code, {}).get('Revenue', 0.0))
            
            anchor_rev = DataUtils.normalize_value(anchor_revenue.get(dept_code, 0.0))
            anchor_pay = DataUtils.normalize_value(anchor_payroll.get(dept_code, 0.0))
            anchor_bud = DataUtils.normalize_value(anchor_budget.get(dept_code, {}).get('Revenue', 0.0))
            
            # Calculate variances
            rev_variance = self._calculate_variance_percentage(comp_rev, anchor_rev)
            pay_variance = self._calculate_variance_percentage(comp_pay, anchor_pay)
            bud_variance = self._calculate_variance_percentage(comp_bud, anchor_bud)
            
            # Calculate ratios for comparison date (normalize inputs first)
            comp_rev = DataUtils.normalize_value(comp_rev)
            comp_pay = DataUtils.normalize_value(comp_pay)
            comp_bud = DataUtils.normalize_value(comp_bud)
            
            # Handle division by zero (including very small values)
            if abs(comp_pay) < 1e-10:
                rev_to_pay_ratio = 0.0
                bud_to_pay_ratio = 0.0
            else:
                try:
                    rev_to_pay_ratio = DataUtils.normalize_value((comp_rev / comp_pay) * 100)
                    bud_to_pay_ratio = DataUtils.normalize_value((comp_bud / comp_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio = 0.0
                    bud_to_pay_ratio = 0.0
            
            rows.append({
                'Department Code': dept_code,
                'Department Title': dept_title,
                'Comparison Revenue': comp_rev,
                'Comparison Payroll': comp_pay,
                'Comparison Budget': comp_bud,
                'Anchor Revenue': anchor_rev,
                'Anchor Payroll': anchor_pay,
                'Anchor Budget': anchor_bud,
                'Revenue Variance %': rev_variance,
                'Payroll Variance %': pay_variance,
                'Budget Variance %': bud_variance,
                'Revenue-to-Payroll %': rev_to_pay_ratio,
                'Budget-to-Payroll %': bud_to_pay_ratio
            })
        
        return pd.DataFrame(rows)

    def _build_visit_analytics(self,
                               comparison_visits: Dict[str, float],
                               anchor_visits: Dict[str, float]) -> pd.DataFrame:
        """
        Build visit-level analytics dataframe with location grouping
        
        Returns:
            DataFrame with columns:
            - Visit Category (location name)
            - Comparison Visits
            - Anchor Visits
            - Visit Variance %
        """
        # Collect all visit categories (locations)
        all_categories = set(comparison_visits.keys()) | set(anchor_visits.keys())
        
        # If no categories found, return empty dataframe with proper columns
        if not all_categories:
            return pd.DataFrame(columns=[
                'Visit Category', 'Comparison Visits', 'Anchor Visits', 'Visit Variance %'
            ])
        
        rows = []
        for category in sorted(all_categories):
            # Normalize values to handle None, NaN, Inf
            comp_visits = DataUtils.normalize_value(comparison_visits.get(category, 0.0))
            anchor_visits_count = DataUtils.normalize_value(anchor_visits.get(category, 0.0))
            
            # Calculate variance
            visit_variance = self._calculate_variance_percentage(comp_visits, anchor_visits_count)
            
            rows.append({
                'Visit Category': category,
                'Comparison Visits': comp_visits,
                'Anchor Visits': anchor_visits_count,
                'Visit Variance %': visit_variance
            })
        
        return pd.DataFrame(rows)

