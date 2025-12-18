"""
Report Generator for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
import pandas as pd
import xlsxwriter
from datetime import datetime
from typing import Dict, Any, Union, List, Tuple, Set

from db_connection import DatabaseConnection
from stored_procedures import StoredProcedures
from utils import DateRangeCalculator, DataUtils
from config import CandidateColumns, VISITS_DEPT_CODE_MAPPING


class ReportGenerator:
    """Generate comprehensive ski resort reports"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✓ Created output directory: {output_dir}")

    # --- Data Processing ---

    def _process_snow(self, data_store: Dict, range_names: List[str]) -> Dict:
        processed_snow = {name: {'snow_24hrs': 0.0, 'base_depth': 0.0} for name in range_names}
        for range_name in range_names:
            dataframe = data_store[range_name]['snow']
            if not dataframe.empty:
                snow_col = DataUtils.get_col(dataframe, CandidateColumns.snow)
                base_col = DataUtils.get_col(dataframe, CandidateColumns.baseDepth)
                if snow_col: 
                    processed_snow[range_name]['snow_24hrs'] = DataUtils.normalize_value(dataframe[snow_col].sum())
                if base_col: 
                    processed_snow[range_name]['base_depth'] = DataUtils.normalize_value(dataframe[base_col].sum())
        return processed_snow

    def _process_visits(self, data_store: Dict, range_names: List[str], all_locations: Set[str]) -> Dict:
        processed_visits = {name: {} for name in range_names}
        for range_name in range_names:
            dataframe = data_store[range_name]['visits']
            if not dataframe.empty:
                location_col = DataUtils.get_col(dataframe, CandidateColumns.location)
                visits_col = DataUtils.get_col(dataframe, CandidateColumns.visits)
                if location_col:
                    if visits_col:
                        grouped = dataframe.groupby(location_col)[visits_col].sum()
                    else:
                        grouped = dataframe.groupby(location_col).size()
                    
                    for location, value in grouped.items():
                        location_str = str(location)
                        processed_visits[range_name][location_str] = DataUtils.normalize_value(value)
                        all_locations.add(location_str)
        return processed_visits

    def _process_revenue(self, data_store: Dict, range_names: List[str], all_departments: Set[str], department_to_title: Dict) -> Dict:
        processed_revenue = {name: {} for name in range_names}
        for range_name in range_names:
            dataframe = data_store[range_name]['revenue']
            if not dataframe.empty:
                code_col = DataUtils.get_col(dataframe, CandidateColumns.departmentCode) or 'department'
                title_col = DataUtils.get_col(dataframe, CandidateColumns.departmentTitle) or 'DepartmentTitle'
                revenue_col = DataUtils.get_col(dataframe, CandidateColumns.revenue) or 'revenue'
                
                if not revenue_col:
                    numeric_cols = dataframe.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0: 
                        revenue_col = numeric_cols[-1]

                if code_col and revenue_col:
                    for _, row in dataframe.iterrows():
                        dept_code = DataUtils.trim_dept_code(row[code_col])
                        if dept_code and title_col in dataframe.columns and pd.notna(row[title_col]):
                            if dept_code not in department_to_title: 
                                department_to_title[dept_code] = str(row[title_col]).strip()
                    
                    grouped = dataframe.groupby(code_col)[revenue_col].sum()
                    for dept, value in grouped.items():
                        dept_str = DataUtils.trim_dept_code(dept)
                        processed_revenue[range_name][dept_str] = DataUtils.normalize_value(value)
                        all_departments.add(dept_str)
                        if dept_str not in department_to_title: 
                            department_to_title[dept_str] = dept_str
        return processed_revenue

    def _process_payroll(self, data_store: Dict, range_names: List[str], is_current_date: bool, 
                         actual_ranges: List[str], processed_revenue: Dict, 
                         all_departments: Set[str], department_to_title: Dict,
                         debug_log_file: Any = None) -> Dict:
        processed_payroll = {name: {} for name in range_names}
        
        for range_name in range_names:
            log_message = f"\n{'='*80}\n  📊 PAYROLL CALCULATION BREAKDOWN - {range_name}\n"
            if is_current_date:
                log_message += "  ⚠️  NOTE: Current date - payroll set to 0 for all departments\n"
                log_message += f"{'='*80}\n"
                for dept_code in processed_revenue[range_name].keys():
                    processed_payroll[range_name][dept_code] = 0.0
                    all_departments.add(dept_code)
            else:
                log_message += f"{'='*80}\n"
                
                # 1. Contract Payroll (Hourly)
                dataframe_payroll = data_store[range_name]['payroll']
                calculated_wages = {}
                contract_rows_by_dept = {}
                
                if not dataframe_payroll.empty:
                    code_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.departmentCode) or 'department'
                    title_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.departmentTitle)
                    start_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.payrollStartTime)
                    end_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.payrollEndTime)
                    rate_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.payrollRate)
                    hours_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.payrollHours)
                    dollar_col = DataUtils.get_col(dataframe_payroll, CandidateColumns.payrollDollarAmount)
                    
                    for _, row in dataframe_payroll.iterrows():
                        dept_code = DataUtils.trim_dept_code(row[code_col])
                        if not dept_code: continue
                        all_departments.add(dept_code)
                        
                        # Update title mapping if present
                        if title_col and pd.notna(row[title_col]) and dept_code not in department_to_title:
                            department_to_title[dept_code] = str(row[title_col]).strip()
                        
                        rate = DataUtils.normalize_value(row[rate_col])
                        hours_from_col = DataUtils.normalize_value(row[hours_col]) if hours_col else 0
                        dollar_amt = DataUtils.normalize_value(row[dollar_col]) if dollar_col else 0
                        
                        # Calculate working hours from punch times
                        if pd.notna(row[start_col]) and pd.notna(row[end_col]):
                            working_hours = (pd.to_datetime(row[end_col]) - pd.to_datetime(row[start_col])).total_seconds() / 3600.0
                        else:
                            working_hours = 0
                        
                        # Apply business logic for wage calculation
                        if hours_from_col > 0:
                            wage = (hours_from_col * rate) + dollar_amt
                        else:
                            wage = (max(0, working_hours) * rate) + dollar_amt
                            
                        calculated_wages[dept_code] = calculated_wages.get(dept_code, 0.0) + wage
                        
                        # Track for logging
                        if dept_code not in contract_rows_by_dept: contract_rows_by_dept[dept_code] = []
                        contract_rows_by_dept[dept_code].append({
                            'start': row[start_col], 'end': row[end_col], 'rate': rate, 
                            'w_hrs': working_hours, 'h_col': hours_from_col, 'd_amt': dollar_amt, 'wage': wage
                        })

                # 2. History & Salary
                dataframe_history = data_store[range_name]['payroll_history']
                dataframe_salary = data_store[range_name]['salary_payroll']
                history_totals = {}
                salary_totals = {}
                
                if not dataframe_history.empty:
                    history_code_column = DataUtils.get_col(dataframe_history, CandidateColumns.departmentCode) or 'department'
                    history_total_column = DataUtils.get_col(dataframe_history, CandidateColumns.historyTotal)
                    for _, row in dataframe_history.iterrows():
                        dept = DataUtils.trim_dept_code(row[history_code_column])
                        if dept: history_totals[dept] = DataUtils.normalize_value(row[history_total_column])
                
                if not dataframe_salary.empty:
                    salary_code_column = DataUtils.get_col(dataframe_salary, CandidateColumns.departmentCode)
                    salary_total_column = DataUtils.get_col(dataframe_salary, CandidateColumns.salaryTotal)
                    salary_title_column = DataUtils.get_col(dataframe_salary, CandidateColumns.departmentTitle)
                    for _, row in dataframe_salary.iterrows():
                        dept = DataUtils.trim_dept_code(row[salary_code_column])
                        if dept: 
                            salary_totals[dept] = DataUtils.normalize_value(row[salary_total_column])
                            if salary_title_column and pd.notna(row[salary_title_column]) and dept not in department_to_title:
                                department_to_title[dept] = str(row[salary_title_column]).strip()

                # 3. Combine Components and Build Log
                relevant_depts = set(calculated_wages.keys()) | set(salary_totals.keys()) | set(history_totals.keys())
                for dept_code in sorted(list(relevant_depts)):
                    dept_title = department_to_title.get(dept_code, dept_code)
                    log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
                    
                    # Log Contract Details
                    log_message += "     📋 Contract Payroll (Hourly):\n"
                    rows = contract_rows_by_dept.get(dept_code, [])
                    for idx, r in enumerate(rows, 1):
                        log_message += f"          Row {idx}: Start={r['start']}, End={r['end']}, WHrs={r['w_hrs']:.2f}, HCol={r['h_col']:.2f}, Rate=${r['rate']:.2f}, Dlr=${r['d_amt']:.2f}, Wage=${r['wage']:.2f}\n"
                    
                    contract_total = calculated_wages.get(dept_code, 0.0)
                    salary_total = salary_totals.get(dept_code, 0.0)
                    history_total = history_totals.get(dept_code, 0.0)
                    
                    if range_name in actual_ranges:
                        final_wage = contract_total + salary_total
                        log_message += f"        • Aggregated Contract: ${contract_total:,.2f}\n"
                        log_message += f"        • Salary for Range: ${salary_total:,.2f}\n"
                        log_message += f"        • History: (Not used for Actual)\n"
                    else:
                        final_wage = history_total
                        log_message += f"        • Contract: (Not used for Prior Year)\n"
                        log_message += f"        • Salary: (Not used for Prior Year)\n"
                        log_message += f"        • Historical Total: ${history_total:,.2f}\n"
                    
                    processed_payroll[range_name][dept_code] = final_wage
                    log_message += f"     ✅ FINAL PAYROLL TOTAL: ${final_wage:,.2f}\n"
                    all_departments.add(dept_code)

            log_message += f"\n{'='*80}\n"
            print(log_message, end='')
            if debug_log_file:
                debug_log_file.write(log_message)
                debug_log_file.flush()
                
        return processed_payroll

    def _process_budget(self, data_store: Dict, range_names: List[str], department_to_title: Dict, visits_mapping: Dict) -> Tuple[Dict, Dict]:
        processed_financial_budget = {name: {} for name in range_names}
        processed_visits_budget = {name: {} for name in range_names}
        
        actual_ranges = ["For The Day (Actual)", "For The Week Ending (Actual)", "Month to Date (Actual)", "For Winter Ending (Actual)"]
        for range_name in actual_ranges:
            dataframe = data_store[range_name]['budget']
            if not dataframe.empty:
                code_col = DataUtils.get_col(dataframe, CandidateColumns.departmentCode)
                type_col = DataUtils.get_col(dataframe, CandidateColumns.budgetType)
                amount_col = DataUtils.get_col(dataframe, CandidateColumns.budgetAmount)
                title_col = DataUtils.get_col(dataframe, CandidateColumns.departmentTitle)
                
                if code_col and type_col and amount_col:
                    for _, row in dataframe.iterrows():
                        dept_code = DataUtils.trim_dept_code(row[code_col])
                        amount = DataUtils.normalize_value(row[amount_col])
                        budget_type = str(row[type_col]).strip().lower() if pd.notna(row[type_col]) else ""
                        
                        if not dept_code: continue
                        
                        if 'visits' in budget_type:
                            if dept_code in visits_mapping:
                                location_name = visits_mapping[dept_code]
                                processed_visits_budget[range_name][location_name] = amount
                        else:
                            if dept_code not in processed_financial_budget[range_name]:
                                processed_financial_budget[range_name][dept_code] = {'Payroll': 0.0, 'Revenue': 0.0}
                            
                            if 'payroll' in budget_type:
                                processed_financial_budget[range_name][dept_code]['Payroll'] = amount
                            elif 'revenue' in budget_type:
                                processed_financial_budget[range_name][dept_code]['Revenue'] = amount
                            
                            if title_col and title_col in row and pd.notna(row[title_col]) and dept_code not in department_to_title:
                                department_to_title[dept_code] = str(row[title_col]).strip()
        return processed_financial_budget, processed_visits_budget

    # --- Utility Logic ---

    def _get_budget_range_name(self, column_name: str) -> str:
        if column_name == "Week Total (Actual) (Budget)":
            return "For The Week Ending (Actual)"
        return column_name.replace(" (Budget)", "")

    # --- Excel Writing ---

    def _write_snow_section(self, worksheet, row, columns, processed_snow, snow_format, row_header_format):
        worksheet.write(row, 0, "Snow 24hrs", row_header_format)
        for i, col_name in enumerate(columns):
            if not col_name.endswith(" (Budget)"):
                value = DataUtils.normalize_value(processed_snow[col_name]['snow_24hrs'])
                worksheet.write(row, i + 1, value, snow_format)
        row += 1
        worksheet.write(row, 0, "Base Depth", row_header_format)
        for i, col_name in enumerate(columns):
            if not col_name.endswith(" (Budget)"):
                value = DataUtils.normalize_value(processed_snow[col_name]['base_depth'])
                worksheet.write(row, i + 1, value, snow_format)
        return row + 2

    def _write_visits_section(self, worksheet, row, columns, processed_visits, processed_budget, 
                              all_locations, resort_name, row_header_format, data_format, header_format):
        worksheet.write(row, 0, "VISITS", header_format)
        row += 1
        for location in sorted(list(all_locations)):
            worksheet.write(row, 0, location, row_header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    loc_key = DataUtils.process_location_name(location, resort_name)
                    value = processed_budget.get(range_key, {}).get(loc_key, 0)
                else:
                    value = processed_visits[col_name].get(location, 0)
                worksheet.write(row, i + 1, DataUtils.normalize_value(value), data_format)
            row += 1
        
        worksheet.write(row, 0, "Total Tickets", header_format)
        for i, col_name in enumerate(columns):
            if col_name.endswith(" (Budget)"):
                range_key = self._get_budget_range_name(col_name)
                total_val = sum(processed_budget.get(range_key, {}).values())
            else:
                total_val = sum(processed_visits[col_name].values())
            worksheet.write(row, i + 1, DataUtils.normalize_value(total_val), data_format)
        return row + 2

    def _write_financials_section(self, worksheet, row, columns, processed_revenue, processed_payroll, 
                                  processed_budget, sorted_depts, dept_to_title, 
                                  row_header_format, data_format, header_format, percent_format):
        worksheet.write(row, 0, "FINANCIALS", header_format)
        row += 1
        for dept_code in sorted_depts:
            trimmed_code = DataUtils.trim_dept_code(dept_code)
            title = dept_to_title.get(trimmed_code, trimmed_code)
            
            # Revenue Row
            worksheet.write(row, 0, f"{title} - Revenue", row_header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    val = processed_budget.get(range_key, {}).get(trimmed_code, {}).get('Revenue', 0)
                else:
                    val = processed_revenue[col_name].get(trimmed_code, 0)
                worksheet.write(row, i + 1, DataUtils.normalize_value(val), data_format)
            row += 1
            
            # Payroll Row
            worksheet.write(row, 0, f"{title} - Payroll", row_header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    val = processed_budget.get(range_key, {}).get(trimmed_code, {}).get('Payroll', 0)
                else:
                    val = processed_payroll[col_name].get(trimmed_code, 0)
                worksheet.write(row, i + 1, DataUtils.normalize_value(val), data_format)
            row += 1
            
            # PR% Row
            row_header_format_text = f"PR % of {title}"
            worksheet.write(row, 0, row_header_format_text, row_header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    budget_data = processed_budget.get(range_key, {}).get(trimmed_code, {})
                    revenue = abs(DataUtils.normalize_value(budget_data.get('Revenue', 0)))
                    payroll = abs(DataUtils.normalize_value(budget_data.get('Payroll', 0)))
                else:
                    revenue = abs(DataUtils.normalize_value(processed_revenue[col_name].get(trimmed_code, 0)))
                    payroll = abs(DataUtils.normalize_value(processed_payroll[col_name].get(trimmed_code, 0)))
                
                percentage = (payroll / revenue * 100) if revenue != 0 else 0
                worksheet.write(row, i + 1, percentage, percent_format)
            row += 1
        return row + 1

    def _write_totals_section(self, worksheet, row, columns, processed_revenue, processed_payroll, 
                              processed_budget, sorted_depts, data_format, header_format, percent_format):
        labels = ["Total Revenue", "Total Payroll", "PR % of Total Revenue", "Net Total Revenue"]
        for label in labels:
            worksheet.write(row, 0, label, header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    revenue_total = sum(DataUtils.normalize_value(processed_budget.get(range_key, {}).get(DataUtils.trim_dept_code(d), {}).get('Revenue', 0)) for d in sorted_depts)
                    payroll_total = sum(DataUtils.normalize_value(processed_budget.get(range_key, {}).get(DataUtils.trim_dept_code(d), {}).get('Payroll', 0)) for d in sorted_depts)
                else:
                    revenue_total = sum(processed_revenue[col_name].values())
                    payroll_total = sum(processed_payroll[col_name].values())
                
                if label == "Total Revenue": 
                    final_value = revenue_total
                elif label == "Total Payroll": 
                    final_value = payroll_total
                elif label == "PR % of Total Revenue": 
                    final_value = (abs(payroll_total) / abs(revenue_total) * 100) if revenue_total != 0 else 0
                else: 
                    final_value = revenue_total - payroll_total
                
                worksheet.write(row, i + 1, final_value, percent_format if "PR %" in label else data_format)
            row += 1

    # --- Main Generator ---

    def generate_comprehensive_report(self, resort_config: Dict, run_date: Union[str, datetime] = None, 
                                    debug: bool = False, file_name_postfix: str = None) -> str:
        """Generate the comprehensive Excel report for a resort."""
        
        # 1. Setup Dates and Config
        current_now = datetime.now()
        if run_date is None:
            report_date, is_current = current_now, True
        elif isinstance(run_date, str):
            report_date = datetime.strptime(run_date, "%m/%d/%Y")
            is_current = (report_date.date() == current_now.date())
        else:
            report_date, is_current = run_date, (run_date.date() == current_now.date())
        
        resort_name = resort_config['resortName']
        db_name = resort_config.get('dbName', resort_name)
        group_num = resort_config.get('groupNum', -1)
        
        date_calculator = DateRangeCalculator(report_date, is_current_date=is_current, use_exact_date=not is_current)
        ranges = date_calculator.get_all_ranges()
        range_names_ordered = list(ranges.keys())
        report_date_string = ranges["For The Day (Actual)"][0].strftime("%Y%m%d")
        
        # 2. Setup Debug Logging
        debug_directory, debug_log_handle = None, None
        if debug:
            sanitized_resort = DataUtils.sanitize_filename(resort_name).lower()
            debug_directory = os.path.join(self.output_dir, f"Debug-{sanitized_resort}-{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}")
            if not os.path.exists(debug_directory): os.makedirs(debug_directory)
            debug_log_handle = open(os.path.join(debug_directory, "DebugLogs.txt"), 'w', encoding='utf-8')

        # 3. Fetch Raw Data
        data_store = {name: {} for name in range_names_ordered}
        actual_range_names = ["For The Day (Actual)", "For The Week Ending (Actual)", "Month to Date (Actual)", "For Winter Ending (Actual)"]
        
        with DatabaseConnection() as conn:
            stored_procedures_handler = StoredProcedures(conn)
            for name in range_names_ordered:
                start, end = ranges[name]
                print(f"   ⏳ Fetching {name} ({start.date()} to {end.date()})...")
                
                data_store[name]['revenue'] = stored_procedures_handler.execute_revenue(db_name, group_num, start, end)
                data_store[name]['visits'] = stored_procedures_handler.execute_visits(resort_name, start, end)
                data_store[name]['snow'] = stored_procedures_handler.execute_weather(resort_name, start, end)
                
                if not is_current:
                    if name in actual_range_names:
                        data_store[name]['payroll'] = stored_procedures_handler.execute_payroll(resort_name, start, end)
                        data_store[name]['salary_payroll'] = stored_procedures_handler.execute_payroll_salary(resort_name, start, end)
                        
                        # Special handling for weekly budget range
                        budget_start, budget_end = (date_calculator.week_total_actual() if name == "For The Week Ending (Actual)" else (start, end))
                        data_store[name]['budget'] = stored_procedures_handler.execute_budget(resort_name, budget_start, budget_end)
                    else:
                        data_store[name]['payroll_history'] = stored_procedures_handler.execute_payroll_history(resort_name, start, end)

                # Ensure all required keys exist and export debug files
                for key in ['revenue', 'visits', 'snow', 'payroll', 'salary_payroll', 'budget', 'payroll_history']:
                    if key not in data_store[name]: data_store[name][key] = pd.DataFrame()
                    if debug and not data_store[name][key].empty:
                        self._export_sp_result(data_store[name][key], name, key.capitalize(), resort_name, debug_directory)

        # 4. Process Raw Data into Structures
        locations_set, departments_set, code_to_title_map = set(), set(), {}
        processed_snow = self._process_snow(data_store, range_names_ordered)
        processed_visits = self._process_visits(data_store, range_names_ordered, locations_set)
        processed_revenue = self._process_revenue(data_store, range_names_ordered, departments_set, code_to_title_map)
        processed_payroll = self._process_payroll(data_store, range_names_ordered, is_current, 
                                                 actual_range_names, processed_revenue, departments_set, 
                                                 code_to_title_map, debug_log_handle)
        processed_budget, processed_visits_budget = self._process_budget(data_store, range_names_ordered, 
                                                                       code_to_title_map, VISITS_DEPT_CODE_MAPPING)

        # 5. Write Final Excel Report
        file_path = os.path.join(self.output_dir, f"{DataUtils.sanitize_filename(resort_name)}_Report_{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}.xlsx")
        workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})
        worksheet = workbook.add_worksheet("Report")
        
        # Formats
        header_format = workbook.add_format({'bold':True,'align':'center','bg_color':'#D3D3D3','border':1,'text_wrap':True})
        row_header_format = workbook.add_format({'bold':True,'border':1})
        data_format = workbook.add_format({'border':1, 'num_format':'#,##0.00'})
        snow_format = workbook.add_format({'border':1, 'num_format':'0.0'})
        percent_format = workbook.add_format({'border':1, 'num_format':'0"%"'})
        
        # Write Title and Column Headers
        day_actual_start = ranges["For The Day (Actual)"][0]
        title_text = f"{resort_name} Resort\nDaily Management Report\nAs of {day_actual_start.strftime('%A')} - {day_actual_start.strftime('%d %B, %Y').lstrip('0')}"
        worksheet.write(0, 0, title_text, header_format)
        
        column_structure = []
        for name in range_names_ordered:
            column_structure.append(name)
            if name in actual_range_names:
                column_structure.append("Week Total (Actual) (Budget)" if name == "For The Week Ending (Actual)" else f"{name} (Budget)")
        
        for i, col_name in enumerate(column_structure):
            if col_name.endswith(" (Budget)"):
                start, end = (date_calculator.week_total_actual() if col_name == "Week Total (Actual) (Budget)" else ranges[self._get_budget_range_name(col_name)])
            else:
                start, end = ranges[col_name]
            worksheet.write(0, i + 1, f"{col_name}\n{start.strftime('%b %d')} - {end.strftime('%b %d')}", header_format)
            worksheet.set_column(i + 1, i + 1, 18)
        
        worksheet.set_column(0, 0, 30)
        worksheet.freeze_panes(1, 1)
        
        # Write Data Sections
        current_row = self._write_snow_section(worksheet, 1, column_structure, processed_snow, snow_format, row_header_format)
        current_row = self._write_visits_section(worksheet, current_row, column_structure, processed_visits, processed_visits_budget, locations_set, resort_name, row_header_format, data_format, header_format)
        current_row = self._write_financials_section(worksheet, current_row, column_structure, processed_revenue, processed_payroll, processed_budget, sorted(list(departments_set)), code_to_title_map, row_header_format, data_format, header_format, percent_format)
        self._write_totals_section(worksheet, current_row + 1, column_structure, processed_revenue, processed_payroll, processed_budget, sorted(list(departments_set)), data_format, header_format, percent_format)
        
        workbook.close()
        if debug_log_handle: debug_log_handle.close()
        print(f"✓ Report saved: {file_path}")
        return file_path

    def _export_sp_result(self, dataframe: pd.DataFrame, range_name: str, stored_procedure_name: str, resort_name: str, export_directory: str = None) -> str:
        sanitized_range = DataUtils.sanitize_filename(range_name)
        sanitized_sp = DataUtils.sanitize_filename(stored_procedure_name)
        file_path = os.path.join(export_directory or self.output_dir, f"{sanitized_range}_{sanitized_sp}.xlsx")
        
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
        header_format, data_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1}), workbook.add_format({'border': 1})
        
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
