"""
Analysis Engine for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
import pandas as pd
import xlsxwriter
from datetime import datetime, timedelta
from typing import Dict, Any, Union, List, Tuple, Set, Optional

from db_connection import DatabaseConnection
from stored_procedures import StoredProcedures
from utils import DateRangeCalculator, DataUtils, execute_with_retry, log
from config import CandidateColumns, VISITS_DEPT_CODE_MAPPING
from webhook_client import send_to_n8n_webhooks


class AnalysisEngine:
    """Analysis engine for generating comprehensive ski resort reports and insights"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        self.insights_dir = os.path.join(current_file_dir, "insights")
        if not os.path.exists(self.insights_dir):
            os.makedirs(self.insights_dir)

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

    def _process_visits_dataframe(self, dataframe: pd.DataFrame, all_locations: Set[str] = None) -> Dict[str, float]:
        processed_visits = {}
        if all_locations is None:
            all_locations = set()
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
                all_locations.add(location_str)
        return processed_visits

    def _process_visits(self, data_store: Dict, range_names: List[str], all_locations: Set[str]) -> Dict:
        processed_visits = {name: {} for name in range_names}
        for range_name in range_names:
            dataframe = data_store[range_name]['visits']
            processed_visits[range_name] = self._process_visits_dataframe(dataframe, all_locations)
        return processed_visits

    def _process_revenue_dataframe(self, dataframe: pd.DataFrame, department_to_title: Dict, 
                                   all_departments: Set[str] = None) -> Dict[str, float]:
        processed_revenue = {}
        if all_departments is None:
            all_departments = set()
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
            for _, row in dataframe.iterrows():
                dept_code = DataUtils.trim_dept_code(row[code_col])
                if dept_code and title_col in dataframe.columns and pd.notna(row[title_col]):
                    if dept_code not in department_to_title:
                        department_to_title[dept_code] = str(row[title_col]).strip()
            grouped = dataframe.groupby(code_col)[revenue_col].sum()
            for dept, value in grouped.items():
                dept_str = DataUtils.trim_dept_code(dept)
                processed_revenue[dept_str] = DataUtils.normalize_value(value)
                all_departments.add(dept_str)
                if dept_str not in department_to_title:
                    department_to_title[dept_str] = dept_str
        return processed_revenue

    def _process_revenue(self, data_store: Dict, range_names: List[str], all_departments: Set[str], department_to_title: Dict, debug_log_file: Any = None) -> Dict:
        processed_revenue = {name: {} for name in range_names}
        for range_name in range_names:
            log_message = f"\n{'='*80}\n  💰 REVENUE CALCULATION BREAKDOWN - {range_name}\n"
            log_message += f"{'='*80}\n"
            
            dataframe = data_store[range_name]['revenue']
            
            if dataframe.empty:
                log_message += "  ⚠️  No revenue data available\n"
                processed_revenue[range_name] = {}
            else:
                code_col = DataUtils.get_col(dataframe, CandidateColumns.departmentCode) or 'department'
                revenue_col = DataUtils.get_col(dataframe, CandidateColumns.revenue) or 'revenue'
                if not revenue_col:
                    numeric_cols = dataframe.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        revenue_col = numeric_cols[-1]
                
                revenue_rows_by_dept = {}
                if code_col and revenue_col:
                    for _, row in dataframe.iterrows():
                        dept_code = DataUtils.trim_dept_code(row[code_col])
                        if not dept_code:
                            continue
                        revenue_value = DataUtils.normalize_value(row[revenue_col])
                        if dept_code not in revenue_rows_by_dept:
                            revenue_rows_by_dept[dept_code] = []
                        revenue_rows_by_dept[dept_code].append({
                            'dept_code_raw': row[code_col],
                            'revenue': revenue_value
                        })
                
                processed_revenue[range_name] = self._process_revenue_dataframe(dataframe, department_to_title, all_departments)
                
                # Log revenue details for each department
                for dept_code in sorted(list(processed_revenue[range_name].keys())):
                    dept_title = department_to_title.get(dept_code, dept_code)
                    revenue_total = processed_revenue[range_name][dept_code]
                    log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
                    log_message += "     📋 Revenue Rows:\n"
                    rows = revenue_rows_by_dept.get(dept_code, [])
                    for idx, r in enumerate(rows, 1):
                        log_message += f"          Row {idx}: DeptCode='{r['dept_code_raw']}', Revenue=${r['revenue']:,.2f}\n"
                    log_message += f"        • Aggregated Revenue: ${revenue_total:,.2f}\n"
                    log_message += f"     ✅ FINAL REVENUE TOTAL: ${revenue_total:,.2f}\n"
            
            log_message += f"\n{'='*80}\n"
            if debug_log_file:
                debug_log_file.write(log_message)
                debug_log_file.flush()
        
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
                        
                        if title_col and pd.notna(row[title_col]) and dept_code not in department_to_title:
                            department_to_title[dept_code] = str(row[title_col]).strip()
                        
                        rate = DataUtils.normalize_value(row[rate_col])
                        hours_from_col = DataUtils.normalize_value(row[hours_col]) if hours_col else 0
                        dollar_amt = DataUtils.normalize_value(row[dollar_col]) if dollar_col else 0
                        
                        working_hours = 0.0
                        if pd.notna(row[start_col]) and pd.notna(row[end_col]):
                            try:
                                start_time = pd.to_datetime(row[start_col])
                                end_time = pd.to_datetime(row[end_col])
                                if pd.notna(start_time) and pd.notna(end_time):
                                    seconds_diff = (end_time - start_time).total_seconds()
                                    working_hours = max(0.0, seconds_diff / 3600.0)
                                    working_hours = DataUtils.normalize_value(working_hours)
                            except (ValueError, TypeError, OverflowError):
                                working_hours = 0.0
                        
                        try:
                            if hours_from_col > 0:
                                wage = (hours_from_col * rate) + dollar_amt
                            else:
                                wage = (working_hours * rate) + dollar_amt
                            wage = DataUtils.normalize_value(wage)
                        except (OverflowError, ValueError, TypeError):
                            wage = 0.0
                            
                        calculated_wages[dept_code] = DataUtils.normalize_value(
                            calculated_wages.get(dept_code, 0.0) + wage
                        )
                        
                        if dept_code not in contract_rows_by_dept: contract_rows_by_dept[dept_code] = []
                        contract_rows_by_dept[dept_code].append({
                            'start': row[start_col], 'end': row[end_col], 'rate': rate, 
                            'w_hrs': working_hours, 'h_col': hours_from_col, 'd_amt': dollar_amt, 'wage': wage
                        })

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

                relevant_depts = set(calculated_wages.keys()) | set(salary_totals.keys()) | set(history_totals.keys())
                for dept_code in sorted(list(relevant_depts)):
                    dept_title = department_to_title.get(dept_code, dept_code)
                    log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
                    
                    log_message += "     📋 Contract Payroll (Hourly):\n"
                    rows = contract_rows_by_dept.get(dept_code, [])
                    for idx, r in enumerate(rows, 1):
                        log_message += f"          Row {idx}: Start={r['start']}, End={r['end']}, WHrs={r['w_hrs']:.2f}, HCol={r['h_col']:.2f}, Rate=${r['rate']:.2f}, Dlr=${r['d_amt']:.2f}, Wage=${r['wage']:.2f}\n"
                    
                    contract_total = DataUtils.normalize_value(calculated_wages.get(dept_code, 0.0))
                    salary_total = DataUtils.normalize_value(salary_totals.get(dept_code, 0.0))
                    history_total = DataUtils.normalize_value(history_totals.get(dept_code, 0.0))
                    
                    if range_name in actual_ranges:
                        try:
                            final_wage = DataUtils.normalize_value(contract_total + salary_total)
                        except (OverflowError, ValueError, TypeError):
                            final_wage = 0.0
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
            if debug_log_file:
                debug_log_file.write(log_message)
                debug_log_file.flush()
                
        return processed_payroll

    def _process_payroll_actual_dataframes(self, payroll_df: pd.DataFrame, salary_df: pd.DataFrame,
                                           department_to_title: Dict, all_departments: Set[str] = None,
                                           date_label: str = "", debug_log_file: Any = None) -> Dict[str, float]:
        if all_departments is None:
            all_departments = set()
        log_message = f"\n{'='*80}\n  📊 PAYROLL CALCULATION BREAKDOWN - {date_label}\n"
        log_message += f"  Method: Actual Ranges (Contract + Salary)\n"
        log_message += f"{'='*80}\n"
        processed_payroll = {}
        calculated_wages = {}
        contract_rows_by_dept = {}
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
                all_departments.add(dept_code)
                if title_col and pd.notna(row[title_col]) and dept_code not in department_to_title:
                    department_to_title[dept_code] = str(row[title_col]).strip()
                rate = DataUtils.normalize_value(row[rate_col])
                hours_from_col = DataUtils.normalize_value(row[hours_col]) if hours_col else 0
                dollar_amt = DataUtils.normalize_value(row[dollar_col]) if dollar_col else 0
                working_hours = 0.0
                if pd.notna(row[start_col]) and pd.notna(row[end_col]):
                    try:
                        start_time = pd.to_datetime(row[start_col])
                        end_time = pd.to_datetime(row[end_col])
                        if pd.notna(start_time) and pd.notna(end_time):
                            seconds_diff = (end_time - start_time).total_seconds()
                            working_hours = max(0.0, seconds_diff / 3600.0)
                            working_hours = DataUtils.normalize_value(working_hours)
                    except (ValueError, TypeError, OverflowError):
                        working_hours = 0.0
                try:
                    if hours_from_col > 0:
                        wage = (hours_from_col * rate) + dollar_amt
                    else:
                        wage = (working_hours * rate) + dollar_amt
                    wage = DataUtils.normalize_value(wage)
                except (OverflowError, ValueError, TypeError):
                    wage = 0.0
                calculated_wages[dept_code] = DataUtils.normalize_value(
                    calculated_wages.get(dept_code, 0.0) + wage
                )
                if dept_code not in contract_rows_by_dept:
                    contract_rows_by_dept[dept_code] = []
                contract_rows_by_dept[dept_code].append({
                    'start': row[start_col], 'end': row[end_col], 'rate': rate,
                    'w_hrs': working_hours, 'h_col': hours_from_col, 'd_amt': dollar_amt, 'wage': wage
                })
        salary_totals = {}
        if not salary_df.empty:
            salary_code_column = DataUtils.get_col(salary_df, CandidateColumns.departmentCode)
            salary_total_column = DataUtils.get_col(salary_df, CandidateColumns.salaryTotal)
            salary_title_column = DataUtils.get_col(salary_df, CandidateColumns.departmentTitle)
            for _, row in salary_df.iterrows():
                dept = DataUtils.trim_dept_code(row[salary_code_column])
                if dept:
                    salary_totals[dept] = DataUtils.normalize_value(row[salary_total_column])
                    all_departments.add(dept)
                    if salary_title_column and pd.notna(row[salary_title_column]) and dept not in department_to_title:
                        department_to_title[dept] = str(row[salary_title_column]).strip()
        relevant_depts = set(calculated_wages.keys()) | set(salary_totals.keys())
        for dept_code in sorted(list(relevant_depts)):
            dept_title = department_to_title.get(dept_code, dept_code)
            log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
            log_message += "     📋 Contract Payroll (Hourly):\n"
            rows = contract_rows_by_dept.get(dept_code, [])
            for idx, r in enumerate(rows, 1):
                log_message += f"          Row {idx}: Start={r['start']}, End={r['end']}, WHrs={r['w_hrs']:.2f}, HCol={r['h_col']:.2f}, Rate=${r['rate']:.2f}, Dlr=${r['d_amt']:.2f}, Wage=${r['wage']:.2f}\n"
            contract_total = DataUtils.normalize_value(calculated_wages.get(dept_code, 0.0))
            salary_total = DataUtils.normalize_value(salary_totals.get(dept_code, 0.0))
            log_message += f"        • Aggregated Contract: ${contract_total:,.2f}\n"
            log_message += f"        • Salary for Range: ${salary_total:,.2f}\n"
            try:
                final_wage = DataUtils.normalize_value(contract_total + salary_total)
            except (OverflowError, ValueError, TypeError):
                final_wage = 0.0
            processed_payroll[dept_code] = final_wage
            log_message += f"     ✅ FINAL PAYROLL TOTAL: ${final_wage:,.2f}\n"
        log_message += f"\n{'='*80}\n"
        if debug_log_file:
            debug_log_file.write(log_message)
            debug_log_file.flush()
        return processed_payroll

    def _process_payroll_prior_year_dataframe(self, history_df: pd.DataFrame,
                                             department_to_title: Dict, all_departments: Set[str] = None,
                                             date_label: str = "", debug_log_file: Any = None) -> Dict[str, float]:
        if all_departments is None:
            all_departments = set()
        log_message = f"\n{'='*80}\n  📊 PAYROLL CALCULATION BREAKDOWN - {date_label}\n"
        log_message += f"  Method: Prior Year Ranges (History Only)\n"
        log_message += f"{'='*80}\n"
        processed_payroll = {}
        if history_df.empty:
            log_message += "  ⚠️  No payroll history data available\n"
            log_message += f"\n{'='*80}\n"
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
                all_departments.add(dept)
        for dept_code in sorted(list(processed_payroll.keys())):
            dept_title = department_to_title.get(dept_code, dept_code)
            history_total = processed_payroll[dept_code]
            log_message += f"\n  📁 Department: {dept_code} ({dept_title})\n     {'─'*76}\n"
            log_message += f"        • Contract: (Not used for Prior Year)\n"
            log_message += f"        • Salary: (Not used for Prior Year)\n"
            log_message += f"        • Historical Total: ${history_total:,.2f}\n"
            log_message += f"     ✅ FINAL PAYROLL TOTAL: ${history_total:,.2f}\n"
        log_message += f"\n{'='*80}\n"
        if debug_log_file:
            debug_log_file.write(log_message)
            debug_log_file.flush()
        return processed_payroll

    def _process_budget_dataframe(self, dataframe: pd.DataFrame, department_to_title: Dict, 
                                  visits_mapping: Dict = None) -> Dict[str, Dict[str, float]]:
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

    def _process_budget(self, data_store: Dict, range_names: List[str], department_to_title: Dict, visits_mapping: Dict) -> Tuple[Dict, Dict]:
        processed_financial_budget = {name: {} for name in range_names}
        processed_visits_budget = {name: {} for name in range_names}
        actual_ranges = ["For The Day (Actual)", "For The Week Ending (Actual)", "Month to Date (Actual)", "For Winter Ending (Actual)"]
        for range_name in actual_ranges:
            # For DMR report: use budget_week_total (full week) for week ending, regular budget for others
            budget_key = 'budget_week_total' if range_name == "For The Week Ending (Actual)" else 'budget'
            dataframe = data_store[range_name].get(budget_key, pd.DataFrame())
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
                        if not dept_code:
                            continue
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

    def _get_budget_range_name(self, column_name: str) -> str:
        if column_name == "Week Total (Actual) (Budget)":
            return "For The Week Ending (Actual)"
        return column_name.replace(" (Budget)", "")

    def _get_range_short_name(self, range_name: str) -> str:
        """Get short name for range used in column headers."""
        mapping = {
            "For The Day (Actual)": "Day",
            "For The Week Ending (Actual)": "Week Ending",
            "Month to Date (Actual)": "Month to Date",
            "For Winter Ending (Actual)": "Winter Ending"
        }
        return mapping.get(range_name, range_name)

    def _get_range_data(self, range_name: str, data_type: str, key: str,
                        processed_visits: Dict, processed_revenue: Dict, processed_payroll: Dict,
                        processed_budget: Dict, processed_visits_budget: Dict, resort_name: str = '') -> Tuple[float, float, float, float, float]:
        """Get actual, budget, prior values and variances for a specific range and key."""
        prior_range_name = range_name.replace("(Actual)", "(Prior Year)")
        
        if data_type == 'visits':
            lookup_key = key
            budget_lookup_key = DataUtils.process_location_name(key, resort_name) if key else key
            actual_val = DataUtils.normalize_value(processed_visits.get(range_name, {}).get(lookup_key, 0.0))
            budget_val = DataUtils.normalize_value(processed_visits_budget.get(range_name, {}).get(budget_lookup_key, 0.0))
            prior_val = DataUtils.normalize_value(processed_visits.get(prior_range_name, {}).get(lookup_key, 0.0))
        elif data_type == 'payroll':
            lookup_key = DataUtils.trim_dept_code(key) if key else key
            actual_val = DataUtils.normalize_value(processed_payroll.get(range_name, {}).get(lookup_key, 0.0))
            budget_val = DataUtils.normalize_value(processed_budget.get(range_name, {}).get(lookup_key, {}).get('Payroll', 0.0))
            prior_val = DataUtils.normalize_value(processed_payroll.get(prior_range_name, {}).get(lookup_key, 0.0))
        elif data_type == 'revenue':
            lookup_key = DataUtils.trim_dept_code(key) if key else key
            actual_val = DataUtils.normalize_value(processed_revenue.get(range_name, {}).get(lookup_key, 0.0))
            budget_val = DataUtils.normalize_value(processed_budget.get(range_name, {}).get(lookup_key, {}).get('Revenue', 0.0))
            prior_val = DataUtils.normalize_value(processed_revenue.get(prior_range_name, {}).get(lookup_key, 0.0))
        else:
            actual_val = budget_val = prior_val = 0.0
        
        var_budget = DataUtils.calculate_variance_percentage(budget_val, actual_val)
        var_prior = DataUtils.calculate_variance_percentage(prior_val, actual_val)
        
        return actual_val, budget_val, prior_val, var_budget, var_prior

    def _build_insights_row(self, row_header: str, dept_code: str, column_names: List[str],
                           range_names: List[str], data_type: str, key: str,
                           processed_visits: Dict, processed_revenue: Dict, processed_payroll: Dict,
                           processed_budget: Dict, processed_visits_budget: Dict, resort_name: str = '') -> Dict:
        """Build a single row for insights dataframe."""
        row = {col: '' for col in column_names}
        row['Row Header'] = row_header
        row['Dept Code'] = dept_code
        
        for range_name in range_names:
            actual, budget, prior, var_budget, var_prior = self._get_range_data(
                range_name, data_type, key, processed_visits, processed_revenue,
                processed_payroll, processed_budget, processed_visits_budget, resort_name
            )
            range_short = self._get_range_short_name(range_name)
            
            row[f'Value ({range_short} - Actual)'] = actual
            row[f'Budget ({range_short} - Actual)'] = budget
            row[f'Value ({range_short} - Prior year)'] = prior
            row[f'Value-Budget Variance % ({range_short} Actual)'] = var_budget
            row[f'Actual-Prior value Variance % ({range_short})'] = var_prior
        
        return row

    def _generate_insights_dataframe(self,
                                     processed_visits: Dict,
                                     processed_revenue: Dict,
                                     processed_payroll: Dict,
                                     processed_budget: Dict,
                                     processed_visits_budget: Dict,
                                     all_locations: Set[str],
                                     all_departments: Set[str],
                                     department_to_title: Dict,
                                     resort_name: str) -> pd.DataFrame:
        """Generate consolidated insights dataframe with all time periods in columns."""
        rows = []
        actual_range_names = ["For The Day (Actual)", "For The Week Ending (Actual)", "Month to Date (Actual)", "For Winter Ending (Actual)"]
        
        column_names = ['Row Header', 'Dept Code']
        for range_name in actual_range_names:
            range_short = self._get_range_short_name(range_name)
            column_names.extend([
                f'Value ({range_short} - Actual)',
                f'Budget ({range_short} - Actual)',
                f'Value ({range_short} - Prior year)',
                f'Value-Budget Variance % ({range_short} Actual)',
                f'Actual-Prior value Variance % ({range_short})'
            ])
        
        row = {col: '' for col in column_names}
        row['Row Header'] = 'Visits'
        rows.append(row)
        
        for location in sorted(all_locations):
            rows.append(self._build_insights_row(location, '', column_names, actual_range_names,
                                                'visits', location, processed_visits, processed_revenue,
                                                processed_payroll, processed_budget, processed_visits_budget, resort_name))
        
        row = {col: '' for col in column_names}
        row['Row Header'] = 'Payroll'
        rows.append(row)
        
        for dept_code in sorted(all_departments):
            dept_title = department_to_title.get(dept_code, dept_code)
            rows.append(self._build_insights_row(dept_title, dept_code, column_names, actual_range_names,
                                                'payroll', dept_code, processed_visits, processed_revenue,
                                                processed_payroll, processed_budget, processed_visits_budget, resort_name))
        
        row = {col: '' for col in column_names}
        row['Row Header'] = 'Revenue'
        rows.append(row)
        
        for dept_code in sorted(all_departments):
            dept_title = department_to_title.get(dept_code, dept_code)
            rows.append(self._build_insights_row(dept_title, dept_code, column_names, actual_range_names,
                                                'revenue', dept_code, processed_visits, processed_revenue,
                                                processed_payroll, processed_budget, processed_visits_budget, resort_name))
        
        return pd.DataFrame(rows)

    def _get_top_bottom_rows(self, df: pd.DataFrame, n: int = 3) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Extract top and bottom N rows for each variance column, categorized by section (Visits, Payroll, Revenue)."""
        if df is None or df.empty:
            return {}
        
        variance_cols = [col for col in df.columns if 'Variance %' in col]
        if not variance_cols:
            return {}
        
        if 'Row Header' not in df.columns:
            return {}
        
        # Get section boundaries
        section_headers = ['Visits', 'Payroll', 'Revenue']
        sections = {}
        current_section = None
        section_start_idx = None
        
        for idx, row in df.iterrows():
            row_header = row.get('Row Header', '')
            if row_header in section_headers:
                # Save previous section if exists
                if current_section and section_start_idx is not None:
                    sections[current_section] = (section_start_idx, idx)
                # Start new section
                current_section = row_header
                section_start_idx = idx
            elif current_section and row_header and row_header not in section_headers:
                # This is a data row in current section
                pass
        
        # Save last section
        if current_section and section_start_idx is not None:
            sections[current_section] = (section_start_idx, len(df))
        
        # Process each section separately
        result = {}
        for variance_col in variance_cols:
            if variance_col not in df.columns:
                continue
            
            result[variance_col] = {}
            
            for section_name, (start_idx, end_idx) in sections.items():
                # Get rows for this section (excluding the section header row)
                section_df = df.iloc[start_idx + 1:end_idx].copy()
                
                if section_df.empty:
                    continue
                
                # Convert variance column to numeric
                if variance_col in section_df.columns:
                    section_df[variance_col] = pd.to_numeric(section_df[variance_col], errors='coerce')
                
                if section_df[variance_col].isna().all():
                    continue
                
                try:
                    sorted_rows_desc = section_df.sort_values(
                        by=variance_col, 
                        ascending=False, 
                        na_position='last'
                    ).copy()
                    
                    sorted_rows_asc = section_df.sort_values(
                        by=variance_col, 
                        ascending=True, 
                        na_position='last'
                    ).copy()
                    
                    top_rows = sorted_rows_desc.head(n).copy() if len(sorted_rows_desc) >= n else sorted_rows_desc.copy()
                    bottom_rows = sorted_rows_asc.head(n).copy() if len(sorted_rows_asc) >= n else sorted_rows_asc.copy()
                    
                    if not top_rows.empty or not bottom_rows.empty:
                        result[variance_col][section_name] = {
                            'top': top_rows,
                            'bottom': bottom_rows
                        }
                except (KeyError, ValueError):
                    continue
        
        return result

    def _log_top_bottom_insights(self, df: pd.DataFrame, insight_type: str, resort_name: str, 
                                 report_date_string: str, file_name_postfix: str = None):
        """Log and export top/bottom 3 insights with full rows for each variance column."""
        if df is None or df.empty:
            return
        
        variance_top_bottom_dict = self._get_top_bottom_rows(df, n=3)
        if not variance_top_bottom_dict:
            return
        
        has_data = any(
            any(not section_data['top'].empty or not section_data['bottom'].empty 
                for section_data in sections_dict.values())
            for sections_dict in variance_top_bottom_dict.values()
        )
        if not has_data:
            return

        try:
            useful_file = os.path.join(
                self.output_dir, 
                f"useful_{insight_type}_Insights_{DataUtils.sanitize_filename(resort_name)}_{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}.xlsx"
            )
            
            workbook = xlsxwriter.Workbook(useful_file, {'nan_inf_to_errors': True})
            worksheet = workbook.add_worksheet("Top & Bottom 3")
            
            header_format = workbook.add_format({
                'bold': True, 
                'align': 'center', 
                'bg_color': '#D3D3D3', 
                'border': 1, 
                'text_wrap': True
            })
            section_header_format = workbook.add_format({
                'bold': True, 
                'align': 'left', 
                'bg_color': '#B8CCE4', 
                'border': 1,
                'font_size': 11
            })
            data_format = workbook.add_format({'border': 1, 'num_format': '#,##0.00'})
            percent_format = workbook.add_format({'border': 1, 'num_format': '0.00"%"'})
            empty_format = workbook.add_format({'border': 1})
            
            # Get column names from first available section
            column_names = []
            for sections_dict in variance_top_bottom_dict.values():
                for section_name, top_bottom in sections_dict.items():
                    if not top_bottom['top'].empty:
                        column_names = list(top_bottom['top'].columns)
                        break
                    elif not top_bottom['bottom'].empty:
                        column_names = list(top_bottom['bottom'].columns)
                        break
                if column_names:
                    break
            
            if not column_names:
                workbook.close()
                return
            
            for col_idx, col_name in enumerate(column_names):
                worksheet.write(0, col_idx, col_name, header_format)
            
            current_row = 1
            
            for variance_col_name, sections_dict in variance_top_bottom_dict.items():
                if not sections_dict:
                    continue
                
                # Variance category header
                if current_row > 1:
                    current_row += 1
                
                variance_header_text = f"VARIANCE CATEGORY: {variance_col_name}"
                worksheet.merge_range(
                    current_row, 0, current_row, len(column_names) - 1,
                    variance_header_text, section_header_format
                )
                current_row += 1
                
                # Process each section (Visits, Payroll, Revenue)
                for section_name in ['Visits', 'Payroll', 'Revenue']:
                    if section_name not in sections_dict:
                        continue
                    
                    top_bottom = sections_dict[section_name]
                    if top_bottom['top'].empty and top_bottom['bottom'].empty:
                        continue
                    
                    # Section header - Top 3
                    section_header_text = f"{section_name} - TOP 3"
                    worksheet.merge_range(
                        current_row, 0, current_row, len(column_names) - 1,
                        section_header_text, section_header_format
                    )
                    current_row += 1
                    
                    if not top_bottom['top'].empty:
                        for _, row in top_bottom['top'].iterrows():
                            self._write_insight_row(
                                worksheet, row, column_names, current_row,
                                data_format, percent_format, empty_format
                            )
                            current_row += 1
                    else:
                        for col_idx in range(len(column_names)):
                            worksheet.write(current_row, col_idx, '', empty_format)
                        current_row += 1
                    
                    current_row += 1
                    
                    # Section header - Bottom 3
                    section_header_text = f"{section_name} - BOTTOM 3"
                    worksheet.merge_range(
                        current_row, 0, current_row, len(column_names) - 1,
                        section_header_text, section_header_format
                    )
                    current_row += 1
                    
                    if not top_bottom['bottom'].empty:
                        for _, row in top_bottom['bottom'].iterrows():
                            self._write_insight_row(
                                worksheet, row, column_names, current_row,
                                data_format, percent_format, empty_format
                            )
                            current_row += 1
                    else:
                        for col_idx in range(len(column_names)):
                            worksheet.write(current_row, col_idx, '', empty_format)
                        current_row += 1
                    
                    current_row += 1
            
            for col_idx in range(len(column_names)):
                worksheet.set_column(col_idx, col_idx, 18)
            
            worksheet.freeze_panes(1, 0)
            workbook.close()
        except Exception as e:
            pass  # Silently fail
    
    def _write_insight_row(self, worksheet, row: pd.Series, column_names: List[str], 
                          row_idx: int, data_format, percent_format, empty_format):
        """Helper method to write a single insight row to Excel worksheet."""
        for col_idx, col_name in enumerate(column_names):
            try:
                if col_name not in row.index:
                    worksheet.write(row_idx, col_idx, '', empty_format)
                    continue
                
                cell_value = row[col_name]
                if pd.isna(cell_value) or cell_value == '':
                    worksheet.write(row_idx, col_idx, '', empty_format)
                elif 'Variance %' in col_name or '%' in col_name:
                    try:
                        val = float(cell_value) if not pd.isna(cell_value) else 0.0
                        worksheet.write(row_idx, col_idx, val, percent_format)
                    except (ValueError, TypeError):
                        worksheet.write(row_idx, col_idx, str(cell_value), empty_format)
                elif any(x in col_name for x in ['Value', 'Budget', 'Revenue', 'Payroll', 'Visits', 'Comparison', 'Anchor']):
                    try:
                        float_val = float(cell_value) if not pd.isna(cell_value) else 0.0
                        worksheet.write(row_idx, col_idx, float_val, data_format)
                    except (ValueError, TypeError):
                        worksheet.write(row_idx, col_idx, str(cell_value), empty_format)
                else:
                    worksheet.write(row_idx, col_idx, str(cell_value), empty_format)
            except Exception:
                worksheet.write(row_idx, col_idx, '', empty_format)

    def _export_insights_to_excel(self, 
                                   insights_dataframe: pd.DataFrame,
                                   resort_name: str,
                                   report_date_string: str,
                                   file_name_postfix: str = None) -> str:
        if insights_dataframe is None or insights_dataframe.empty or len(insights_dataframe.columns) == 0:
            return None
        
        file_path = os.path.join(self.output_dir, f"{DataUtils.sanitize_filename(resort_name)}_dmr_insights_{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}.xlsx")
        
        workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})
        
        header_format = workbook.add_format({'bold': True, 'align': 'center', 'bg_color': '#D3D3D3', 'border': 1, 'text_wrap': True})
        section_header_format = workbook.add_format({'bold': True, 'bg_color': '#E6E6E6', 'border': 1})
        row_header_format = workbook.add_format({'bold': True, 'border': 1})
        data_format = workbook.add_format({'border': 1, 'num_format': '#,##0.00'})
        percent_format = workbook.add_format({'border': 1, 'num_format': '0.00"%"'})
        empty_format = workbook.add_format({'border': 1})
        
        worksheet = workbook.add_worksheet("Insights")
        
        for col_idx, col_name in enumerate(insights_dataframe.columns):
            worksheet.write(0, col_idx, col_name, header_format)
        
        for row_idx, (_, row) in enumerate(insights_dataframe.iterrows(), start=1):
            for col_idx, col_name in enumerate(insights_dataframe.columns):
                cell_value = row[col_name]
                
                if col_name == 'Row Header':
                    if pd.notna(cell_value) and cell_value != '':
                        format_to_use = section_header_format if cell_value in ['Visits', 'Payroll', 'Revenue'] else row_header_format
                        worksheet.write(row_idx, col_idx, cell_value, format_to_use)
                    else:
                        worksheet.write(row_idx, col_idx, '', empty_format)
                elif col_name == 'Dept Code':
                    worksheet.write(row_idx, col_idx, cell_value if pd.notna(cell_value) else '', empty_format)
                elif 'Variance %' in col_name:
                    format_to_use = percent_format if (pd.notna(cell_value) and cell_value != '') else empty_format
                    worksheet.write(row_idx, col_idx, cell_value if pd.notna(cell_value) else '', format_to_use)
                elif 'Value' in col_name or 'Budget' in col_name:
                    format_to_use = data_format if (pd.notna(cell_value) and cell_value != '') else empty_format
                    worksheet.write(row_idx, col_idx, cell_value if pd.notna(cell_value) else '', format_to_use)
                else:
                    worksheet.write(row_idx, col_idx, cell_value if pd.notna(cell_value) else '', empty_format)
        
        worksheet.set_column(0, 0, 30)
        worksheet.set_column(1, 1, 15)
        worksheet.set_column(2, len(insights_dataframe.columns) - 1, 18)
        worksheet.freeze_panes(1, 2)
            
        workbook.close()
        return file_path

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
            
            worksheet.write(row, 0, f"{title} - Revenue", row_header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    val = processed_budget.get(range_key, {}).get(trimmed_code, {}).get('Revenue', 0)
                else:
                    val = processed_revenue[col_name].get(trimmed_code, 0)
                worksheet.write(row, i + 1, DataUtils.normalize_value(val), data_format)
            row += 1
            
            worksheet.write(row, 0, f"{title} - Payroll", row_header_format)
            for i, col_name in enumerate(columns):
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    val = processed_budget.get(range_key, {}).get(trimmed_code, {}).get('Payroll', 0)
                else:
                    val = processed_payroll[col_name].get(trimmed_code, 0)
                worksheet.write(row, i + 1, DataUtils.normalize_value(val), data_format)
            row += 1
            
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

    def _build_report_json(self, resort_name, report_date, ranges, date_calculator, column_structure,
                          actual_range_names, processed_snow, processed_visits, processed_visits_budget,
                          processed_revenue, processed_payroll, processed_budget, locations_set,
                          departments_set, code_to_title_map):
        """Build JSON structure matching Excel report layout"""

        # Build header row with date ranges
        headers = [{"name": "Row Header", "display": ""}]
        for col_name in column_structure:
            if col_name.endswith(" (Budget)"):
                start, end = (date_calculator.week_total_actual() if col_name == "Week Total (Actual) (Budget)"
                             else ranges[self._get_budget_range_name(col_name)])
            else:
                start, end = ranges[col_name]
            headers.append({
                "name": col_name,
                "display": f"{col_name}\n{start.strftime('%b %d')} - {end.strftime('%b %d')}"
            })

        rows = []

        # Title row
        title_text = f"{resort_name} Resort - Daily Management Report - As of {report_date.strftime('%A')} - {report_date.strftime('%d %B, %Y').lstrip('0')}"
        rows.append({
            "type": "title",
            "row_header": title_text,
            "values": [""] * len(column_structure)
        })

        # Snow section
        snow_24hrs_row = {"type": "data", "row_header": "Snow 24hrs", "values": []}
        for col_name in column_structure:
            if not col_name.endswith(" (Budget)"):
                value = DataUtils.normalize_value(processed_snow[col_name]['snow_24hrs'])
                snow_24hrs_row["values"].append(value)
            else:
                snow_24hrs_row["values"].append(None)
        rows.append(snow_24hrs_row)

        base_depth_row = {"type": "data", "row_header": "Base Depth", "values": []}
        for col_name in column_structure:
            if not col_name.endswith(" (Budget)"):
                value = DataUtils.normalize_value(processed_snow[col_name]['base_depth'])
                base_depth_row["values"].append(value)
            else:
                base_depth_row["values"].append(None)
        rows.append(base_depth_row)

        # Empty row
        rows.append({"type": "empty", "row_header": "", "values": [""] * len(column_structure)})

        # Visits section
        rows.append({"type": "section_header", "row_header": "VISITS", "values": [""] * len(column_structure)})

        for location in sorted(list(locations_set)):
            location_row = {"type": "data", "row_header": location, "values": []}
            for col_name in column_structure:
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    loc_key = DataUtils.process_location_name(location, resort_name)
                    value = processed_budget.get(range_key, {}).get(loc_key, 0)
                else:
                    value = processed_visits[col_name].get(location, 0)
                location_row["values"].append(DataUtils.normalize_value(value))
            rows.append(location_row)

        # Total Tickets row
        total_tickets_row = {"type": "total", "row_header": "Total Tickets", "values": []}
        for col_name in column_structure:
            if col_name.endswith(" (Budget)"):
                range_key = self._get_budget_range_name(col_name)
                total_val = sum(processed_visits_budget.get(range_key, {}).values())
            else:
                total_val = sum(processed_visits[col_name].values())
            total_tickets_row["values"].append(DataUtils.normalize_value(total_val))
        rows.append(total_tickets_row)

        # Empty row
        rows.append({"type": "empty", "row_header": "", "values": [""] * len(column_structure)})

        # Financials section
        rows.append({"type": "section_header", "row_header": "FINANCIALS", "values": [""] * len(column_structure)})

        for dept_code in sorted(list(departments_set)):
            trimmed_code = DataUtils.trim_dept_code(dept_code)
            title = code_to_title_map.get(trimmed_code, trimmed_code)

            # Revenue row
            revenue_row = {"type": "data", "row_header": f"{title} - Revenue", "values": []}
            for col_name in column_structure:
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    value = processed_budget.get(range_key, {}).get(trimmed_code, {}).get('Revenue', 0)
                else:
                    value = processed_revenue[col_name].get(trimmed_code, 0)
                revenue_row["values"].append(DataUtils.normalize_value(value))
            rows.append(revenue_row)

            # Payroll row
            payroll_row = {"type": "data", "row_header": f"{title} - Payroll", "values": []}
            for col_name in column_structure:
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    value = processed_budget.get(range_key, {}).get(trimmed_code, {}).get('Payroll', 0)
                else:
                    value = processed_payroll[col_name].get(trimmed_code, 0)
                payroll_row["values"].append(DataUtils.normalize_value(value))
            rows.append(payroll_row)

        # Empty row
        rows.append({"type": "empty", "row_header": "", "values": [""] * len(column_structure)})

        # Totals section
        total_labels = ["Total Revenue", "Total Payroll", "Contribution", "PR %"]
        for label in total_labels:
            total_row = {"type": "total", "row_header": label, "values": []}
            for col_name in column_structure:
                if col_name.endswith(" (Budget)"):
                    range_key = self._get_budget_range_name(col_name)
                    if label == "Total Revenue":
                        final_value = sum(processed_budget.get(range_key, {}).get(dept, {}).get('Revenue', 0)
                                         for dept in departments_set)
                    elif label == "Total Payroll":
                        final_value = sum(processed_budget.get(range_key, {}).get(dept, {}).get('Payroll', 0)
                                         for dept in departments_set)
                    elif label == "Contribution":
                        revenue_total = sum(processed_budget.get(range_key, {}).get(dept, {}).get('Revenue', 0)
                                           for dept in departments_set)
                        payroll_total = sum(processed_budget.get(range_key, {}).get(dept, {}).get('Payroll', 0)
                                           for dept in departments_set)
                        final_value = revenue_total - payroll_total
                    else:  # PR %
                        revenue_total = sum(processed_budget.get(range_key, {}).get(dept, {}).get('Revenue', 0)
                                           for dept in departments_set)
                        payroll_total = sum(processed_budget.get(range_key, {}).get(dept, {}).get('Payroll', 0)
                                           for dept in departments_set)
                        final_value = (abs(payroll_total) / abs(revenue_total) * 100) if revenue_total != 0 else 0
                else:
                    revenue_total = sum(processed_revenue[col_name].get(DataUtils.trim_dept_code(dept), 0)
                                       for dept in departments_set)
                    payroll_total = sum(processed_payroll[col_name].get(DataUtils.trim_dept_code(dept), 0)
                                       for dept in departments_set)

                    if label == "Total Revenue":
                        final_value = revenue_total
                    elif label == "Total Payroll":
                        final_value = payroll_total
                    elif label == "PR %":
                        final_value = (abs(payroll_total) / abs(revenue_total) * 100) if revenue_total != 0 else 0
                    else:
                        final_value = revenue_total - payroll_total

                total_row["values"].append(DataUtils.normalize_value(final_value))
            rows.append(total_row)

        return {
            "resort_name": resort_name,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(),
            "headers": headers,
            "rows": rows
        }

    def generate_analysis(self, resort_config: Dict = None, run_date: Union[str, datetime] = None,
                         debug: bool = False, file_name_postfix: str = None,
                         analysis_type: str = "rep") -> Dict[str, str]:
        print(f"Here in the function")
        result = {'report_path': None, 'insights_path': None}
        analysis_type = analysis_type.lower()
        generate_report = analysis_type in ["rep", "both"]
        generate_insights = analysis_type in ["ins", "both"]
        current_now = datetime.now()

        # Track if resort_config came from environment variables
        resort_config_from_env = False
        if resort_config is None:
            resort_config_from_env = True
            resort_config = {
                'resortName': os.getenv('RESORT_NAME'),
                'dbName': os.getenv('DB_NAME'),
                'groupNum': int(os.getenv('GROUP_NUM', '-1'))
            }

            if run_date is None:
                env_run_date = os.getenv('RUN_DATE')
                if env_run_date:
                    run_date = datetime.strptime(env_run_date, "%m/%d/%Y")
                else:
                    run_date = current_now - timedelta(days=1)

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
        
        debug_directory, debug_log_handle = None, None
        if debug:
            sanitized_resort = DataUtils.sanitize_filename(resort_name).lower()
            debug_directory = os.path.join(self.output_dir, f"Debug-{sanitized_resort}-{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}")
            if not os.path.exists(debug_directory): os.makedirs(debug_directory)
            debug_log_handle = open(os.path.join(debug_directory, "DebugLogs.txt"), 'w', encoding='utf-8')

        log(f"Generating report for {resort_name} for {report_date.strftime('%Y-%m-%d')}")

        data_store = {name: {} for name in range_names_ordered}
        actual_range_names = ["For The Day (Actual)", "For The Week Ending (Actual)", "Month to Date (Actual)", "For Winter Ending (Actual)"]

        try:
            with DatabaseConnection() as conn:
                stored_procedures_handler = StoredProcedures(conn)
                for name in range_names_ordered:
                    start, end = ranges[name]
                    log(f"Fetching data for {name} ({start.date()} to {end.date()})")

                    data_store[name]['revenue'] = execute_with_retry(
                        f"Revenue data for {name}",
                        lambda: stored_procedures_handler.execute_revenue(db_name, group_num, start, end),
                        logger_func=log
                    )
                    data_store[name]['visits'] = execute_with_retry(
                        f"Visits data for {name}",
                        lambda: stored_procedures_handler.execute_visits(resort_name, start, end),
                        logger_func=log
                    )
                    data_store[name]['snow'] = execute_with_retry(
                        f"Weather data for {name}",
                        lambda: stored_procedures_handler.execute_weather(resort_name, start, end),
                        logger_func=log
                    )

                    if not is_current:
                        if name in actual_range_names:
                            data_store[name]['payroll'] = execute_with_retry(
                                f"Payroll data for {name}",
                                lambda: stored_procedures_handler.execute_payroll(resort_name, start, end),
                                logger_func=log
                            )
                            data_store[name]['salary_payroll'] = execute_with_retry(
                                f"Salary payroll data for {name}",
                                lambda: stored_procedures_handler.execute_payroll_salary(resort_name, start, end),
                                logger_func=log
                            )
                            if name == "For The Week Ending (Actual)":
                                # Full week total budget (Monday-Sunday) for DMR report
                                budget_week_total_start, budget_week_total_end = date_calculator.week_total_actual()
                                data_store[name]['budget_week_total'] = execute_with_retry(
                                    f"Budget week total for {name}",
                                    lambda: stored_procedures_handler.execute_budget(resort_name, budget_week_total_start, budget_week_total_end),
                                    logger_func=log
                                )
                                # Week-to-date budget (Monday to report date) for insights comparison
                                budget_week_to_date_start, budget_week_to_date_end = start, end
                                data_store[name]['budget_week_to_date'] = execute_with_retry(
                                    f"Budget week to date for {name}",
                                    lambda: stored_procedures_handler.execute_budget(resort_name, budget_week_to_date_start, budget_week_to_date_end),
                                    logger_func=log
                                )
                            else:
                                budget_start, budget_end = start, end
                                data_store[name]['budget'] = execute_with_retry(
                                    f"Budget data for {name}",
                                    lambda: stored_procedures_handler.execute_budget(resort_name, budget_start, budget_end),
                                    logger_func=log
                                )
                        else:
                            data_store[name]['payroll_history'] = execute_with_retry(
                                f"Payroll history for {name}",
                                lambda: stored_procedures_handler.execute_payroll_history(resort_name, start, end),
                                logger_func=log
                            )

                    for key in ['revenue', 'visits', 'snow', 'payroll', 'salary_payroll', 'budget', 'budget_week_total', 'budget_week_to_date', 'payroll_history']:
                        if key not in data_store[name]: data_store[name][key] = pd.DataFrame()
                        if debug and not data_store[name][key].empty:
                            self._export_sp_result(data_store[name][key], name, key.capitalize(), resort_name, debug_directory)

            locations_set, departments_set, code_to_title_map = set(), set(), {}
            processed_snow = self._process_snow(data_store, range_names_ordered)
            processed_visits = self._process_visits(data_store, range_names_ordered, locations_set)
            processed_revenue = self._process_revenue(data_store, range_names_ordered, departments_set, code_to_title_map, debug_log_handle)
            processed_payroll = self._process_payroll(data_store, range_names_ordered, is_current,
                                                     actual_range_names, processed_revenue, departments_set,
                                                     code_to_title_map, debug_log_handle)
            processed_budget, processed_visits_budget = self._process_budget(data_store, range_names_ordered,
                                                                           code_to_title_map, VISITS_DEPT_CODE_MAPPING)

            # For insights: Use budget_week_to_date (week-to-date) instead of budget_week_total (full week) for "For The Week Ending (Actual)"
            insights_budget = processed_budget.copy() if generate_insights else None
            if generate_insights and "For The Week Ending (Actual)" in data_store:
                budget_week_to_date_df = data_store["For The Week Ending (Actual)"].get('budget_week_to_date', pd.DataFrame())
                if not budget_week_to_date_df.empty:
                    # Process budget_week_to_date using _process_budget_dataframe directly (returns {dept_code: {Payroll: X, Revenue: Y}})
                    week_to_date_budget_dict = self._process_budget_dataframe(budget_week_to_date_df, code_to_title_map, VISITS_DEPT_CODE_MAPPING)
                    # Replace the week ending budget with week-to-date budget for insights
                    insights_budget["For The Week Ending (Actual)"] = week_to_date_budget_dict

            if generate_report:
                # Build column structure (same for Excel and JSON)
                column_structure = []
                for name in range_names_ordered:
                    column_structure.append(name)
                    if name in actual_range_names:
                        column_structure.append("Week Total (Actual) (Budget)" if name == "For The Week Ending (Actual)" else f"{name} (Budget)")

                # If config came from env, return JSON instead of Excel
                if resort_config_from_env:
                    day_actual_start = ranges["For The Day (Actual)"][0]
                    report_json = self._build_report_json(
                        resort_name=resort_name,
                        report_date=day_actual_start,
                        ranges=ranges,
                        date_calculator=date_calculator,
                        column_structure=column_structure,
                        actual_range_names=actual_range_names,
                        processed_snow=processed_snow,
                        processed_visits=processed_visits,
                        processed_visits_budget=processed_visits_budget,
                        processed_revenue=processed_revenue,
                        processed_payroll=processed_payroll,
                        processed_budget=processed_budget,
                        locations_set=locations_set,
                        departments_set=departments_set,
                        code_to_title_map=code_to_title_map
                    )
                    result['report_json'] = report_json

                    # Send to N8N webhooks
                    webhook_results = send_to_n8n_webhooks(resort_name, report_json)
                    result['webhook_results'] = webhook_results
                else:
                    # Generate Excel file (existing logic)
                    file_path = os.path.join(self.output_dir, f"{DataUtils.sanitize_filename(resort_name)}_Report_{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}.xlsx")
                    workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})
                    worksheet = workbook.add_worksheet("Report")

                    header_format = workbook.add_format({'bold':True,'align':'center','bg_color':'#D3D3D3','border':1,'text_wrap':True})
                    row_header_format = workbook.add_format({'bold':True,'border':1})
                    data_format = workbook.add_format({'border':1, 'num_format':'#,##0.00'})
                    snow_format = workbook.add_format({'border':1, 'num_format':'0.0'})
                    percent_format = workbook.add_format({'border':1, 'num_format':'0"%"'})

                    day_actual_start = ranges["For The Day (Actual)"][0]
                    title_text = f"{resort_name} Resort\nDaily Management Report\nAs of {day_actual_start.strftime('%A')} - {day_actual_start.strftime('%d %B, %Y').lstrip('0')}"
                    worksheet.write(0, 0, title_text, header_format)

                    for i, col_name in enumerate(column_structure):
                        if col_name.endswith(" (Budget)"):
                            start, end = (date_calculator.week_total_actual() if col_name == "Week Total (Actual) (Budget)" else ranges[self._get_budget_range_name(col_name)])
                        else:
                            start, end = ranges[col_name]
                        worksheet.write(0, i + 1, f"{col_name}\n{start.strftime('%b %d')} - {end.strftime('%b %d')}", header_format)
                        worksheet.set_column(i + 1, i + 1, 18)

                    worksheet.set_column(0, 0, 30)
                    worksheet.freeze_panes(1, 1)

                    current_row = self._write_snow_section(worksheet, 1, column_structure, processed_snow, snow_format, row_header_format)
                    current_row = self._write_visits_section(worksheet, current_row, column_structure, processed_visits, processed_visits_budget, locations_set, resort_name, row_header_format, data_format, header_format)
                    current_row = self._write_financials_section(worksheet, current_row, column_structure, processed_revenue, processed_payroll, processed_budget, sorted(list(departments_set)), code_to_title_map, row_header_format, data_format, header_format, percent_format)
                    self._write_totals_section(worksheet, current_row + 1, column_structure, processed_revenue, processed_payroll, processed_budget, sorted(list(departments_set)), data_format, header_format, percent_format)

                    workbook.close()
                    result['report_path'] = file_path

            if generate_insights:
                try:
                    insights_df = self._generate_insights_dataframe(
                        processed_visits=processed_visits,
                        processed_revenue=processed_revenue,
                        processed_payroll=processed_payroll,
                        processed_budget=insights_budget,  # Use insights_budget which has budget_week_to_date for week ending
                        processed_visits_budget=processed_visits_budget,
                        all_locations=locations_set,
                        all_departments=departments_set,
                        department_to_title=code_to_title_map,
                        resort_name=resort_name
                    )
                    if insights_df is not None and not insights_df.empty:
                        insights_path = self._export_insights_to_excel(
                            insights_dataframe=insights_df,
                            resort_name=resort_name,
                            report_date_string=report_date_string,
                            file_name_postfix=file_name_postfix
                        )
                        if insights_path:
                            result['insights_path'] = insights_path
                            self._log_top_bottom_insights(insights_df, "DMR", resort_name, report_date_string, file_name_postfix)
                except Exception as e:
                    log(f"Failed to generate DMR insights: {e}", "WARNING")

            log(f"Report processing completed successfully for {resort_name}", "SUCCESS")

        except Exception as e:
            log(f"Report generation failed for {resort_name}: {str(e)}", "ERROR")

            # Send error to N8N webhook if config was from env
            if resort_config_from_env:
                error_response = {
                    "status": "error",
                    "error": str(e),
                    "resort_name": resort_name,
                    "report_date": report_date.strftime("%Y-%m-%d"),
                    "timestamp": datetime.now().isoformat()
                }
                send_to_n8n_webhooks(resort_name, error_response)

            result['error'] = str(e)

            if debug_log_handle:
                debug_log_handle.write(f"\n\nERROR: {str(e)}\n")
                debug_log_handle.close()

            # Re-raise the exception so caller knows it failed
            raise

        if debug_log_handle: debug_log_handle.close()
        return result

    def generate_comprehensive_report(self, resort_config: Dict = None, run_date: Union[str, datetime] = None,
                                    debug: bool = False, file_name_postfix: str = None) -> str:
        result = self.generate_analysis(
            resort_config=resort_config,
            run_date=run_date,
            debug=debug,
            file_name_postfix=file_name_postfix,
            analysis_type="rep"
        )
        return result.get('report_path', '')

    def _export_sp_result(self, dataframe: pd.DataFrame, range_name: str = None, stored_procedure_name: str = None, 
                         resort_name: str = None, export_directory: str = None, 
                         date_label: str = None) -> str:
        if date_label:
            sanitized_range = DataUtils.sanitize_filename(date_label)
            sanitized_sp = DataUtils.sanitize_filename(stored_procedure_name)
            file_path = os.path.join(export_directory or self.output_dir, f"{sanitized_range}_{sanitized_sp}.xlsx")
        else:
            sanitized_range = DataUtils.sanitize_filename(range_name)
            sanitized_sp = DataUtils.sanitize_filename(stored_procedure_name)
            file_path = os.path.join(export_directory or self.output_dir, f"{sanitized_range}_{sanitized_sp}.xlsx")
        
        dataframe_to_write = dataframe
        if stored_procedure_name in ['Revenue', 'Payroll']:
            dept_column = DataUtils.get_col(dataframe, CandidateColumns.departmentCode + CandidateColumns.departmentTitle)
            if dept_column:
                dataframe_to_write = dataframe.copy()
                dataframe_to_write['_sort_key'] = dataframe_to_write[dept_column].astype(str).str.strip()
                dataframe_to_write = dataframe_to_write.sort_values(by='_sort_key', na_position='last').drop(columns=['_sort_key'])
        
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

    def _is_within_one_year(self, date: datetime) -> bool:
        one_year_ago = datetime.now() - timedelta(days=365)
        return date >= one_year_ago

    def _fetch_single_day_data(self, resort_config: Dict[str, Any], target_date: datetime,
                               is_within_year: bool, is_current_date: bool = False,
                               debug: bool = False, debug_directory: str = None,
                               date_label: str = "") -> Dict[str, Any]:
        resort_name = resort_config['resortName']
        db_name = resort_config.get('dbName', resort_name)
        group_num = resort_config.get('groupNum', -1)
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
            data['revenue'] = stored_procedures_handler.execute_revenue(db_name, group_num, start, end)
            data['visits'] = stored_procedures_handler.execute_visits(resort_name, start, end)
            data['budget'] = stored_procedures_handler.execute_budget(resort_name, start, end)
            if is_within_year:
                data['payroll'] = stored_procedures_handler.execute_payroll(resort_name, start, end)
                data['salary_payroll'] = stored_procedures_handler.execute_payroll_salary(resort_name, start, end)
            else:
                data['payroll_history'] = stored_procedures_handler.execute_payroll_history(resort_name, start, end)
            if debug and debug_directory:
                for key in ['revenue', 'visits', 'budget', 'payroll', 'salary_payroll', 'payroll_history']:
                    if not data[key].empty:
                        self._export_sp_result(data[key], date_label=date_label, stored_procedure_name=key.capitalize(),
                                              export_directory=debug_directory)
        return data

    def _calculate_comparison_variance_percentage(self, comparison_value: float, anchor_value: float) -> float:
        comparison_value = DataUtils.normalize_value(comparison_value)
        anchor_value = DataUtils.normalize_value(anchor_value)
        if abs(anchor_value) < 1e-10:
            return 0.0
        try:
            result = ((comparison_value - anchor_value) * 100) / anchor_value
            return DataUtils.normalize_value(result)
        except (ZeroDivisionError, OverflowError, ValueError):
            return 0.0

    def _generate_visit_insights(self, comparison_visits: Dict[str, float],
                                 anchor_visits: Dict[str, float]) -> pd.DataFrame:
        rows = []
        
        all_visit_categories = set(comparison_visits.keys()) | set(anchor_visits.keys())
        for category in sorted(all_visit_categories):
            comp_val = DataUtils.normalize_value(comparison_visits.get(category, 0.0))
            anchor_val = DataUtils.normalize_value(anchor_visits.get(category, 0.0))
            visit_variance = self._calculate_comparison_variance_percentage(comp_val, anchor_val)
            
            rows.append({
                'Visit Category': category,
                'Comparison Visits': comp_val,
                'Anchor Visits': anchor_val,
                'Visit Variance %': visit_variance
            })
        
        return pd.DataFrame(rows)
    
    def _generate_financial_insights(self, comparison_revenue: Dict[str, float],
                                     comparison_payroll: Dict[str, float],
                                     comparison_budget: Dict[str, Dict[str, float]],
                                     anchor_revenue: Dict[str, float],
                                     anchor_payroll: Dict[str, float],
                                     anchor_budget: Dict[str, Dict[str, float]],
                                     department_to_title: Dict[str, str]) -> pd.DataFrame:
        rows = []
        
        all_depts = set(comparison_payroll.keys()) | set(anchor_payroll.keys()) | set(comparison_revenue.keys()) | set(anchor_revenue.keys())
        
        for dept_code in sorted(all_depts):
            dept_title = department_to_title.get(dept_code, dept_code)
            
            comp_rev = DataUtils.normalize_value(comparison_revenue.get(dept_code, 0.0))
            anchor_rev = DataUtils.normalize_value(anchor_revenue.get(dept_code, 0.0))
            comp_pay = DataUtils.normalize_value(comparison_payroll.get(dept_code, 0.0))
            anchor_pay = DataUtils.normalize_value(anchor_payroll.get(dept_code, 0.0))
            
            rev_budget = DataUtils.normalize_value(comparison_budget.get(dept_code, {}).get('Revenue', 0.0))
            pay_budget = DataUtils.normalize_value(comparison_budget.get(dept_code, {}).get('Payroll', 0.0))
            
            rev_variance = self._calculate_comparison_variance_percentage(comp_rev, anchor_rev)
            pay_variance = self._calculate_comparison_variance_percentage(comp_pay, anchor_pay)
            
            if abs(rev_budget) < 1e-10:
                rev_budget_variance = 0.0
            else:
                rev_budget_variance = self._calculate_comparison_variance_percentage(comp_rev, rev_budget)
            
            if abs(pay_budget) < 1e-10:
                pay_budget_variance = 0.0
            else:
                pay_budget_variance = self._calculate_comparison_variance_percentage(comp_pay, pay_budget)
            
            if abs(comp_pay) < 1e-10:
                rev_to_pay_ratio_comp = 0.0
            else:
                try:
                    rev_to_pay_ratio_comp = DataUtils.normalize_value((comp_rev / comp_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio_comp = 0.0
            
            if abs(anchor_pay) < 1e-10:
                rev_to_pay_ratio_anchor = 0.0
            else:
                try:
                    rev_to_pay_ratio_anchor = DataUtils.normalize_value((anchor_rev / anchor_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio_anchor = 0.0
            
            if abs(comp_pay) < 1e-10:
                bud_to_pay_ratio_comp = 0.0
            else:
                try:
                    bud_to_pay_ratio_comp = DataUtils.normalize_value((pay_budget / comp_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    bud_to_pay_ratio_comp = 0.0
            
            if abs(anchor_pay) < 1e-10:
                bud_to_pay_ratio_anchor = 0.0
            else:
                anchor_pay_bud = DataUtils.normalize_value(anchor_budget.get(dept_code, {}).get('Payroll', 0.0))
                try:
                    bud_to_pay_ratio_anchor = DataUtils.normalize_value((anchor_pay_bud / anchor_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    bud_to_pay_ratio_anchor = 0.0
            
            rev_to_pay_variance = self._calculate_comparison_variance_percentage(rev_to_pay_ratio_comp, rev_to_pay_ratio_anchor)
            bud_to_pay_variance = self._calculate_comparison_variance_percentage(bud_to_pay_ratio_comp, bud_to_pay_ratio_anchor)
            
            rows.append({
                'Department Title': dept_title,
                'Dept Code': dept_code,
                'Comparison Revenue': comp_rev,
                'Anchor Revenue': anchor_rev,
                'Revenue Budget': rev_budget,
                'Revenue Variance %': rev_variance,
                'Revenue Budget Variance %': rev_budget_variance,
                'Comparison Payroll': comp_pay,
                'Anchor Payroll': anchor_pay,
                'Payroll Budget': pay_budget,
                'Payroll Variance %': pay_variance,
                'Payroll Budget Variance %': pay_budget_variance,
                'Revenue-to-Payroll %': rev_to_pay_ratio_comp,
                'Budget-to-Payroll %': bud_to_pay_ratio_comp,
                'Revenue-to-Payroll Variance %': rev_to_pay_variance,
                'Budget-to-Payroll Variance %': bud_to_pay_variance
            })
        
        return pd.DataFrame(rows)

    def generate_comparison_insights(self, resort_config: Dict[str, Any],
                                    comparison_date: Union[str, datetime],
                                    anchor_date: Union[str, datetime],
                                    debug: bool = False) -> Dict[str, pd.DataFrame]:
        current_now = datetime.now()
        if isinstance(comparison_date, str):
            comparison_date = datetime.strptime(comparison_date, "%m/%d/%Y")
        if isinstance(anchor_date, str):
            anchor_date = datetime.strptime(anchor_date, "%m/%d/%Y")
        comparison_is_current = (comparison_date.date() == current_now.date())
        if comparison_is_current:
            anchor_is_current = True
        else:
            anchor_is_current = False
        comparison_is_within_year = self._is_within_one_year(comparison_date)
        anchor_is_within_year = self._is_within_one_year(anchor_date)
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
        comparison_data = self._fetch_single_day_data(
            resort_config, comparison_date, comparison_is_within_year,
            is_current_date=comparison_is_current, debug=debug,
            debug_directory=debug_directory, date_label="Comparison"
        )
        anchor_data = self._fetch_single_day_data(
            resort_config, anchor_date, anchor_is_within_year,
            is_current_date=anchor_is_current, debug=debug,
            debug_directory=debug_directory, date_label="Anchor"
        )
        department_to_title = {}
        all_departments = set()
        comparison_visits = {}
        comparison_revenue = {}
        comparison_budget = {}
        comparison_payroll = {}
        if not comparison_data['visits'].empty:
            comparison_visits = self._process_visits_dataframe(comparison_data['visits'])
        if not comparison_data['revenue'].empty:
            comparison_revenue = self._process_revenue_dataframe(comparison_data['revenue'], department_to_title, all_departments)
        if not comparison_data['budget'].empty:
            comparison_budget = self._process_budget_dataframe(comparison_data['budget'], department_to_title)
        if comparison_is_within_year:
            if not comparison_data['payroll'].empty or not comparison_data['salary_payroll'].empty:
                comparison_payroll = self._process_payroll_actual_dataframes(
                    comparison_data['payroll'],
                    comparison_data['salary_payroll'],
                    department_to_title,
                    all_departments,
                    date_label="Comparison Date",
                    debug_log_file=debug_log_handle
                )
        else:
            if not comparison_data['payroll_history'].empty:
                comparison_payroll = self._process_payroll_prior_year_dataframe(
                    comparison_data['payroll_history'],
                    department_to_title,
                    all_departments,
                    date_label="Comparison Date",
                    debug_log_file=debug_log_handle
                )
        anchor_visits = {}
        anchor_revenue = {}
        anchor_budget = {}
        anchor_payroll = {}
        if not anchor_data['visits'].empty:
            anchor_visits = self._process_visits_dataframe(anchor_data['visits'])
        if not anchor_data['revenue'].empty:
            anchor_revenue = self._process_revenue_dataframe(anchor_data['revenue'], department_to_title, all_departments)
        if not anchor_data['budget'].empty:
            anchor_budget = self._process_budget_dataframe(anchor_data['budget'], department_to_title)
        if anchor_is_within_year:
            if not anchor_data['payroll'].empty or not anchor_data['salary_payroll'].empty:
                anchor_payroll = self._process_payroll_actual_dataframes(
                    anchor_data['payroll'],
                    anchor_data['salary_payroll'],
                    department_to_title,
                    all_departments,
                    date_label="Anchor Date",
                    debug_log_file=debug_log_handle
                )
        else:
            if not anchor_data['payroll_history'].empty:
                anchor_payroll = self._process_payroll_prior_year_dataframe(
                    anchor_data['payroll_history'],
                    department_to_title,
                    all_departments,
                    date_label="Anchor Date",
                    debug_log_file=debug_log_handle
                )
        visit_insights = self._generate_visit_insights(
            comparison_visits,
            anchor_visits
        )
        
        financial_insights = self._generate_financial_insights(
            comparison_revenue, comparison_payroll, comparison_budget,
            anchor_revenue, anchor_payroll, anchor_budget,
            department_to_title
        )
        
        comparison_date_str = comparison_date.strftime("%Y%m%d")
        anchor_date_str = anchor_date.strftime("%Y%m%d")
        report_date_string = f"{comparison_date_str}-{anchor_date_str}"
        
        if debug and debug_directory:
            insights_file = os.path.join(debug_directory, "comparison_insights.xlsx")
            with pd.ExcelWriter(insights_file, engine='xlsxwriter') as writer:
                visit_insights.to_excel(writer, sheet_name='Visit Analytics', index=False)
                financial_insights.to_excel(writer, sheet_name='Department Analytics', index=False)
            if debug_log_handle:
                debug_log_handle.write(f"\n{'='*80}\nInsight generation complete!\n{'='*80}\n")
                debug_log_handle.close()
        
        if not visit_insights.empty:
            self._log_top_bottom_insights(visit_insights, "Comparison", resort_name, report_date_string)
        if not financial_insights.empty:
            self._log_top_bottom_insights(financial_insights, "Comparison", resort_name, report_date_string)
        
        return {
            'visit_analytics': visit_insights,
            'department_analytics': financial_insights
        }


