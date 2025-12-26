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
from utils import DateRangeCalculator, DataUtils
from config import CandidateColumns, VISITS_DEPT_CODE_MAPPING


class AnalysisEngine:
    """Analysis engine for generating comprehensive ski resort reports and insights"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✓ Created output directory: {output_dir}")
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

    def _process_revenue(self, data_store: Dict, range_names: List[str], all_departments: Set[str], department_to_title: Dict) -> Dict:
        processed_revenue = {name: {} for name in range_names}
        for range_name in range_names:
            dataframe = data_store[range_name]['revenue']
            processed_revenue[range_name] = self._process_revenue_dataframe(dataframe, department_to_title, all_departments)
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
            print(log_message, end='')
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
        print(log_message, end='')
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
        print(log_message, end='')
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

    def _generate_insights_dataframe(self, 
                                     range_name: str,
                                     processed_visits: Dict,
                                     processed_revenue: Dict,
                                     processed_payroll: Dict,
                                     processed_budget: Dict,
                                     processed_visits_budget: Dict,
                                     all_locations: Set[str],
                                     all_departments: Set[str],
                                     department_to_title: Dict,
                                     resort_name: str) -> pd.DataFrame:
        prior_range_name = range_name.replace("(Actual)", "(Prior Year)")
        rows = []
        
        rows.append({
            'Row Header': 'Visits',
            'Dept Code': '',
            'Actual': '',
            'Budget': '',
            'Actual-Budget Variance %': '',
            'Prior Year': '',
            'Actual-Prior Variance %': ''
        })
        
        actual_visits = processed_visits.get(range_name, {})
        budget_visits = processed_visits_budget.get(range_name, {})
        prior_visits = processed_visits.get(prior_range_name, {})
        
        for location in sorted(all_locations):
            actual_val = DataUtils.normalize_value(actual_visits.get(location, 0.0))
            budget_val = DataUtils.normalize_value(budget_visits.get(location, 0.0))
            prior_val = DataUtils.normalize_value(prior_visits.get(location, 0.0))
            
            rows.append({
                'Row Header': location,
                'Dept Code': '',
                'Actual': actual_val,
                'Budget': budget_val,
                'Actual-Budget Variance %': DataUtils.calculate_variance_percentage(budget_val, actual_val),
                'Prior Year': prior_val,
                'Actual-Prior Variance %': DataUtils.calculate_variance_percentage(prior_val, actual_val)
            })
        
        rows.append({
            'Row Header': 'Payroll',
            'Dept Code': '',
            'Actual': '',
            'Budget': '',
            'Actual-Budget Variance %': '',
            'Prior Year': '',
            'Actual-Prior Variance %': ''
        })
        
        actual_payroll = processed_payroll.get(range_name, {})
        budget_payroll = processed_budget.get(range_name, {})
        prior_payroll = processed_payroll.get(prior_range_name, {})
        
        for dept_code in sorted(all_departments):
            dept_title = department_to_title.get(dept_code, dept_code)
            actual_val = DataUtils.normalize_value(actual_payroll.get(dept_code, 0.0))
            budget_val = DataUtils.normalize_value(budget_payroll.get(dept_code, {}).get('Payroll', 0.0))
            prior_val = DataUtils.normalize_value(prior_payroll.get(dept_code, 0.0))
            
            rows.append({
                'Row Header': dept_title,
                'Dept Code': dept_code,
                'Actual': actual_val,
                'Budget': budget_val,
                'Actual-Budget Variance %': DataUtils.calculate_variance_percentage(budget_val, actual_val),
                'Prior Year': prior_val,
                'Actual-Prior Variance %': DataUtils.calculate_variance_percentage(prior_val, actual_val)
            })
        
        rows.append({
            'Row Header': 'Revenue',
            'Dept Code': '',
            'Actual': '',
            'Budget': '',
            'Actual-Budget Variance %': '',
            'Prior Year': '',
            'Actual-Prior Variance %': ''
        })
        
        actual_revenue = processed_revenue.get(range_name, {})
        budget_data = processed_budget.get(range_name, {})
        prior_revenue = processed_revenue.get(prior_range_name, {})
        
        for dept_code in sorted(all_departments):
            dept_title = department_to_title.get(dept_code, dept_code)
            actual_val = DataUtils.normalize_value(actual_revenue.get(dept_code, 0.0))
            budget_val = DataUtils.normalize_value(budget_data.get(dept_code, {}).get('Revenue', 0.0))
            prior_val = DataUtils.normalize_value(prior_revenue.get(dept_code, 0.0))
            
            rows.append({
                'Row Header': dept_title,
                'Dept Code': dept_code,
                'Actual': actual_val,
                'Budget': budget_val,
                'Actual-Budget Variance %': DataUtils.calculate_variance_percentage(budget_val, actual_val),
                'Prior Year': prior_val,
                'Actual-Prior Variance %': DataUtils.calculate_variance_percentage(prior_val, actual_val)
            })
        
        return pd.DataFrame(rows)

    def _export_insights_to_excel(self, 
                                   insights_dataframes: Dict[str, pd.DataFrame],
                                   resort_name: str,
                                   report_date_string: str,
                                   file_name_postfix: str = None) -> str:
        if not insights_dataframes:
            return None
        
        file_path = os.path.join(self.output_dir, f"{DataUtils.sanitize_filename(resort_name)}_dmr_insights_{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}.xlsx")
        
        workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})
        
        header_format = workbook.add_format({'bold': True, 'align': 'center', 'bg_color': '#D3D3D3', 'border': 1, 'text_wrap': True})
        section_header_format = workbook.add_format({'bold': True, 'bg_color': '#E6E6E6', 'border': 1})
        row_header_format = workbook.add_format({'bold': True, 'border': 1})
        data_format = workbook.add_format({'border': 1, 'num_format': '#,##0.00'})
        percent_format = workbook.add_format({'border': 1, 'num_format': '0.00"%"'})
        empty_format = workbook.add_format({'border': 1})
        
        sheets_created = 0
        for range_name, df in insights_dataframes.items():
            if df is None or df.empty or len(df.columns) == 0:
                continue
                
            sheet_name = DataUtils.sanitize_filename(range_name)[:31]
            worksheet = workbook.add_worksheet(sheet_name)
            sheets_created += 1
            
            for col_idx, col_name in enumerate(df.columns):
                worksheet.write(0, col_idx, col_name, header_format)
            
            for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
                for col_idx, col_name in enumerate(df.columns):
                    cell_value = row[col_name]
                    
                    if col_name == 'Row Header':
                        if pd.notna(cell_value) and cell_value != '':
                            if cell_value in ['Visits', 'Payroll', 'Revenue']:
                                worksheet.write(row_idx, col_idx, cell_value, section_header_format)
                            else:
                                worksheet.write(row_idx, col_idx, cell_value, row_header_format)
                        else:
                            worksheet.write(row_idx, col_idx, '', empty_format)
                    elif col_name == 'Dept Code':
                        worksheet.write(row_idx, col_idx, cell_value if pd.notna(cell_value) else '', empty_format)
                    elif 'Variance %' in col_name:
                        if pd.notna(cell_value) and cell_value != '':
                            worksheet.write(row_idx, col_idx, cell_value, percent_format)
                        else:
                            worksheet.write(row_idx, col_idx, '', empty_format)
                    elif col_name in ['Actual', 'Budget', 'Prior Year']:
                        if pd.notna(cell_value) and cell_value != '':
                            worksheet.write(row_idx, col_idx, cell_value, data_format)
                        else:
                            worksheet.write(row_idx, col_idx, '', empty_format)
                    else:
                        worksheet.write(row_idx, col_idx, cell_value if pd.notna(cell_value) else '', empty_format)
            
            worksheet.set_column(0, 0, 30)
            worksheet.set_column(1, 1, 15)
            worksheet.set_column(2, 5, 18)
            worksheet.freeze_panes(1, 0)
        
        if sheets_created == 0:
            workbook.close()
            return None
            
        workbook.close()
        print(f"✓ DMR Insights saved: {file_path}")
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

    def generate_analysis(self, resort_config: Dict, run_date: Union[str, datetime] = None, 
                         debug: bool = False, file_name_postfix: str = None,
                         analysis_type: str = "both") -> Dict[str, str]:
        result = {'report_path': None, 'insights_path': None}
        
        analysis_type = analysis_type.lower()
        generate_report = analysis_type in ["rep", "both"]
        generate_insights = analysis_type in ["ins", "both"]
        
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
        
        debug_directory, debug_log_handle = None, None
        if debug:
            sanitized_resort = DataUtils.sanitize_filename(resort_name).lower()
            debug_directory = os.path.join(self.output_dir, f"Debug-{sanitized_resort}-{report_date_string}{f'-{file_name_postfix}' if file_name_postfix else ''}")
            if not os.path.exists(debug_directory): os.makedirs(debug_directory)
            debug_log_handle = open(os.path.join(debug_directory, "DebugLogs.txt"), 'w', encoding='utf-8')

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
                        budget_start, budget_end = (date_calculator.week_total_actual() if name == "For The Week Ending (Actual)" else (start, end))
                        data_store[name]['budget'] = stored_procedures_handler.execute_budget(resort_name, budget_start, budget_end)
                    else:
                        data_store[name]['payroll_history'] = stored_procedures_handler.execute_payroll_history(resort_name, start, end)

                for key in ['revenue', 'visits', 'snow', 'payroll', 'salary_payroll', 'budget', 'payroll_history']:
                    if key not in data_store[name]: data_store[name][key] = pd.DataFrame()
                    if debug and not data_store[name][key].empty:
                        self._export_sp_result(data_store[name][key], name, key.capitalize(), resort_name, debug_directory)

        locations_set, departments_set, code_to_title_map = set(), set(), {}
        processed_snow = self._process_snow(data_store, range_names_ordered)
        processed_visits = self._process_visits(data_store, range_names_ordered, locations_set)
        processed_revenue = self._process_revenue(data_store, range_names_ordered, departments_set, code_to_title_map)
        processed_payroll = self._process_payroll(data_store, range_names_ordered, is_current, 
                                                 actual_range_names, processed_revenue, departments_set, 
                                                 code_to_title_map, debug_log_handle)
        processed_budget, processed_visits_budget = self._process_budget(data_store, range_names_ordered, 
                                                                       code_to_title_map, VISITS_DEPT_CODE_MAPPING)

        if generate_report:
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
            
            current_row = self._write_snow_section(worksheet, 1, column_structure, processed_snow, snow_format, row_header_format)
            current_row = self._write_visits_section(worksheet, current_row, column_structure, processed_visits, processed_visits_budget, locations_set, resort_name, row_header_format, data_format, header_format)
            current_row = self._write_financials_section(worksheet, current_row, column_structure, processed_revenue, processed_payroll, processed_budget, sorted(list(departments_set)), code_to_title_map, row_header_format, data_format, header_format, percent_format)
            self._write_totals_section(worksheet, current_row + 1, column_structure, processed_revenue, processed_payroll, processed_budget, sorted(list(departments_set)), data_format, header_format, percent_format)
            
            workbook.close()
            print(f"✓ Report saved: {file_path}")
            result['report_path'] = file_path

        if generate_insights:
            insights_dataframes = {}
            for range_name in actual_range_names:
                try:
                    insights_df = self._generate_insights_dataframe(
                        range_name=range_name,
                        processed_visits=processed_visits,
                        processed_revenue=processed_revenue,
                        processed_payroll=processed_payroll,
                        processed_budget=processed_budget,
                        processed_visits_budget=processed_visits_budget,
                        all_locations=locations_set,
                        all_departments=departments_set,
                        department_to_title=code_to_title_map,
                        resort_name=resort_name
                    )
                    if insights_df is not None and not insights_df.empty:
                        insights_dataframes[range_name] = insights_df
                except Exception as e:
                    print(f"⚠️  Warning: Failed to generate insights for {range_name}: {e}")
                    continue
            
            if insights_dataframes:
                try:
                    insights_path = self._export_insights_to_excel(
                        insights_dataframes=insights_dataframes,
                        resort_name=resort_name,
                        report_date_string=report_date_string,
                        file_name_postfix=file_name_postfix
                    )
                    if insights_path:
                        result['insights_path'] = insights_path
                except Exception as e:
                    print(f"⚠️  Warning: Failed to export DMR insights: {e}")

        if debug_log_handle: debug_log_handle.close()
        return result

    def generate_comprehensive_report(self, resort_config: Dict, run_date: Union[str, datetime] = None, 
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
            print(f"   ⏳ Fetching {date_label} data ({start.date()} to {end.date()})...")
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

    def _generate_comparison_insights(self, comparison_revenue: Dict[str, float],
                                          comparison_payroll: Dict[str, float],
                                          comparison_budget: Dict[str, Dict[str, float]],
                                          comparison_visits: Dict[str, float],
                                          anchor_revenue: Dict[str, float],
                                          anchor_payroll: Dict[str, float],
                                          anchor_budget: Dict[str, Dict[str, float]],
                                          anchor_visits: Dict[str, float],
                                          department_to_title: Dict[str, str]) -> pd.DataFrame:
        rows = []
        
        rows.append({
            'Row Header': 'Visits',
            'Dept Code': '',
            'Value': '',
            'Anchor Value': '',
            'Budget': '',
            'Value-Budget Variance %': '',
            'Value-Anchor Variance %': '',
            'Revenue-to-Payroll %': '',
            'Budget-to-Payroll %': '',
            'Revenue-to-Payroll Variance %': '',
            'Budget-to-Payroll Variance %': ''
        })
        
        all_visit_categories = set(comparison_visits.keys()) | set(anchor_visits.keys())
        for category in sorted(all_visit_categories):
            comp_val = DataUtils.normalize_value(comparison_visits.get(category, 0.0))
            anchor_val = DataUtils.normalize_value(anchor_visits.get(category, 0.0))
            budget_val = 0.0
            visit_variance = self._calculate_comparison_variance_percentage(comp_val, anchor_val)
            budget_variance = 0.0
            
            rows.append({
                'Row Header': category,
                'Dept Code': '',
                'Value': comp_val,
                'Anchor Value': anchor_val,
                'Budget': budget_val,
                'Value-Budget Variance %': budget_variance,
                'Value-Anchor Variance %': visit_variance,
                'Revenue-to-Payroll %': '',
                'Budget-to-Payroll %': '',
                'Revenue-to-Payroll Variance %': '',
                'Budget-to-Payroll Variance %': ''
            })
        
        rows.append({
            'Row Header': 'Payroll',
            'Dept Code': '',
            'Value': '',
            'Anchor Value': '',
            'Budget': '',
            'Value-Budget Variance %': '',
            'Value-Anchor Variance %': '',
            'Revenue-to-Payroll %': '',
            'Budget-to-Payroll %': '',
            'Revenue-to-Payroll Variance %': '',
            'Budget-to-Payroll Variance %': ''
        })
        
        all_depts = set(comparison_payroll.keys()) | set(anchor_payroll.keys())
        for dept_code in sorted(all_depts):
            dept_title = department_to_title.get(dept_code, dept_code)
            comp_val = DataUtils.normalize_value(comparison_payroll.get(dept_code, 0.0))
            anchor_val = DataUtils.normalize_value(anchor_payroll.get(dept_code, 0.0))
            budget_val = DataUtils.normalize_value(comparison_budget.get(dept_code, {}).get('Payroll', 0.0))
            
            pay_variance = self._calculate_comparison_variance_percentage(comp_val, anchor_val)
            
            if abs(budget_val) < 1e-10:
                budget_variance = 0.0
            else:
                budget_variance = self._calculate_comparison_variance_percentage(comp_val, budget_val)
            
            comp_rev = DataUtils.normalize_value(comparison_revenue.get(dept_code, 0.0))
            if abs(comp_val) < 1e-10:
                rev_to_pay_ratio_comp = 0.0
            else:
                try:
                    rev_to_pay_ratio_comp = DataUtils.normalize_value((comp_rev / comp_val) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio_comp = 0.0
            
            anchor_rev = DataUtils.normalize_value(anchor_revenue.get(dept_code, 0.0))
            if abs(anchor_val) < 1e-10:
                rev_to_pay_ratio_anchor = 0.0
            else:
                try:
                    rev_to_pay_ratio_anchor = DataUtils.normalize_value((anchor_rev / anchor_val) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio_anchor = 0.0
            
            if abs(comp_val) < 1e-10:
                bud_to_pay_ratio_comp = 0.0
            else:
                try:
                    bud_to_pay_ratio_comp = DataUtils.normalize_value((budget_val / comp_val) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    bud_to_pay_ratio_comp = 0.0
            
            anchor_bud = DataUtils.normalize_value(anchor_budget.get(dept_code, {}).get('Payroll', 0.0))
            if abs(anchor_val) < 1e-10:
                bud_to_pay_ratio_anchor = 0.0
            else:
                try:
                    bud_to_pay_ratio_anchor = DataUtils.normalize_value((anchor_bud / anchor_val) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    bud_to_pay_ratio_anchor = 0.0
            
            rev_to_pay_variance = self._calculate_comparison_variance_percentage(rev_to_pay_ratio_comp, rev_to_pay_ratio_anchor)
            bud_to_pay_variance = self._calculate_comparison_variance_percentage(bud_to_pay_ratio_comp, bud_to_pay_ratio_anchor)
            
            rows.append({
                'Row Header': dept_title,
                'Dept Code': dept_code,
                'Value': comp_val,
                'Anchor Value': anchor_val,
                'Budget': budget_val,
                'Value-Budget Variance %': budget_variance,
                'Value-Anchor Variance %': pay_variance,
                'Revenue-to-Payroll %': rev_to_pay_ratio_comp,
                'Budget-to-Payroll %': bud_to_pay_ratio_comp,
                'Revenue-to-Payroll Variance %': rev_to_pay_variance,
                'Budget-to-Payroll Variance %': bud_to_pay_variance
            })
        
        rows.append({
            'Row Header': 'Revenue',
            'Dept Code': '',
            'Value': '',
            'Anchor Value': '',
            'Budget': '',
            'Value-Budget Variance %': '',
            'Value-Anchor Variance %': '',
            'Revenue-to-Payroll %': '',
            'Budget-to-Payroll %': '',
            'Revenue-to-Payroll Variance %': '',
            'Budget-to-Payroll Variance %': ''
        })
        
        all_rev_depts = set(comparison_revenue.keys()) | set(anchor_revenue.keys())
        for dept_code in sorted(all_rev_depts):
            dept_title = department_to_title.get(dept_code, dept_code)
            comp_val = DataUtils.normalize_value(comparison_revenue.get(dept_code, 0.0))
            anchor_val = DataUtils.normalize_value(anchor_revenue.get(dept_code, 0.0))
            budget_val = DataUtils.normalize_value(comparison_budget.get(dept_code, {}).get('Revenue', 0.0))
            
            rev_variance = self._calculate_comparison_variance_percentage(comp_val, anchor_val)
            
            if abs(budget_val) < 1e-10:
                budget_variance = 0.0
            else:
                budget_variance = self._calculate_comparison_variance_percentage(comp_val, budget_val)
            
            comp_pay = DataUtils.normalize_value(comparison_payroll.get(dept_code, 0.0))
            anchor_pay = DataUtils.normalize_value(anchor_payroll.get(dept_code, 0.0))
            
            if abs(comp_pay) < 1e-10:
                rev_to_pay_ratio_comp = 0.0
            else:
                try:
                    rev_to_pay_ratio_comp = DataUtils.normalize_value((comp_val / comp_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio_comp = 0.0
            
            if abs(anchor_pay) < 1e-10:
                rev_to_pay_ratio_anchor = 0.0
            else:
                try:
                    rev_to_pay_ratio_anchor = DataUtils.normalize_value((anchor_val / anchor_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    rev_to_pay_ratio_anchor = 0.0
            
            if abs(comp_pay) < 1e-10:
                bud_to_pay_ratio_comp = 0.0
            else:
                try:
                    bud_to_pay_ratio_comp = DataUtils.normalize_value((budget_val / comp_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    bud_to_pay_ratio_comp = 0.0
            
            if abs(anchor_pay) < 1e-10:
                bud_to_pay_ratio_anchor = 0.0
            else:
                anchor_bud = DataUtils.normalize_value(anchor_budget.get(dept_code, {}).get('Revenue', 0.0))
                try:
                    bud_to_pay_ratio_anchor = DataUtils.normalize_value((anchor_bud / anchor_pay) * 100)
                except (ZeroDivisionError, OverflowError, ValueError):
                    bud_to_pay_ratio_anchor = 0.0
            
            rev_to_pay_variance = self._calculate_comparison_variance_percentage(rev_to_pay_ratio_comp, rev_to_pay_ratio_anchor)
            bud_to_pay_variance = self._calculate_comparison_variance_percentage(bud_to_pay_ratio_comp, bud_to_pay_ratio_anchor)
            
            rows.append({
                'Row Header': dept_title,
                'Dept Code': dept_code,
                'Value': comp_val,
                'Anchor Value': anchor_val,
                'Budget': budget_val,
                'Value-Budget Variance %': budget_variance,
                'Value-Anchor Variance %': rev_variance,
                'Revenue-to-Payroll %': rev_to_pay_ratio_comp,
                'Budget-to-Payroll %': bud_to_pay_ratio_comp,
                'Revenue-to-Payroll Variance %': rev_to_pay_variance,
                'Budget-to-Payroll Variance %': bud_to_pay_variance
            })
        
        return pd.DataFrame(rows)

    def generate_comparison_insights(self, resort_config: Dict[str, Any],
                                    comparison_date: Union[str, datetime],
                                    anchor_date: Union[str, datetime],
                                    debug: bool = False) -> pd.DataFrame:
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
            print(header, end='')
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
        insights = self._generate_comparison_insights(
            comparison_revenue, comparison_payroll, comparison_budget, comparison_visits,
            anchor_revenue, anchor_payroll, anchor_budget, anchor_visits,
            department_to_title
        )
        if debug and debug_directory:
            insights_file = os.path.join(debug_directory, "comparison_insights.xlsx")
            with pd.ExcelWriter(insights_file, engine='xlsxwriter') as writer:
                insights.to_excel(writer, sheet_name='Comparison Insights', index=False)
            print(f"✓ Comparison insights exported: {insights_file}")
            if debug_log_handle:
                debug_log_handle.write(f"\n{'='*80}\nInsight generation complete!\n{'='*80}\n")
                debug_log_handle.close()
                print(f"✓ Debug log saved: {os.path.join(debug_directory, 'debugLog.txt')}")
        return insights


