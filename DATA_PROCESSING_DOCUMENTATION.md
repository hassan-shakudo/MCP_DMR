# MCP Daily Management Report - Data Processing Documentation

## Table of Contents
1. [Overview](#overview)
2. [Date Ranges](#date-ranges)
3. [Stored Procedures and Data Retrieval](#stored-procedures-and-data-retrieval)
4. [Data Processing by Category](#data-processing-by-category)
5. [Report Generation](#report-generation)
6. [Data Export for Debugging](#data-export-for-debugging)

---

## Overview

The MCP Daily Management Report system generates comprehensive Excel reports for ski resorts by:
1. Calculating 9 different date ranges
2. Executing stored procedures for each range to retrieve raw data
3. Processing and aggregating the data
4. Mapping departments between revenue and payroll
5. Calculating financial metrics (Revenue, Payroll, PR%)
6. Exporting results to Excel with formatted rows and columns

---

## Date Ranges

The system calculates 9 date ranges based on a reference date (typically yesterday or a specified date):

### 1. For The Day (Actual)
- **Definition**: The specific day being reported (start of day to end of day, or start of day to current time if reporting for today)
- **Calculation**: 
  - Start: `00:00:00` of the base date
  - End: `23:59:59` of the base date (or current time if current date)
- **Usage**: Primary comparison point for daily metrics

### 2. For The Day (Prior Year)
- **Definition**: Same day of week from 52 weeks ago (day-of-week aligned, not calendar date)
- **Calculation**: 
  - Start: `00:00:00` of (base_date - 52 weeks)
  - End: `23:59:59` of (base_date - 52 weeks)
- **Usage**: Year-over-year daily comparison

### 3. For The Week Ending (Actual)
- **Definition**: Monday of the current week through the report date
- **Calculation**:
  - Start: Monday `00:00:00` of the week containing base_date
  - End: base_date (or current time if current date)
- **Usage**: Week-to-date metrics for current week

### 4. For The Week Ending (Prior Year)
- **Definition**: Monday through same day-of-week from 52 weeks ago
- **Calculation**:
  - Start: Monday `00:00:00` of the week containing (base_date - 52 weeks)
  - End: (base_date - 52 weeks)
- **Usage**: Year-over-year week-to-date comparison

### 5. Week Total (Prior Year)
- **Definition**: Complete week (Monday to Sunday) from 52 weeks ago
- **Calculation**:
  - Start: Monday `00:00:00` of the week containing (base_date - 52 weeks)
  - End: Sunday `23:59:59` of that same week
- **Usage**: Full prior year week comparison

### 6. Month to Date (Actual)
- **Definition**: First day of current month through the report date
- **Calculation**:
  - Start: `00:00:00` on the 1st of the current month
  - End: base_date (or current time if current date)
- **Usage**: Month-to-date cumulative metrics

### 7. Month to Date (Prior Year)
- **Definition**: First day of same month last year through same calendar date
- **Calculation**:
  - Start: `00:00:00` on the 1st of the same month in prior year
  - End: Same calendar date in prior year (handles leap years)
- **Usage**: Year-over-year month-to-date comparison

### 8. For Winter Ending (Actual)
- **Definition**: November 1st of current season through the report date
- **Calculation**:
  - Season start: November 1st
    - If current month is Nov/Dec: season start year = current year
    - If current month is Jan-Oct: season start year = previous year
  - Start: `00:00:00` on November 1st of season start year
  - End: base_date (or current time if current date)
- **Usage**: Season-to-date cumulative metrics

### 9. For Winter Ending (Prior Year)
- **Definition**: November 1st of prior season through same calendar date last year
- **Calculation**:
  - End: Same calendar date in prior year
  - Start: November 1st of the prior season (determined by end date's month)
- **Usage**: Year-over-year season-to-date comparison

---

## Stored Procedures and Data Retrieval

For each date range, the system executes the following stored procedures:

### 1. Revenue Stored Procedure
- **SP Name**: `Shakudo_DMRGetRevenue`
- **Parameters**:
  - `@database`: Database/resort name (e.g., 'Purgatory', 'Snowbowl')
  - `@group_no`: Group number for the resort (e.g., 46 for PURGATORY)
  - `@date_ini`: Start date (datetime)
  - `@date_end`: End date (datetime)
- **Returns**: DataFrame with columns:
  - Department code/identifier (e.g., 'Department', 'DepartmentCode', 'deptCode')
  - Department title (e.g., 'DepartmentTitle', 'departmentTitle')
  - Revenue amount (e.g., 'Revenue', 'revenue', 'Amount')
- **Data Structure**: One row per transaction or aggregated revenue per department
- **Export**: Sorted by department code/column

### 2. Payroll Contract Stored Procedure
- **SP Name**: `Shakudo_DMRGetPayroll`
- **Parameters**:
  - `@resort`: Resort name (e.g., 'Purgatory', 'Snowbowl')
  - `@date_ini`: Start date (datetime)
  - `@date_end`: End date (datetime)
- **Returns**: DataFrame with columns:
  - Department code/identifier
  - Department title
  - `start_punchtime` or `StartPunchTime`: Employee clock-in time
  - `end_punchtime` or `EndPunchTime`: Employee clock-out time
  - `rate` or `Rate`: Hourly wage rate
- **Data Structure**: One row per employee time punch/record
- **Export**: Sorted by department code/column
- **Note**: Only fetched for past dates (not for current date reports)

### 3. Payroll Salary Active Stored Procedure
- **SP Name**: `Shakudo_DMRGetPayrollSalary`
- **Parameters**:
  - `@resort`: Resort name
- **Returns**: DataFrame with columns:
  - `deptcode` or `DeptCode`: Department code
  - `DepartmentTitle`: Department name
  - `rate_per_day` or `RatePerDay`: Daily salary rate for salaried employees
- **Data Structure**: One row per department with active salaried employees
- **Export**: Exported as "SalaryPayroll_PayrollSalary.xlsx" (not range-specific)
- **Note**: Fetched once per resort, not per range

### 4. Payroll History Stored Procedure
- **SP Name**: `Shakudo_DMRGetPayrollHistory`
- **Parameters**:
  - `@resort`: Resort name
  - `@date_ini`: Start date (datetime)
  - `@date_end`: End date (datetime)
- **Returns**: DataFrame with columns:
  - `department` or `Department`: Department code
  - `total` or `Total`: Historical payroll total for the department
- **Data Structure**: One row per department with aggregated historical totals
- **Usage**: Used for date ranges older than 7 days (for Month to Date and Winter Ending ranges)
- **Export**: Exported per range when applicable

### 5. Visits Stored Procedure
- **SP Name**: `Shakudo_DMRGetVists`
- **Parameters**:
  - `@resort`: Resort name
  - `@date_ini`: Start date (datetime)
  - `@date_end`: End date (datetime)
- **Returns**: DataFrame with columns:
  - `Location` or `location`: Location/resort identifier
  - `Visits` or `visits`: Number of visits (or count column)
- **Data Structure**: One row per location or aggregated visits per location
- **Export**: Not sorted (no department column)

### 6. Weather/Snow Stored Procedure
- **SP Name**: `Shakudo_GetSnow`
- **Parameters**:
  - `@resort`: Resort name
  - `@date_ini`: Start date (datetime)
  - `@date_end`: End date (datetime)
- **Returns**: DataFrame with columns:
  - `snow_24hrs` or `Snow24Hrs`: 24-hour snowfall amount
  - `base_depth` or `BaseDepth`: Base snow depth
- **Data Structure**: One row per measurement or aggregated data
- **Export**: Not sorted (no department column)

---

## Data Processing by Category

### Snow/Weather Processing

**Input**: DataFrame from `Shakudo_GetSnow` stored procedure

**Processing Steps**:
1. **Identify Columns**: 
   - Snow column: Look for `snow_24hrs`, `Snow24Hrs`, or `Snow_24hrs`
   - Base depth column: Look for `base_depth`, `BaseDepth`, or `Base_Depth`
2. **Aggregation**:
   - `snow_24hrs`: Sum all values in the snow column
   - `base_depth`: Sum all values in the base depth column
3. **Output**: 
   - Single value per range: `{'snow_24hrs': <sum>, 'base_depth': <sum>}`

**Formula**: 
```
snow_24hrs = SUM(snow_column)
base_depth = SUM(base_depth_column)
```

**Note**: If multiple rows exist, they are summed. If no data, values default to 0.0.

---

### Visits Processing

**Input**: DataFrame from `Shakudo_DMRGetVists` stored procedure

**Processing Steps**:
1. **Identify Columns**:
   - Location column: Look for `Location`, `location`, `Resort`, or `resort`
   - Visits column: Look for `Visits`, `visits`, `Count`, or `count`
2. **Grouping and Aggregation**:
   - Group by location column
   - If visits column exists: Sum visits per location
   - If no visits column: Count rows per location
3. **Output**: 
   - Dictionary: `{location: sum_of_visits}` per range
   - All unique locations collected across all ranges

**Formula**:
```
visits_per_location = GROUP BY location, SUM(visits_column)
OR
visits_per_location = GROUP BY location, COUNT(rows)
```

**Total Calculation**:
```
Total Tickets = SUM(all location visits)
```

---

### Revenue Processing

**Input**: DataFrame from `Shakudo_DMRGetRevenue` stored procedure

**Processing Steps**:
1. **Identify Columns**:
   - Department code: Look for `Department`, `department`, `DepartmentCode`, `department_code`, `deptCode`, `DeptCode`, `dept_code`, `Dept`, or `dept`
   - Department title: Look for `DepartmentTitle`, `department_title`, `departmentTitle`, `DeptTitle`, or `dept_title`
   - Revenue amount: Look for `Revenue`, `revenue`, `Amount`, or `amount`
2. **Department Mapping**:
   - Build `department_code_to_title` mapping from department code → department title
   - Trim whitespace from department codes for consistent matching
   - Store mapping for use in report generation
3. **Grouping and Aggregation**:
   - Group by department code column
   - Sum revenue amount per department
4. **Output**: 
   - Dictionary: `{department_code: sum_of_revenue}` per range
   - All unique departments collected across all ranges

**Formula**:
```
revenue_per_department = GROUP BY department_code, SUM(revenue_amount)
```

**Normalization**:
- All values converted to float (handles Decimal, None, etc.)
- None/null values converted to 0.0

---

### Payroll Processing

Payroll processing is the most complex, involving multiple data sources and different logic based on the date range type.

#### Data Sources:
1. **Payroll Contract** (`Shakudo_DMRGetPayroll`): Hourly employee time punches
2. **Payroll Salary Active** (`Shakudo_DMRGetPayrollSalary`): Daily rates for salaried employees
3. **Payroll History** (`Shakudo_DMRGetPayrollHistory`): Historical totals for ranges > 7 days old

#### Step 1: Calculate Contract Payroll

**Input**: DataFrame from `Shakudo_DMRGetPayroll`

**Processing Steps**:
1. **Identify Columns**:
   - Department code: Same candidates as revenue
   - Department title: Same candidates as revenue
   - Start time: `start_punchtime`, `StartPunchTime`, or `StartTime`
   - End time: `end_punchtime`, `EndPunchTime`, or `EndTime`
   - Rate: `rate`, `Rate`, or `HourlyRate`
2. **Data Type Conversion**:
   - Convert start/end times to datetime (coerce errors)
   - Convert rate to numeric (fill NaN with 0)
3. **Calculate Hours Worked**:
   ```
   hours_worked = (end_time - start_time).total_seconds() / 3600.0
   ```
4. **Calculate Wages**:
   ```
   wages = hours_worked * rate
   ```
   - **Note**: Currently uses simple linear calculation (no overtime logic)
   - Overtime logic (commented out): 
     - If hours_worked <= 8: `wages = hours_worked * rate`
     - If hours_worked > 8: `wages = (8 * rate) + ((hours_worked - 8) * rate * 1.5)`
5. **Aggregation**:
   - Group by department code
   - Sum wages per department
6. **Output**: Dictionary `{department_code: total_calculated_wages}`

**Formula**:
```
calculated_payroll[dept] = SUM(hours_worked * rate) for all employees in dept
```

#### Step 2: Process Salary Payroll Rates

**Input**: DataFrame from `Shakudo_DMRGetPayrollSalary` (fetched once per resort)

**Processing Steps**:
1. **Identify Columns**:
   - Department code: `deptcode`, `DeptCode`, `dept_code`, `Department`, or `department`
   - Rate per day: `rate_per_day`, `RatePerDay`, or `Rate`
   - Department title: Same candidates as revenue
2. **Build Rate Dictionary**:
   - Create `salary_payroll_rates = {dept_code: rate_per_day}`
   - Update `department_code_to_title` mapping
3. **Output**: Dictionary `{department_code: rate_per_day}`

#### Step 3: Process History Payroll (if applicable)

**Input**: DataFrame from `Shakudo_DMRGetPayrollHistory`

**Processing Steps**:
1. **Identify Columns**:
   - Department: `department`, `Department`, `Dept`, or `dept`
   - Total: `total`, `Total`, `amount`, or `Amount`
2. **Build History Dictionary**:
   - Create `history_payroll = {dept_code: total}`
3. **Usage**: Only fetched for:
   - Month to Date (Actual) - if range > 7 days
   - For Winter Ending (Actual) - if range > 7 days
   - All Prior Year ranges
4. **Date Range Adjustment**:
   - For Month to Date and Winter Ending: History fetched for range excluding recent 7 days
   - History end date = range end date - 7 days

#### Step 4: Combine Payroll Components by Range Type

The final payroll calculation varies by range type:

##### A. For The Day (Actual)
```
For each department:
  total_payroll = calculated_payroll[dept] + salary_payroll_rates[dept]
```

**Logic**: Contract payroll (from time punches) + one day of salary payroll

##### B. For The Week Ending (Actual)
```
For each department:
  days_in_range = number of days from Monday to report date
  salary_total = salary_payroll_rates[dept] * days_in_range
  total_payroll = calculated_payroll[dept] + salary_total
```

**Logic**: Contract payroll + (salary rate × number of days in range)

##### C. Month to Date (Actual) / For Winter Ending (Actual)

**If range <= 7 days**:
```
For each department:
  days_in_range = number of days in range
  salary_total = salary_payroll_rates[dept] * days_in_range
  total_payroll = calculated_payroll[dept] + salary_total
```

**If range > 7 days**:
```
For each department:
  recent_week_salary = salary_payroll_rates[dept] * 7
  rest_range_salary = history_payroll[dept]  (from history SP for range excluding recent 7 days)
  total_payroll = calculated_payroll[dept] + recent_week_salary + rest_range_salary
```

**Logic**: 
- Recent 7 days: Use current salary rates
- Older than 7 days: Use historical totals from PayrollHistory SP

##### D. All Prior Year Ranges
```
For each department:
  total_payroll = calculated_payroll[dept] + history_payroll[dept]
```

**Logic**: Contract payroll (if any) + historical payroll totals

#### Step 5: Handle Current Date Reports

**Special Case**: If report is for current date (today):
- Payroll Contract SP is skipped
- Payroll History SP is skipped
- All payroll values set to 0.0 for all departments found in revenue

**Output**: Dictionary `{department_code: total_payroll}` per range

---

## Report Generation

### Excel File Structure

The final report is written to an Excel file with the following structure:

#### Header Section
- **Cell A1**: Resort name, "Daily Management Report", and date information
  - Format: `"{resort_name} Resort\nDaily Management Report\nAs of {day_name} - {day_date}"`
  - Example: `"PURGATORY Resort\nDaily Management Report\nAs of Wednesday - 19 November, 2025"`

#### Column Headers (Row 1, Columns B-J)
- Each column represents one of the 9 date ranges
- Format: `"{range_name}\n{start_date} - {end_date}"`
- Example: `"For The Day (Actual)\nNov 19 - Nov 19"`

#### Row Structure

##### 1. Snow Section
- **Row**: "Snow 24hrs"
  - Values: `processed_snow[range]['snow_24hrs']` for each range
  - Format: Number with 1 decimal place
- **Row**: "Base Depth"
  - Values: `processed_snow[range]['base_depth']` for each range
  - Format: Number with 1 decimal place
- **Spacer**: Empty row

##### 2. Visits Section
- **Header Row**: "VISITS" (bold, gray background)
- **Location Rows**: One row per location (sorted alphabetically)
  - Row label: Location name
  - Values: `processed_visits[range].get(location, 0)` for each range
  - Format: Number with 2 decimal places
- **Total Row**: "Total Tickets"
  - Values: `SUM(processed_visits[range].values())` for each range
  - Format: Number with 2 decimal places
- **Spacer**: Empty row

##### 3. Financials Section
- **Header Row**: "FINANCIALS" (bold, gray background)

**For each department** (sorted by department code):
1. **Revenue Row**: `"{department_title} - Revenue"`
   - Values: `processed_revenue[range].get(dept_code, 0)` for each range
   - Format: Number with 2 decimal places, comma separator
   
2. **Payroll Row**: `"{department_title} - Payroll"`
   - Values: `processed_payroll[range].get(dept_code, 0)` for each range
   - Format: Number with 2 decimal places, comma separator
   
3. **PR% Row**: `"PR % of {department_title}"`
   - Formula: `(Revenue / Payroll) × 100`
   - Special handling:
     - Uses absolute values: `abs(revenue) / abs(payroll) × 100`
     - If revenue == 0 OR payroll == 0: Show 0%
     - Format: Percentage with 0 decimal places

**Totals Section**:
1. **Total Revenue Row**: "Total Revenue"
   - Values: `SUM(processed_revenue[range].values())` for each range
   
2. **Total Payroll Row**: "Total Payroll"
   - Values: `SUM(processed_payroll[range].values())` for each range
   
3. **PR % of Total Revenue Row**: "PR % of Total Revenue"
   - Formula: `(Total Revenue / Total Payroll) × 100`
   - Same special handling as department PR%
   
4. **Net Total Revenue Row**: "Net Total Revenue"
   - Formula: `Total Revenue - Total Payroll`
   - Format: Number with 2 decimal places

### Department Mapping Logic

The system matches departments between revenue and payroll using department codes:

1. **Code Normalization**:
   - All department codes are trimmed of whitespace: `trim_dept_code(code)`
   - Matching is case-sensitive but whitespace-insensitive

2. **Title Resolution**:
   - Titles are collected from:
     - Revenue SP results (department_title column)
     - Payroll Contract SP results (department_title column)
     - Payroll Salary SP results (DepartmentTitle column)
   - Mapping: `department_code_to_title[trimmed_code] = title`
   - If no title found, code is used as title (with warning)

3. **Row Generation**:
   - Rows are generated based on **payroll departments** (primary key)
   - For each payroll department:
     - Revenue row shows revenue for matching department (0 if not in revenue)
     - Payroll row shows payroll for the department
     - PR% row calculates percentage

4. **Department Sorting**:
   - Departments are sorted alphabetically by department code for consistent display

### Excel Formatting

- **Frozen Panes**: First row and first column are frozen
- **Column Widths**: 
  - Row header column (A): 30 characters
  - Data columns (B-J): 18 characters
- **Number Formats**:
  - Revenue/Payroll: `#,##0.00` (thousands separator, 2 decimals)
  - Snow: `0.0` (1 decimal)
  - PR%: `0"%"` (percentage, 0 decimals)
- **Borders**: All cells have borders
- **Header Format**: Bold, gray background (#D3D3D3), centered, text wrap

---

## Data Export for Debugging

### Export Functionality

Every stored procedure result is automatically exported to a separate Excel file for debugging purposes.

### File Naming Convention

**Format**: `{RangeName}_{SPname}.xlsx`

**Examples**:
- `For_The_Day_Actual_Revenue.xlsx`
- `For_The_Day_Actual_Payroll.xlsx`
- `For_The_Week_Ending_Actual_Visits.xlsx`
- `Month_to_Date_Actual_Weather.xlsx`
- `SalaryPayroll_PayrollSalary.xlsx` (not range-specific)

### File Sanitization

Invalid filename characters are replaced with underscores:
- Characters replaced: `< > : " / \ | ? *`
- Leading/trailing spaces and dots are removed

### Sorting Behavior

- **Revenue SP exports**: Sorted by department code/column (ascending)
- **Payroll SP exports**: Sorted by department code/column (ascending)
- **Other SP exports**: No sorting (original order)

### Export Location

All exported files are saved in the `reports/` directory (same as final reports).

### Export Structure

Each exported file contains:
- **Worksheet Name**: "Data"
- **Header Row**: Bold, gray background, with column names
- **Data Rows**: All data from the stored procedure result
- **Column Widths**: Auto-adjusted based on content (max 50 characters)
- **Formatting**: Borders on all cells

### When Exports Are Created

Exports are created during the report generation process:
1. **Salary Payroll**: Exported once per resort (before range processing)
2. **Per Range**: Each SP result is exported immediately after fetching:
   - Revenue
   - Payroll (if not current date)
   - Visits
   - Weather
   - Payroll History (if applicable)

### Empty Data Handling

If a stored procedure returns no data (empty DataFrame):
- Export is skipped
- Processing continues with empty/default values (0.0, empty dictionaries)

---

## Data Flow Summary

```
1. Calculate 9 Date Ranges
   ↓
2. For Each Range:
   ├─ Execute Revenue SP → Export → Process (group by dept, sum revenue)
   ├─ Execute Payroll SP → Export → Process (calculate wages, group by dept)
   ├─ Execute Visits SP → Export → Process (group by location, sum visits)
   └─ Execute Weather SP → Export → Process (sum snow_24hrs, sum base_depth)
   ↓
3. Fetch Salary Payroll Rates (once) → Export
   ↓
4. Fetch Payroll History (if needed) → Export
   ↓
5. Combine Payroll Components by Range Type
   ↓
6. Build Department Mapping (code → title)
   ↓
7. Generate Excel Report:
   ├─ Write Header
   ├─ Write Snow Rows
   ├─ Write Visits Rows
   └─ Write Financial Rows (Revenue, Payroll, PR% per department)
   ↓
8. Save Report File
```

---

## Key Formulas and Calculations

### Revenue
```
Revenue[dept, range] = SUM(revenue_amount) WHERE department = dept AND date IN range
```

### Payroll (varies by range)
```
For The Day (Actual):
  Payroll[dept] = ContractPayroll[dept] + SalaryRate[dept]

For The Week Ending (Actual):
  Payroll[dept] = ContractPayroll[dept] + (SalaryRate[dept] × days_in_range)

Month to Date / Winter Ending (Actual) - if range > 7 days:
  Payroll[dept] = ContractPayroll[dept] + (SalaryRate[dept] × 7) + HistoryPayroll[dept]

Prior Year Ranges:
  Payroll[dept] = ContractPayroll[dept] + HistoryPayroll[dept]
```

### PR% (Profit Ratio Percentage)
```
PR%[dept, range] = (Revenue[dept, range] / Payroll[dept, range]) × 100
  - Uses absolute values
  - Returns 0% if revenue = 0 OR payroll = 0
```

### Net Total Revenue
```
Net Total Revenue[range] = Total Revenue[range] - Total Payroll[range]
```

---

## Error Handling and Edge Cases

### Missing Data
- Empty DataFrames: Default to 0.0 or empty dictionaries
- Missing columns: System tries multiple column name candidates
- Missing departments: Revenue defaults to 0 if department not in revenue data

### Data Type Issues
- Decimal values: Converted to float via `normalize_value()`
- None/null values: Converted to 0.0
- Invalid dates: Coerced to NaT, rows dropped

### Department Matching
- Whitespace differences: Handled by trimming codes
- Missing titles: Code used as title (with warning)
- Departments only in payroll: Still displayed with revenue = 0

### Current Date Handling
- Payroll SPs skipped (no data available yet)
- All payroll values set to 0.0
- Report still generated with revenue and visits data

---

## Configuration

### Resort Configuration
Defined in `config.py`:
```python
RESORT_MAPPING = [
    {"dbName": "Purgatory", "resortName": "PURGATORY", "groupNum": 46},
    ...
]
```

### Column Name Candidates
Defined in `config.py` as `CandidateColumns`:
- Flexible column name matching (handles variations in SP output)
- Case-insensitive matching attempted in export sorting

### Stored Procedure Names
Defined in `config.py`:
```python
STORED_PROCEDURES = {
    'Revenue': 'exec Shakudo_DMRGetRevenue ...',
    'PayrollContract': 'exec Shakudo_DMRGetPayroll ...',
    ...
}
```

---

## Conclusion

This documentation provides a complete overview of how the MCP Daily Management Report system:
1. Retrieves data from stored procedures
2. Processes and aggregates the data
3. Combines multiple data sources (especially for payroll)
4. Maps departments between revenue and payroll
5. Calculates financial metrics
6. Generates formatted Excel reports
7. Exports raw data for debugging

The system is designed to handle variations in column names, missing data, and different date range requirements while maintaining accuracy and providing comprehensive reporting capabilities.

