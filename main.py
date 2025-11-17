"""
MCP Database Report Generator - Main Entry Point
Mountain Capital Partners - Ski Resort Data Analysis
"""

from report_generator import ReportGenerator


def main():
    """Generate comprehensive reports and save them as CSV files"""
    
    # Configuration - Edit these values as needed
    RESORT = "Purgatory"           # Resort name
    DATABASE = "Purgatory"         # Database name (usually same as resort)
    GROUP_NO = 46                  # Group number (46 for Purgatory, -1 for all)
    DAYS_BACK = 30                 # Number of days to look back
    OUTPUT_DIR = "reports"         # Output directory for CSV files
    
    # Generate reports
    generator = ReportGenerator(OUTPUT_DIR)
    saved_files = generator.generate_all_reports(
        resort=RESORT,
        database=DATABASE,
        group_no=GROUP_NO,
        days_back=DAYS_BACK
    )
    
    return saved_files
