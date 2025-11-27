"""
MCP Database Report Generator - Main Entry Point
Mountain Capital Partners - Ski Resort Data Analysis
"""

from datetime import datetime
from report_generator import ReportGenerator
from config import RESORT_MAPPING


def main():
    """Generate comprehensive reports for all configured resorts"""
    
    # Configuration
    OUTPUT_DIR = "reports"
    
    # Initialize generator
    generator = ReportGenerator(OUTPUT_DIR)
    
    # Get list of resorts to process
    # You can filter this list if you only want specific resorts
    resorts = RESORT_MAPPING
    
    saved_files = []
    
    print(f"Starting batch report generation for {len(resorts)} resorts...")
    
    for resort_config in resorts:
        try:
            file_path = generator.generate_comprehensive_report(
                resort_config=resort_config,
                run_date=datetime.now()
            )
            if file_path:
                saved_files.append(file_path)
        except Exception as e:
            print(f"❌ Failed to generate report for {resort_config.get('resortName')}: {e}")
            import traceback
            traceback.print_exc()
            
    print(f"\n✨ Generation complete! {len(saved_files)} reports created.")
    return saved_files


if __name__ == "__main__":
    main()
