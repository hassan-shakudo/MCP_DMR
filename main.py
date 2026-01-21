"""
MCP Database Report Generator - Main Entry Point
Mountain Capital Partners - Ski Resort Data Analysis
"""

from datetime import datetime
from analysis_engine import AnalysisEngine
from config import RESORT_MAPPING


def main(analysis_type: str = "both"):
    OUTPUT_DIR = "reports"
    analysisEngine = AnalysisEngine(OUTPUT_DIR)
    resorts = RESORT_MAPPING
    saved_files = {'reports': [], 'insights': []}
    
    print(f"Starting batch generation (analysis_type='{analysis_type}') for {len(resorts)} resorts...")
    
    for resort_config in resorts:
        try:
            result = analysisEngine.generate_analysis(
                resort_config=resort_config,
                run_date=datetime.now(),
                analysis_type=analysis_type
            )
            if result.get('report_path'):
                saved_files['reports'].append(result['report_path'])
            if result.get('insights_path'):
                saved_files['insights'].append(result['insights_path'])
        except Exception as e:
            print(f"❌ Failed to generate analysis for {resort_config.get('resortName')}: {e}")
            import traceback
            traceback.print_exc()
    
    report_count = len(saved_files['reports'])
    insights_count = len(saved_files['insights'])
    print(f"\n✨ Generation complete! {report_count} reports and {insights_count} insights files created.")
    return saved_files


if __name__ == "__main__":
    main()
