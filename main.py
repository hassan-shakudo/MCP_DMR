"""
MCP Database Report Generator - Single Resort Entry Point
Reads configuration from environment variables
"""

print(f"[{__name__}] main.py loaded - __name__ = '{__name__}'")

import sys
import os
from analysis_engine import AnalysisEngine
from utils import log

def main():
    try:
        log("Starting DMR Generator...")

        # Check required environment variables
        required_vars = ['RESORT_NAME', 'DB_NAME', 'GROUP_NUM']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            log(f"Missing required environment variables: {', '.join(missing_vars)}", "ERROR")
            log("Please set: RESORT_NAME, DB_NAME, GROUP_NUM", "ERROR")
            return 1

        log(f"Configuration: RESORT_NAME={os.getenv('RESORT_NAME')}, DB_NAME={os.getenv('DB_NAME')}, GROUP_NUM={os.getenv('GROUP_NUM')}", "INFO")

        analysisEngine = AnalysisEngine(output_dir="reports")
        result = analysisEngine.generate_analysis()

        log(f"Generation complete! Results: {result}", "SUCCESS")
        return 0

    except Exception as e:
        log(f"Fatal error: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
