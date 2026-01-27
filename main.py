"""
MCP Database Report Generator - Single Resort Entry Point
Reads configuration from environment variables
"""

from analysis_engine import AnalysisEngine

def main():
    analysisEngine = AnalysisEngine(output_dir="reports")
    result = analysisEngine.generate_analysis()

    return result


if __name__ == "__main__":
    main()
