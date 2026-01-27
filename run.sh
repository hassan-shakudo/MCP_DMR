#!/bin/bash
# Run script for MCP Database Report Generator
# Ensures dependencies are installed and runs the DMR generator

set -e  # Exit on error

echo "🔧 Checking dependencies..."

# Check if setup has been run by checking for ODBC driver
if ! odbcinst -q -d | grep -q "ODBC Driver 18"; then
    echo "📦 ODBC Driver not found. Running setup..."
    bash setup.sh
else
    echo "✅ ODBC Driver 18 found"

    # Still install/update Python dependencies
    echo "📦 Installing Python dependencies..."
    pip install -r requirements.txt
fi

echo ""
echo "🚀 Starting DMR Generator..."
echo "📍 Configuration will be read from environment variables:"
echo "   - RESORT_NAME"
echo "   - DB_NAME"
echo "   - GROUP_NUM"
echo "   - RUN_DATE (optional, defaults to yesterday)"
echo ""

# Run the DMR generator
python main.py

echo ""
echo "✅ DMR Generator completed!"
