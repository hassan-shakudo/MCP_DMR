#!/bin/bash
# Run script for MCP Database Report Generator using a prebuilt runtime image.

set -euo pipefail

echo "🔧 Validating runtime image dependencies..."
if ! command -v odbcinst >/dev/null 2>&1 || ! odbcinst -q -d | grep -q "ODBC Driver 18"; then
    echo "❌ ODBC Driver 18 for SQL Server is not available in the runtime image."
    echo "   Please use the prebuilt DMR runtime image/environment config."
    exit 1
fi

echo "✅ Runtime image dependencies detected"
echo ""
echo "🚀 Starting DMR Generator..."
echo "📍 Configuration will be read from environment variables:"
echo "   - RESORT_NAME"
echo "   - DB_NAME"
echo "   - GROUP_NUM"
echo "   - RUN_DATE (optional, defaults to yesterday)"
echo ""

python3 main.py

echo ""
echo "✅ DMR Generator completed!"
