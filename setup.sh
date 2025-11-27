#!/bin/bash
# Setup script for MCP Database Report Generator
# Installs system dependencies and Microsoft ODBC Driver 18 for SQL Server
# For Debian/Ubuntu systems

set -e  # Exit on error

echo "🚀 Setting up MCP Database Report Generator..."

# Add Microsoft GPG key
echo "📦 Adding Microsoft GPG key..."
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Add Microsoft package repository
echo "📦 Adding Microsoft package repository..."
curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

# Update package lists
echo "📦 Updating package lists..."
apt-get update

# Install system dependencies required for ODBC
echo "📦 Installing system dependencies..."
apt install -y curl gnupg apt-transport-https unixodbc-dev

# Install Microsoft ODBC Driver 18 for SQL Server
echo "📦 Installing Microsoft ODBC Driver 18 for SQL Server..."
ACCEPT_EULA=Y apt install -y msodbcsql18

# Verify ODBC driver installation
echo "✅ Verifying ODBC Driver installation..."
if odbcinst -q -d | grep -q "ODBC Driver 18"; then
    echo "✅ ODBC Driver 18 installed successfully!"
else
    echo "⚠️  Warning: ODBC Driver 18 not found. Please check installation."
fi

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "✨ Setup complete!"
echo ""
echo "To verify installation, run: odbcinst -q -d"

