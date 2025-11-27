# MCP Database Report Generator - Setup Instructions

## Quick Setup

### Option 1: Automated Setup (Recommended)
Run the setup script:
```bash
sudo ./setup.sh
```

### Option 2: Manual Setup

#### 1. Install System Dependencies (Debian/Ubuntu)

```bash
# Add Microsoft GPG key
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Add Microsoft package repository
curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

# Update package lists
apt-get update

# Install system dependencies
apt install -y curl gnupg apt-transport-https unixodbc-dev

# Install Microsoft ODBC Driver 18 for SQL Server
ACCEPT_EULA=Y apt install -y msodbcsql18
```

#### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

#### 3. Verify Installation

```bash
# Check ODBC Driver
odbcinst -q -d | grep "ODBC Driver 18"

# Should output: ODBC Driver 18 for SQL Server
```

## Required Packages

### System Packages (installed via apt)
- `curl` - For downloading packages
- `gnupg` - For GPG key management
- `apt-transport-https` - For HTTPS package repositories
- `unixodbc-dev` - ODBC development libraries
- `msodbcsql18` - Microsoft ODBC Driver 18 for SQL Server

### Python Packages (installed via pip)
- `pandas>=1.5.0` - Data manipulation
- `pyodbc>=4.0.0` - ODBC database connectivity
- `xlsxwriter>=3.0.0` - Excel file generation
- `python-dotenv>=1.0.0` - Environment variable management


