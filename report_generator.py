"""
Report Generator for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
from datetime import datetime, timedelta
from typing import List
import pandas as pd

from db_connection import DatabaseConnection
from stored_procedures import StoredProcedures
from db_queries import execute_query_to_dataframe, get_revenue_query


class ReportGenerator:
    """Generate comprehensive ski resort reports"""
    
    def __init__(self, output_dir: str = "reports"):
        """
        Initialize report generator
        
        Args:
            output_dir: Directory to save CSV reports
        """
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✓ Created output directory: {output_dir}")
    
    def generate_revenue_report(self, conn, database: str, group_no: int, 
                               date_ini: datetime, date_end: datetime) -> pd.DataFrame:
        """Generate revenue report"""
        print(f"\n📊 Generating Revenue Report for {database}...")
        
        sp = StoredProcedures(conn)
        df = sp.execute_revenue(database, group_no, date_ini, date_end)
        
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} revenue records")
            print(f"   ✓ Total Revenue: ${df.iloc[:, -1].sum():,.2f}" if len(df.columns) > 0 else "")
        else:
            print("   ⚠ No revenue data found")
        
        return df
    
    def generate_payroll_report(self, conn, resort: str, 
                               date_ini: datetime, date_end: datetime) -> pd.DataFrame:
        """Generate payroll report"""
        print(f"\n💼 Generating Payroll Report for {resort}...")
        
        sp = StoredProcedures(conn)
        df = sp.execute_payroll(resort, date_ini, date_end)
        
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} payroll records")
        else:
            print("   ⚠ No payroll data found")
        
        return df
    
    def generate_visits_report(self, conn, resort: str, 
                              date_ini: datetime, date_end: datetime) -> pd.DataFrame:
        """Generate visits report"""
        print(f"\n👥 Generating Visits Report for {resort}...")
        
        sp = StoredProcedures(conn)
        df = sp.execute_visits(resort, date_ini, date_end)
        
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} visit records")
            if 'visits' in df.columns or len(df.columns) > 0:
                total_visits = df.iloc[:, -1].sum() if len(df) > 0 else 0
                print(f"   ✓ Total Visits: {total_visits:,.0f}")
        else:
            print("   ⚠ No visits data found")
        
        return df
    
    def generate_weather_report(self, conn, resort: str, 
                               date_ini: datetime, date_end: datetime) -> pd.DataFrame:
        """Generate weather/snow report"""
        print(f"\n🌨️  Generating Weather Report for {resort}...")
        
        sp = StoredProcedures(conn)
        df = sp.execute_weather(resort, date_ini, date_end)
        
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} weather records")
        else:
            print("   ⚠ No weather data found")
        
        return df
    
    def generate_complex_revenue_report(self, conn, database: str, group_no: int,
                                       date_ini: str, date_end: str) -> pd.DataFrame:
        """Generate complex revenue report with department breakdown"""
        print(f"\n💰 Generating Complex Revenue Report for {database}...")
        
        query = get_revenue_query(database, group_no, date_ini, date_end)
        df = execute_query_to_dataframe(conn, query)
        
        if not df.empty:
            print(f"   ✓ Retrieved {len(df)} detailed revenue records")
            if 'revenue' in df.columns:
                print(f"   ✓ Total Revenue: ${df['revenue'].sum():,.2f}")
        else:
            print("   ⚠ No complex revenue data found")
        
        return df
    
    def save_report(self, df: pd.DataFrame, report_name: str, resort: str = "") -> str:
        """
        Save DataFrame as CSV
        
        Args:
            df: DataFrame to save
            report_name: Name of the report (e.g., 'revenue', 'payroll')
            resort: Resort name (optional)
            
        Returns:
            Path to saved file
        """
        if df.empty:
            print(f"   ⚠ Skipping empty report: {report_name}")
            return ""
        
        # Create filename
        resort_prefix = f"{resort}_" if resort else ""
        filename = f"{resort_prefix}{report_name}_{self.timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        print(f"   ✓ Saved: {filepath}")
        
        return filepath
    
    def generate_all_reports(self, resort: str, database: str = None, 
                            group_no: int = -1, days_back: int = 30) -> List[str]:
        """
        Generate all reports for a resort
        
        Args:
            resort: Resort name (e.g., 'Purgatory', 'Snowbowl')
            database: Database name (defaults to resort name)
            group_no: Group number for the resort (-1 for all groups)
            days_back: Number of days to look back from today
            
        Returns:
            List of saved file paths
        """
        if database is None:
            database = resort
        
        # Calculate date range
        date_end = datetime.now()
        date_ini = date_end - timedelta(days=days_back)
        
        print(f"\n{'='*70}")
        print(f"  MCP Database Report Generator")
        print(f"  Mountain Capital Partners")
        print(f"{'='*70}")
        print(f"\n🏔️  Resort: {resort}")
        print(f"📅 Date Range: {date_ini.strftime('%Y-%m-%d')} to {date_end.strftime('%Y-%m-%d')}")
        print(f"📁 Output Directory: {self.output_dir}")
        print(f"{'='*70}")
        
        saved_files = []
        
        try:
            with DatabaseConnection() as conn:
                print("\n✓ Database connection established")
                
                # Generate Revenue Report
                try:
                    revenue_df = self.generate_revenue_report(
                        conn, database, group_no, date_ini, date_end
                    )
                    file_path = self.save_report(revenue_df, "revenue", resort)
                    if file_path:
                        saved_files.append(file_path)
                except Exception as e:
                    print(f"   ✗ Error generating revenue report: {e}")
                
                # Generate Payroll Report
                try:
                    payroll_df = self.generate_payroll_report(
                        conn, resort, date_ini, date_end
                    )
                    file_path = self.save_report(payroll_df, "payroll", resort)
                    if file_path:
                        saved_files.append(file_path)
                except Exception as e:
                    print(f"   ✗ Error generating payroll report: {e}")
                
                # Generate Visits Report
                try:
                    visits_df = self.generate_visits_report(
                        conn, resort, date_ini, date_end
                    )
                    file_path = self.save_report(visits_df, "visits", resort)
                    if file_path:
                        saved_files.append(file_path)
                except Exception as e:
                    print(f"   ✗ Error generating visits report: {e}")
                
                # Generate Weather Report
                try:
                    weather_df = self.generate_weather_report(
                        conn, resort, date_ini, date_end
                    )
                    file_path = self.save_report(weather_df, "weather", resort)
                    if file_path:
                        saved_files.append(file_path)
                except Exception as e:
                    print(f"   ✗ Error generating weather report: {e}")
                
                # Generate Complex Revenue Report
                try:
                    complex_revenue_df = self.generate_complex_revenue_report(
                        conn, database, group_no,
                        date_ini.strftime('%Y-%m-%d'),
                        date_end.strftime('%Y-%m-%d 23:59:59')
                    )
                    file_path = self.save_report(complex_revenue_df, "revenue_detailed", resort)
                    if file_path:
                        saved_files.append(file_path)
                except Exception as e:
                    print(f"   ✗ Error generating complex revenue report: {e}")
                
        except Exception as e:
            print(f"\n✗ Database connection error: {e}")
            return saved_files
        
        # Summary
        print(f"\n{'='*70}")
        print(f"  Report Generation Complete")
        print(f"{'='*70}")
        print(f"\n✓ Generated {len(saved_files)} report(s)")
        
        if saved_files:
            print("\n📄 Saved Files:")
            for file_path in saved_files:
                print(f"   • {file_path}")
        
        return saved_files

