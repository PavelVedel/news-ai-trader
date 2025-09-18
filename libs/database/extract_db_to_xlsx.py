#!/usr/bin/env python3
"""
Extract database tables to Excel files
Specifically designed to extract the fundamentals table from news.db to xlsx format

Usage: python extract_db_to_xlsx.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# Add the parent directory to the path to import our database module
sys.path.append(str(Path(__file__).parent.parent.parent))

from libs.database.connection import DatabaseConnection


def extract_fundamentals_to_xlsx(db_path: str = "data/db/news.db", output_path: str = None) -> bool:
    """
    Extract fundamentals table from SQLite database to Excel file
    
    Args:
        db_path: Path to the SQLite database file
        output_path: Path for the output Excel file (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set default output path if not provided
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"fundamentals_export_{timestamp}.xlsx"
        
        # Ensure output directory exists
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to database
        db = DatabaseConnection(db_path)
        
        # Get all fundamentals data
        print("Extracting fundamentals data from database...")
        fundamentals_data = db.get_all_fundamentals()
        
        if not fundamentals_data:
            print("No fundamentals data found in the database.")
            return False
        
        print(f"Found {len(fundamentals_data)} records in fundamentals table")
        
        # Convert to DataFrame
        df = pd.DataFrame(fundamentals_data)
        
        # Display basic info about the data
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Show some basic statistics
        print("\nBasic statistics:")
        print(f"Symbols with market cap data: {df['market_cap'].notna().sum()}")
        print(f"Symbols with P/E ratio data: {df['pe_ratio'].notna().sum()}")
        print(f"Symbols with sector data: {df['sector'].notna().sum()}")
        print(f"Symbols with industry data: {df['industry'].notna().sum()}")
        
        # Save to Excel
        print(f"\nSaving to Excel file: {output_path}")
        df.to_excel(output_path, index=False, sheet_name='Fundamentals')
        
        print("✅ Successfully exported fundamentals data to Excel!")
        print(f"📁 File saved as: {output_path.absolute()}")
        
        # Show sample of the data
        print("\n📊 Sample of exported data:")
        print(df.head().to_string())
        
        return True
        
    except Exception as e:
        print(f"❌ Error extracting fundamentals data: {e}")
        return False
    finally:
        if 'db' in locals():
            db.close()


def extract_infos_to_xlsx(db_path: str = "data/db/news.db", output_path: str = None) -> bool:
    """
    Extract infos table from SQLite database to Excel file
    
    Args:
        db_path: Path to the SQLite database file
        output_path: Path for the output Excel file (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set default output path if not provided
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"infos_export_{timestamp}.xlsx"
        
        # Ensure output directory exists
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to database
        db = DatabaseConnection(db_path)
        
        # Get all infos data
        print("Extracting infos data from database...")
        infos_data = db.get_all_infos()
        
        if not infos_data:
            print("No infos data found in the database.")
            return False
        
        print(f"Found {len(infos_data)} records in infos table")
        
        # Convert to DataFrame
        df = pd.DataFrame(infos_data)
        
        # Display basic info about the data
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Show some basic statistics
        print("\nBasic statistics:")
        print(f"Symbols with sector data: {df['sector'].notna().sum()}")
        print(f"Symbols with industry data: {df['industry'].notna().sum()}")
        print(f"Symbols with country data: {df['country'].notna().sum()}")
        print(f"Symbols with website data: {df['website'].notna().sum()}")
        
        # Save to Excel
        print(f"\nSaving to Excel file: {output_path}")
        df.to_excel(output_path, index=False, sheet_name='Infos')
        
        print("✅ Successfully exported infos data to Excel!")
        print(f"📁 File saved as: {output_path.absolute()}")
        
        # Show sample of the data
        print("\n📊 Sample of exported data:")
        print(df.head().to_string())
        
        return True
        
    except Exception as e:
        print(f"❌ Error extracting infos data: {e}")
        return False
    finally:
        if 'db' in locals():
            db.close()


def extract_all_tables_to_xlsx(db_path: str = "data/db/news.db", output_dir: str = None) -> bool:
    """
    Extract all available tables from SQLite database to Excel files
    
    Args:
        db_path: Path to the SQLite database file
        output_dir: Directory for output Excel files (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if output_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = f"db_export_{timestamp}"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Creating export directory: {output_dir.absolute()}")
        
        # Extract fundamentals
        fundamentals_path = output_dir / "fundamentals.xlsx"
        success1 = extract_fundamentals_to_xlsx(db_path, str(fundamentals_path))
        
        # Extract infos
        infos_path = output_dir / "infos.xlsx"
        success2 = extract_infos_to_xlsx(db_path, str(infos_path))
        
        # Extract news_raw if it exists
        try:
            db = DatabaseConnection(db_path)
            with db.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM news_raw")
                news_count = cursor.fetchone()[0]
                
                if news_count > 0:
                    print(f"\nExtracting {news_count} news records...")
                    cursor.execute("SELECT * FROM news_raw ORDER BY created_at_utc DESC")
                    news_data = cursor.fetchall()
                    
                    df_news = pd.DataFrame([dict(row) for row in news_data])
                    news_path = output_dir / "news_raw.xlsx"
                    df_news.to_excel(news_path, index=False, sheet_name='News')
                    print(f"✅ News data exported to: {news_path}")
                else:
                    print("ℹ️  No news data found in database")
            
            db.close()
            
        except Exception as e:
            print(f"⚠️  Could not extract news data: {e}")
        
        return success1 or success2
        
    except Exception as e:
        print(f"❌ Error in bulk extraction: {e}")
        return False


def main():
    """Main function - extracts fundamentals table by default"""
    print("🚀 Database to Excel Extractor")
    print("=" * 50)
    
    # Default settings
    db_path = "data/db/news.db"
    table_choice = "fundamentals"  # Default to fundamentals table
    
    print(f"Database: {db_path}")
    print(f"Table: {table_choice}")
    print("=" * 50)
    
    # Check if database exists
    if not Path(db_path).exists():
        print(f"❌ Database file not found: {db_path}")
        print("Please make sure the database file exists at the specified path.")
        return False
    
    # Ask user what they want to extract
    print("\nWhat would you like to extract?")
    print("1. Fundamentals table (default)")
    print("2. Infos table") 
    print("3. All tables")
    
    try:
        choice = input("\nEnter your choice (1-3) or press Enter for default [1]: ").strip()
        
        if choice == "2":
            table_choice = "infos"
        elif choice == "3":
            table_choice = "all"
        elif choice == "" or choice == "1":
            table_choice = "fundamentals"
        else:
            print("Invalid choice, using default (fundamentals)")
            table_choice = "fundamentals"
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return False
    
    # Ask for custom output path
    try:
        custom_output = input("\nEnter custom output path (or press Enter for auto-generated): ").strip()
        output_path = custom_output if custom_output else None
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return False
    
    # Extract the data
    success = False
    
    if table_choice == "fundamentals":
        success = extract_fundamentals_to_xlsx(db_path, output_path)
    elif table_choice == "infos":
        success = extract_infos_to_xlsx(db_path, output_path)
    elif table_choice == "all":
        success = extract_all_tables_to_xlsx(db_path, output_path)
    
    if success:
        print("\n🎉 Extraction completed successfully!")
    else:
        print("\n❌ Extraction failed!")
    
    return success


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("Please check your database file and try again.")
