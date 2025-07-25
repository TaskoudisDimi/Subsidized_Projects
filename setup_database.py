import psycopg2
from psycopg2 import sql
import csv
import os
import tempfile
import re

# Database connection parameters
DB_HOST = "localhost"
DB_USER = "postgres"
DB_PASSWORD = "9963"
DB_PORT = "5432"
DB_NAME = "thesis"

def connect_to_postgres():
    """Connect to PostgreSQL server without specifying a database."""
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def connect_to_database():
    """Connect to the 'thesis' database."""
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        database=DB_NAME
    )

def create_database():
    """Create the thesis database if it doesn't exist."""
    try:
        conn = connect_to_postgres()
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        print(f"Database {DB_NAME} created successfully.")
        
        cursor.close()
        conn.close()
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database {DB_NAME} already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")

def create_tables():
    """Drop and create the tables in the thesis database."""
    drop_tables = """
    DROP TABLE IF EXISTS table_countries;
    DROP TABLE IF EXISTS table_projects;
    """
    create_countries_table = """
    CREATE TABLE table_countries (
        id SERIAL PRIMARY KEY,
        country VARCHAR(1000) NOT NULL,
        name VARCHAR(500) NOT NULL,
        sector VARCHAR(500) NOT NULL
    );
    """
    
    create_projects_table = """
    CREATE TABLE table_projects (
        id SERIAL PRIMARY KEY,
        name VARCHAR(1000) NOT NULL,
        date VARCHAR(20) NOT NULL,
        amount NUMERIC NOT NULL
    );
    """
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        cursor.execute(drop_tables)
        cursor.execute(create_countries_table)
        cursor.execute(create_projects_table)
        
        conn.commit()
        print("Tables created successfully.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating tables: {e}")

def is_valid_numeric(value):
    """Check if a string is a valid numeric value."""
    return bool(re.match(r'^-?\d*\.?\d+$', value))

def clean_csv_file(input_path, output_path, expected_columns, delimiter=';'):
    """Clean CSV file, skip invalid rows, and handle missing id."""
    delimiter = ',' if 'projects' in input_path.lower() else ';'
    skipped_rows = []
    written_rows = []
    output_line_number = 0
    with open(input_path, 'r') as infile, open(output_path, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter=delimiter)
        writer = csv.writer(outfile, delimiter=delimiter, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
        
        # Write header (exclude id)
        expected_header = ['country', 'name', 'sector'] if 'countries' in input_path.lower() else ['name', 'date', 'amount']
        writer.writerow(expected_header)
        output_line_number += 1
        
        # Skip header in input
        header = next(reader)
        row_number = 1
        
        # Process rows
        for row in reader:
            row_number += 1
            # Skip completely empty rows
            if not any(row):
                skipped_rows.append((row_number, row, "Empty row"))
                continue
            
            # Strip whitespace and remove trailing empty fields
            cleaned_row = [field.strip() for field in row if field.strip()]
            
            # Log rows with too many fields
            if len(cleaned_row) > 4:
                skipped_rows.append((row_number, row, f"Too many fields: {len(cleaned_row)}"))
                continue
            
            # Adjust row to expected columns (exclude id)
            if 'countries' in input_path.lower():
                # For countries: [id],country,name,sector
                if len(cleaned_row) >= 3:  # Need at least country,name,sector
                    start_idx = 1 if len(cleaned_row) >= 4 and cleaned_row[0].isdigit() else 0
                    data_row = cleaned_row[start_idx:start_idx+3]
                else:
                    skipped_rows.append((row_number, row, "Insufficient columns"))
                    continue
            else:
                # For projects: [id],name,date,amount
                if len(cleaned_row) >= 3:  # Need at least name,date,amount
                    start_idx = 1 if len(cleaned_row) >= 4 and cleaned_row[0].isdigit() else 0
                    data_row = cleaned_row[start_idx:start_idx+3]
                else:
                    skipped_rows.append((row_number, row, "Insufficient columns"))
                    continue
            
            # Ensure exactly the expected number of columns
            if len(data_row) != len(expected_header):
                skipped_rows.append((row_number, row, f"Expected {len(expected_header)} columns, got {len(data_row)}"))
                continue
            
            # Validate NOT NULL columns
            is_valid = True
            for i, value in enumerate(data_row):
                if value == '':  # Strict check for empty string
                    skipped_rows.append((row_number, row, f"Empty value for {expected_header[i]}"))
                    is_valid = False
                    break
                # For projects, validate amount is numeric
                if 'projects' in input_path.lower() and i == 2:  # amount column
                    if not is_valid_numeric(value):
                        skipped_rows.append((row_number, row, f"Invalid numeric value for amount: {value}"))
                        is_valid = False
                        break
            
            # Skip summary rows (e.g., 'Total')
            if is_valid and data_row[0].lower() == 'total':
                skipped_rows.append((row_number, row, "Summary row (Total)"))
                is_valid = False
            
            if is_valid:
                # For projects, unquote amount by writing manually
                if 'projects' in input_path.lower():
                    output_line_number += 1
                    # Debug specific line
                    if output_line_number == 9937:
                        print(f"Debug Line 9937 (output CSV): Raw: {row}, Cleaned: {data_row}")
                    writer.writerow([data_row[0], data_row[1], float(data_row[2])])  # Convert amount to float to remove quotes
                else:
                    writer.writerow(data_row)
                    output_line_number += 1
                written_rows.append((row_number, row, data_row))
    
    print(f"Cleaned CSV file saved to {output_path}")
    if skipped_rows:
        print(f"Skipped {len(skipped_rows)} rows due to invalid data:")
        for row_num, row, reason in skipped_rows[:10]:  # Limit to 10 for brevity
            print(f"Line {row_num}: Raw: {row} - Reason: {reason}")
        if len(skipped_rows) > 10:
            print(f"... and {len(skipped_rows) - 10} more skipped rows")
    print(f"Wrote {len(written_rows)} valid rows to {output_path}")
    for row_num, raw_row, clean_row in written_rows[:10]:  # Limit to 10 for brevity
        print(f"Line {row_num}: Raw: {raw_row} -> Cleaned: {clean_row}")
    if len(written_rows) > 10:
        print(f"... and {len(written_rows) - 10} more written rows")
    
    # Log first few lines of cleaned CSV
    with open(output_path, 'r') as f:
        print(f"First 5 lines of {output_path}:")
        for i, line in enumerate(f.readlines()[:5], 1):
            print(f"Line {i}: {line.strip()}")

def import_csv_to_table(file_path, table_name, columns, delimiter=';'):
    """Import data from CSV file to the specified table."""
    delimiter = ',' if table_name == 'table_projects' else ';'
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Validate cleaned CSV
        with open(file_path, 'r') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader)  # Skip header
            row_count = 0
            for row_num, row in enumerate(reader, start=1):
                row_count += 1
                if len(row) != len(columns):
                    print(f"Warning: Row {row_num} in {file_path} has {len(row)} columns, expected {len(columns)}: {row}")
                    if row_num == 9937:
                        print(f"Debug Line 9937 in cleaned CSV: {row}")
        
        print(f"Importing {file_path} with {row_count} data rows")
        
        with open(file_path, 'r') as f:
            next(f)  # Skip header row
            cursor.copy_from(f, table_name, sep=delimiter, columns=columns, null='')
        
        conn.commit()
        print(f"Data imported successfully into {table_name} from {file_path}.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error importing data into {table_name}: {e}")
        raise  # Re-raise for debugging

def execute_queries():
    """Execute the specified SQL queries and print results."""
    queries = [
        (
            "Countries with exactly one project",
            """
            SELECT country
            FROM table_countries
            GROUP BY country
            HAVING COUNT(name) = 1;
            """
        ),
        (
            "Countries with more than 1000 projects",
            """
            SELECT country
            FROM table_countries
            GROUP BY country
            HAVING COUNT(name) > 1000;
            """
        ),
        (
            "Projects with amount > 1 billion",
            """
            SELECT tc.name
            FROM table_projects tp
            INNER JOIN table_countries tc ON tp.name = tc.name
            WHERE tp.amount > 1000000000;
            """
        ),
        (
            "Projects with amount < 10,000",
            """
            SELECT tc.name
            FROM table_projects tp
            INNER JOIN table_countries tc ON tp.name = tc.name
            WHERE tp.amount < 10000;
            """
        ),
        (
            "All records from table_countries",
            "SELECT * FROM table_countries;"
        ),
        (
            "All records from table_projects",
            "SELECT * FROM table_projects;"
        )
    ]
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        for query_name, query in queries:
            print(f"\nExecuting: {query_name}")
            cursor.execute(query)
            results = cursor.fetchall()
            if results:
                print("Results:")
                for row in results:
                    print(row)
            else:
                print("No results found.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error executing queries: {e}")

def main():
    # Paths to CSV files
    countries_csv = "data/Table_Countries.csv"
    projects_csv = "data/Table_Projects.csv"
    
    # Ensure CSV files exist
    for file_path in [countries_csv, projects_csv]:
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} not found.")
            return
    
    # Create temporary cleaned CSV files
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_countries, \
         tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_projects:
        clean_csv_file(countries_csv, temp_countries.name, expected_columns=4, delimiter=';')
        clean_csv_file(projects_csv, temp_projects.name, expected_columns=4, delimiter=',')
        
        # Create database and tables
        create_database()
        create_tables()
        
        # Import data (excluding id column)
        import_csv_to_table(
            temp_countries.name, 
            "table_countries", 
            columns=['country', 'name', 'sector'], 
            delimiter=';'
        )
        import_csv_to_table(
            temp_projects.name, 
            "table_projects", 
            columns=['name', 'date', 'amount'], 
            delimiter=','
        )
        
        # Clean up temporary files
        os.unlink(temp_countries.name)
        os.unlink(temp_projects.name)
    
    # Execute queries
    execute_queries()

if __name__ == "__main__":
    main()
