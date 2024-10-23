import os
import pymysql

# Database configuration
db_user = "root"
db_host = "localhost"
db_password = "actowiz"
db_database = "blinkit_"

# Step 1: Establish a connection to the MySQL server (without specifying the database initially)
connection = pymysql.connect(
    host=db_host,   # Your MySQL host (e.g., 'localhost')
    user=db_user,   # Your MySQL username
    password=db_password  # Your MySQL password
)

try:
    # Step 2: Create a cursor object
    cursor = connection.cursor()

    # Step 3: Execute a query to create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_database}")
    print(f"Database '{db_database}' created or already exists.")

finally:
    # Step 4: Close the cursor and connection
    cursor.close()
    connection.close()

# Step 5: Reconnect to the newly created or existing database
connection = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_database
)

# List of SQL file paths (you can add more paths to this list)
sql_file_paths = [
    r'C:\Shalu\LiveProjects\blinkit_\input_files\blinkit_links_comp.sql',
    r'C:\Shalu\LiveProjects\blinkit_\input_files\blinkit_links_roshi.sql'  # Add more SQL files as needed
#
]

# Step 6: Loop through the SQL files and execute them
try:
    with connection.cursor() as cursor:
        for sql_file_path in sql_file_paths:
            if os.path.exists(sql_file_path):
                print(f"Executing SQL file: {sql_file_path}")

                with open(sql_file_path, 'r') as file:
                    sql_script = file.read()

                # Split SQL script into individual statements using ';' as a separator
                sql_statements = sql_script.split(';')

                # Execute each SQL statement
                for statement in sql_statements:
                    if statement.strip():  # Skip empty statements
                        cursor.execute(statement)

                connection.commit()  # Commit the transaction after executing the SQL file
            else:
                print(f"SQL file not found: {sql_file_path}")

    print("All SQL files executed successfully.")

finally:
    # Step 7: Close the connection to the database
    connection.close()
