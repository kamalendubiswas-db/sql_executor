import os

def read_sql_file(file_name, directory_name):
    """
    Reads the content of an SQL file into a variable.

    Args:
        file_name (str): The name of the SQL file.
        directory_name (str): The name of the directory containing the SQL file.

    Returns:
        str: The content of the SQL file, or None if an error occurs.
    """
    # Construct the full path to the file by appending '.sql' extension
    file_path = os.path.join(directory_name, file_name + '.sql')

    try:
        # Open the file and read its content
        with open(file_path, 'r') as file:
            file_content = file.read()
        return file_content
    except FileNotFoundError:
        print(f"The file {file_name}.sql was not found in {directory_name}.")
    except Exception as e:
        print(f"An error occurred while reading the file {file_name}.sql: {e}")

def execute_task(file_name, directory_name, cursor):
    """
    Executes an SQL query read from a file.

    Args:
        file_name (str): The name of the SQL file without the extension.
        directory_name (str): The name of the directory containing the SQL file.
        cursor: Database cursor for executing SQL queries.
    """
    # Read the file content
    sql_query = read_sql_file(file_name, directory_name)
    if sql_query is not None:
        try:
            # Execute the SQL query
            cursor.execute(sql_query)
        except Exception as e:
            print(f"An error occurred while executing the SQL query from {file_name}.sql: {e}")