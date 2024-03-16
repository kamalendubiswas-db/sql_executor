import os

def read_sql_file(file_name, directory_name):
    """
    Reads the content of an SQL file into a variable.

    Args:
        file_name (str): The name of the SQL file.
        directory_name (str): The name of the directory containing the SQL file.

    Returns:
        str: The content of the SQL file.
    """
    # Construct the full path to the file
    file_path = os.path.join(directory_name, file_name+'.sql')

    try:
        # Open the file and read its content
        with open(file_path, 'r') as file:
            file_content = file.read()
        return file_content
    except FileNotFoundError:
        print(f"The file {file_name} was not found in {directory_name}.")
    except Exception as e:
        print(f"An error occurred while reading the file {file_name}: {e}")

# Example usage
if __name__ == "__main__":
    # Accept file name and directory name as input parameters
    file_name = input("Enter the SQL file name: ")
    directory_name = input("Enter the directory name: ")

    # Read the file content
    content = read_sql_file(file_name, directory_name)
    if content is not None:
        print("File content:")
        print(content)