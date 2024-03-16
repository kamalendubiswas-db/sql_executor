import os
from sqlglot import parse_one, exp
import yaml
import shutil

def parse_sql_file(file_path):
    """
    Parse the SQL file to find table dependencies using sqlglot.
    
    Args:
        file_path (str): The path to the SQL file to be parsed.
        
    Returns:
        list: A list of table names that the parent table depends on.
    """
    try:
        with open(file_path, 'r') as file:
            parent_table_name = os.path.basename(file_path).rsplit('.', 1)[0]
            sql_content = file.read()
            table_names = []
            for child_table in parse_one(sql_content).find_all(exp.Table):
                if parent_table_name != child_table.name:
                    table_names.append(child_table.name)
            return table_names
    except Exception as e:
        print(f"Error parsing file {file_path}: {e}")
        return []

def find_sql_files_and_parse(source_sql_directory, executed_sql_directory):
    """
    Find all SQL files in the given directory and its subdirectories,
    parse them to find table dependencies, and copy the files to a new directory.
    
    Args:
        source_sql_directory (str): The source directory containing SQL files.
        executed_sql_directory (str): The directory to copy the parsed SQL files to.
        
    Returns:
        dict: A dictionary mapping file names to their table dependencies.
    """
    table_dependencies = {}
    try:
        os.mkdir(executed_sql_directory)
    except FileExistsError:
        print(f"Directory {executed_sql_directory} already exists.")
    except Exception as e:
        print(f"Error creating directory {executed_sql_directory}: {e}")
        return table_dependencies

    for root, dirs, files in os.walk(source_sql_directory):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                try:
                    shutil.copy(file_path, executed_sql_directory)
                    tables = parse_sql_file(file_path)
                    if tables:
                        file_name = os.path.basename(file_path).rsplit('.', 1)[0]
                        table_dependencies[file_name] = tables
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
    return table_dependencies

def write_dependencies_to_yaml(dependencies, output_file):
    """
    Write the table dependencies to a YAML file.
    
    Args:
        dependencies (dict): The table dependencies to write.
        output_file (str): The path to the output YAML file.
    """
    try:
        with open(output_file, 'w') as file:
            yaml.dump(dependencies, file, default_flow_style=False)
    except Exception as e:
        print(f"Error writing to YAML file {output_file}: {e}")
