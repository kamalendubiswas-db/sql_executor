import os
from sqlglot import parse_one, exp
import yaml
from datetime import datetime

def parse_sql_file(file_path):
    """
    Parse the SQL file to find table dependencies using sqlglot.
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

def find_sql_files_and_parse(root_directory):
    """
    Find all SQL files in the given directory and its subdirectories,
    and parse them to find table dependencies, storing only the file name.
    """
    table_dependencies = {}
    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                tables = parse_sql_file(file_path)
                if tables:
                    # Store only the file name instead of the full path
                    file_name = os.path.basename(file_path).rsplit('.', 1)[0]
                    table_dependencies[file_name] = tables
    return table_dependencies

def write_dependencies_to_yaml(dependencies, output_file):
    """
    Write the table dependencies to a YAML file.
    """
    with open(output_file, 'w') as file:
        yaml.dump(dependencies, file, default_flow_style=False)

# Specify the root directory where your SQL files are located
root_directory = 'sql/'

# Generate a unique filename for each run using the current Unix timestamp
current_time = datetime.now()
unix_timestamp = int(current_time.timestamp())
output_yaml_file = f'dependencies/{unix_timestamp}_dependencies.yaml'

# Find SQL files and parse them
dependencies = find_sql_files_and_parse(root_directory)

# Write the dependencies to a YAML file
write_dependencies_to_yaml(dependencies, output_yaml_file)

print(f"Table dependencies have been written to {output_yaml_file}")