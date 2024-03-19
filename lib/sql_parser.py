import os
from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope
import yaml
import shutil
import logging

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
            sql_content = parse_one(file.read())
            root = build_scope(sql_content)
            table_names = []

            # Traverse the Scope tree, not the AST
            for scope in root.traverse():
                
                # `selected_sources` contains sources that have been selected in this scope, e.g. in a FROM or JOIN clause.
                # `alias` is the name of this source in this particular scope.
                # `node` is the AST node instance
                # if the selected source is a subquery (including common table expressions),
                # then `source` will be the Scope instance for that subquery.
                # if the selected source is a table,
                # then `source` will be a Table instance.

                for alias, (node, source) in scope.selected_sources.items():
                        if isinstance(source, exp.Table):
                            table_names.append(source.name)
            return table_names
        
    except Exception as e:
        logging.error(f"Error parsing file {file_path}: {e}")
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
        logging.error(f"Directory {executed_sql_directory} already exists.")
    except Exception as e:
        logging.error(f"Error creating directory {executed_sql_directory}: {e}")
        return table_dependencies

    for root, dirs, files in os.walk(source_sql_directory):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                try:
                    shutil.copy(file_path, executed_sql_directory)
                    tables = parse_sql_file(file_path)
                    file_name = os.path.basename(file_path).rsplit('.', 1)[0]
                    table_dependencies[file_name] = tables
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
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
            for key, value in dependencies.items():
                if not value: # Check if the list is empty
                    dependencies[key] = None # Replace with None
            
        # Convert the modified dictionary to YAML
            yaml.dump(dependencies, file, sort_keys=True, default_flow_style=False)
    
    except Exception as e:
        logging.error(f"Error writing to YAML file {output_file}: {e}")
