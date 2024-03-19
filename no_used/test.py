import os
from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope

def parse_sql_file(file_path):
    """
    Parse the SQL file to find table dependencies using sqlglot.
    
    Args:
        file_path (str): The path to the SQL file to be parsed.
        
    Returns:
        list: A list of table names that the parent table depends on, or an empty list if no 'from' clause is available.
    """
    try:
        with open(file_path, 'r') as file:
            sql_content = parse_one(file.read())
            root = build_scope(sql_content)
            table_names = []

            # Initialize a flag to check if a 'FROM' clause is found
            from_clause_found = False

            for scope in root.traverse():
                for alias, (node, source) in scope.selected_sources.items():
                    if isinstance(source, exp.Table):
                        from_clause_found = True
                        table_names.append(source.name)

            # If no 'FROM' clause is found, return an empty list
            if not from_clause_found:
                return []
            print(table_names)
            return table_names
    except Exception as e:
        print(f"Error parsing file {file_path}: {e}")
        return []
    
parse_sql_file('source_sql/raw/test_view.sql')