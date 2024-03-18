from sqlglot import exp, parse_one
from sqlglot.optimizer.scope import build_scope
import os

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
            sql_content = parse_one(file.read())
            root = build_scope(sql_content)
            table_names = []
            table_object = [
                        source.name

                        # Traverse the Scope tree, not the AST
                        for scope in root.traverse()

                        # `selected_sources` contains sources that have been selected in this scope, e.g. in a FROM or JOIN clause.
                        # `alias` is the name of this source in this particular scope.
                        # `node` is the AST node instance
                        # if the selected source is a subquery (including common table expressions),
                        #     then `source` will be the Scope instance for that subquery.
                        # if the selected source is a table,
                        #     then `source` will be a Table instance.
                        for alias, (node, source) in scope.selected_sources.items()
                        if isinstance(source, exp.Table)
                    ]
            for table in table_object:
                if table not in table_names:
                    print(table)
                    table_names.append(table)
        return table_names
    except Exception as e:
        print(f"Error parsing file {file_path}: {e}")
        return []
    
parse_sql_file('source_sql/business/business_order.sql')