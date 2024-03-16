from datetime import datetime

# Import custom library functions
from lib.sql_parser import find_sql_files_and_parse, write_dependencies_to_yaml
from lib.task_generator import generate_graph, execute_tasks_sequentially, save_dag
from lib.connection import get_connection

try:
    connection = get_connection()
    cursor = connection.cursor()
except Exception as e:
    print(f"**An error occurred while connecting to Databricks SQL warehouse: {e}**")
    exit()

# Specify the root directories for source SQL, dependencies, and DAGs
source_sql_directory = 'source_sql/'
target_sql_directory = 'runs/executed_sql'
dependency_directory = 'runs/dependencies'
dag_directory = 'runs/DAGs'

# Generate a unique filename for each run using the current Unix Epoch timestamp
current_time = datetime.now()
run_epoch_timestamp = int(current_time.timestamp())
output_yaml_file = f'{dependency_directory}/{run_epoch_timestamp}_dependencies.yaml'
executed_sql_directory = f'{target_sql_directory}/{run_epoch_timestamp}'

# Define properties for the DAG visualization
node_color = "#FF3621"
edge_color = "#00A972"
dag_name = f'{dag_directory}/{run_epoch_timestamp}_dag_run.png'

try:
    # Find SQL files, parse them to generate the dependency YAML
    dependencies = find_sql_files_and_parse(source_sql_directory, executed_sql_directory)
    write_dependencies_to_yaml(dependencies, output_yaml_file)

    # Generate the graph and determine execution order through topological sort
    G, execution_order = generate_graph(output_yaml_file)

    # Execute tasks considering their dependencies
    execute_tasks_sequentially(G, execution_order, executed_sql_directory, cursor)

    # Save the DAG visualization to a file
    save_dag(G, dag_name, node_color, edge_color)

    cursor.close()
    connection.close()

except Exception as e:
    print(f"An error occurred during the process: {e}")