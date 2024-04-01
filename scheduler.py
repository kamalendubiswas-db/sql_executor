from datetime import datetime
import warnings
import logging

# Import custom library functions
from lib.sql_parser import find_sql_files_and_parse, write_dependencies_to_yaml
from lib.task_generator import generate_graph, execute_tasks_sequentially, save_dag
from lib.connection import get_connection

# Specify the root directories for source SQL, dependencies, and DAGs
source_sql_directory = "source_sql"
target_sql_directory = "runs/executed_sql"
dependency_directory = "runs/dependencies"
dag_directory = "runs/DAGs"
metadata_directory = "runs/metadata"

# Generate a unique filename for each run using the current Unix Epoch timestamp
current_time = datetime.now()
run_epoch_timestamp = int(current_time.timestamp())
output_yaml_file = f"{dependency_directory}/{run_epoch_timestamp}_dependencies.yaml"
executed_sql_directory = f"{target_sql_directory}/{run_epoch_timestamp}"
executed_metadata_directory = f"{metadata_directory}/{run_epoch_timestamp}"

# Define properties for the DAG visualization
node_color = "#FF3621"
edge_color = "#00A972"
node_size = 1000
dag_name = f"{dag_directory}/{run_epoch_timestamp}_dag_run.png"

warnings.filterwarnings("ignore")

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

# Create a file handler for writing logs to a file
file_handler = logging.FileHandler(f"{run_epoch_timestamp}_sql_executor.log")
file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_format)

# Create a console handler for outputting logs to the terminal
console_handler = logging.StreamHandler()
console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(console_format)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Establish connection
try:
    logging.info(f"Initiate DBSQL connection")
    connection = get_connection()
    cursor = connection.cursor()
    logging.info(f"Connection established")

except Exception as e:
    logging.error(f"An error occurred while connecting to Databricks SQL warehouse: {e}")
    exit()

# Start the process
try:
    # Find SQL files, parse them to generate the dependency YAML
    logging.info(f"Starting run id: {run_epoch_timestamp}")
    logging.info("Starting sql file parsing" )
    dependencies = find_sql_files_and_parse(source_sql_directory, executed_sql_directory, executed_metadata_directory)
    logging.info("Completed SQL file parsing" )
    
    logging.info("Starting writing dependencies to YAML" )
    write_dependencies_to_yaml(dependencies, output_yaml_file)
    logging.info("Completed writing dependencies to YAML" )

    # Generate the graph and determine execution order through topological sort
    logging.info("Starting generating dependency graph" )
    G, execution_order = generate_graph(output_yaml_file)
    logging.info("Completed generating dependency graph" )

    # Execute tasks considering their dependencies
    logging.info("Starting executing sql to DBSQL" )
    execute_tasks_sequentially(G, execution_order, executed_sql_directory, cursor)
    logging.info("Completed executing sql to DBSQL" )

    # Save the DAG visualization to a file
    logging.info("Starting saving the DAG" )
    save_dag(G, dag_name, node_color, edge_color, node_size)
    logging.info("Completed saving the DAG" )

    cursor.close()
    connection.close()
    logging.info(f"Connection closed")
    logging.info(f"Completed run id: {run_epoch_timestamp}")

except Exception as e:
    logging.error(f"An error occurred during the process: {e}")