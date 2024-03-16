import yaml
import networkx as nx
import matplotlib.pyplot as plt
import concurrent.futures

from lib.task_executor import execute_task

def generate_graph(dependecy_yaml_file):
    """
    Generate a directed graph from a YAML file containing task dependencies.
    
    Args:
        dependecy_yaml_file (str): The path to the YAML file with dependencies.
        
    Returns:
        tuple: A tuple containing the directed graph (DiGraph) and the execution order (list).
    """
    try:
        with open(dependecy_yaml_file, 'r') as file:
            dependencies = yaml.safe_load(file)
    except Exception as e:
        print(f"Error reading YAML file {dependecy_yaml_file}: {e}")
        raise

    # Create a directed graph
    G = nx.DiGraph()

    # Add nodes and edges based on the YAML configuration
    for task, deps in dependencies.items():
        G.add_node(task)  # Add task node
        for dep in deps:
            G.add_node(dep)  # Add dependency node
            G.add_edge(dep, task)  # Add directed edge from dependency to task

    # Ensure the graph is a DAG
    if not nx.is_directed_acyclic_graph(G):
        raise Exception("The graph must be a Directed Acyclic Graph (DAG)")

    # Generate a topological sort of the graph to determine execution order
    execution_order = list(nx.topological_sort(G))
    return G, execution_order

def execute_tasks_sequentially(graph, execution_order,directory_name, cursor):
    """
    Execute tasks in a graph sequentially according to the topological sort order.
    
    Args:
        graph (DiGraph): The directed graph representing task dependencies.
        execution_order (list): The list representing the order of task execution.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        node_to_future = {}
        for node in execution_order:
            # Wait for the parent nodes' tasks to complete before submitting the current node's task
            parent_futures = [node_to_future[parent] for parent in graph.predecessors(node) if parent in node_to_future]
            concurrent.futures.wait(parent_futures)  # Wait for all parent tasks to complete
            
            # Submit the current node's task for execution
            future = executor.submit(execute_task, node, directory_name, cursor)
            node_to_future[node] = future

        # Wait for all tasks to complete and handle any exceptions
        for future in concurrent.futures.as_completed(node_to_future.values()):
            try:
                future.result()
            except Exception as e:
                print(f"Task execution resulted in an exception: {e}")


def save_dag(G, dag_name, node_color, edge_color):
    """
    Save the directed graph as a PNG image.
    
    Args:
        G (DiGraph): The directed graph to be saved.
        dag_name (str): The filename for the saved PNG image.
        node_color (str): The color code for the nodes.
        edge_color (str): The color code for the edges.
    """
    try:
        pos = nx.spectral_layout(G)  # positions for all nodes
        nx.draw(G, pos, with_labels=True, node_color=node_color, edge_color=edge_color, arrows=True)
        plt.savefig(dag_name, format="png", bbox_inches='tight')
    except Exception as e:
        print(f"Error saving DAG image {dag_name}: {e}")
    finally:
        plt.close()  # Close the figure to free up memory