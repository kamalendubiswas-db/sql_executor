import yaml
import networkx as nx
import matplotlib.pyplot as plt

# Define the YAML configuration as a string
yaml_config = """
business_order:
- fact_order
dim_customer:
- raw_customers
dim_order:
- raw_orders
fact_order:
- dim_order
- dim_customer
"""

# Parse the YAML configuration
dependencies = yaml.safe_load(yaml_config)

# Create a directed graph
G = nx.DiGraph()

# Add nodes and edges based on the YAML configuration
for task, deps in dependencies.items():
    G.add_node(task)  # Add task node
    for dep in deps:
        G.add_node(dep)  # Add dependency node
        G.add_edge(dep, task)  # Add directed edge from dependency to task

# Draw the graph
pos = nx.spring_layout(G)  # positions for all nodes
nx.draw(G, pos, with_labels=True, arrows=True)
plt.show()  # Display the graph