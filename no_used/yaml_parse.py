import yaml
import networkx as nx
import matplotlib.pyplot as plt

# YAML configuration as a multi-line string
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
insert_test: null
raw_customers: null
raw_orders: null
"""

# Parse the YAML configuration
config = yaml.safe_load(yaml_config)

# Create a Directed Graph
G = nx.DiGraph()

# Add nodes and edges based on the YAML configuration
for parent, children in config.items():
    G.add_node(parent)  # Ensure the parent node is added
    if children:  # Check if there are any children (not null)
        for child in children:
            G.add_node(child)  # Ensure the child node is added
            G.add_edge(parent, child)  # Add a directed edge from parent to child

# Visualize the DAG
pos = nx.spring_layout(G)  # Generate positions for all nodes
nx.draw(G, pos, with_labels=True, arrows=True, node_size=1000, node_color='lightblue', font_size=10, font_weight='bold')
plt.show()