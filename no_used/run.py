import yaml
import networkx as nx
import matplotlib.pyplot as plt

# YAML content as a multi-line string
yaml_content = """
business_order:
    table:
        - name: fact_order
        - name: fact_order_line
    view:
        - name: vw_fact_order
        - name: vw_fact_order_line
dim_customer:
    table:
        - name: raw_customers
    view:
        - name: vw_raw_customers
    cte:
        - name: test_cte
"""

# Parse the YAML content
data = yaml.safe_load(yaml_content)

# Create a Directed Graph
G = nx.DiGraph()

# Function to add nodes and edges based on the YAML structure
def add_nodes_and_edges(parent, children, final_node):
    for child in children:
        G.add_node(child['name'])
        if parent:
            G.add_edge(parent, child['name'])
    # Connect all children to the final node
    for child in children:
        G.add_edge(child['name'], final_node)

# Iterate through the YAML data to add nodes and edges
for key, value in data.items():
    parent = None
    final_node = key  # 'business_order' or 'dim_customer'
    G.add_node(final_node)  # Add final node
    if 'table' in value:
        add_nodes_and_edges(parent, value['table'], final_node)
    if 'view' in value:
        add_nodes_and_edges(parent, value['view'], final_node)

# Draw the DAG
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, arrows=True)
plt.show()