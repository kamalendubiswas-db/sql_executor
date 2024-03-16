import time
import random

def perform_task(node):
    """
    Perform a task on a given node with a simulated duration.

    Args:
        node (str): The name of the node on which to perform the task.
    """
    try:
        # Simulate task duration with random sleep
        duration = random.uniform(2, 5)  # Random duration between 2 and 5 seconds
        print(f"Starting task on node {node} at {time.asctime(time.gmtime())}")
        time.sleep(duration)  # Sleep to simulate task execution time
        print(f"Completed task on node {node} at {time.asctime(time.gmtime())}")
    except Exception as e:
        print(f"An error occurred while performing task on node {node}: {e}")