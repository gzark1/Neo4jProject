from neo4j import GraphDatabase
import pprint

# Replace 'bolt://localhost:7687' with the actual URL of your Neo4j database
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
database_name = "Graph DBMS"  # Replace with your database name

# Create the driver with the specified database name
driver = GraphDatabase.driver(uri, auth=(username, password), database=database_name)

# File paths for dataset files
action_features_file = "mooc_action_features.tsv"
action_labels_file = "mooc_action_labels.tsv"
actions_file = "mooc_actions.tsv"

# Create a Neo4j session
driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()


# load action features
def load_action_features():
    actions_features = dict()
    # Load action features
    with open(action_features_file, 'r') as file:
        lines = file.readlines()[1:]  # Slice the list to exclude the first line
        for line in lines:
            features = line.strip().split('\t')
            action_id = int(features[0])
            actions_features[action_id] = features[1:]
    return actions_features


def load_action_labels():
    actions_labels = dict()
    # Load action labels
    with open(action_labels_file, 'r') as file:
        lines = file.readlines()[1:]  # Slice the list to exclude the first line
        for line in lines:
            labels = line.strip().split('\t')
            action_id = int(labels[0])
            actions_labels[action_id] = int(labels[1])
    return actions_labels


def load_dataset():
    # Load users
    users = set()
    targets = set()
    action_labels = load_action_labels()
    action_feature = load_action_features()
    with open(actions_file, 'r') as file:
        lines = file.readlines()[1:]  # Slice the list to exclude the first line
        for line in lines:
            actions = line.strip().split('\t')
            action_id = int(actions[0])
            user_id = actions[1]
            target_id = actions[2]
            timestamp = actions[3]
            # Create a user node if it does not exist
            if user_id not in users:
                session.run("CREATE (u:User {id: $id})", id=user_id)
                users.add(user_id)
            # Create a target node if it does not exist
            if target_id not in targets:
                session.run("CREATE (t:Target {id: $id})", id=target_id)
                targets.add(target_id)
            # Create performed action edge between user and target
            features = action_feature[action_id]
            # Check if action has label if not set it to -1
            if action_id in action_labels:
                label = action_labels[action_id]
            else:
                label = -1
            session.run("MATCH (u:User {id: $user_id}), (t:Target {id: $target_id}) "
                        "CREATE (u)-[:PERFORMED_ACTION {action_id: $action_id, timestamp: $timestamp,"
                        " feature0:$feature0, feature1:$feature1, feature2:$feature2, feature3:$feature3,"
                        " label:$label }]->(t)",
                        user_id=user_id, target_id=target_id, action_id=action_id, timestamp=timestamp,
                        feature0=features[0], feature1=features[1], feature2=features[2], feature3=features[3],
                        label=label)


load_dataset()
