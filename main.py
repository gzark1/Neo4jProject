from neo4j import GraphDatabase

# Replace 'bolt://localhost:7687' with the actual URL of your Neo4j database
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
database_name = "Graph DBMS"  # Replace with your database name

# Create the driver with the specified database name
driver = GraphDatabase.driver(uri, auth=(username, password), database=database_name)


#File paths for dataset files
action_features_file = "mooc_action_features.tsv"
action_labels_file = "mooc_action_labels.tsv"
actions_file = "mooc_actions.tsv"

# Create a Neo4j session
driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()
result = session.run("MATCH (n) RETURN n")
for record in result:
    print(record)
#Function to load action features into Neo4j
def load_action_features():
    with open(action_features_file, 'r') as file:
        next(file)  # Skip the header line
        for line in file:
            action_id, feature0, feature1, feature2, feature3 = line.strip().split('\t')
            query = """
            MERGE (a:Action {actionID: $actionID})
            SET a.feature0 = $feature0, a.feature1 = $feature1, a.feature2 = $feature2, a.feature3 = $feature3
            """
            session.run(query, actionID=int(action_id), feature0=float(feature0), feature1=float(feature1),
                        feature2=float(feature2), feature3=float(feature3))

# Function to load action labels into Neo4j
def load_action_labels():
    with open(action_labels_file, 'r') as file:
        next(file)  # Skip the header line
        for line in file:
            action_id, label = line.strip().split('\t')
            query = """
            MATCH (a:Action {actionID: $actionID})
            SET a.label = $label
            """
            session.run(query, actionID=int(action_id), label=int(label))

# Function to load actions into Neo4j
def load_actions():
    with open(actions_file, 'r') as file:
        next(file)  # Skip the header line
        for line in file:
            action_id, user_id, target_id, timestamp = line.strip().split('\t')
            query = """
            MATCH (u:User {userID: $userID})
            MERGE (a:Action {actionID: $actionID})
            MERGE (t:Target {targetID: $targetID})
            CREATE (u)-[:PERFORMED {timestamp: $timestamp}]->(a)
            CREATE (a)-[:TARGETS]->(t)
            """
            session.run(query, userID=int(user_id), actionID=int(action_id), targetID=int(target_id),
                        timestamp=int(timestamp))

# Load the data into Neo4j
load_action_features()
load_action_labels()
load_actions()

# Close the Neo4j session
session.close()
driver.close()
