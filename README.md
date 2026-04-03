🌌 OrkutSphere: Scalable Social Network Analysis

A distributed graph analytics platform built using NebulaGraph to process and analyze large-scale social network data from the Orkut dataset.

📌 Overview

OrkutSphere is designed to demonstrate how massive social graphs (millions of users and connections) can be efficiently:

Ingested
Stored
Queried
Analyzed

using a distributed graph database architecture.

The system processes:

👥 3M+ users
🔗 117M+ friendships
🌐 5000+ communities
🚀 Key Features
⚡ Distributed Graph Processing using NebulaGraph
🐳 Docker-based Multi-node Cluster Deployment
🔁 Fault Tolerance via RAFT Consensus
📊 High-throughput Data Ingestion Pipelines
🔍 Advanced Graph Queries & Analytics
🏗️ System Architecture

The system is built on NebulaGraph’s 3-tier architecture:

Meta Service (metad) → Manages schema & cluster metadata
Storage Service (storaged) → Stores graph data (vertices & edges)
Graph Service (graphd) → Handles queries
Cluster Configuration
3 Meta Nodes
5 Storage Nodes
2 Graph Nodes

Deployed using Docker containers for scalability and isolation.

📂 Data Model
Entities
User(id) → Represents each user
Community(community_index, is_top5000)
Relationships
FRIEND(ts) → Friendship connections
IN_COMMUNITY(dummy) → User-community membership
⚙️ Core Functionalities
1. Data Ingestion
Batch processing for efficiency
Python-based ingestion scripts
Connection pooling for scalability
2. Graph Analytics
👥 Mutual Friend Detection
🌐 Community Recommendations
📈 Influential User Identification
🔗 Multi-hop Traversal Queries
🌍 Small-world Network Analysis
3. Example Query
MATCH (u:User)-[:FRIEND]->(f:User)-[:IN_COMMUNITY]->(c:Community)
WHERE id(u) == 1000 AND NOT (u)-[:IN_COMMUNITY]->(c)
RETURN id(c), count(DISTINCT f)
ORDER BY count(DISTINCT f) DESC
LIMIT 10;
📊 Scalability & Performance
Horizontal Scaling
Add new storage nodes dynamically
Redistribute workload using:
SUBMIT JOB BALANCE LEADER;
Ingestion Performance
Parallel ingestion pipelines
Batch inserts (~10,000 records per batch)
Distributed writes across partitions
🛡️ Fault Tolerance

Powered by RAFT Consensus Algorithm:

Automatic leader election
Replication across nodes
Zero downtime during node failure

✔️ System continues running even if a storage node fails
✔️ Automatic recovery via log replay

🧪 Experiments Conducted
One-hop neighborhood exploration
Mutual friend analysis
Community recommendation ranking

These experiments validate:

High clustering in social graphs
Presence of triadic closure
Strong community structures
🧠 Key Learnings
Graph databases outperform relational systems for multi-hop queries
Partitioning + replication = scalability + reliability
Leader balancing is critical for performance optimization
🔮 Future Improvements
Real-time graph streaming
Graph-based recommendation systems
Visualization dashboards
Integration with ML models for predictive analytics
🛠️ Tech Stack
NebulaGraph
Docker
Python (Nebula Client API)
Distributed Systems (RAFT)
