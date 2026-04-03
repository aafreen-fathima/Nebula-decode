# 🌌 OrkutSphere: Scalable Social Network Analysis

A distributed graph analytics platform built using **NebulaGraph** to process and analyze large-scale social network data from the Orkut dataset.

---

## 📌 Overview

OrkutSphere demonstrates how massive social graphs can be efficiently ingested, stored, queried, and analyzed using a distributed graph database.

**Dataset Scale:**
- 👥 3M+ users  
- 🔗 117M+ friendships  
- 🌐 5000+ communities  

---

## 🚀 Key Features

- ⚡ Distributed Graph Processing using NebulaGraph  
- 🐳 Docker-based Multi-node Deployment  
- 🔁 Fault Tolerance via RAFT Consensus  
- 📊 High-throughput Data Ingestion  
- 🔍 Advanced Graph Analytics  

---

## 🏗️ System Architecture

NebulaGraph follows a 3-tier architecture:

- **Meta Service (metad)** → Schema & cluster metadata  
- **Storage Service (storaged)** → Graph data storage  
- **Graph Service (graphd)** → Query execution  

### Cluster Setup

- 3 Meta Nodes  
- 5 Storage Nodes  
- 2 Graph Nodes  

---

## 📂 Data Model

### Entities

- `User(id)`
- `Community(community_index, is_top5000)`

### Relationships

- `FRIEND(ts)`
- `IN_COMMUNITY(dummy)`

---

## ⚙️ Core Functionalities

### 1. Data Ingestion

- Batch inserts for efficiency  
- Python ingestion scripts  
- Connection pooling  

---

### 2. Graph Analytics

- Mutual Friend Detection  
- Community Recommendation  
- Influential User Identification  
- Multi-hop Traversals  
- Small-world Analysis  

---

## 📊 Example Query

```sql
MATCH (u:User)-[:FRIEND]->(f:User)-[:IN_COMMUNITY]->(c:Community)
WHERE id(u) == 1000 AND NOT (u)-[:IN_COMMUNITY]->(c)
RETURN id(c), count(DISTINCT f)
ORDER BY count(DISTINCT f) DESC
LIMIT 10;
