# DOT

DOT is an automated tool to detect optimization bugs caused by incorrect implementation in Gremlin-based Graph Database Systems (GDBs).  

DOT first randomly generates a graph database and randomly generates valid Gremlin queries. Then, for a generated Gremlin query 𝑄, DOT generates candidate optimization configurations, and execute 𝑄 on the generated graph database with two different optimization configurations. Any inconsistency of their query results reveals an optimization bug in the target GDB. Once we detect an optimization bug, DOT can locate the faulty optimization strategies that trigger bugs. 


# Getting Started

## Requirements

- Java 11
- Maven 3 
- Operate System: Linux
- The GDBs that you want to test 
  
We take [TinkerGraph](https://github.com/tinkerpop/blueprints/wiki/tinkergraph) as an example to show how to use DOT. 

## Setting GDBs

1. Download GDBs

Download TinkerGraph server (version 3.6.2) from its [official website](https://www.apache.org/dyn/closer.lua/tinkerpop/3.6.2/apache-tinkerpop-gremlin-server-3.6.2-bin.zip), 
  unzip it to the folder `/opt/`.

2. Starting GDBs

Run the following commands to start, restart, and stop TinkerGraph.

```
/opt/apache-tinkerpop-gremlin-server-3.6.2/bin/gremlin-server.sh start 
/opt/apache-tinkerpop-gremlin-server-3.6.2/bin/gremlin-server.sh restart 
/opt/apache-tinkerpop-gremlin-server-3.6.2/bin/gremlin-server.sh stop 
```

When you see the following message, you have successfully started TinkerGraph:

```
Server started XXX.
```

Then, you can modify `conf/tinkergraph.yaml` and `conf/tinkergraph-traversal.yaml` with your own TinkerGraph settings (i.e., hosts and port).

## Start Testing
1. If it is your first time running DOT, you may run
```
cd DOT
mvn package -DskipTests
``` 
2. Modify `conf/tinkergraph.yaml` with your own TinkerGraph settings.

3. Run DOT with the command: `java -jar XXX.jar QueryDepth VerMaxNum EdgeMaxNum EdgeLabelNum VerLabelNum QueryNum TimeBudget tinkergraph` to test TinkerGraph.

You can configure the following parameters:
- `QueryDepth`, the max length of the query we generated, e.g, 10.
- `VerMaxNum`, the maximum number of the Vertex in the generated graph, e.g., 100.
- `EdgeMaxNum`, the maximum number of the Edge in the generated graph, e.g., 50.
- `EdgeLabelNum`, the maximum number of the edge label in the generated graph, e.g., 10.
- `VerLabelNum`, the maximum number of the vertex label in the generated graph, e.g., 10.
- `QueryNum`, the number of query generated in a test round, e.g., 1000.
- `TimeBudget`, the time to test target GDBs, e.g., 5 minutes.
- `TargetDb`, the target GDB, e.g., tinkergraph, hugegraph, janusgraph, arcadedb, orientdb, or neo4j.

## Logs

DOT stores logs in the `log-*` directory, including:
- `pairwise-check-1.txt`, record the reported potential bugs for a GDB
- `pairwise-result-1.txt`, record the query result of each Gremlin query for a GDB
- `pairwise-strategy-1.txt`, record the located faulty optimization strategies for each detected optimization bug
- `schema.txt`, record the schema information of the generated graph database.
- `xxx-graphdata.txt`, record the graph data information of each GDB.
- `xxx-Map.txt`, record the mapping information of vertices and edges of each GDB.
- `xxx-unique.txt`, record the number of unique combinations of optimization strategies.

## Found Bugs
DOT has found 28 optimization bugs in six popular and widely-used GDBs, i.e., [Neo4j](https://neo4j.com), [JanusGraph](https://janusgraph.org), [TinkerGraph](https://github.com/tinkerpop/blueprints/wiki/tinkergraph), [Arcadedb](https://arcadedb.com/), [Orientdb](https://www.orientdb.org/), and [HugeGraph](https://hugegraph.github.io/hugegraph-doc/). Among them, developers have confirmed 16 bugs, and fixed 5 bugs.

Note that two optimization bugs that we detect in Neo4j are caused by the Neo4j-Gremlin plugin developed by Apache TinkerPop instead of Neo4j itself. 
Furthermore, these two bugs are also similar to the bug reports we have submitted to the TinkerPop community earlier. 
Therefore, we do not generate new bug reports for them, and consider them as duplicate.

**Bug List**

| ID | GDB  |  Issue                                                                    | Status         | 
| -- | --------- | ------------------------------------------------------------------------ | -------------- | 
| 1  | HugeGraph | [HugeGraph-1951](https://github.com/apache/incubator-hugegraph/issues/1951) | Fixed |
| 2  | HugeGraph | [HugeGraph-2129](https://github.com/apache/incubator-hugegraph/issues/2129) | Unconfirmed |
| 3  | HugeGraph | [HugeGraph-2121](https://github.com/apache/incubator-hugegraph/issues/2121) | Confirmed |
| 4  | HugeGraph | [HugeGraph-2132](https://github.com/apache/incubator-hugegraph/issues/2132) | Unconfirmed |
| 5  | HugeGraph | [HugeGraph-2133](https://github.com/apache/incubator-hugegraph/issues/2133) | Confirmed |
| 6  | HugeGraph | [HugeGraph-2135](https://github.com/apache/incubator-hugegraph/issues/2135) | Unconfirmed |
| 7  | HugeGraph | [HugeGraph-1966](https://github.com/apache/incubator-hugegraph/issues/1966) | Intended |
| 8  | HugeGraph | [HugeGraph-2198](https://github.com/apache/incubator-hugegraph/issues/2198) | Confirmed |
| 9  | HugeGraph | [HugeGraph-1736](https://github.com/apache/incubator-hugegraph/issues/1736) | Fixed |
| 10 | HugeGraph | [HugeGraph-2163](https://github.com/apache/incubator-hugegraph/issues/2163) | Fixed |
| 11 | HugeGraph | [HugeGraph-2164](https://github.com/apache/incubator-hugegraph/issues/2164) | Confirmed |
| 12 | HugeGraph | [HugeGraph-2197](https://github.com/apache/incubator-hugegraph/issues/2197) | Unconfirmed |
| 13 | HugeGraph | [HugeGraph-2189](https://github.com/apache/incubator-hugegraph/issues/2189) | Confirmed |
| 14 | HugeGraph | [HugeGraph-1967](https://github.com/apache/incubator-hugegraph/issues/1967) | Confirmed |
| 15 | HugeGraph | [HugeGraph-1582](https://github.com/apache/incubator-hugegraph/issues/1582) | Confirmed |
| 16  | JanusGraph | [JanusGraph-3642](https://github.com/JanusGraph/janusgraph/issues/3642) | Unconfirmed |
| 17  | JanusGraph | [JanusGraph-3641](https://github.com/JanusGraph/janusgraph/issues/3641) | Unconfirmed |
| 18  | JanusGraph | [JanusGraph-3200](https://github.com/JanusGraph/janusgraph/issues/3200) | Confirmed |
| 19  | JanusGraph | [JanusGraph-3645](https://github.com/JanusGraph/janusgraph/issues/3645) | Unconfirmed |
| 20  | JanusGraph | [JanusGraph-3216](https://github.com/JanusGraph/janusgraph/issues/3216) | Confirmed |
| 21  | TinkerGraph | [TinkerGraph-2892](https://issues.apache.org/jira/browse/TINKERPOP-2892) | Duplicate |
| 22  | TinkerGraph | [TinkerGraph-2891](https://issues.apache.org/jira/browse/TINKERPOP-2891) | Fixed |
| 23  | TinkerGraph | [TinkerGraph-2893](https://issues.apache.org/jira/browse/TINKERPOP-2893) | Fixed |
| 24  | Arcadedb | [Arcadedb-999](https://github.com/ArcadeData/arcadedb/issues/999) | Confirmed |
| 25  | Orientdb | [Orientdb-9960](https://github.com/orientechnologies/orientdb/issues/9960) | Confirmed |
| 26  | Orientdb | [Orientdb-9885](https://github.com/orientechnologies/orientdb/issues/9885) | Intended |
| 27  | Neo4j | [TinkerGraph-2891](https://issues.apache.org/jira/browse/TINKERPOP-2891) | Duplicate |
| 28  | Neo4j | [TinkerGraph-2893](https://issues.apache.org/jira/browse/TINKERPOP-2893) | Duplicate |





