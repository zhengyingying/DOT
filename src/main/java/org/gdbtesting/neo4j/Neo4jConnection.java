package org.gdbtesting.neo4j;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.GraphDB;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.query.ResultWrapper;
import org.gdbtesting.janusgraph.JanusGraphConnection;


import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Neo4jConnection extends GremlinConnection {

    public Neo4jConnection(String version, String filename) {
        super(version, "Neo4j", filename);
    }

    public void connect(){
        try {
            String file = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
//            file = file.substring(0,file.lastIndexOf("target")+6);
//            String exactfile = file + "/classes/conf/neo4j.yaml";
            String exactfile = System.getProperty("user.dir")+"/conf/neo4j.yaml";
            //String remote = file + "/classes/conf/remote-neo4j.properties";
            //String exactfile = "/opt/database/gdbtesting/conf/neo4j.yaml";
            //String remote = "/opt/database/gdbtesting/conf/remote-neo4j.properties";
            cluster = Cluster.open(exactfile);
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);
            // connect 2
//            g = traversal().withRemote(remote);
            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
            setG(g);
            setGraph(g.getGraph());

            // traversal feedback
            clusterFeed = Cluster.open(System.getProperty("user.dir")+"/conf/neo4j-traversal.yaml");
            clientFeed = clusterFeed.connect();
            setClusterFeed(clusterFeed);
            setClientFeed(clientFeed);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public ResultWrapper execute(String query) {
        try {
            List<Result> results = getClient().submit(query).all().get();
            ResultWrapper wrapper = new ResultWrapper();
            wrapper.fetch(results);
            wrapper.setName(GraphDB.NEO4J);
            return wrapper;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object executeExplain(String query){
        Object result = null;
        try{
            result = getClientFeed().submit(query).all().get().get(0).getObject();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        Neo4jConnection test = new Neo4jConnection("3.5.27", "conf/remote-neo4j.properties");
        GraphTraversalSource g = test.getG();

        g.E().drop().iterate();
        g.V().drop().iterate();

        Vertex bob = g.addV("person").property("name", "Bob").next();
        Vertex alice = g.addV("person").property("age", Double.POSITIVE_INFINITY ).next();
        Vertex ali = g.addV("person").property("age", 500 ).next();
        Vertex book = g.addV("person").property("name", "book1").next();

        Edge edge1 = g.addE("knows").from(bob).to(alice).property("d", 0.9,"C",55).next();
        Edge edge2 = g.addE("write").from(alice).to(book).property("d", 0.94461).next();
        //g.E(edge2.id()).property("d", 0.94461).iterate();

        String query1 = "g.V().order().by(asc)";

        try{
            List<Result> results = test.getClient().submit(query1).all().get();
            System.out.println(results.size());
            for (Result r : results) {
                System.out.println(r.toString());
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        String query2 = "g.V().order().by(asc).explain()";
        try{
            List<Result> results = test.getClient().submit(query2).all().get();
            System.out.println(results.size());
            for (Result r : results) {
                System.out.println(r.toString());
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        System.exit(0);
    }
}
