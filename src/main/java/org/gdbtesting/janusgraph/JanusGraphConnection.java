package org.gdbtesting.janusgraph;


import afu.org.checkerframework.checker.oigj.qual.O;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.GraphDB;
import org.gdbtesting.Randomly;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.ConstantType;
import org.gdbtesting.gremlin.GraphDBExecutor;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.query.ResultWrapper;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.util.system.ConfigurationUtil;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphConnection extends GremlinConnection {

    private JanusGraphManagement mgmt;

    public JanusGraph getJanusGraph() {
        return janusGraph;
    }

    public void setJanusGraph(JanusGraph janusGraph) {
        this.janusGraph = janusGraph;
    }

    private JanusGraph janusGraph;


    public JanusGraphManagement getMgmt() {
        return mgmt;
    }


    public JanusGraphConnection(String version, String filename) {
        super(version, "JanusGraph", filename);
    }

    public void connect(){
        try {
            // connect 1
            /*
            String file = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            file = file.substring(0, file.lastIndexOf("target")+6);
            String exactfile = this.getClass().getClassLoader().getResource("conf/janusgraph.yaml").getPath();*/
            // IDEA use
            String exactfile = System.getProperty("user.dir")+"/conf/janusgraph.yaml";
            cluster = Cluster.open(exactfile);
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);

            PropertiesConfiguration conf = ConfigurationUtil.loadPropertiesConfig("conf/janusgraph-inmemory.properties");
            JanusGraph graph = JanusGraphFactory.open(conf);
            janusGraph = graph;
            mgmt = graph.openManagement();
            setJanusGraph(graph);

            // connect 2
            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
            setG(g);
            setGraph(g.getGraph());

            // traversal feedback
            clusterFeed = Cluster.open(System.getProperty("user.dir")+"/conf/janusgraph-traversal.yaml");
            clientFeed = clusterFeed.connect();
            setClusterFeed(clusterFeed);
            setClientFeed(clientFeed);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clear()  {
        try {
            JanusGraphFactory.drop(this.janusGraph);
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ResultWrapper execute(String query){
        try {
            List<Result> results = getClient().submit(query).all().get();
            ResultWrapper wrapper = new ResultWrapper();
            wrapper.fetch(results);
            wrapper.setName(GraphDB.JANUSGRAPH);
            return wrapper;
        } catch (InterruptedException | ExecutionException e) {
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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        JanusGraphConnection test = new JanusGraphConnection("0.6.2", "conf/remote-janusgraph.properties");
        test.connect();
        GraphTraversalSource g = test.getG();
        Vertex v = g.addV("person").next();
        g.V(v).property("name", "Alice").iterate();
        g.V(v).property("age", "13").iterate();
        System.out.println(g.V().count());
        JanusGraphManagement mgmt = test.getMgmt();
        JanusGraph graph = test.getJanusGraph();
        try {
            JanusGraphFactory.drop(graph);
        } catch (BackendException e) {
            e.printStackTrace();
        }
        System.out.println(g.V().count());
//        test.getGraph().tx().rollback(); //Never create new indexes while a transaction is active
//        List<PropertyKey> list = new ArrayList<>();
//        mgmt = graph.openManagement();
//        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
//        list.add(name);
//        mgmt.buildIndex("name", Vertex.class).addKey(name,Mapping.TEXT.asParameter()).buildMixedIndex("search");
//        System.out.println(mgmt.printIndexes());
//        mgmt.commit();
//        //Reindex the existing data
//        mgmt = graph.openManagement();
//        PropertyKey age1 = mgmt.makePropertyKey("age").dataType(Integer.class).make();
//        mgmt.buildIndex("nameAndAge", Vertex.class).addKey(list.get(0),Mapping.TEXT.asParameter()).addKey(age1).buildMixedIndex("search");
//        System.out.println(mgmt.printIndexes());
//        mgmt.commit();


        System.exit(0);
    }
}
