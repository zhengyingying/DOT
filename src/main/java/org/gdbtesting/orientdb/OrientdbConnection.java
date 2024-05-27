package org.gdbtesting.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.GraphDB;
import org.gdbtesting.connection.GremlinConnection;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.gdbtesting.gremlin.query.ResultWrapper;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class OrientdbConnection extends GremlinConnection {

    public void setOrientGraph(OrientGraph orientGraph) {
        this.orientGraph = orientGraph;
    }

    private OrientGraph orientGraph;

    public OrientGraph getOrientGraph(){return this.orientGraph;}

    public ODatabaseSession getDb() {
        return db;
    }

    private ODatabaseSession db;

    public OrientdbConnection(String version, String filename) {
        super(version, "OrientDB", filename);
    }

    public void connect() {
        try {
            // connect 1
            String file = this.getClass().getClassLoader().getResource("conf/orient.yaml").getPath();
            cluster = Cluster.open(file);
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);

//            OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/demodb","root","123456789");
            //Graph graph = factory.getNoTx();
            OrientDB orientDB = new OrientDB("remote:192.168.1.102", OrientDBConfig.defaultConfig());
            // Open the database session
            db = orientDB.open("demodb", "root", "root");

            OrientGraph graph = OrientGraph.open("remote:192.168.1.102/demodb", "root", "root");
            g = graph.traversal();
            setOrientGraph(graph);
            clear();

            setG(g);
            setGraph(graph);

            // traversal feedback
            clusterFeed = Cluster.open(System.getProperty("user.dir")+"/conf/orient-traversal.yaml");
            clientFeed = clusterFeed.connect();
            setClusterFeed(clusterFeed);
            setClientFeed(clientFeed);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        g.V().drop().iterate();
        g.E().drop().iterate();
        // Get the database schema
        db.getMetadata().getSchema().getClasses().clear();
    }

    @Override
    public ResultWrapper execute(String query) {
        try {
            List<Result> results = getClient().submit(query).all().get();
            ResultWrapper wrapper = new ResultWrapper();
            wrapper.fetch(results);
            wrapper.setName(GraphDB.ORIENTDB);
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

    public static void main(String[] args) {
        org.gdbtesting.orientdb.OrientdbConnection test = new org.gdbtesting.orientdb.OrientdbConnection("3.2.16", "conf/remote-orient.properties");
//        test.execute("g.V()");
        GraphTraversalSource g = test.getG();
        Graph graph = test.getGraph();

        g.E().drop().iterate();
        g.V().drop().iterate();

        Vertex Ironman = g.addV("Hero").property("name", "Tony").property("ATK", 100.00).next();
        Vertex Superman = g.addV("Hero").property("name", "Clark").property("ATK", Double.POSITIVE_INFINITY).next();
        Edge edge = Ironman.addEdge("Kill", Superman, "Time", "Today");
        Vertex Moly = g.addV("student").property("grade", 9).next();
        Vertex notebook = g.addV("homework").property("subject", "Math").next();
        Edge newedge = g.addE("write").from(Moly).to(notebook).property("date", "0.8").next();
        g.V(Moly).property("Class", "5").iterate();

        test.getOrientGraph().drop();

        String query1 = "g.V()";

        try {
            List<Result> results = test.getClient().submit(query1).all().get();
            System.out.println(results.size());
            for (Result r : results) {
                System.out.println(r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}