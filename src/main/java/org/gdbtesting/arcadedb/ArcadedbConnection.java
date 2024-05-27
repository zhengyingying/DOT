package org.gdbtesting.arcadedb;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.gdbtesting.GraphDB;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.query.ResultWrapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class ArcadedbConnection extends GremlinConnection {

    public ArcadedbConnection(String version, String filename) {
        super(version, "ArcadeDB", filename);
    }

    public void connect() {
        try {
            // connect 1
            String exactfile = System.getProperty("user.dir")+"/conf/arcade.yaml";
//            String file = this.getClass().getClassLoader().getResource("conf/arcade.yaml").getPath();
            cluster = Cluster.open(exactfile);
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);

            // connect 2
            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
            setG(g);
            setGraph(g.getGraph());
            clear();

            setClusterFeed(cluster);
            setClientFeed(client);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        g.V().drop();
        g.E().drop();
    }

    @Override
    public ResultWrapper execute(String query) {
        try {
            List<Result> results = getClient().submit(query).all().get();
            ResultWrapper wrapper = new ResultWrapper();
            wrapper.fetch(results);
            wrapper.setName(GraphDB.ARCADEDB);
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

//        g.E().drop().iterate();
//        g.V().drop().iterate();

//        Vertex bob = g.addV("person").property("name", "Bob").next();
//        Vertex alex = g.addV("person").property("age", 0.29027268300579956).next();
//        Vertex jhon = g.addV("person").property("age", 2.94858941E8).next();
//        Vertex alice = g.addV("person").property("age", POSITIVE_INFINITY).next();
//        Vertex book = g.addV("person").property("name", "book1").next();
//        g.V(alex).property("name","Alex").iterate();
//        //Vertex alice = g.addV("person").property("age", POSITIVE_INFINITY ).next();
//
//        Edge edge1 = g.addE("knows").from(bob).to(alice).property("d", 0.9,"C",55).next();
//        Edge edge2 = g.addE("write").from(alice).to(book).property("d", 0.94461).next();
//        //g.E(edge2.id()).property("d", 0.94461).iterate();
//        //System.out.println(POSITIVE_INFINITY);
        String query = "g.addV('vl1').property('vp1',1)";
        String query1 = "g.addV('vl2').property('vp1',1)";

//        String query1 = "g.V().has('vl3', 'vp0',gte(7473443626965510961))";
        try{
            ArcadedbConnection connection = new ArcadedbConnection("23.2.1", "conf/remote-arcade.properties");
            connection.execute(query);
            connection.execute(query1);
//            connection.execute("g.V().drop().iterator()");
//            connection.execute("g.E().drop().iterator()");
//            connection.execute(
//                    "v1=g.addV('vl1').next()\n" +
//                    "v2=g.addV('vl2').next()\n" +
//                    "v3=g.addV('vl3').next()\n" +
//                    "g.V(v1).addE('el1').to(v2).next()\n" +
//                    "g.V(v3).addE('el2').to(v1).next()\n" +
//                    "g.V(v2).addE('el3').to(v3).next()\n");

//            connection.execute("g.addV('vt').property('pid','#224:101').next()");
//            connection.execute("g.addV('vl1').next()");
//            connection.execute("g.addV('vl4')");
//            ResultWrapper results = connection.execute("g.V('#148:74','#154:73','#151:74','#148:74','#154:73','#151:74').outE('el0')");
//            ResultWrapper results = connection.execute("g.E().as('a').id().as('b').V().hasLabel('vt').properties('pid').value().as('c').select('a').where('b',P.eq('c')).id()");
//            ResultWrapper results = connection.execute("g.V().hasLabel('vt').properties('pid').value().as('a').V().outE().as('b').id().as('c').where('a',P.eq('c')).select('b')");
//            ResultWrapper results = connection.execute("g.E().inV().in('el1','el2','el3').hasLabel('vl1')");
//            connection.execute("g.addV('vt').property('step','1').property('pid','1').next()");
//            ResultWrapper results = connection.execute("g.V().has('vt','step','2')");
//            ResultWrapper results = connection.execute("g.V().as('a').id().as('b').V().hasLabel('vt').properties('pid').value().as('c').select('a')");
//            ResultWrapper results = connection.execute("g.V().has('vt','step','2').properties('pid').value().as('a').V().as('b').id().as('c').where('a',P.eq('c')).select('b').hasLabel('vl1')");
//            for (String i : results.extractIds()) {
//                connection.execute("g.addV('vt').property('step','2').property('pid','" + i + "').next()");
//            }
//            for (String r : results.extractIds()) {
//                System.out.println(r);
//            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
