package org.gdbtesting.tinkergraph;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import org.apache.tinkerpop.gremlin.driver.Cluster;
//import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.GraphDB;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.query.ResultWrapper;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


public class TinkerGraphConnection extends GremlinConnection {

    public TinkerGraph getTinkerGraph() {
        return tinkerGraph;
    }

    public void setTinkerGraph(TinkerGraph tinkerGraph) {
        this.tinkerGraph = tinkerGraph;
    }

    private TinkerGraph tinkerGraph;

    public TinkerGraphConnection(String version) {
        super(version, "TinkerGraph");
    }

    public TinkerGraphConnection(String version, String filename) {
        super(version, "TinkerGraph", filename);
    }


    public void connect() {
        try {
//            String file = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
//            file = file.substring(0, file.lastIndexOf("target") + 6);
//            String exactfile = System.getProperty("user.dir")+"/conf/tinkergraph.yaml";
            // IDEA use
            String exactfile = "./conf/tinkergraph.yaml";
//            String file = "/opt/database/conf/tinkergraph.yaml";
            cluster = Cluster.open(exactfile);
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);

            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
            setG(g);
            setGraph(g.getGraph());

            Configuration conf = new BaseConfiguration();
            conf.setProperty("gremlin.remote.remoteConnectionClass", "org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection");
            conf.setProperty("gremlin.remote.driver.clusterFile", "conf/tinkergraph.yaml");
            conf.setProperty("gremlin.remote.driver.sourceName", "g");
            tinkerGraph = TinkerGraph.open(conf);
            clear();

            // traversal feedback
            clusterFeed = Cluster.open(System.getProperty("user.dir")+"/conf/tinkergraph-traversal.yaml");
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
    }

    @Override
    public ResultWrapper execute(String query) {
        try {
            List<Result> results = getClient().submit(query).all().get();
            ResultWrapper wrapper = new ResultWrapper();
            wrapper.fetch(results);
            wrapper.setName(GraphDB.TINKERGRAPH);
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

    public TinkerGraph getGraph() {
        return (TinkerGraph) graph;
    }

    public static void main(String[] args) {


        TinkerGraphConnection connection = new TinkerGraphConnection("");
        GraphTraversalSource g = connection.getG();
//        g.E().drop().iterate();
//        g.V().drop().iterate();
//
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
        //g.E(edge2.id()).property("d", 0.94461).iterate();
        //System.out.println(POSITIVE_INFINITY);
//        String query1 = "g.V().has('vp1').inE('el2').where(__.inV().values('vp2').is(lt(true))).bothV().bothE('el0','el2','el1').has('ep1').and(__.count())";
        String query1 = "g.V(826,836).values('vp0').max()";
        //g.V().has('vp1', inside(0.24070676216155018,4.13998472E8)).inE('el0','el4').outV().not(__.values('vp1'))

        try {
            connection.execute("g.V().drop().iterator()");
            connection.execute("g.E().drop().iterator()");
//            connection.execute(
//                    "v1=g.addV('vl1').next()\n" +
//                            "v2=g.addV('vl1').next()\n" +
//                            "v3=g.addV('vl1').next()\n" +
//                            "g.V(v1).addE('el0').to(v2).next()\n" +
//                            "g.V(v3).addE('el0').to(v1).next()\n" +
//                            "g.V(v2).addE('el0').to(v3).next()\n");
            connection.execute("g.addV('vl1').property('vp0',1).next()\n" +
                    "g.addV('vl2').property('vp0',2).next()\n" +
                    "g.addV('vl1').property('vp0',1).next()\n");
            List<String> tra = new ArrayList<>(Arrays.asList("order()","by('vp0')","count()"));
            List<String[]> ty = new ArrayList<String[]>(Arrays.<String[]>asList(new String[]{"v","v"},new String[]{"v","n"}));
            ResultWrapper results = connection.execute("g.V().order().by('vp0').count()");
//            ResultWrapper results = connection.execute("g.V().bothE('el0').outV().outE('el0')");
            for (String i : results.extractIds())
                System.out.println(i);
            System.out.println("=====================================");
            ResultWrapper result2 = connection.execute("g.V().barrier().order().by('vp0').barrier().count()");
//            ResultWrapper results = connection.execute("g.V().bothE('el0').outV().outE('el0')");
            for (String i : results.extractIds())
                System.out.println(i);
//            ExternalDivider divider = new ExternalDivider(connection, 0, null);
//            divider.test(tra,ty);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }

}
