package org.gdbtesting.hugegraph;


import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.HugeClientBuilder;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.gdbtesting.GraphDB;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.query.ResultWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HugeGraphConnection extends GremlinConnection {

    public HugeClient getHugeClient() {
        return hugeClient;
    }

    public void setHugeClient(HugeClient hugeClient) {
        this.hugeClient = hugeClient;
    }

    private HugeClient hugeClient;

    public void setup() {
        try {
            HugeClientBuilder builder = new HugeClientBuilder("http://192.168.1.101:8080", "hugegraph");
            HugeClient hugeClient = new HugeClient(builder);
            GraphManager graph = hugeClient.graph();
            System.out.println(graph.graph());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connect() {
        try {
//            hugeClient = HugeClient.builder("http://127.0.0.1:8080", "hugegraph").configTimeout(3000).build();
            hugeClient = HugeClient.builder("http://192.168.1.101:8080", "hugegraph").configTimeout(3000).build();
            hugespecial = hugeClient;
            clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        hugeClient.schema().getSchema().clear();
        hugeClient.schema().getPropertyKeys().clear();
        hugeClient.schema().getIndexLabels().clear();
        this.execute("g.V().drop().iterator()");
        this.execute("g.E().drop().iterator()");
    }

    // TODO
    public void downloadData(int count) {
        // shell: bin/hugegraph backup -t all -d data
        JSch jsch = new JSch();
        Session session = null;
        ChannelExec channel = null;
        boolean success = true;
        try {
            session = jsch.getSession("root", "192.168.1.101", 22);
            session.setPassword("admin");
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(1000 * 10);
            // TODO: configure this parameter
            String command = "/opt/database/hugegraph-tools-1.6.0/bin/hugegraph backup -t all -d data";
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            channel.connect();
        } catch (JSchException e) {
            success = false;
            e.printStackTrace();
        } finally {
            channel.disconnect();
            session.disconnect();
            if (success) {
                System.out.println("download successfully!");
            } else {
                System.out.println("download failed!");
            }
        }
    }

    @Override
    public ResultWrapper execute(String query) {
        GremlinManager gremlinManager = hugespecial.gremlin();
        ResultWrapper wrapper = new ResultWrapper();
        wrapper.fetch(gremlinManager.gremlin(query).execute());
        wrapper.setName(GraphDB.HUGEGRAPH);
        return wrapper;
    }


    @Override
    public Object executeExplain(String query){
        Object result = null;
        try{
            result = hugespecial.gremlin().gremlin(query).execute().get(0).getObject();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public HugeGraphConnection(String version, String filename) {
        super(version, "HugeGraph", filename);
    }

    public static void main(String[] args) {
        HugeGraphConnection connection = new HugeGraphConnection("0.12.0", "conf/hugegraph.yaml");
        HugeClient graph = connection.getHugespecial();
        GraphManager g = connection.getHugespecial().graph();
        try {
            connection.connect();
            connection.getHugespecial().schema().propertyKey("ep0").asBoolean().ifNotExist().create();
            connection.getHugespecial().schema().propertyKey("ep1").asBoolean().ifNotExist().create();
            connection.getHugespecial().schema().propertyKey("vp0").asBoolean().ifNotExist().create();

            // vertex
            connection.getHugespecial().schema().vertexLabel("vl0").properties("vp0").nullableKeys("vp0").create();
            connection.getHugespecial().schema().indexLabel("vl0Byvp0").onV("vl0").by("vp0").shard().ifNotExist().create();

            // edges
            connection.getHugespecial().schema().edgeLabel("el0").sourceLabel("vl0").targetLabel("vl0").properties("ep0").ifNotExist().create();
            connection.getHugespecial().schema().edgeLabel("el1").sourceLabel("vl0").targetLabel("vl0").properties("ep1").ifNotExist().create();
            connection.getHugespecial().schema().indexLabel("el0Byep0").onE("el0").by("ep0").shard().ifNotExist().create();
            connection.getHugespecial().schema().indexLabel("el1Byep0").onE("el1").by("ep1").shard().ifNotExist().create();

            /** create graph data */
            Vertex vertex1 = new Vertex("vl0").property("vp0", true);
            Vertex vertex2 = new Vertex("vl0").property("vp0", false);
            Vertex vertex3 = new Vertex("vl0").property("vp0", true);
            g.addVertices(Arrays.asList(vertex1, vertex2, vertex3));

            Edge edge1 = new Edge("el0").source(vertex1).target(vertex2).property("ep0", true);
            Edge edge2 = new Edge("el1").source(vertex1).target(vertex3).property("ep1", false);
            g.addEdges(Arrays.asList(edge1, edge2));
//            connection.getHugespecial().schema().vertexLabel("vl1").ifNotExist().create();
//            connection.getHugespecial().schema().vertexLabel("vl2").ifNotExist().create();
//            connection.getHugespecial().schema().vertexLabel("vl3").ifNotExist().create();
//            connection.getHugespecial().schema().propertyKey("ep1").asText().ifNotExist().create();
//            connection.getHugespecial().schema().edgeLabel("el1").sourceLabel("vl1").targetLabel("vl2").properties("ep1").ifNotExist().create();
//            connection.getHugespecial().schema().indexLabel("ep1").onE("el1").by("ep1").shard().ifNotExist().create();
//
//            connection.getHugespecial().schema().edgeLabel("el2").sourceLabel("vl3").targetLabel("vl1").ifNotExist().create();
//            connection.getHugespecial().schema().edgeLabel("el3").sourceLabel("vl2").targetLabel("vl3").ifNotExist().create();
//
//            connection.execute(
//                    "v1=g.addV('vl1').next()\n" +
//                            "v2=g.addV('vl2').next()\n" +
//                            "v3=g.addV('vl3').next()\n" +
//                            "g.V(v1).addE('el1').to(v2).property('ep1','1').next()\n" +
//                            "g.V(v3).addE('el2').to(v1).next()\n" +
//                            "g.V(v2).addE('el3').to(v3).next()\n");
            List<String> tra = new ArrayList<>(Arrays.asList("outE('el0','el1')","has('ep0',true)"));
            List<String[]> ty = new ArrayList<String[]>(Arrays.<String[]>asList(new String[]{"v", "e"}, new String[]{"e", "e"}));
            try{
            ResultWrapper results = connection.execute("g.V().outE('el0','el1').has('ep0',true)");

            System.out.println("done");
            if (results.size() == 0)
                System.out.println("EMPTY");
            for (String r : results.extractIds()) {
                System.out.println(r);
            }}catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("=====================================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
