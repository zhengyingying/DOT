package org.gdbtesting.reproduce;


import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.hugegraph.HugeGraphConnection;
import org.gdbtesting.janusgraph.JanusGraphConnection;
import org.gdbtesting.neo4j.Neo4jConnection;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class RestoreGraph {

    public static List<GremlinConnection> connections;
    public static Map<String,String> typeMap;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if(args.length != 2)
            System.out.println("Missing args, should be: LogFilePath Query");
        String LogFilePath = args[0];
        LogFilePath = "/mnt/g/gdbtesting/log-1";
        makeconnections();
        restoreHugeSchema(LogFilePath);
        restoreGraphData(LogFilePath);
        System.out.println("Finish Restore");
        rexcuteQuery(args[1]);
        System.exit(0);
    }

    private static void rexcuteQuery(String arg) throws ExecutionException, InterruptedException {
        for(GremlinConnection connection: connections) {
            if (connection.getDatabase().equals("HugeGraph")) {
                GremlinManager gremlin = connection.getHugespecial().gremlin();
                try{
                    org.apache.hugegraph.structure.gremlin.ResultSet hugeResult = gremlin.gremlin(arg).execute();
                    System.out.println(connection.getDatabase() + ": " + arg );
                    Iterator<org.apache.hugegraph.structure.gremlin.Result> huresult = hugeResult.iterator();
                    huresult.forEachRemaining(result -> {
                        Object object = result.getObject();
                        if (object instanceof org.apache.hugegraph.structure.graph.Vertex) {
                            try {
                                System.out.println("v[" + ((org.apache.hugegraph.structure.graph.Vertex) object).id() + "]");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else if (object instanceof org.apache.hugegraph.structure.graph.Edge) {
                            try {
                                System.out.println("e[" + ((org.apache.hugegraph.structure.graph.Edge) object).id() + "]");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else if (object instanceof org.apache.hugegraph.structure.graph.Path) {
                            List<Object> elements = ((org.apache.hugegraph.structure.graph.Path) object).objects();
                            elements.forEach(element -> {
                                System.out.println(element.getClass());
                                System.out.println(element);
                            });
                        } else {
                            System.out.println(object);
                        }
                    });
                }
                catch (Exception e){
                    System.out.println(e.toString());
                    continue;
                }
            }
            else{
                List<org.apache.tinkerpop.gremlin.driver.Result> results;
                results = connection.getClient().submit(arg).all().get();
                System.out.println(connection.getDatabase() + ": " + arg );
                for (org.apache.tinkerpop.gremlin.driver.Result r : results) {
                    System.out.println(String.valueOf(r.getElement()));
                }
            }
        }
    }

    private static void restoreGraphData(String logFilePath) throws IOException {
        String DataFilePath = logFilePath + "/JanusGraph-graphdata.txt";
        Map<String,org.apache.hugegraph.structure.graph.Vertex> hugeVerticesMap = new HashMap<>();
        Map<String,Vertex> VerticesMap = new HashMap<>();
        for(GremlinConnection connection: connections){
            BufferedReader in = new BufferedReader(new FileReader(DataFilePath));
            String line = in.readLine();
            line = in.readLine();
            line = in.readLine();
            GraphTraversalSource g = connection.getG();

            while(!line.contains("Edge:")) {
                String VertexID = line.substring(line.indexOf(":")+2);
                line = in.readLine();
                String labelname = line.substring(line.indexOf(":") + 2);
                line = in.readLine();
                org.apache.hugegraph.structure.graph.Vertex hugeadd = null;
                Vertex add = null;
                if (connection.getDatabase().equals("HugeGraph")) {
                    hugeadd = new org.apache.hugegraph.structure.graph.Vertex(labelname);
                } else {
                    add = g.addV(labelname).next();
                }
                while((line = in.readLine()).contains("Key")){
                    String propertyName = line.substring(line.indexOf(":")+2,line.indexOf(" Value"));
                    String value = line.substring(line.indexOf("e:")+3);
                    Object typeValue = null;
                    String propertyType = typeMap.get(propertyName);
                    switch (propertyType){
                        case "INTEGER":
                            typeValue = Integer.parseInt(value);
                            break;
                        case "STRING":
                            typeValue = value;
                            if(containnumber(line,"'") < 2){
                                line = in.readLine();
                                typeValue += line;
                            }
                            break;
                        case "DOUBLE":
                            typeValue = Double.parseDouble(value);
                            break;
                        case "BOOLEAN":
                            typeValue = Boolean.parseBoolean(value);
                            break;
                        case "FLOAT":
                            typeValue = Float.parseFloat(value);
                            break;
                        case "LONG":
                            typeValue = Long.parseLong(value);
                            break;
                    }
                    if (connection.getDatabase().equals("HugeGraph")) {
                        hugeadd.property(propertyName,typeValue);
                    } else {
                        g.V(add).property(propertyName,typeValue).iterate();
                    }
                }
                if (connection.getDatabase().equals("HugeGraph")){
                    hugeadd = connection.getHugespecial().graph().addVertex(hugeadd);
                    hugeVerticesMap.put(VertexID,hugeadd);
                }
                else{
                    VerticesMap.put(VertexID,add);
                }
                line = in.readLine();
            }

            while(line!=null){
                line = in.readLine();
                line = in.readLine();
                if(!line.contains(":"))
                    break;
                String labelname = line.substring(line.indexOf(":") + 2);
                line = in.readLine();
                String OutID = line.substring(line.indexOf(":") + 2);
                line = in.readLine();
                String InID = line.substring(line.indexOf(":") + 2);
                Object OutVertex = null;
                Object InVertex = null;
                org.apache.hugegraph.structure.graph.Edge HugeaddEdge = null;
                Edge addEdge = null;
                if (connection.getDatabase().equals("HugeGraph")){
                    OutVertex = hugeVerticesMap.get(OutID);
                    InVertex = hugeVerticesMap.get(InID);
                    HugeaddEdge = new org.apache.hugegraph.structure.graph.Edge(labelname).source((org.apache.hugegraph.structure.graph.Vertex) OutVertex).target((org.apache.hugegraph.structure.graph.Vertex) InVertex);
                }
                else{
                    OutVertex = VerticesMap.get(OutID);
                    InVertex = VerticesMap.get(InID);
                    addEdge = g.addE(labelname).from((Vertex) OutVertex).to((Vertex) InVertex).next();
                }
                line = in.readLine();
                while((line = in.readLine()).contains("Key")) {
                    String propertyName = line.substring(line.indexOf(":") + 2, line.indexOf("  Value"));
                    String value = line.substring(line.indexOf("e:") + 3);
                    Object typeValue = null;
                    String propertyType = typeMap.get(propertyName);
                    switch (propertyType) {
                        case "INTEGER":
                            typeValue = Integer.parseInt(value);
                            break;
                        case "STRING":
                            typeValue = value;
                            if(containnumber(line,"'") < 2){
                                line = in.readLine();
                                typeValue += line;
                            }
                            break;
                        case "DOUBLE":
                            typeValue = Double.parseDouble(value);
                            break;
                        case "BOOLEAN":
                            typeValue = Boolean.parseBoolean(value);
                            break;
                        case "FLOAT":
                            typeValue = Float.parseFloat(value);
                            break;
                        case "LONG":
                            typeValue = Long.parseLong(value);
                            break;
                    }
                    if (connection.getDatabase().equals("HugeGraph")) {
                        HugeaddEdge.property(propertyName,typeValue);
                    } else {
                        g.E(addEdge).property(propertyName,typeValue).iterate();
                    }
                }
                if (connection.getDatabase().equals("HugeGraph"))
                    connection.getHugespecial().graph().addEdge(HugeaddEdge);
            }
            in.close();
        }
    }

    private static int containnumber(String line, String s) {
        int number = 0;
        for(int i=0;i<line.length();i++){
            String c = String.valueOf(line.charAt(i));
            if(c.equals(s))
                number++;
        }
        return number;
    }

    private static void makeconnections() {
        RestoreGraph.connections = Arrays.asList(
                new Neo4jConnection("3.5.27", "conf/remote-neo4j.properties"),
                new JanusGraphConnection("0.5.3", "conf/remote-janusgraph.properties"),
                new HugeGraphConnection("0.11.2", "conf/remote-hugegraph.properties"),
                new TinkerGraphConnection("3.4.10", "conf/remote-tinkergraph.properties")
        );
        GraphTraversalSource g = RestoreGraph.connections.get(0).getG();
        g.E().drop().iterate();
        g.V().drop().iterate();
        g = RestoreGraph.connections.get(3).getG();
        g.E().drop().iterate();
        g.V().drop().iterate();
    }

    private static void restoreHugeSchema(String logFilePath) throws IOException {
        String SchemaFilePath = logFilePath + "/schema.txt";
        typeMap = new HashMap<>();
        SchemaManager hugeschema= RestoreGraph.connections.get(2).getHugespecial().schema();
        BufferedReader in = new BufferedReader(new FileReader(SchemaFilePath));
        String line = in.readLine();
        while(!line.contains("index:")){
            System.out.println("VertexLable>>>>");
            while(line=="" || line.contains(":"))
                line = in.readLine();
            String labelname = line;
            hugeschema.vertexLabel(labelname).ifNotExist().create();
            while((line = in.readLine()).contains("vp")){
                String propertyName = line.substring(line.indexOf("v"),line.indexOf(":"));
                String type = line.substring(line.indexOf(":")+2);
                typeMap.put(propertyName,type);
                switch (type){
                    case "INTEGER":
                        hugeschema.propertyKey(propertyName).asInt().ifNotExist().create();
                        break;
                    case "STRING":
                        hugeschema.propertyKey(propertyName).asText().ifNotExist().create();
                        break;
                    case "DOUBLE":
                        hugeschema.propertyKey(propertyName).asDouble().ifNotExist().create();
                        break;
                    case "BOOLEAN":
                        hugeschema.propertyKey(propertyName).asBoolean().ifNotExist().create();
                        break;
                    case "FLOAT":
                        hugeschema.propertyKey(propertyName).asFloat().ifNotExist().create();
                        break;
                    case "LONG":
                        hugeschema.propertyKey(propertyName).asLong().ifNotExist().create();
                        break;
                }
                hugeschema.vertexLabel(labelname).properties(propertyName).nullableKeys(propertyName).append();
            }
            line = in.readLine();
        }
        while(!line.contains("edge:")){
            System.out.println("VertexIndex>>>>");
            line = in.readLine();
            if(line.contains("Shard"))
                hugeschema.indexLabel(line).onV(line.substring(0,line.indexOf("by"))).by(line.substring(line.indexOf("vp"),line.indexOf("Shard"))).shard().ifNotExist().create();
        }
        line = in.readLine();
        while(!line.contains("Edgeindex")){
            System.out.println("EdgeLabel>>>>");
            String labelname = line;
            line = in.readLine();
            String outvertex = line.substring(line.indexOf(":")+2);
            while(!line.contains("inVertex"))
                line = in.readLine();
            String invertex = line.substring(line.indexOf(":")+2);
            hugeschema.edgeLabel(labelname).link(outvertex,invertex).ifNotExist().create();
            while(!line.contains("ep"))
                line = in.readLine();
            while(line.contains("ep")){
                String propertyName = line.substring(line.indexOf("e"),line.indexOf(":"));
                String type = line.substring(line.indexOf(":")+2);
                typeMap.put(propertyName,type);
                switch (type){
                    case "INTEGER":
                        hugeschema.propertyKey(propertyName).asInt().ifNotExist().create();
                        break;
                    case "STRING":
                        hugeschema.propertyKey(propertyName).asText().ifNotExist().create();
                        break;
                    case "DOUBLE":
                        hugeschema.propertyKey(propertyName).asDouble().ifNotExist().create();
                        break;
                    case "BOOLEAN":
                        hugeschema.propertyKey(propertyName).asBoolean().ifNotExist().create();
                        break;
                    case "FLOAT":
                        hugeschema.propertyKey(propertyName).asFloat().ifNotExist().create();
                        break;
                    case "LONG":
                        hugeschema.propertyKey(propertyName).asLong().ifNotExist().create();
                        break;
                }
                hugeschema.edgeLabel(labelname).properties(propertyName).nullableKeys(propertyName).append();
                line = in.readLine();
            }
            line = in.readLine();
        }
        while(!(line == null)){
            System.out.println("EdgeIndex>>>>");
            line = in.readLine();
            if(line != null && line.contains("Shard"))
                hugeschema.indexLabel(line).onE(line.substring(0,line.indexOf("by"))).by(line.substring(line.indexOf("ep"),line.indexOf("Shard"))).shard().ifNotExist().create();
        }
        in.close();
    }
}
