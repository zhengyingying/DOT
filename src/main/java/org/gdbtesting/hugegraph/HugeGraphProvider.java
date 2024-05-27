package org.gdbtesting.hugegraph;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.gdbtesting.GraphDB;
import org.gdbtesting.Randomly;
import org.gdbtesting.common.GDBCommon;
import org.gdbtesting.gremlin.GremlinGraphProvider;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.gen.GraphExpressionGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HugeGraphProvider extends GremlinGraphProvider {
    private static final GraphDB DB = GraphDB.HUGEGRAPH;
    private HugeGraphConnection connection;
    private GraphGlobalState state;
    private String version;
    private HugeClient client;
    private SchemaManager schema;
    private Randomly randomly;
    private GraphExpressionGenerator generator;
    private List<String> vertexPropertyList;

    public HugeGraphProvider(GraphGlobalState state) {
        super(state, Arrays.asList(new HugeGraphConnection("1.0.0", "conf/remote-hugegraph.properties")));
        this.state = state;
        this.version = state.getDbVersion();
        randomly = state.getRandomly();
        // generator = new GraphExpressionGenerator(state);
    }


    public String getDBMSName() {
        return DB.toString();
    }



    public void generateGraph() throws Exception {

        createGraph();
//        createGraphSchema();


    }

    public void createGraph(){
        connection = new HugeGraphConnection(version, state.getRemoteFile());
    }


    public enum Type{
        TEXT, INT, DOUBLE, DATE, BOOLEAN, FLOAT, LONG
    }

    public void createGraphSchema(){
        // VertexPropertyKey
        for(int i = 0; i < randomly.smallNumber(); i++){
            String name = GDBCommon.createVertexPropertyName(i);
            vertexPropertyList.add(name);
            createPropertySchema(name, Randomly.fromOptions(Type.values()));
        }

        for(int i = 0; i < randomly.smallNumber(); i++){
            // for each label
            String properties = randomly.fromList(vertexPropertyList);
            schema.vertexLabel(GDBCommon.createVertexLabelName(i))
                    .properties()
                    .primaryKeys("name")
                    .ifNotExist()
                    .create();
        }

    }





    public PropertyKey createPropertySchema(String name, Type type){
        switch (type){
            case BOOLEAN:
                return schema.propertyKey(name).asBoolean().ifNotExist().create();
            case INT:
                return schema.propertyKey(name).asInt().ifNotExist().create();
            case DOUBLE:
                return schema.propertyKey(name).asDouble().ifNotExist().create();
            case DATE:
                return schema.propertyKey(name).asDate().ifNotExist().create();
            case FLOAT:
                return schema.propertyKey(name).asFloat().ifNotExist().create();
            case LONG:
                return schema.propertyKey(name).asLong().ifNotExist().create();
            default:
                return schema.propertyKey(name).asText().ifNotExist().create();
        }
    }


    public GraphDB getDB() {
        return DB;
    }

    public HugeGraphConnection getConnection() {
        return connection;
    }

    public String getVersion() {
        return version;
    }

    public static void main(String[] args) {
        // If connect failed will throw a exception.

        HugeClient hugeClient ;
        GraphManager graph;
        SchemaManager schema;
        try {
            hugeClient = HugeClient.builder("http://127.0.0.1:8080", "hugegraph").build();
            graph = hugeClient.graph();
            schema = hugeClient.schema();
            schema.vertexLabel("person").properties("name", "age").ifNotExist().create();
        Vertex marko = new Vertex("person").property("name", "markomarko")
                .property("age", 29);
        Vertex vadas = new Vertex("person").property("name", "vadasmarko")
                .property("age", 27);
        Vertex lop = new Vertex("software").property("name", "lopmarko")
                .property("lang", "java")
                .property("price", 328);
        Vertex josh = new Vertex("person").property("name", "joshmarko")
                .property("age", 32);
        Vertex ripple = new Vertex("software").property("name", "ripplemarko")
                .property("lang", "java")
                .property("price", 199);
        Vertex peter = new Vertex("person").property("name", "petermarko")
                .property("age", 35);

        Edge markoKnowsVadas = new Edge("knows").source(marko).target(vadas)
                .property("date", "2016-01-10");
        Edge markoKnowsJosh = new Edge("knows").source(marko).target(josh)
                .property("date", "2013-02-20");
        Edge markoCreateLop = new Edge("created").source(marko).target(lop)
                .property("date",
                        "2017-12-10");
        Edge joshCreateRipple = new Edge("created").source(josh).target(ripple)
                .property("date",
                        "2017-12-10");
        Edge joshCreateLop = new Edge("created").source(josh).target(lop)
                .property("date", "2009-11-11");
        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
                .property("date",
                        "2017-03-24");

        List<Vertex> vertices = new ArrayList<>();
        vertices.add(marko);
        vertices.add(vadas);
        vertices.add(lop);
        vertices.add(josh);
        vertices.add(ripple);
        vertices.add(peter);

        List<Edge> edges = new ArrayList<>();
        edges.add(markoKnowsVadas);
        edges.add(markoKnowsJosh);
        edges.add(markoCreateLop);
        edges.add(joshCreateRipple);
        edges.add(joshCreateLop);
        edges.add(peterCreateLop);

        vertices = graph.addVertices(vertices);
        vertices.forEach(vertex -> System.out.println(vertex));

        edges = graph.addEdges(edges, false);
        edges.forEach(edge -> System.out.println(edge));

        hugeClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
