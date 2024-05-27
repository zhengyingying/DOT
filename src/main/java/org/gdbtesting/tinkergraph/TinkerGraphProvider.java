package org.gdbtesting.tinkergraph;


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.GraphDB;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GremlinGraphProvider;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.gen.*;
import org.gdbtesting.gremlin.query.GraphTraversalGenerator;
import org.gdbtesting.hugegraph.HugeGraphConnection;
import org.gdbtesting.tinkergraph.gen.TinkerGraphDropIndexGenerator;
import org.gdbtesting.tinkergraph.gen.TinkerGraphEdgeIndexGeneration;
import org.gdbtesting.tinkergraph.gen.TinkerGraphVertexIndexGeneration;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TinkerGraphProvider extends GremlinGraphProvider {

    private static final GraphDB DB = GraphDB.TINKERGRAPH;
    private TinkerGraphConnection connection;
    private GraphTraversalSource g;

    public Graph getGraph() {
        return graph;
    }

    private Graph graph;

    public TinkerGraphProvider(GraphGlobalState state){
        super(state, Arrays.asList(new TinkerGraphConnection("3.6.2", "conf/remote-tinkergraph.properties")));
    }

    private enum Action {
        ADD_VERTEX_PROPERTY,
        ADD_EDGE_PROPERTY,
        ALTER_EDGE_PROPERTY,
        ALTER_VERTEX_PROPERTY,
        DROP_INDEX,
        DROP_VERTEX,
        DROP_EDGE;
    }

    private int mapActions(Action a){
        Randomly r = state.getRandomly();
        int number = 0;
        switch (a){
            case ADD_VERTEX_PROPERTY:
                number = r.getInteger(0, (int) state.getVerticesMaxNum());
                break;
            case ADD_EDGE_PROPERTY:
                number = r.getInteger(0, (int) state.getEdgesMaxNum());
                break;
            case ALTER_VERTEX_PROPERTY:
                number = r.getInteger(0, 8);
                break;
            case ALTER_EDGE_PROPERTY:
                number = r.getInteger(0, 9);
                break;
            case DROP_EDGE:
                number = r.getInteger(0, 3);
                break;
            case DROP_VERTEX:
                number = r.getInteger(0, 5);
                break;
            case DROP_INDEX:
                number = r.getInteger(0, 2);
                break;
            default:
                throw new AssertionError(a);
        }
        return number;
    }

    public void executeActions(){
        Action[] actions = Action.values();
        int number = 0;
        for(Action a : actions){
            number = mapActions(a);
            if(number != 0){
                executeAction(a, number);
            }
        }
    }

    public void executeAction(Action a, int number){
        switch (a){
            case ADD_VERTEX_PROPERTY:
                addVertexAndProperty(number);
                break;
            case ADD_EDGE_PROPERTY:
                addEdgeAndProperty(number);
                break;
            case ALTER_VERTEX_PROPERTY:
                alterVertexProperty(number);
                break;
            case ALTER_EDGE_PROPERTY:
                alterEdgeProperty(number);
                break;
            case DROP_EDGE:
                drop(number, "edge");
                break;
            case DROP_VERTEX:
                drop(number, "vertex");
                break;
            case DROP_INDEX:
                dropIndex(number);
                break;
            default:
                throw new AssertionError(a);
        }
    }

    public void addVertexAndProperty(int number){
        GraphAddVertexAndProeprtyGenerator addV = new GraphAddVertexAndProeprtyGenerator(state);
        addV.generateVerticesAndProperties(number);
    }

    public void addEdgeAndProperty(int number){
        GraphAddEdgeAndPropertyGenerator addE = new GraphAddEdgeAndPropertyGenerator(state);
        addE.generateEdgesAndProperties(number);
    }

    public void alterVertexProperty(int number){
        GraphAlterVertexPropertyGeneration avpg = new GraphAlterVertexPropertyGeneration(state);
        for(int i = 0; i < number; i++){
            avpg.alterVertexProperty();
        }
    }

    public void alterEdgeProperty(int number){
        GraphAlterEdgePropertyGenerator aepg = new GraphAlterEdgePropertyGenerator(state);
        for(int i = 0; i < number; i++){
            aepg.alterEdgeProperty();
        }
    }

    public void drop(int number, String type){
        GraphDropGenerator dvg = new GraphDropGenerator(state);
        switch (type){
            case "edge":
                for(int i = 0; i < number; i++){
                    dvg.dropEdge();
                }
            case "vertex":
                for(int i = 0; i < number; i++){
                    dvg.dropVertex();
                }
        }
    }

    public void dropIndex(int number){
        TinkerGraphDropIndexGenerator tdig = new TinkerGraphDropIndexGenerator(connection, state);
        for(int i = 0; i < number; i++){
            tdig.dropIndex();
        }
    }



    public String getDBMSName() {
        return DB.toString();
    }



    public void generateGraph(int repeat) throws Exception {
        createGraph();
        createGraphSchema(repeat);
        //generate graph data
        executeActions();
        createIndex();
    }

    public void generateRandomlyTest(){
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        gtg.generateRandomlyTraversal();
    }

    public void createIndex(){
        TinkerGraphVertexIndexGeneration vig = new TinkerGraphVertexIndexGeneration(connection, state);
        vig.generateVertexIndex();
        TinkerGraphEdgeIndexGeneration eig = new TinkerGraphEdgeIndexGeneration(connection, state);
        eig.generateEdgeIndex();
    }


    public void createGraph() {
        connection = new TinkerGraphConnection(state.getDbVersion(), state.getRemoteFile());
        state.setConnection(connection);
        g = connection.getG();
        if(g.getGraph() != null) {
            GraphDropGenerator dropGenerator = new GraphDropGenerator(state);
            dropGenerator.dropAllVertex();
            //g = traversal().withEmbedded(TinkerGraph.open());
        }
        graph = connection.getGraph();
    }

    public GraphDB getDB() {
        return DB;
    }

    public TinkerGraphConnection getConnection() {
        return connection;
    }

    public String getVersion() {
        return version;
    }

    public static void main(String[] args) {
        GraphGlobalState state = new GraphGlobalState(2);
        TinkerGraphProvider provider = new TinkerGraphProvider(state);
        try {
            provider.generateGraph(1);
            provider.generateRandomlyTest();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
