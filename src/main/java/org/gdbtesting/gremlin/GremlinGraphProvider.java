package org.gdbtesting.gremlin;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.GDBCommon;
import org.gdbtesting.common.GraphDBProvider;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.gen.GraphAddEdgeAndPropertyGenerator;
import org.gdbtesting.gremlin.gen.GraphAddVertexAndProeprtyGenerator;
import org.gdbtesting.hugegraph.HugeGraphConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class GremlinGraphProvider implements GraphDBProvider<GraphGlobalState, GraphOptions, GremlinConnection> {

    private static final Logger logger = LoggerFactory.getLogger(GremlinGraphProvider.class);

    protected GremlinConnection connection;
    protected GraphGlobalState state;
    private GraphDBExecutor graphDBSetup;

    public boolean containsHuge = false;


    protected String version;
    protected Randomly randomly;

    List<GraphSchema.GraphVertexProperty> vertexProperties = new ArrayList<>();
    List<GraphSchema.GraphVertexIndex> vertexIndices = new ArrayList<>();
    List<GraphSchema.GraphVertexLabel> vertexLabels = new ArrayList<>();
    List<GraphSchema.GraphEdgeProperty> edgeProperties = new ArrayList<>();
    List<GraphSchema.GraphEdgeIndex> edgeIndices = new ArrayList<>();
    List<GraphSchema.GraphRelationship> edgeLabels = new ArrayList<>();
    List<String> indexList = new ArrayList<>();
    List<String> EdgeindexList = new ArrayList<>();

    public GremlinGraphProvider(GraphGlobalState globalState, List<GremlinConnection> connections) {
        this.state = globalState;
        this.randomly = globalState.getRandomly();
        this.version = globalState.getDbVersion();
        // TODO: User-defined graph database
        for(GremlinConnection connection1: connections){
            if(connection1 instanceof HugeGraphConnection){
                containsHuge = true;
                System.out.println("contains hugegraph");
                break;
            }
        }
        graphDBSetup = new GraphDBExecutor(connections, globalState);
        connection = connections.get(0);
    }


    private enum Action {
        ADD_VERTEX_PROPERTY,
        ADD_EDGE_PROPERTY,
        /*ALTER_EDGE_PROPERTY,
        ALTER_VERTEX_PROPERTY,
        DROP_VERTEX,
        DROP_EDGE*/;
    }

    private int mapActions(Action a) {
        Randomly r = state.getRandomly();
        int number = 0;
        switch (a) {
            case ADD_VERTEX_PROPERTY:
                number = (int) state.getVerticesMaxNum();
                break;
            case ADD_EDGE_PROPERTY:
                number = (int) state.getEdgesMaxNum();
                break;
            default:
                throw new AssertionError(a);
        }
        return number;
    }

    public void executeActions() {
        Action[] actions = Action.values();
        int number = 0;
        for (Action a : actions) {
            number = mapActions(a);
            if (number != 0) {
                executeAction(a, number);
            }
        }
    }

    public void executeAction(Action a, int number) {
        switch (a) {
            case ADD_VERTEX_PROPERTY:
                addVertexAndProperty(number);
                state.getGraphData().setVertices(addVMap);
                state.getGraphData().updateVertices();
                break;
            case ADD_EDGE_PROPERTY:
                addEdgeAndProperty(number);
                state.getGraphData().setEdges(addEMap);
                state.getGraphData().updateEdges();
                break;
            /*case ALTER_VERTEX_PROPERTY:
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
                break;*/

            default:
                throw new AssertionError(a);
        }
    }

    private List<GraphData.VertexObject> addVMap;
    private List<GraphData.EdgeObject> addEMap;

    public void addVertexAndProperty(int number) {
        GraphAddVertexAndProeprtyGenerator add = new GraphAddVertexAndProeprtyGenerator(state);
        addVMap = add.addVertices(number);
    }

    public void addEdgeAndProperty(int number) {
        GraphAddEdgeAndPropertyGenerator addE = new GraphAddEdgeAndPropertyGenerator(state);
        addEMap = addE.addEdges(number);
    }


    @Override
    public Class getGlobalStateClass() {
        return null;
    }

    @Override
    public Class getOptionClass() {
        return null;
    }

    @Override
    public void generateAndTestDatabase(GraphGlobalState globalState) throws Exception {
        for (int repeat = 0; repeat < 1; repeat++) {

            createGraphSchema(repeat);

            creatGraphData();

            // test graph database
            generateRandomlyTest();
        }
    }

    public void createLogFile(int repeat){
        System.out.println("create file");
        File file = new File(System.getProperty("user.dir") + "/log-" + repeat);
        System.out.println(file);
        if(!file.exists()){
            file.mkdirs();
        }
    }

    public void generateAndTestGDBWithDOT(GraphGlobalState globalState, int repeat) throws IOException {
            System.out.println("generate and test with DOT");
            createGraphSchema(repeat);

            creatGraphData();

            generateRandomlyQuery();

            setupAndMutate(repeat);

    }

    public void init(){
        vertexProperties = new ArrayList<>();
        vertexIndices = new ArrayList<>();
        vertexLabels = new ArrayList<>();
        edgeProperties = new ArrayList<>();
        edgeIndices = new ArrayList<>();
        edgeLabels = new ArrayList<>();
        indexList = new ArrayList<>();
        EdgeindexList = new ArrayList<>();
    }

    public void creatGraphData() {
        System.out.println("creatGraphData");
        GraphData graphData = new GraphData();
        state.setGraphData(graphData);
        executeActions();
        graphData.setEpValuesMap();
        graphData.setVpValuesMap();
    }

    public void generateRandomlyQuery(){
        System.out.println("generate query");
        graphDBSetup.generateRandomQuery();
    }

    // Grand
    public void generateRandomlyTest(){
        try {
            //create graph with a randomly schema
            System.out.println("generate query");
            graphDBSetup.generateRandomQuery();
            System.out.println("setup graph and test");
            graphDBSetup.setupGraphAndTest(addVMap, addEMap);
            System.out.println("check result");
            graphDBSetup.checkResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // DOT
    public void setupAndMutate(int repeat){
        // define mutation strategy
        try {
            graphDBSetup.setupGraphAndDOT(addVMap, addEMap, state.getType(), repeat);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * Vertex Schema:
     * label1
     * property1
     * property2
     * label2
     * property3
     * property1
     * ...
     */
    public void createGraphSchema(int repeat) throws IOException {
        System.out.println("createGraphSchema");
        // Vertex Property
        createVertexProperty();

        // Vertex Label
        createVertexLabel();

        // Edge Property
        createEdgeProperty();

        // Edge index
        createEdgeIndex();

        // Edge Label
        createEdgeLabel();

        // initial GraphSchema
        String cur = System.getProperty("period");
        GraphSchema newSchema =
                new GraphSchema(vertexLabels, edgeLabels, vertexProperties, vertexIndices, edgeProperties, edgeIndices, indexList, EdgeindexList);
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0" + "/schema-" + repeat + ".txt"));
        //this.getConnection().getHugespecial().graph().addVertices(newSchema.vertexIndices);
        out.write(newSchema.toString());
        out.close();
//        System.out.println(newSchema.toString());
        state.setSchema(newSchema);
    }

    public void createVertexProperty() {
        for (int j = 0; j < randomly.getInteger(state.getPropertyMaxNum()); j++) {
            // for each property
            String propertyName = GDBCommon.createVertexPropertyName(j);
            ConstantType type = Randomly.fromOptions(ConstantType.getRandom());
            whetherHuge(propertyName, type);
            GraphSchema.GraphVertexProperty vertexProperty = new GraphSchema.GraphVertexProperty(propertyName, type, null);
            state.getVertexPropertyIndex().addAndGet(1);
            vertexProperties.add(vertexProperty);
        }
//        GraphSchema.GraphVertexProperty stepProp = new GraphSchema.GraphVertexProperty("step", ConstantType.INTEGER, null);
//        whetherHuge("step", ConstantType.INTEGER);
//        vertexProperties.add(stepProp);
    }

    public void createVertexLabel() throws IOException {
        for (int i = 0; i < randomly.getInteger(state.getVertexLabelNum()); i++) {
            // for each label
            String labelName = GDBCommon.createVertexLabelName(i);
            // generate randomly properties
            List<GraphSchema.GraphVertexProperty> list = new ArrayList<>();
            if (this.getConnection().getHugespecial() != null) {
                this.getConnection().getHugespecial().schema().vertexLabel(labelName).ifNotExist().create();
            }
            int random = (int) randomly.getInteger(vertexProperties.size());
            for (int j = 0; j < random; j++) {
                GraphSchema.GraphVertexProperty gvp =
                        vertexProperties.get((int) randomly.getInteger(vertexProperties.size() - 1) - 1);
                if (!list.contains(gvp)) {
                    list.add(gvp);
                    if (containsHuge) {
                        this.getConnection().getHugespecial().schema().vertexLabel(labelName).properties(gvp.getVertexPropertyName()).nullableKeys(gvp.getVertexPropertyName()).append();
                        // index property
                        String indexname = labelName + "by" + gvp.getVertexPropertyName() + "Shard";
                        indexList.add(indexname);
                        // optimization
//                        this.getConnection().getHugespecial().schema().indexLabel(indexname).onV(labelName).by(gvp.getVertexPropertyName()).shard().ifNotExist().create();
                    }
                }
            }
//            GraphSchema.GraphVertexProperty gvp =
//                    vertexProperties.get(vertexProperties.size() - 1);
//            list.add(gvp);
//            if (containsHuge) {
//                this.getConnection().getHugespecial().schema().vertexLabel(labelName).properties(gvp.getVertexPropertyName()).nullableKeys(gvp.getVertexPropertyName()).append();
//                // index property
//                String indexname = labelName + "by" + gvp.getVertexPropertyName() + "Shard";
//                System.out.println(indexname);
//                indexList.add(indexname);
//                this.getConnection().getHugespecial().schema().indexLabel(indexname).onV(labelName).by(gvp.getVertexPropertyName()).shard().ifNotExist().create();
//            }
            GraphSchema.GraphVertexLabel vertexLabel =
                    new GraphSchema.GraphVertexLabel(labelName, list, null);
            // generate randomly index
            GraphSchema.GraphVertexIndex index = GraphSchema.GraphVertexIndex.create(vertexLabel, Randomly.nonEmptySubList(list));
            vertexIndices.add(index);
            vertexLabel.setIndexes(index);
            state.getVertexLabelIndex().addAndGet(1);
            vertexLabels.add(vertexLabel);
            /*if(containsHuge){
                String[] indexlist = new String[index.getVpList().size()];
                for(int j=0;j<indexlist.length;j++){
                    indexlist[j] = index.getVpList().get(j).getVertexPropertyName();
                }
                this.getConnection().getHugespecial().schema().indexLabel(index.getIndexName()).onV(vertexLabel.getLabelName()).by(indexlist).shard().ifNotExist().create();
            }*/
        }
    }

    public void createEdgeProperty() {
        for (int j = 0; j < randomly.getInteger(state.getPropertyMaxNum()); j++) {
            // for each property
            String propertyName = GDBCommon.createEdgePropertyName(j);
            ConstantType type = Randomly.fromOptions(ConstantType.values());
            GraphSchema.GraphEdgeProperty edgeProperty =
                    new GraphSchema.GraphEdgeProperty(propertyName, type, null);
            whetherHuge(propertyName, type);
            state.getEdgePropertyIndex().addAndGet(1);
            edgeProperties.add(edgeProperty);
        }
//        GraphSchema.GraphEdgeProperty stepProp = new GraphSchema.GraphEdgeProperty("step", ConstantType.INTEGER, null);
//        whetherHuge("step", ConstantType.INTEGER);
//        edgeProperties.add(stepProp);
    }

    public void whetherHuge(String propertyName, ConstantType type) {
        if (containsHuge)
            switch (type) {
                case INTEGER:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asInt().ifNotExist().create();
                    break;
                case STRING:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asText().ifNotExist().create();
                    break;
                case DOUBLE:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asDouble().ifNotExist().create();
                    break;
                case BOOLEAN:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asBoolean().ifNotExist().create();
                    break;
                case FLOAT:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asFloat().ifNotExist().create();
                    break;
                case LONG:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asLong().ifNotExist().create();
                    break;
            }
    }

    public void createEdgeIndex() {
        List<GraphSchema.GraphEdgeProperty> subEL = Randomly.nonEmptySubList(edgeProperties);
        for (int i = 0; i < subEL.size(); i++) {
            edgeIndices.add(GraphSchema.GraphEdgeIndex.create(subEL.get(i).getEdgePropertyName()));
        }
        /*if(containsHuge){
            String[] indexlist = new String[subEL.size()];
            for(int j=0;j<indexlist.length;j++){
                indexlist[j] = subEL.get(j).getEdgePropertyName();
            }
            this.getConnection().getHugespecial().schema().indexLabel(index.getIndexName()).onV(vertexLabel.getLabelName()).by(indexlist).shard().ifNotExist().create();
        }*/
    }

    public void createEdgeLabel() {
        for (int i = 0; i < randomly.getInteger(state.getEdgeLabelNum()); i++) {
            // for each label
            String labelName = GDBCommon.createEdgeLabelName(i);
            ArrayList<GraphSchema.GraphEdgeProperty> list = new ArrayList<>();
            // generate randomly index
            GraphSchema.GraphRelationship edgeLabel = new GraphSchema.GraphRelationship(labelName,
                    vertexLabels.get((int) randomly.getInteger(vertexLabels.size()) - 1),
                    vertexLabels.get((int) randomly.getInteger(vertexLabels.size()) - 1),
                    list, Randomly.nonEmptySubList(edgeIndices));
            state.getEdgeLabelIndex().addAndGet(1);
            edgeLabels.add(edgeLabel);
            if (containsHuge) {
                this.getConnection().getHugespecial().schema().edgeLabel(labelName).link(edgeLabel.getOutLabel().getLabelName(), edgeLabel.getInLabel().getLabelName()).ifNotExist().create();
            }
            // generate randomly properties
            int random = (int) randomly.getInteger(edgeProperties.size());
            for (int j = 0; j < random; j++) {
                GraphSchema.GraphEdgeProperty gep =
                        edgeProperties.get((int) randomly.getInteger(edgeProperties.size() - 1) - 1);
                if (!list.contains(gep)) {
                    list.add(gep);
                    if (containsHuge)
                        this.getConnection().getHugespecial().schema().edgeLabel(labelName).properties(gep.getEdgePropertyName()).nullableKeys(gep.getEdgePropertyName()).append();
                    // index edge property
                    String indexname = labelName + "by" + gep.getEdgePropertyName() + "Shard";
                    EdgeindexList.add(indexname);
                    // optimization
//                    if (containsHuge)
//                        this.getConnection().getHugespecial().schema().indexLabel(indexname).onE(labelName).by(gep.getEdgePropertyName()).shard().ifNotExist().create();
                }

            }
//            GraphSchema.GraphEdgeProperty gep =
//                    edgeProperties.get(edgeProperties.size() - 1);
//            list.add(gep);
//            if (containsHuge)
//                this.getConnection().getHugespecial().schema().edgeLabel(labelName).properties(gep.getEdgePropertyName()).nullableKeys(gep.getEdgePropertyName()).append();
//            // index property
//            String indexname = labelName + "by" + gep.getEdgePropertyName() + "Shard";
//            System.out.println(indexname);
//            indexList.add(indexname);
//            if (containsHuge)
//                this.getConnection().getHugespecial().schema().indexLabel(indexname).onE(labelName).by(gep.getEdgePropertyName()).shard().ifNotExist().create();

        }
    }

    public GremlinConnection getConnection() {
        return connection;
    }

}
