package org.gdbtesting.gremlin;

import org.gdbtesting.GraphDB;
import org.gdbtesting.Randomly;
import org.gdbtesting.connection.GremlinConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class GraphGlobalState<C extends GremlinConnection>{

    private GraphDB dbType;
    private final int generateDepth;
    private String dbVersion;
    private GraphSchema schema;
    private static Randomly r;
    private C connection;
    private int vertexLabelNum = 20;
    private int edgeLabelNum = 20;
    private int propertyMaxNum = 20;

    // TODO: modify
    private long verticesMaxNum = 100;
    private long edgesMaxNum = 100;

    // the number of max traversal steps
    private long stepMaxNum = 100;
    private int repeatTimes = 1;

    public long getQueryNum() {
        return queryNum;
    }

    public void setQueryNum(long queryNum) {
        this.queryNum = queryNum;
    }

    private long queryNum = 100;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    private String type = "rand";

    private int timeBudget = 10;
    private String strategy;

    public int getTimeBudget() {
        return timeBudget;
    }

    public void setTimeBudget(int timeBudget) {
        this.timeBudget = timeBudget;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public List<String> getUniqueCombination() {
        return uniqueCombination;
    }

    public void setUniqueCombination(List<String> uniqueCombination) {
        this.uniqueCombination = uniqueCombination;
    }

    // record unique combination of optimization strategies
    private List<String> uniqueCombination = new ArrayList<>();

    //TODO: remove
    protected String remoteFile;
    public String getRemoteFile() {
        return remoteFile;
    }

    public void setRemoteFile(String remoteFile) {
        this.remoteFile = remoteFile;
    }


    public static volatile AtomicLong vertexLabelIndex = new AtomicLong(1);
    public static volatile AtomicLong vertexPropertyIndex = new AtomicLong(1);
    public static volatile AtomicLong edgeLabelIndex = new AtomicLong(1);
    public static volatile AtomicLong edgePropertyIndex = new AtomicLong(1);


    public void setRandomly(Randomly r) {
        this.r = r;
    }
    public static Randomly getRandomly() {
        return r;
    }

    public long getVerticesMaxNum() {
        return verticesMaxNum;
    }

    public void setVerticesMaxNum(long verticesMaxNum) {
        this.verticesMaxNum = verticesMaxNum;
    }

    public long getEdgesMaxNum() {
        return edgesMaxNum;
    }

    public void setEdgesMaxNum(long edgesMaxNum) {
        this.edgesMaxNum = edgesMaxNum;
    }

    public C getConnection() {
        return connection;
    }

    public void setConnection(C connection) {
        this.connection = connection;
    }

    public GraphGlobalState(int generateDepth){
        this.generateDepth = generateDepth;
    }

    public int getGenerateDepth() {
        return generateDepth;
    }

//    public void setGenerateDepth(int generateDepth) {
//        this.generateDepth = generateDepth;
//    }

    public String getDbVersion() {
        return dbVersion;
    }

    public void setDbVersion(String dbVersion) {
        this.dbVersion = dbVersion;
    }

    public GraphDB getDbType() {
        return dbType;
    }

    public static AtomicLong getVertexLabelIndex() {
        return vertexLabelIndex;
    }

    public static AtomicLong getVertexPropertyIndex() {
        return vertexPropertyIndex;
    }


    public static AtomicLong getEdgeLabelIndex() {
        return edgeLabelIndex;
    }

    public static AtomicLong getEdgePropertyIndex() {
        return edgePropertyIndex;
    }

    private GraphData graphData;

    public void setGraphData(GraphData graphData){this.graphData = graphData;}

    public GraphData getGraphData() {
        return graphData;
    }

    public GraphSchema getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError();
            }
        }
        return schema;
    }

    public void setSchema(GraphSchema schema) {
        this.schema = schema;
    }

    public void updateSchema() throws Exception {
        setSchema(readSchema());
    }

    // TODO
    public GraphSchema readSchema(){
        return null;
    }

    public int getVertexLabelNum() {
        return vertexLabelNum;
    }

    public void setVertexLabelNum(int vertexLabelNum) {
        this.vertexLabelNum = vertexLabelNum;
    }

    public int getEdgeLabelNum() {
        return edgeLabelNum;
    }

    public void setEdgeLabelNum(int edgeLabelNum) {
        this.edgeLabelNum = edgeLabelNum;
    }

    public int getPropertyMaxNum() {
        return propertyMaxNum;
    }

    public void setPropertyMaxNum(int propertyMaxNum) {
        this.propertyMaxNum = propertyMaxNum;
    }


    public long getStepMaxNum() {
        return stepMaxNum;
    }

    public void setStepMaxNum(long stepMaxNum) {
        this.stepMaxNum = stepMaxNum;
    }


    public void setRepeatTimes(int repeatTimes) {
        this.repeatTimes = repeatTimes;
    }

    public int getRepeatTimes() {
        return this.repeatTimes;
    }
}
