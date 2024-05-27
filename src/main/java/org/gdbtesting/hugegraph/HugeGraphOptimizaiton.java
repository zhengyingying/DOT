package org.gdbtesting.hugegraph;

import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HugeGraphOptimizaiton {

    private HugeGraphConnection connection;
    private List<String> indexNames = new ArrayList<>();
    private List<GraphSchema.GraphVertexLabel> vertexLabels;
    private List<GraphSchema.GraphRelationship> edgeLabels;

    private BufferedWriter writer;

    public HugeGraphOptimizaiton(GraphGlobalState state, HugeGraphConnection connection, BufferedWriter writer){
        this.connection = connection;
        this.writer = writer;
        this.vertexLabels = state.getSchema().getVertexList();
        this.edgeLabels = state.getSchema().getEdgeList();
    }

    public void createHugeGraphIndex(){
        createVertexIndex();
        createEdgeIndex();
    }

    public void createVertexIndex(){
        GraphSchema.GraphVertexLabel label = Randomly.fromList(vertexLabels);
        String labelName = label.getLabelName();
        String property = label.getRandomVertexProperties().getVertexPropertyName();
        String indexname = labelName + "by" + property + "Shard";
        if(indexNames.contains(indexname)){
            return;
        }
        indexNames.add(indexname);
        try {
            this.writer.write(indexname);
            this.writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        System.out.println("create index " + indexname);
        this.connection.getHugespecial().schema().indexLabel(indexname).onV(labelName).by(property).shard().ifNotExist().create();
    }

    public void createEdgeIndex(){
        GraphSchema.GraphRelationship label = Randomly.fromList(edgeLabels);
        String labelName = label.getLabelName();
        String property = label.getRandomEdgeProperties().getEdgePropertyName();
        String indexname = labelName + "by" + property + "Shard";
        if(indexNames.contains(indexname)){
            return;
        }
        indexNames.add(indexname);
        try {
            this.writer.write(indexname);
            this.writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        System.out.println("create index " + indexname);
        this.connection.getHugespecial().schema().indexLabel(indexname).onE(labelName).by(property).shard().ifNotExist().create();
    }
}
