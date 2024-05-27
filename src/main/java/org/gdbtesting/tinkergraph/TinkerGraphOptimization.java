package org.gdbtesting.tinkergraph;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TinkerGraphOptimization {

    private List<String> vertexIndex = new ArrayList<>();
    private List<String> edgeIndex = new ArrayList<>();
    private List<String> indexNames= new ArrayList<>();
    private List<GraphSchema.GraphVertexProperty> vertexProperties;
    private List<GraphSchema.GraphEdgeProperty> edgeProperties;
    private TinkerGraph graph;

    private BufferedWriter writer;

    public TinkerGraphOptimization(GraphGlobalState state, TinkerGraphConnection connection, BufferedWriter writer){
        this.graph = connection.getTinkerGraph();
        this.writer = writer;
        vertexProperties = state.getSchema().getVertexProperties();
        edgeProperties = state.getSchema().getEdgeProperties();
    }

    public void createTinkerGraphIndex(){
        createVertexIndex();
        createEdgeIndex();
    }

    public void createVertexIndex(){
        String property = Randomly.fromList(vertexProperties).getVertexPropertyName();
        String indexname = property + "Index";
        try{
            if(indexNames.contains(indexname)){
                graph.dropIndex(property, Vertex.class);
                try {
                    this.writer.write("drop index: " + indexname);
                    this.writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                indexNames.add(indexname);
                graph.createIndex(property, Vertex.class);
                try {
                    this.writer.write("create index: " + indexname);
                    this.writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void createEdgeIndex(){
        String property = Randomly.fromList(edgeProperties).getEdgePropertyName();
        String indexname = property + "Index";
        try{
            if(indexNames.contains(indexname)){
                graph.dropIndex(property, Edge.class);
                try {
                    this.writer.write("drop index: " + indexname);
                    this.writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                indexNames.add(indexname);
                graph.createIndex(property, Edge.class);
                try {
                    this.writer.write("create index: " + indexname);
                    this.writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // create index based on used properties
    public void createTinkerGraphIndex(List<String> vertexProperties, List<String> edgeProperties){
        if(vertexProperties.size() != 0){
            createVertexIndex(vertexProperties);
        }
        if(edgeProperties.size() != 0){
            createEdgeIndex(edgeProperties);
        }
    }

    public void createVertexIndex(List<String> vertexProperties){
        for(String vp : vertexIndex){
            if(!vertexProperties.contains(vp)){
                System.out.println("drop vertex index: " + vp);
                graph.dropIndex(vp, Vertex.class);
            }
        }
        for(String vp : vertexProperties){
            if(!vertexIndex.contains(vp)){
                System.out.println("add vertex index: " + vp);
                graph.createIndex(vp, Vertex.class);
            }
        }
    }

    public void createEdgeIndex(List<String> edgeProperties){
        for(String ep : edgeIndex){
            if(!edgeProperties.contains(ep)){
                System.out.println("drop edge index: " + ep);
                graph.dropIndex(ep, Edge.class);
            }
        }
        for(String ep : edgeProperties){
            if(!edgeIndex.contains(ep)){
                System.out.println("add edge index: " + ep);
                graph.createIndex(ep, Edge.class);
            }
        }
    }
}

