package org.gdbtesting.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.GraphDB;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.*;
import org.gdbtesting.gremlin.gen.*;
import org.gdbtesting.gremlin.query.GraphTraversalGenerator;
import org.gdbtesting.janusgraph.gen.JanusGraphAddVertexGeneration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.SchemaAction;


import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphOptimization {

    private List<String> janusgraphIndexList = new ArrayList<>();
    private int indexCount = 0;
    private GraphGlobalState state ;
    private Map<String, ConstantType> vertices = new HashMap<>();
    private Map<String, ConstantType> edges = new HashMap<>();
    private BufferedWriter writer;
    private JanusGraphConnection connection;
    private List<String> indexnames = new ArrayList<>();

    private List<String> vertexPropertyIndex = new ArrayList<>();
    private List<String> edgePropertyIndex = new ArrayList<>();

    private List<GraphSchema.GraphVertexProperty> vertexList;
    private List<GraphSchema.GraphEdgeProperty> edgeList;


    private enum IndexType {
        MIX,
        COMPOSITE,
    }

    public JanusGraphOptimization(GraphGlobalState state, JanusGraphConnection connection, BufferedWriter writer){
        this.state = state;
        this.connection = connection;
        this.writer = writer;
        vertexList = state.getSchema().getVertexProperties();
        edgeList = state.getSchema().getEdgeProperties();
        for(GraphSchema.GraphVertexProperty key : vertexList){
            vertices.put(key.getVertexPropertyName(), key.getDataType());
        }
        for(GraphSchema.GraphEdgeProperty key : edgeList){
            edges.put(key.getEdgePropertyName(), key.getDataType());
        }
    }

    public void createJanusGraphIndex(JanusGraphConnection connection){
        try {
            createVertexIndex(connection);
            createEdgeIndex(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createVertexIndex(JanusGraphConnection connection){
        GraphSchema.GraphVertexProperty property = Randomly.fromList(vertexList);
        String vp = property.getVertexPropertyName();
        if(indexnames.contains(vp)) {
            return;
        }else {
            indexnames.add(vp);
            ConstantType type = property.getDataType();
            String indexName;
            if(Randomly.getBoolean()){
                indexName = vp + "Mixed";
                createVertexMixedIndex(connection, indexName, vp, type);

            }else{
                indexName = vp + "Composite";
                createVertexCompositeIndex(connection, indexName, vp, type);
            }
            try {
                this.writer.write("create index: " + indexName);
                this.writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void createVertexMixedIndex(JanusGraphConnection connection, String indexName, String vp, ConstantType type){
        try {
            JanusGraph graph = connection.getJanusGraph();
            JanusGraphManagement mgmt = graph.openManagement();
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class);
            PropertyKey pk = makePropertyKey(vp, type, mgmt);
            if(type.equals(ConstantType.STRING.toString())){
                if(Randomly.getBoolean()){
                    builder.addKey(pk, Mapping.TEXT.asParameter());
                }else{
                    builder.addKey(pk,Mapping.STRING.asParameter());
                }
            }else{
                builder.addKey(pk);
            }
            builder.buildMixedIndex("search");
            System.out.println(mgmt.printIndexes());
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createVertexCompositeIndex(JanusGraphConnection connection, String indexName, String vp, ConstantType type){
        try {
            JanusGraph graph = connection.getJanusGraph();
            JanusGraphManagement mgmt = graph.openManagement();
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class);
            PropertyKey pk = makePropertyKey(vp, type, mgmt);
            if(pk!=null){
                builder.addKey(pk);
            }
            builder.buildCompositeIndex();
            System.out.println(mgmt.printIndexes());
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createEdgeIndex(JanusGraphConnection connection){
        GraphSchema.GraphEdgeProperty property = Randomly.fromList(edgeList);
        String ep = property.getEdgePropertyName();
        if(indexnames.contains(ep)) {
            return;
        }else {
            indexnames.add(ep);
            ConstantType type = property.getDataType();
            String indexName;
            if(Randomly.getBoolean()){
                indexName = ep + "Mixed";
                createEdgeMixedIndex(connection, indexName, ep, type);

            }else{
                indexName = ep + "Composite";
                createEdgeCompositeIndex(connection, indexName, ep, type);
            }
            try {
                this.writer.write("create index: " + indexName);
                this.writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void createEdgeMixedIndex(JanusGraphConnection connection, String indexName, String vp, ConstantType type){
        try {
            JanusGraph graph = connection.getJanusGraph();
            JanusGraphManagement mgmt = graph.openManagement();
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Edge.class);
            PropertyKey pk = makePropertyKey(vp, type, mgmt);
            if(type.equals(ConstantType.STRING.toString())){
                if(Randomly.getBoolean()){
                    builder.addKey(pk, Mapping.TEXT.asParameter());
                }else{
                    builder.addKey(pk,Mapping.STRING.asParameter());
                }
            }else{
                builder.addKey(pk);
            }
            builder.buildMixedIndex("search");
            System.out.println(mgmt.printIndexes());
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createEdgeCompositeIndex(JanusGraphConnection connection, String indexName, String vp, ConstantType type){
        try {
            JanusGraph graph = connection.getJanusGraph();
            JanusGraphManagement mgmt = graph.openManagement();
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Edge.class);
            PropertyKey pk = makePropertyKey(vp, type, mgmt);
            if(pk!=null){
                builder.addKey(pk);
            }
            builder.buildCompositeIndex();
            System.out.println(mgmt.printIndexes());
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createJanusGraphIndex(JanusGraphConnection connection, List<String> vertexProperties, List<String> edgeProperties){
        if(vertexProperties.size() != 0){
            createVertexIndex(connection, vertexProperties);
        }
        if(edgeProperties.size() != 0){
            createEdgeIndex(connection, edgeProperties);
        }
    }

    public void createVertexIndex(JanusGraphConnection connection, List<String> vertexProperties){
        List<String> properties = new ArrayList<>();
        // make index for properties that are not used in any indices
        for(String key : vertexProperties){
            if(!vertexPropertyIndex.contains(key)){
                properties.add(key);
            }
        }
        if(properties.size() == 0){
            return;
        }
        vertexPropertyIndex.addAll(properties);
        String indexName = "vmixIndex" + indexCount;
        if(Randomly.getBoolean()){
            createVertexMixedIndex(connection, properties, indexName);
        }else{
            createVertexCompositeIndex(connection, vertexProperties, indexName);
        }
    }

    public void createVertexMixedIndex(JanusGraphConnection connection, List<String> properties, String indexName){
        System.out.println("create JanusGraph Vertex MixedIndex");
        JanusGraph graph = connection.getJanusGraph();
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class);
            for(String key: properties){
                PropertyKey pk = makePropertyKey(key, vertices.get(key), mgmt);
                if(vertices.get(key).equals(ConstantType.STRING.toString())){
                    if(Randomly.getBoolean()){
                        builder.addKey(pk, Mapping.TEXT.asParameter());
                    }else{
                        builder.addKey(pk,Mapping.STRING.asParameter());
                    }
                }else{
                    builder.addKey(pk);
                }
            }
            builder.buildMixedIndex("search");
            indexCount++;
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createVertexCompositeIndex(JanusGraphConnection connection, List<String> properties, String indexName){
        System.out.println("create JanusGraph Vertex CompositeIndex");
        JanusGraph graph = connection.getJanusGraph();
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class);
            for(String key : properties){
                PropertyKey pk = makePropertyKey(key, vertices.get(key), mgmt);
                if(pk!=null){
                    builder.addKey(pk);
                }
            }
            builder.buildCompositeIndex();
            indexCount++;
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createEdgeIndex(JanusGraphConnection connection, List<String> edgeProperties){
        List<String> properties = new ArrayList<>();
        for(String key : edgeProperties){
            if(!edgePropertyIndex.contains(key)){
                properties.add(key);
            }
        }
        if(properties.size() == 0){
            return;
        }
        edgePropertyIndex.addAll(properties);
        String indexName = "emixIndex" + indexCount;
        if(Randomly.getBoolean()){
            createEdgeMixedIndex(connection, properties, indexName);
        }else{
            createEdgeCompositeIndex(connection, properties, indexName);
        }

    }

    public void createEdgeMixedIndex(JanusGraphConnection connection, List<String> properties, String indexName){
        System.out.println("create JanusGraph Edge MixedIndex");
        JanusGraph graph = connection.getJanusGraph();
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Edge.class);
            for(String key: properties){
                PropertyKey pk = makePropertyKey(key, edges.get(key), mgmt);
                if(edges.get(key).equals(ConstantType.STRING.toString())){
                    if(Randomly.getBoolean()){
                        builder.addKey(pk, Mapping.TEXT.asParameter());
                    }else{
                        builder.addKey(pk,Mapping.STRING.asParameter());
                    }
                }else{
                    builder.addKey(pk);
                }
            }
            builder.buildMixedIndex("search");
            indexCount++;
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createEdgeCompositeIndex(JanusGraphConnection connection, List<String> properties, String indexName){
        System.out.println("create JanusGraph Edge CompositeIndex");
        JanusGraph graph = connection.getJanusGraph();
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Edge.class);
            for(String key : properties){
                PropertyKey pk = makePropertyKey(key, edges.get(key), mgmt);
                if(pk!=null){
                    builder.addKey(pk);
                }
            }
            builder.buildCompositeIndex();
            indexCount++;
            mgmt.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void createJanusGraphRandomIndex(){
        System.out.println("create JanusGraph Index randomly");
        JanusGraphManagement mgmt = connection.getMgmt();
        try {
            // Vertex Index
            if(Randomly.getBoolean()){
                createVertexRandomIndex(mgmt);
            }
            // Edge Index
            if(Randomly.getBoolean()){
                createEdgeRandomIndex(mgmt);
            }
            System.out.println(mgmt.printIndexes());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mgmt.commit();
        }
    }

    public void createVertexRandomIndex(JanusGraphManagement mgmt){
        // Randomly choose properties
        List<GraphSchema.GraphVertexProperty> properties = Randomly.nonEmptySubList(vertexList);
        int size = properties.size();
        int num = 0;
        while(size > 0){
            // Randomly choose composite index properties
            int count = Randomly.getInteger(1, size);
            List<GraphSchema.GraphVertexProperty> subProp = new ArrayList<>();
            for(int i = 0; i< count; i++){
                subProp.add(properties.get(num++));
            }
            size = size - count;
            // Randomly choose index type
            switch (Randomly.fromOptions(IndexType.values())){
                case MIX: createJanusGraphVertexMixIndex(subProp, mgmt, "vmixIndex" + indexCount); break;
                case COMPOSITE: createJanusGraphVertexCompositeIndex(subProp, mgmt, "vcompositeIndex" + indexCount); break;
            }
            indexCount++;
        }
    }

    public void createEdgeRandomIndex(JanusGraphManagement mgmt){
        // Randomly choose properties
        List<GraphSchema.GraphEdgeProperty> properties = Randomly.nonEmptySubList(state.getSchema().getEdgeProperties());
        int size = properties.size();
        int num = 0;
        while(size > 0){
            // Randomly choose composite index properties
            int count = Randomly.getInteger(1, size);
            List<GraphSchema.GraphEdgeProperty> subProp = new ArrayList<>();
            for(int i = 0; i< count; i++){
                subProp.add(properties.get(num++));
            }
            size = size - count;
            // Randomly choose index type
            switch (Randomly.fromOptions(IndexType.values())){
                case MIX: createJanusGraphEdgeMixIndex(subProp, mgmt, "emixIndex" + indexCount); break;
                case COMPOSITE: createJanusGraphEdgeCompositeIndex(subProp, mgmt, "ecompositeIndex" + indexCount); break;
            }
            indexCount++;
        }
    }

    //  update
    //  mgmt = graph.openManagement();
    //  mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REINDEX).get();
    //  mgmt.commit();
    public void disableJanusGraphIndex(JanusGraphConnection connection){
        JanusGraph graph = (JanusGraph) connection.getGraph();
        JanusGraphManagement mgmt = graph.openManagement();
        for(String indexName : janusgraphIndexList) {
            System.out.println("indexName = " + indexName);
            try {
                mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.DISABLE_INDEX).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        mgmt.commit();
    }

    public void createJanusGraphVertexMixIndex(List<GraphSchema.GraphVertexProperty> subProp, JanusGraphManagement mgmt, String indexName){
        try {
            janusgraphIndexList.add(indexName);
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class);
            for(GraphSchema.GraphVertexProperty prop : subProp){
                PropertyKey pk = makePropertyKey(prop.getVertexPropertyName(), prop.getDataType(), mgmt);
                if(prop.getDataType().equals(ConstantType.STRING.toString())){
                    if(Randomly.getBoolean()){
                        builder.addKey(pk, Mapping.TEXT.asParameter());
                    }else{
                        builder.addKey(pk,Mapping.STRING.asParameter());
                    }
                }else{
                    builder.addKey(pk);
                }
            }
            builder.buildMixedIndex("search");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createJanusGraphVertexCompositeIndex(List<GraphSchema.GraphVertexProperty> subProp, JanusGraphManagement mgmt, String indexName){
        try {
            janusgraphIndexList.add(indexName);
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Vertex.class);
            for(GraphSchema.GraphVertexProperty prop : subProp){
                PropertyKey pk = makePropertyKey(prop.getVertexPropertyName(), prop.getDataType(), mgmt);
                if(pk!=null){
                    builder.addKey(pk);
                }
            }
            builder.buildCompositeIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createJanusGraphEdgeMixIndex(List<GraphSchema.GraphEdgeProperty> subProp, JanusGraphManagement mgmt, String indexName){
        try {
            janusgraphIndexList.add(indexName);
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Edge.class);
            for(GraphSchema.GraphEdgeProperty prop : subProp){
                PropertyKey pk = makePropertyKey(prop.getEdgePropertyName(), prop.getDataType(), mgmt);
                if(prop.getDataType().equals(ConstantType.STRING.toString())){
                    if(Randomly.getBoolean()){
                        builder.addKey(pk,Mapping.TEXT.asParameter());
                    }else{
                        builder.addKey(pk,Mapping.STRING.asParameter());
                    }
                }else{
                    builder.addKey(pk);
                }
            }
            builder.buildMixedIndex("search");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createJanusGraphEdgeCompositeIndex(List<GraphSchema.GraphEdgeProperty> subProp, JanusGraphManagement mgmt, String indexName){
        try {
            janusgraphIndexList.add(indexName);
            JanusGraphManagement.IndexBuilder builder = mgmt.buildIndex(indexName, Edge.class);
            for(GraphSchema.GraphEdgeProperty prop : subProp){
                PropertyKey pk = makePropertyKey(prop.getEdgePropertyName(), prop.getDataType(), mgmt);
                if(pk!=null){
                    builder.addKey(pk);
                }
            }
            builder.buildCompositeIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: optimization
    public PropertyKey makePropertyKey(String name, ConstantType type, JanusGraphManagement mgmt){
        PropertyKey pk = null;
        switch (type){
            case STRING:
                pk = mgmt.makePropertyKey(name).dataType(String.class).make();
                break;
            case LONG:
                pk = mgmt.makePropertyKey(name).dataType(Long.class).make();
                break;
            case INTEGER:
                pk = mgmt.makePropertyKey(name).dataType(Integer.class).make();
                break;
            case DOUBLE:
                pk = mgmt.makePropertyKey(name).dataType(Double.class).make();
                break;
            case FLOAT:
                pk = mgmt.makePropertyKey(name).dataType(Float.class).make();
                break;
            case BOOLEAN:
                pk = mgmt.makePropertyKey(name).dataType(Boolean.class).make();
                break;
            default: ;
        }
        return pk;
    }

}
