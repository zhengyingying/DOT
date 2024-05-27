package org.gdbtesting.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.ConstantType;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;

import java.io.BufferedWriter;
import java.util.*;

public class OrientDBOptimization {

    private List<String> indexNames = new ArrayList<>();
    private OrientGraph graph;
    private GraphGlobalState state;
    private BufferedWriter writer;
    private List<GraphSchema.GraphVertexLabel> vertexLabels = new ArrayList<>();
    private List<GraphSchema.GraphRelationship> edgeLabels = new ArrayList<>();

    public OrientDBOptimization(GraphGlobalState state, OrientdbConnection connection, BufferedWriter writer){
        this.state = state;
        this.writer = writer;
        this.graph = connection.getOrientGraph();
        this.vertexLabels = state.getSchema().getVertexList();
        this.edgeLabels = state.getSchema().getEdgeList();
    }

    public void createOrientDBIndex() {
        createVertexIndex();
        createEdgeIndex();
    }

    // index name is vl0.vp0
    public void createVertexIndex() {
        try {
            GraphSchema.GraphVertexLabel label = Randomly.fromList(vertexLabels);
            GraphSchema.GraphVertexProperty property = label.getRandomVertexProperties();
            String indexName = label.getLabelName() + "." + property.getVertexPropertyName();
            if (indexNames.contains(indexName)) {
                return;
            }else{
                indexNames.add(indexName);
                Configuration conf = new BaseConfiguration();
                conf.setProperty("type", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString());
                conf.setProperty("keytype", getOType(property.getDataType()));
                graph.createVertexIndex(property.getVertexPropertyName(), label.getLabelName(), conf);
                this.writer.write("create index: " + indexName);
                this.writer.newLine();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void createEdgeIndex() {
        try {
            GraphSchema.GraphRelationship label = Randomly.fromList(edgeLabels);
            GraphSchema.GraphEdgeProperty property = label.getRandomEdgeProperties();
            String indexName = label.getLabelName() + "." + property.getEdgePropertyName();
            if (indexNames.contains(indexName)) {
                return;
            }else{
                indexNames.add(indexName);
                Configuration conf = new BaseConfiguration();
                conf.setProperty("type", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString());
                conf.setProperty("keytype", getOType(property.getDataType()));
                graph.createEdgeIndex(property.getEdgePropertyName(), label.getLabelName(), conf);
                this.writer.write("create index: " + indexName);
                this.writer.newLine();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //INTEGER, /*NULL,*/ STRING, DOUBLE, BOOLEAN, FLOAT, LONG;
    public OType getOType(ConstantType type){
        switch (type){
            case INTEGER: return OType.INTEGER;
            case STRING: return OType.STRING;
            case DOUBLE: return OType.DOUBLE;
            case BOOLEAN: return OType.BOOLEAN;
            case FLOAT: return OType.FLOAT;
            case LONG: return OType.LONG;
        }
        return null;
    }
}

