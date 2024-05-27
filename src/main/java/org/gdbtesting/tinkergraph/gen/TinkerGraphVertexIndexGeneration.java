package org.gdbtesting.tinkergraph.gen;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.gen.GraphVertexIndexGeneration;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;

import java.util.List;

public class TinkerGraphVertexIndexGeneration extends GraphVertexIndexGeneration {

    private TinkerGraph graph;

    public TinkerGraphVertexIndexGeneration(TinkerGraphConnection connection, GraphGlobalState state) {
        super(state);
        this.graph = connection.getGraph();
    }

    @Override
    public void generateVertexIndex() {
        List<GraphSchema.GraphVertexIndex> list = state.getSchema().getVertexIndices();
        for(int i = 0; i < list.size(); i++){
            graph.createIndex(list.get(i).getIndexName(), Vertex.class);
        }
    }
}
