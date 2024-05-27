package org.gdbtesting.tinkergraph.gen;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.gen.GraphEdgeIndexGeneration;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;

import java.util.List;

public class TinkerGraphEdgeIndexGeneration extends GraphEdgeIndexGeneration {

    private TinkerGraph graph;

    public TinkerGraphEdgeIndexGeneration(TinkerGraphConnection connection, GraphGlobalState state) {
        super(state);
        this.graph = connection.getGraph();
    }

    @Override
    public void generateEdgeIndex() {
        List<GraphSchema.GraphEdgeIndex> list = state.getSchema().getEdgeIndices();
        for(int i = 0; i < list.size(); i++){
            graph.createIndex(list.get(i).getIndexName(), Edge.class);
        }
    }
}
