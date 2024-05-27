package org.gdbtesting.janusgraph.gen;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphData;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.gen.GraphAddVertexAndProeprtyGenerator;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;

public class JanusGraphAddVertexGeneration extends GraphAddVertexAndProeprtyGenerator {

    private GraphTraversalSource g;
    private GraphGlobalState state;

    public JanusGraphAddVertexGeneration(GraphGlobalState state) {
        super(state);
        state = state;
        g = state.getConnection().getG();
    }

    public void addVertex(){
        List<GraphData.VertexObject> map = addVertices(Randomly.getInteger(0, (int) state.getVerticesMaxNum()));
        // build execute script

    }

}
