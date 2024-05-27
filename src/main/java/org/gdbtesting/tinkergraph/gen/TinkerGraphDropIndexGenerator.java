package org.gdbtesting.tinkergraph.gen;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GremlinPrint;
import org.gdbtesting.gremlin.gen.GraphDropIndexGenerator;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;

public class TinkerGraphDropIndexGenerator extends GraphDropIndexGenerator {

    private TinkerGraph graph;
    private GremlinPrint print;

    public TinkerGraphDropIndexGenerator(TinkerGraphConnection connection, GraphGlobalState state) {
        super(state);
        this.graph = connection.getGraph();
        this.print = new GremlinPrint();
    }

    enum Type{
        vertex, edge
    }

    public void dropIndex(){
        String type = Randomly.fromOptions(Type.values()).toString();
        dropIndex(type);
    }

    @Override
    public void dropIndex(String type) {
        switch (type){
            case "edge":
                dropEdgeIndex();
            case "vertex":
                dropVertexIndex();
        }
    }

    public void dropVertexIndex() {
        System.out.println("Before drop vertex index");
        print.printIndex(getVertexIndex(), "vertex");
        graph.dropIndex(Randomly.fromList(state.getSchema().getVertexIndices()).getIndexName(), Vertex.class);
        System.out.println("After drop vertex index");
        print.printIndex(getVertexIndex(), "vertex");
    }

    public void dropEdgeIndex(){
        System.out.println("Before drop edge index");
        print.printIndex(getEdgeIndex(), "edge");
        graph.dropIndex(Randomly.fromList(state.getSchema().getEdgeIndices()).getIndexName(), Edge.class);
        System.out.println("After drop edge index");
        print.printIndex(getEdgeIndex(), "edge");
    }

    public Object[] getVertexIndex(){
        return graph.getIndexedKeys(Vertex.class).toArray();
    }

    public Object[] getEdgeIndex(){
        return graph.getIndexedKeys(Edge.class).toArray();
    }

}
