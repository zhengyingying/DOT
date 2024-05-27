package org.gdbtesting.gremlin.query;

import org.apache.hugegraph.structure.gremlin.Result;
import org.apache.hugegraph.structure.gremlin.ResultSet;
import org.gdbtesting.GraphDB;
import org.gdbtesting.janusgraph.JanusGraphConnection;
import org.gdbtesting.orientdb.OrientdbConnection;

import java.util.ArrayList;
import java.util.List;

import static org.gdbtesting.GraphDB.*;

public class ResultWrapper {
    private Object result;

    private GraphDB graphDB;

    public void setName(GraphDB db) {
        this.graphDB = db;
    }

    public GraphDB getName() {
        return this.graphDB;
    }

    public void fetch(Object r) {
        this.result = r;
    }

    public ResultWrapper(ResultWrapper result) {
        this.result = result.getResult();
        setName(result.getName());
    }

    public ResultWrapper() {
    }


    public Object getResult() {
        return result;
    }

    public List<String> extractIds() {
        List<String> ids = new ArrayList<>();
        if (this.graphDB.equals(GraphDB.HUGEGRAPH)) {
            for (int i = 0; i < ((ResultSet) result).size(); i++) {
                Result r = ((ResultSet) result).get(i);
                if (r.getObject() instanceof org.apache.hugegraph.structure.graph.Vertex)
                    ids.add(r.getVertex().id().toString());
                else if (r.getObject() instanceof org.apache.hugegraph.structure.graph.Edge)
                    ids.add(r.getEdge().id());
                else if (r.getObject() instanceof Integer)
                    ids.add(String.valueOf(r.getInt()));
                else if (r.getObject() instanceof String)
                    ids.add(r.getString());
                else if (r.getObject() instanceof Long)
                    ids.add(String.valueOf(r.getLong()));
            }
        }
        if (this.graphDB.equals(JANUSGRAPH) || this.graphDB.equals(ORIENTDB) || this.graphDB.equals(TINKERGRAPH) || this.graphDB.equals(ARCADEDB)) {
            List<org.apache.tinkerpop.gremlin.driver.Result> results = (List<org.apache.tinkerpop.gremlin.driver.Result>) this.result;
            for (org.apache.tinkerpop.gremlin.driver.Result r : results) {
                if (r.getObject() instanceof org.apache.tinkerpop.gremlin.structure.Vertex)
                    ids.add(r.getVertex().id().toString());
                else if (r.getObject() instanceof org.apache.tinkerpop.gremlin.structure.Edge)
                    ids.add((r.getEdge().id().toString()));
                else if (r.getObject() instanceof Integer)
                    ids.add(String.valueOf(r.getInt()));
                else if (r.getObject() instanceof String)
                    ids.add(r.getString());
                else if (r.getObject() instanceof Long)
                    ids.add(String.valueOf(r.getLong()));
                else {
                    ids.add(String.valueOf(r.getObject()));
                }
            }
        }
        return ids;
    }

    public int size() {
        if (this.graphDB.equals(GraphDB.HUGEGRAPH))
            return ((ResultSet) result).size();
        if (this.graphDB.equals(JANUSGRAPH) || this.graphDB.equals(ORIENTDB) || this.graphDB.equals(TINKERGRAPH) || this.graphDB.equals(ARCADEDB))
            return ((List<org.apache.tinkerpop.gremlin.driver.Result>) this.result).size();
        else return -1;
    }

    public static void main(String[] args) {
        JanusGraphConnection connection = new JanusGraphConnection("0.6.2", "./conf/janusgraph.yaml");
        connection.connect();
    }
}
