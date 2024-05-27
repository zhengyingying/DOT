package org.gdbtesting.common.schema;

import org.gdbtesting.gremlin.GraphSchema;

import java.util.List;

public class AbstractGraphVertexIndex {

    private final String vertexIndexName;

    protected AbstractGraphVertexIndex(String vertexIndexName) {
        this.vertexIndexName = vertexIndexName;
    }

    public String getIndexName() {
        return vertexIndexName;
    }

    @Override
    public String toString() {
        return vertexIndexName;
    }
}
