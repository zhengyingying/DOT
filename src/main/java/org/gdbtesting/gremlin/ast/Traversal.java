package org.gdbtesting.gremlin.ast;

import org.gdbtesting.gremlin.ConstantType;

import java.util.ArrayList;
import java.util.List;

public class Traversal implements GraphExpression{

    protected String startStep = "null";
    protected String endStep = "null";

    protected List<String> vertexProperties = new ArrayList<>();

    public List<String> getVertexProperties() {
        return vertexProperties;
    }

    public void setVertexProperties(List<String> vertexProperties) {
        this.vertexProperties = vertexProperties;
    }

    public List<String> getEdgeProperties() {
        return edgeProperties;
    }

    public void setEdgeProperties(List<String> edgeProperties) {
        this.edgeProperties = edgeProperties;
    }

    protected List<String> edgeProperties = new ArrayList<>();

    protected ConstantType dataType = ConstantType.STRING;

    public String getTraversalType() {
        return traversalType;
    }

    public void setTraversalType(String traversalType) {
        this.traversalType = traversalType;
    }

    protected String traversalType;

    public String getStartStep() {
        return startStep;
    }

    protected List<Traversal> traversalList = null;

    public void setTraversalList(List<Traversal> list){this.traversalList = list;}

    public List<Traversal> getTraversalList(){return this.traversalList;}

    public void setStartStep(String startStep) {
        this.startStep = startStep;
    }

    public String getEndStep() {
        return endStep;
    }

    public void setEndStep(String endStep) {
        this.endStep = endStep;
    }

    public ConstantType getDataType() {
        return dataType;
    }

    public void setDataType(ConstantType dataType) {
        this.dataType = dataType;
    }

}
