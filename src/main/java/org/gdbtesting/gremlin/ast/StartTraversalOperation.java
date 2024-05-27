package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

import java.util.List;

public class StartTraversalOperation extends Traversal implements GraphExpression{

    public enum Start{
/*        addV("addV"),
        addV_label("addV_label"),
        addV_Traversal("addV_Traversal"),
        addE_label("addE_label"),
        addE_Traversal("addE_Traversal"),*/
        V("V"),
        /*V_ids("V_ids"),
        E_ids("E_ids"),*/
        E("E"),
        /*tx("tx")*/;

        private String start;

        Start(String start){this.start = start;}

        public String getStart(){
            return start;
        }
    }

    public static Start getRandomStartTraversal(){
        return Randomly.fromOptions(Start.values());
    }

    public static class AddV extends StartTraversalOperation {
        public AddV(){}

        public String getStartType(){return "vertex";}

        public String getEndType(){return "end";}

        @Override
        public String toString(){
            return "addV()";
        }
    }

    public static class AddVWithLabel extends StartTraversalOperation{
        private String label;

        public AddVWithLabel(String label){this.label = label;}

        public String getStartType(){return "vertex";}

        public String getEndType(){return "vertex";}

        public String getLabel(){return label;}

        @Override
        public String toString(){
            return "addV(" + label + ")";
        }
    }

    //Traversal<?, String> vertexLabelTraversal
    public static class AddVTraversal extends StartTraversalOperation{
        private Traversal traversal;

        public AddVTraversal(Traversal traversal){this.traversal = traversal;}

        public String getStartType(){return "vertex";}

        public String getEndType(){return "vertex";}

        public Traversal getTraversal(){return traversal;}

        @Override
        public String toString(){
            return "addV(" + traversal + ")";
        }
    }

    public static class AddEWithLabel extends StartTraversalOperation{
        private String label;

        public AddEWithLabel(String label){this.label = label;}

        public String getStartType(){return "edge";}

        public String getEndType(){return "edge";}

        public String getLabel(){return label;}

        @Override
        public String toString(){
            return "addE(" + label + ")";
        }
    }

    public static class AddETraversal extends StartTraversalOperation{
        private Traversal traversal;

        public AddETraversal(Traversal traversal){this.traversal = traversal;}

        public String getStartType(){return "edge";}

        public String getEndType(){return "edge";}

        public Traversal getTraversal(){return traversal;}

        @Override
        public String toString(){
            return "addE(" + traversal + ")";
        }
    }

    public static class VWithIds extends StartTraversalOperation{
        private List<Object> vertexIds;

        public VWithIds(List<Object> vertexIds){this.vertexIds = vertexIds;}

        public String getStartType(){return "vertex";}

        public String getEndType(){return "vertex";}

        public List<Object> getVertexIds(){return vertexIds;}

        @Override
        public String toString(){
            return "V(" + vertexIds + ")";
        }
    }

    public static class V extends StartTraversalOperation{

        public String getStartType(){return "vertex";}

        public String getEndType(){return "vertex";}

        @Override
        public String toString(){
            return "V()";
        }
    }

    public static class EWithIds extends StartTraversalOperation{
        private List<Object> edgeIds;

        public EWithIds(List<Object> edgeIds){this.edgeIds = edgeIds;}

        public String getStartType(){return "edge";}

        public String getEndType(){return "edge";}

        public List<Object> getEdgeIds(){return edgeIds;}

        @Override
        public String toString(){
            return "E(" + edgeIds + ")";
        }
    }

    public static class E extends StartTraversalOperation{

        public String getStartType(){return "edge";}

        public String getEndType(){return "edge";}

        @Override
        public String toString(){
            return "E()";
        }
    }

    public static class Tx extends StartTraversalOperation{
        public String toString(){
            return "tx()";
        }
    }

    public static AddV createAddV(){return new AddV();}
    public static AddVWithLabel createAddVWithLabel(String label){return new AddVWithLabel(label);}
    public static AddVTraversal createAddVTraversal(Traversal traversal){return new AddVTraversal(traversal);}
    public static AddEWithLabel createAddEWithLabel(String label){return new AddEWithLabel(label);}
    public static AddETraversal createAddETraversal(Traversal traversal){return new AddETraversal(traversal);}
    public static V createV(){return new V();}
    public static E createE(){return new E();}
    public static VWithIds createVWithIds(List<Object> ids){return new VWithIds(ids);}
    public static EWithIds createEWithIds(List<Object> ids){return new EWithIds(ids);}
    public static Tx createTx(){return new Tx();}


}
