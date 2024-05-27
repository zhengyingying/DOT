package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.Node;
import org.gdbtesting.common.ast.Operator;
import org.gdbtesting.common.ast.OrderingTermNode;

public class OrderingTermOperation extends Traversal {

    public enum OrderOperator implements Operator {
        ASC("asc"),
        DESC("desc"),
        ASCBYP("ascbyp"),
        DESCBYP("descbyp"),
        ORDERBYP("orderbyp")
        ;

        String textRepresentation;

        OrderOperator(String textRepresentation){this.textRepresentation = textRepresentation;}

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public static OrderOperator getRandom() {
        return Randomly.fromOptions(OrderOperator.values());
    }

    public static class OrderByProperty extends OrderingTermOperation{

        private String property;

        public OrderByProperty(String property){
            this.property = property;
        }

        public String toString(){return "order().by('" + property + "')";}

    }

    public static class ASCByProperty extends OrderingTermOperation{

        private String property;

        public ASCByProperty(String property){
            this.property = property;
        }

        public String toString(){return "order().by('" + property + "', asc)";}

    }

    public static class DESCByProperty extends OrderingTermOperation{

        private String property;

        public DESCByProperty(String property){
            this.property = property;
        }

        public String toString(){return "order().by('" + property + "', desc)";}

    }

    public static class ASC extends OrderingTermOperation{

        public String toString(){
            return "order().by(asc)";
        }
    }

    public static class DESC extends OrderingTermOperation{

        public String toString(){
            return "order().by(desc)";
        }
    }

    public static ASC createASC(){ return new ASC();}
    public static DESC createDESC(){ return new DESC();}
    public static ASCByProperty createASCBYP(String prop) {return new ASCByProperty(prop);}
    public static DESCByProperty createDESCBYP(String prop) {return new DESCByProperty(prop);}
    public static OrderByProperty createOrder(String prop){return new OrderByProperty(prop);}

}
