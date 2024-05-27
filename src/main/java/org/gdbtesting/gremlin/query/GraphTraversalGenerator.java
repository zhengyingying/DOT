package org.gdbtesting.gremlin.query;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.GremlinPrint;
import org.gdbtesting.gremlin.gen.GraphExpressionGenerator;
import org.gdbtesting.gremlin.gen.GraphHasFilterGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * g.V()/E() // begin with this
 * .has(...) // filter some vertices
 * .in()/inV()/... // filter some vertices
 * .where(Predicate) // filter some vertices bu generating some predicates with logical operation, i.e., is(), and(), or(), not()...
 * .or/and/not() // filter
 * .values()/properties()... // get the chosen vertices information
 */
public class GraphTraversalGenerator {

    private GraphGlobalState state;
   /* private GraphTraversalSource g;*/
    private GremlinPrint print = new GremlinPrint();

    public enum Traversal{
        FILTER, // has(key, value), has(label, key, value), hasLabel(labels)
                // hasId(ids), hasKey(keys), hasValue(values), has(key), hasNot(key)
        Neighbor,
        Property,
        /*ORDER, // order(), order().by("", asc)
        LOGICAL, // is(), and(), or(), not()
        AGGREGATE, // sum(), max(), min(), mean(), count()
        COMPARISON,
        ITERATOR, // repeat(), times(), until(), emit(), loops(),
        PATH, // path(), simplePath(), cyclicPath()
        TRANSFORM, // map(), flatMap()
        WHERE,
        BRANCH, // choose(), optional()*/
        START, // g.V() / g.E()
    }

    public GraphTraversalGenerator(GraphGlobalState state){
        this.state = state;
    }

    public List<org.gdbtesting.gremlin.ast.Traversal> generateRandomlyTraversal(){
        return getExpression();
    }

    public List<org.gdbtesting.gremlin.ast.Traversal> getExpression(){
        /*// TODO:
        // Graph information should be lead to GraphExpressionGenerator.
        List<GraphSchema.GraphVertexLabel> labels = state.getSchema().getVertexLabelsRandomSubsetNotEmpty();
        for(GraphSchema.GraphVertexLabel label : labels){
            System.out.println(label.getLabelName());
        }
        List<GraphSchema.GraphVertexProperty> properties = new ArrayList<>();
        for(GraphSchema.GraphVertexLabel label : labels){
            properties.addAll(label.getVertexProperties());
        }
        state.getSchema().setInOutVertexLabelRelations();*/
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        return geg.generateGraphTraversal();
    }

    public GraphTraversal chooseVertex(GraphTraversal previous){
        // candidate labels
        List<GraphSchema.GraphVertexLabel> labels = Randomly.nonEmptySubList(state.getSchema().getVertexList());
        System.out.println("choose vertex labels: ");
        for(GraphSchema.GraphVertexLabel label : labels){
            System.out.println(label.getLabelName());
        }
        List<GraphSchema.GraphVertexProperty> properties = new ArrayList<>();
        System.out.println("choose vertex properties: ");
        for(GraphSchema.GraphVertexLabel label : labels){
            for(GraphSchema.GraphVertexProperty p : label.getVertexProperties()){
                if(!properties.contains(p)){
                    System.out.println(p.getVertexPropertyName());
                    properties.add(p);
                }
            }
        }
        // has filter vertex
        GraphHasFilterGenerator ghfg = new GraphHasFilterGenerator(labels, properties, state);
        return ghfg.getHasFilter(previous);
    }
}
