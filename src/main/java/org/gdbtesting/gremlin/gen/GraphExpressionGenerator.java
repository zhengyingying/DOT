package org.gdbtesting.gremlin.gen;

import afu.org.checkerframework.checker.oigj.qual.O;
import org.apache.commons.lang.RandomStringUtils;
import org.gdbtesting.Randomly;
import org.gdbtesting.common.gen.UntypedExpressionGenerator;
import org.gdbtesting.gremlin.ConstantType;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.ast.*;

import java.util.*;


public class GraphExpressionGenerator extends UntypedExpressionGenerator<GraphExpression, GraphSchema.GraphVertexProperty> {

    private final GraphGlobalState state;
    private GraphSchema.GraphVertexProperty vertexProperties;
    private GraphSchema.GraphEdgeProperty edgeProperties;

    public StringBuilder getExpression() {
        return expression;
    }

    private StringBuilder expression = new StringBuilder("g");
    // record the out Vertex Label
    protected Map<String, List<GraphSchema.GraphVertexLabel>> outVertexLabelMap;
    // record the in Vertex Label
    protected Map<String, List<GraphSchema.GraphVertexLabel>> inVertexLabelMap;

    /*private GraphTraversalSource g;

    public GraphTraversalSource getG() {
        return g;
    }*/

    public GraphGlobalState getState() {
        return state;
    }


    public GraphExpressionGenerator(GraphGlobalState state) {
        this.state = state;
        /*this.g = state.getConnection().getG();*/
    }

    private enum Actions {
        VERTEX_PROPERTY, /*EDGE_PROPERTY*/ LITERAL, BINARY_COMPARISON, BINARY_LOGICAL/*, UNARY_PREFIX*/;
    }


    private enum GraphFilterTraversal {
        PROPERTY, FILTER_TRAVERSAL, NEIGHBOR_TRAVERSAL, STATISTIC, ORDER, BRANCH;
//        , PATH;
    }

    List<Traversal> list = new ArrayList<>();

    public List<Traversal> generateGraphTraversal() {
//        System.out.println("generate depth : " + state.getGenerateDepth());
        int length = Randomly.getInteger(2, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateExpressionTraversal(i);
            }
            list.add(t);
            String Type = t.getTraversalType();
            if (Type.contains("property") || Type.contains("statistic")) {
                if (Type.contains("property") && Randomly.getBoolean()) {
                    Traversal ta = createStatistic(t);
                    list.add(ta);
                }
                break;
            }
        }
        return list;
    }

    public Traversal generateExpressionTraversal(int depth) {
//        System.out.println("depth:" + depth);
        if (depth == 0) return createStartTraversal();
//        if(depth == state.getGenerateDepth() - 1) return generateLeafNode(list.get(depth-1));
        Traversal startTraversal = list.get(depth - 1);
//        if(startTraversal.getEndStep().equals("path")){
//            return createPathTraversal(startTraversal);
//        }
        return generateRandomExpressionTraversal(startTraversal);
    }

    public Traversal generateRandomExpressionTraversal(Traversal startTraversal) {
        int x = Randomly.getInteger(0, 100);
        if (0 <= x && x < 50) {
            return createFilterTraversalOperation(startTraversal);
        } else if (50 <= x && x < 95) {
            return createNeighborTraversalOperation(startTraversal);
        } else if (95 <= x && x < 97) {
            return createPropertyOrConstant(startTraversal);
        } else if (97 <= x && x < 99) {
            return createBranchTraversal(startTraversal);
        } else {
            return createOrder(startTraversal);
        }
    }

    public Traversal createBranchTraversal(Traversal startTraversal) {
        switch (BranchTraversalOperation.getRandomBranch()) {
            case union:
                int size = Randomly.getInteger(1, 5);
                List<Traversal> list = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    Traversal t = null;
                    while(t == null){
                        t = createFilterTraversalOperation(startTraversal);
                    }
                    list.add(t);
                }
                BranchTraversalOperation.Union union = BranchTraversalOperation.createUnion(list);
                union.setStartStep(startTraversal.getEndStep());
                union.setEndStep(list.get(list.size()-1).getEndStep());
                union.setTraversalType("filter");
                union.setTraversalList(list);
                return union;
//            case repeat:
//                Traversal t = null;
//                while(t == null){
//                    int x = Randomly.getInteger(0,100);
//                    if (0 <= x && x < 50) {
//                        t = NeighborTraversalOperation.createOut(getEdgeLabelList());
//                    } else if (50 <= x && x < 95) {
//                        t = NeighborTraversalOperation.createIn(getEdgeLabelList());
//                    } else if (95 <= x && x < 97) {
//                        t = NeighborTraversalOperation.createBoth(getEdgeLabelList());
//                    }
//                }
//                if(startTraversal.getEndStep().equals("vertex")){
//                    BranchTraversalOperation.Repeat repeat = BranchTraversalOperation.createRepeat(t);
//                    repeat.setTraversalType(startTraversal.getEndStep());
//                    repeat.setEndStep(startTraversal.getEndStep());
//                    repeat.setTraversalType("neighbor");
//                    repeat.setTraversalList(Arrays.asList(t));
//                    return repeat;
//                }else{
//                    t = createFilterTraversalOperation(startTraversal);
//                    BranchTraversalOperation.Repeat repeat = BranchTraversalOperation.createRepeat(t);
//                    repeat.setTraversalType(startTraversal.getEndStep());
//                    repeat.setEndStep(startTraversal.getEndStep());
//                    repeat.setTraversalType("neighbor");
//                    repeat.setTraversalList(Arrays.asList(t));
//                    return repeat;
//                }

//            case repeatEmit:
//                Traversal te = null;
//                while(te == null){
//                    int x = Randomly.getInteger(0,100);
//                    if (0 <= x && x < 50) {
//                        te = NeighborTraversalOperation.createOut(getEdgeLabelList());
//                    } else if (50 <= x && x < 95) {
//                        te = NeighborTraversalOperation.createIn(getEdgeLabelList());
//                    } else if (95 <= x && x < 100) {
//                        te = NeighborTraversalOperation.createBoth(getEdgeLabelList());
//                    }
//                }
//                if(startTraversal.getEndStep().equals("vertex")){
//                    Traversal f = createFilterTraversalOperation(startTraversal);
//                    BranchTraversalOperation.RepeatEmit repeatEmit = BranchTraversalOperation.createRepeatEmit(te, f);
//                    repeatEmit.setTraversalType(startTraversal.getEndStep());
//                    repeatEmit.setEndStep(startTraversal.getEndStep());
//                    repeatEmit.setTraversalType("neighbor");
//                    repeatEmit.setTraversalList(Arrays.asList(te, f));
//                    return repeatEmit;
//                }else{
//                    te = createFilterTraversalOperation(startTraversal);
//                    BranchTraversalOperation.Repeat repeat = BranchTraversalOperation.createRepeat(te);
//                    repeat.setTraversalType(startTraversal.getEndStep());
//                    repeat.setEndStep(startTraversal.getEndStep());
//                    repeat.setTraversalType("neighbor");
//                    repeat.setTraversalList(Arrays.asList(te));
//                    return repeat;
//                }
            default:
                throw new AssertionError();
        }
    }

    public Traversal createPathTraversal(Traversal startTraversal) {
        switch (PathTraversalOperation.getRandomPathTraversal()) {
            case path:
                PathTraversalOperation.Path path = PathTraversalOperation.createPath();
                path.setEndStep("path");
                path.setStartStep(startTraversal.getEndStep());
                path.setTraversalType("path");
                return path;
            case tree:
                PathTraversalOperation.Tree tree = PathTraversalOperation.createTree();
                tree.setEndStep("path");
                tree.setStartStep(startTraversal.getEndStep());
                tree.setTraversalType("path");
                return tree;
            default:
                throw new AssertionError();
        }
    }

    public Traversal createPropertyOrConstant(Traversal startTraversal) {
        Traversal t = null;
        while (t == null) {
            switch (Randomly.fromOptions(GraphFilterTraversal.values())) {
                case PROPERTY:
                    return createProperties(startTraversal);
                case STATISTIC:
                    return createStatistic(startTraversal);
                default:
                    continue;
            }
        }
        return t;
    }

    public String getExpressionString() {
        expression.append(".").append(createStartTraversal().toString());
        return expression.toString();
    }

    public StartTraversalOperation createStartTraversal() {
        switch (StartTraversalOperation.getRandomStartTraversal()) {
            /*case addV:
                StartTraversalOperation.AddV addV = StartTraversalOperation.createAddV();
                addV.setStartStep("vertex");
                addV.setEndStep("vertex");
                return addV;
            case addV_label:
                StartTraversalOperation.AddVWithLabel addVWithLabel =
                        StartTraversalOperation.createAddVWithLabel(Randomly.fromList(state.getSchema().getVertexList()).getLabelName());
                addVWithLabel.setStartStep("vertex");
                addVWithLabel.setEndStep("vertex");
                return addVWithLabel;
            case addE_label:
                StartTraversalOperation.AddEWithLabel addEWithLabel =
                        StartTraversalOperation.createAddEWithLabel(Randomly.fromList(state.getSchema().getEdgeList()).getLabelName());
                addEWithLabel.setStartStep("edge");
                addEWithLabel.setEndStep("edge");
                return addEWithLabel;*/
            case V:
                StartTraversalOperation.V v = StartTraversalOperation.createV();
                v.setTraversalType("V");
                v.setStartStep("vertex");
                v.setEndStep("vertex");
                return v;
            /*case V_ids:
                StartTraversalOperation.VWithIds vWithIds = StartTraversalOperation.createVWithIds(getVertexIds());
                vWithIds.setStartStep("vertex");
                vWithIds.setEndStep("vertex");
                return vWithIds;*/
            case E:
                StartTraversalOperation.E e = StartTraversalOperation.createE();
                e.setTraversalType("E");
                e.setStartStep("edge");
                e.setEndStep("edge");
                return e;
           /* case E_ids:
                StartTraversalOperation.EWithIds eWithIds = StartTraversalOperation.createEWithIds(getEdgeIds());
                eWithIds.setStartStep("edge");
                eWithIds.setEndStep("edge");
                return eWithIds;*/
            /*case tx:
                return StartTraversalOperation.createTx();*/
            default:
                throw new AssertionError();
        }
    }

    public StatisticTraversalOperation createStatistic(Traversal startTraversal) {
        if (startTraversal.getEndStep().equals("vertex") || startTraversal.getEndStep().equals("edge")) {
            StatisticTraversalOperation.Count count = StatisticTraversalOperation.createCount();
            count.setStartStep(startTraversal.getEndStep());
            count.setEndStep(ConstantType.LONG.toString());
            count.setTraversalType("statistic");
            return count;
        } else if (ConstantType.isNumber(startTraversal.getEndStep())) {
            switch (Randomly.fromOptions(StatisticTraversalOperation.AggregateOperator.values())) {
                case sum:
                    StatisticTraversalOperation.Sum sum = StatisticTraversalOperation.createSum();
                    sum.setStartStep(startTraversal.getEndStep());
                    sum.setEndStep(startTraversal.getEndStep());
                    sum.setTraversalType("statistic");
                    return sum;
                case mean:
                    StatisticTraversalOperation.Mean mean = StatisticTraversalOperation.createMean();
                    mean.setStartStep(startTraversal.getEndStep());
                    mean.setEndStep(startTraversal.getEndStep());
                    mean.setTraversalType("statistic");
                    return mean;
                case max:
                    StatisticTraversalOperation.Max max = StatisticTraversalOperation.createMax();
                    max.setStartStep(startTraversal.getEndStep());
                    max.setEndStep(startTraversal.getEndStep());
                    max.setTraversalType("statistic");
                    return max;
                case min:
                    StatisticTraversalOperation.Min min = StatisticTraversalOperation.createMin();
                    min.setStartStep(startTraversal.getEndStep());
                    min.setEndStep(startTraversal.getEndStep());
                    min.setTraversalType("statistic");
                    return min;
                default:
                    throw new AssertionError();
            }
        } else {
            // max, min can be used for any object
            if (Randomly.getBoolean()) {
                StatisticTraversalOperation.Max max = StatisticTraversalOperation.createMax();
                max.setStartStep(startTraversal.getEndStep());
                max.setEndStep(startTraversal.getEndStep());
                max.setTraversalType("statistic");
                return max;
            } else {
                StatisticTraversalOperation.Min min = StatisticTraversalOperation.createMin();
                min.setStartStep(startTraversal.getEndStep());
                min.setEndStep(startTraversal.getEndStep());
                min.setTraversalType("statistic");
                return min;
            }
        }
    }

    public Traversal getRandomTraversal(Traversal startTraversal) {
        switch (Randomly.fromOptions(GraphFilterTraversal.values())) {
            case FILTER_TRAVERSAL:
                return createFilterTraversalOperation(startTraversal);
            case NEIGHBOR_TRAVERSAL:
                return createNeighborTraversalOperation(startTraversal);
            case PROPERTY:
                return createProperties(startTraversal);
            case STATISTIC:
                return createStatistic(startTraversal);
            // TODO: check
            case ORDER:
                return createOrder(startTraversal);
            case BRANCH:
                return createBranchTraversal(startTraversal);
            default:
                throw new AssertionError();
        }
    }

    public Traversal getIsPredicate(String type) {
        FilterTraversalOperation.IsPredicate isPredicate = FilterTraversalOperation.createIsPredicate(createPredicate(ConstantType.valueOf(type)));
        isPredicate.setStartStep(type);
        isPredicate.setEndStep(type);
        isPredicate.setTraversalType("filter");
        return isPredicate;
    }

    public OrderingTermOperation createOrder(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        String property = "null";
        // choose a property
        if (type.equals("vertex")) {
            property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties().getVertexPropertyName();
        } else if (type.equals("edge")){
            property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties().getEdgePropertyName();
        }
        switch (OrderingTermOperation.getRandom()) {
            case ASC:
                OrderingTermOperation.ASC asc = OrderingTermOperation.createASC();
                asc.setStartStep(type);
                asc.setEndStep(type);
                asc.setTraversalType("order");
                return asc;
            case DESC:
                OrderingTermOperation.DESC desc = OrderingTermOperation.createDESC();
                desc.setStartStep(type);
                desc.setEndStep(type);
                desc.setTraversalType("order");
                return desc;
            case DESCBYP:
                OrderingTermOperation.DESCByProperty descByProperty = OrderingTermOperation.createDESCBYP(property);
                descByProperty.setStartStep(type);
                descByProperty.setEndStep(type);
                descByProperty.setTraversalType("order");
                return descByProperty;
            case ASCBYP:
                OrderingTermOperation.ASCByProperty ascByProperty = OrderingTermOperation.createASCBYP(property);
                ascByProperty.setStartStep(type);
                ascByProperty.setEndStep(type);
                ascByProperty.setTraversalType("order");
                return ascByProperty;
            case ORDERBYP:
                OrderingTermOperation.OrderByProperty orderByProperty = OrderingTermOperation.createOrder(property);
                orderByProperty.setStartStep(type);
                orderByProperty.setEndStep(type);
                orderByProperty.setTraversalType("order");
                return orderByProperty;
            default:
                throw new AssertionError();
        }
    }


    public FilterTraversalOperation createFilterTraversalOperation(Traversal startTraversal) {
        Traversal t = getRandomTraversal(startTraversal);
        String type = startTraversal.getEndStep();
//        System.out.println("start type: " + type);
        if (type.equals("vertex") || type.equals("edge")) {
            switch (FilterTraversalOperation.getRandomFilter()) {
                case or:
                    FilterTraversalOperation.Or or = FilterTraversalOperation.createOr(t);
                    or.setStartStep(type);
                    or.setEndStep(type);
                    or.setTraversalType("filter");
                    or.setTraversalList(Arrays.asList(t));
                    return or;
                case and:
                    FilterTraversalOperation.And and = FilterTraversalOperation.createAnd(t);
                    and.setStartStep(type);
                    and.setEndStep(type);
                    and.setTraversalType("filter");
                    and.setTraversalList(Arrays.asList(t));
                    return and;
                case not:
                    FilterTraversalOperation.Not not = FilterTraversalOperation.createNot(t);
                    not.setStartStep(type);
                    not.setEndStep(type);
                    not.setTraversalType("filter");
                    not.setTraversalList(Arrays.asList(t));
                    return not;

                case where_traversal:
                    FilterTraversalOperation.WhereTraversal whereTraversal =
                            FilterTraversalOperation.createWhereTraversal(t);
                    whereTraversal.setStartStep(type);
                    whereTraversal.setEndStep(type);
                    whereTraversal.setTraversalType("filter");
                    whereTraversal.setTraversalList(Arrays.asList(t));
                    return whereTraversal;
                case has_key_predicate:
                    FilterTraversalOperation.HasKeyPredicate hasKeyPredicate = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        hasKeyPredicate =
                                FilterTraversalOperation.createHasKeyPredicate(property.getVertexPropertyName(), createPredicate(property.getDataType()));
                        hasKeyPredicate.getVertexProperties().add(property.getVertexPropertyName());
                    } else {
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        hasKeyPredicate =
                                FilterTraversalOperation.createHasKeyPredicate(property.getEdgePropertyName(), createPredicate(property.getDataType()));
                        hasKeyPredicate.getEdgeProperties().add(property.getEdgePropertyName());
                    }
                    hasKeyPredicate.setStartStep(type);
                    hasKeyPredicate.setEndStep(type);
                    hasKeyPredicate.setTraversalType("filter");
                    return hasKeyPredicate;
                case has_key_value:
                    FilterTraversalOperation.HasKeyValue hasKeyValue;
                    Boolean iterate;
                    do{
                        hasKeyValue = null;
                        iterate = false;
                        if (type.equals("vertex")) {
                            GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                            String vp = property.getVertexPropertyName();
                            List<GraphConstant> vlist = state.getGraphData().getVpValuesMap().get(vp);
                            hasKeyValue =
                                    FilterTraversalOperation.createHasKeyValue(vp, vlist.get(Randomly.getInteger(0, vlist.size())));
                            hasKeyValue.getVertexProperties().add(vp);
                        } else {
                            GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                            String ep = property.getEdgePropertyName();
                            List<GraphConstant> elist = state.getGraphData().getEpValueMap().get(ep);
                            try {
                                hasKeyValue =
                                        FilterTraversalOperation.createHasKeyValue(ep, elist.get(Randomly.getInteger(0, elist.size())));
                                hasKeyValue.getEdgeProperties().add(ep);
                            }catch (Exception e){
                                iterate = true;
                                continue;
                            }
                        }
                        hasKeyValue.setStartStep(type);
                        hasKeyValue.setEndStep(type);
                        hasKeyValue.setTraversalType("filter");
                    }while(iterate);

                    return hasKeyValue;
                /*case has_key_traversal:
                    FilterTraversalOperation.HasKeyTraversal hasKeyTraversal = null;
                    if(type.equals("vertex")){
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        t = getRandomTraversal(createProperties(startTraversal));
                        hasKeyTraversal = FilterTraversalOperation.createHasKeyTraversal(property.getVertexPropertyName(), t);
                    }else{
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        while(t.getTraversalType().equals("property")){
                            t = getRandomTraversal(startTraversal);
                        }
                        hasKeyTraversal = FilterTraversalOperation.createHasKeyTraversal(property.getEdgePropertyName(), t);
                    }
                    hasKeyTraversal.setStartStep(type);
                    hasKeyTraversal.setEndStep(type);
                    hasKeyTraversal.setTraversalType("filter");
                    return hasKeyTraversal;*/
                case has_label_key_value:
                    FilterTraversalOperation.HasLabelKeyPredicate hasLabelKeyPredicate = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexLabel label = Randomly.fromList(getVertexLabels());
                        GraphSchema.GraphVertexProperty property = label.getRandomVertexProperties();
                        hasLabelKeyPredicate =
                                FilterTraversalOperation.createHasLabelKeyPredicate(label.getLabelName(),
                                        property.getVertexPropertyName(), createPredicate(property.getDataType()));
                        hasLabelKeyPredicate.getVertexProperties().add(property.getVertexPropertyName());
                    } else {
                        GraphSchema.GraphRelationship label = Randomly.fromList(getEdgeLabels());
                        GraphSchema.GraphEdgeProperty property = label.getRandomEdgeProperties();
                        hasLabelKeyPredicate =
                                FilterTraversalOperation.createHasLabelKeyPredicate(label.getLabelName(),
                                        property.getEdgePropertyName(), createPredicate(property.getDataType()));
                        hasLabelKeyPredicate.getEdgeProperties().add(property.getEdgePropertyName());
                    }
                    hasLabelKeyPredicate.setStartStep(type);
                    hasLabelKeyPredicate.setEndStep(type);
                    hasLabelKeyPredicate.setTraversalType("filter");
                    return hasLabelKeyPredicate;
                case has:
                    FilterTraversalOperation.HasKey hasKey = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        hasKey = FilterTraversalOperation.
                                createHasKey(property.getVertexPropertyName());
                        hasKey.getVertexProperties().add(property.getVertexPropertyName());
                    } else {
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        hasKey = FilterTraversalOperation
                                .createHasKey(property.getEdgePropertyName());
                        hasKey.getEdgeProperties().add(property.getEdgePropertyName());
                    }
                    hasKey.setStartStep(type);
                    hasKey.setEndStep(type);
                    hasKey.setTraversalType("filter");
                    return hasKey;
                case hasNot:
                    FilterTraversalOperation.HasNotKey hasNotKey = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        hasNotKey = FilterTraversalOperation
                                .createHasNotKey(property.getVertexPropertyName());
                        hasNotKey.getVertexProperties().add(property.getVertexPropertyName());
                    } else {
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        hasNotKey = FilterTraversalOperation
                                .createHasNotKey(property.getEdgePropertyName());
                        hasNotKey.getEdgeProperties().add(property.getEdgePropertyName());
                    }
                    hasNotKey.setStartStep(type);
                    hasNotKey.setEndStep(type);
                    hasNotKey.setTraversalType("filter");
                    return hasNotKey;
                case hasLabel:
                    FilterTraversalOperation.HasLabel hasLabel = null;
                    if (type.equals("vertex")) {
                        hasLabel = FilterTraversalOperation.createHasLabel(getVertexLabelList());
                    } else {
                        hasLabel = FilterTraversalOperation.createHasLabel(getEdgeLabelList());
                    }
                    hasLabel.setStartStep(type);
                    hasLabel.setEndStep(type);
                    hasLabel.setTraversalType("filter");
                    return hasLabel;
                /*case hasId:
                    FilterTraversalOperation.HasId hasId = null;
                    if(type.equals("vertex")){
                        hasId = FilterTraversalOperation.createHasId(getVertexIds());
                    }else{
                        hasId = FilterTraversalOperation.createHasId(getEdgeIds());
                    }
                    hasId.setStartStep(type);
                    hasId.setEndStep(type);
                    hasId.setTraversalType("filter");
                    return hasId;*/
                case where_count_is:
                    NeighborTraversalOperation neighbor = type.equals("vertex") ? createVertexNeighbor(type) : createEdgeNeighbor(startTraversal);
                    FilterTraversalOperation.WhereCountIs whereCountIs =
                            FilterTraversalOperation.createWhereCountIs((FilterTraversalOperation.IsPredicate) getIsPredicate(ConstantType.LONG.toString()),
                                    neighbor, (StatisticTraversalOperation.Count) createStatistic(startTraversal));
                    whereCountIs.setStartStep(type);
                    whereCountIs.setEndStep(type);
                    whereCountIs.setTraversalType("filter");
                    return whereCountIs;
                case where_value_is:
                    NeighborTraversalOperation neighbor1 = type.equals("vertex") ? createVertexNeighbor(type) : createEdgeNeighbor(startTraversal);
                    FilterTraversalOperation.WhereValuesIs whereValuesIs = null;
                    if (type.equals("vertex")) {
                        EdgePropertyReference edgePropertyReference = (EdgePropertyReference) getEdgeProperty(type);
                        whereValuesIs =
                                FilterTraversalOperation.createWhereValueIs((FilterTraversalOperation.IsPredicate) getIsPredicate(edgePropertyReference.getDataType().toString()),
                                        neighbor1, edgePropertyReference);
                    } else if (type.equals("edge")) {
                        VertexPropertyReference vertexPropertyReference = (VertexPropertyReference) getVertexProperty(type);
                        whereValuesIs =
                                FilterTraversalOperation.createWhereValueIs((FilterTraversalOperation.IsPredicate) getIsPredicate(vertexPropertyReference.getDataType().toString()),
                                        neighbor1, vertexPropertyReference);
                    }
                    whereValuesIs.setStartStep(type);
                    whereValuesIs.setEndStep(type);
                    whereValuesIs.setTraversalType("filter");
                    return whereValuesIs;
                case where_statistic_is:
                    StatisticTraversalOperation statistic = null;
                    FilterTraversalOperation.IsPredicate isPredicate = null;
                    PropertyTraversalOperation.Values properties = null;
                    ConstantType constantType =
                            (ConstantType) (type.equals("vertex")
                                    ? state.getSchema().getVertexPropertyMap().keySet().toArray()[Randomly.getInteger(0, state.getSchema().getVertexPropertyMap().size() - 1)]
                                    : state.getSchema().getEdgePropertyMap().keySet().toArray()[Randomly.getInteger(0, state.getSchema().getEdgePropertyMap().size() - 1)]);
                    if (type.equals("vertex")) {
                        List list = state.getSchema().getVertexPropertyMap().get(constantType);
                        GraphSchema.GraphVertexProperty property = (GraphSchema.GraphVertexProperty) list.get(Randomly.getInteger(0, list.size() - 1));
                        properties = new PropertyTraversalOperation.Values(Arrays.asList(property.getVertexPropertyName()));
                    } else {
                        List list = state.getSchema().getEdgePropertyMap().get(constantType);
                        GraphSchema.GraphEdgeProperty property = (GraphSchema.GraphEdgeProperty) list.get(Randomly.getInteger(0, list.size() - 1));
                        properties = new PropertyTraversalOperation.Values(Arrays.asList(property.getEdgePropertyName()));
                    }
                    if (ConstantType.isNumber(constantType.toString())) {
                        switch (Randomly.fromOptions(StatisticTraversalOperation.AggregateOperator.values())) {
                            case sum:
                                statistic = StatisticTraversalOperation.createSum();
                                break;
                            case mean:
                                statistic = StatisticTraversalOperation.createMean();
                                break;
                            case max:
                                statistic = StatisticTraversalOperation.createMax();
                                break;
                            case min:
                                statistic = StatisticTraversalOperation.createMin();
                                break;
                        }
                    } else {
                        statistic = Randomly.getBoolean() ? StatisticTraversalOperation.createMax() : StatisticTraversalOperation.createMin();
                    }
                    isPredicate = (FilterTraversalOperation.IsPredicate) getIsPredicate(constantType.toString());
                    FilterTraversalOperation.WhereStatisticIs whereStatisticIs = FilterTraversalOperation.createWhereStatisticIs(isPredicate, properties, statistic);
                    List<String> keys = properties.getPropertyKeys();
                    if(type.equals("vertex")){
                        for(String key : keys){
                            whereStatisticIs.getVertexProperties().add(key);
                        }
                    }else{
                        for(String key : keys){
                            whereStatisticIs.getEdgeProperties().add(key);
                        }
                    }
                    whereStatisticIs.setStartStep(type);
                    whereStatisticIs.setEndStep(type);
                    whereStatisticIs.setTraversalType("filter");

                    return whereStatisticIs;
//                case limit:
//                    FilterTraversalOperation.Limit limit = FilterTraversalOperation.createLimit((int) state.getRandomly().getInteger(200));
//                    limit.setStartStep(type);
//                    limit.setEndStep(type);
//                    limit.setTraversalType("filter");
//                    return limit;
//                case range:
//                    int low = (int) state.getRandomly().getInteger(100);
//                    FilterTraversalOperation.Range range = FilterTraversalOperation.createRange(low, low + (int) state.getRandomly().getInteger(100));
//                    range.setStartStep(type);
//                    range.setEndStep(type);
//                    range.setTraversalType("filter");
//                    return range;
//                case tail:
//                    FilterTraversalOperation.Tail tail = FilterTraversalOperation.createTail((int) state.getRandomly().getInteger(200));
//                    tail.setStartStep(type);
//                    tail.setEndStep(type);
//                    tail.setTraversalType("filter");
//                    return tail;
                default:
                    throw new AssertionError();
            }
        } else {
            return null;
        }
    }

    public Predicate createPredicate(ConstantType type) {
        switch (Predicate.getRandomPredicate()) {
            case eq:
                return Predicate.createEq(generateConstant(type));
            case neq:
                return Predicate.createNeq(generateConstant(type));
            case lt:
                return Predicate.createLt(generateConstant(type));
            case lte:
                return Predicate.createLte(generateConstant(type));
            case gt:
                return Predicate.createGt(generateConstant(type));
            case gte:
                return Predicate.createGte(generateConstant(type));
            case inside:
                return Predicate.createInside(generateConstant(type),
                        generateConstant(type));
            case outside:
                return Predicate.createOutside(generateConstant(type),
                        generateConstant(type));
            case between:
                return Predicate.createBetween(generateConstant(type),
                        generateConstant(type));
            case not:
                return Predicate.createNot(createPredicate(type));
            case and:
                return Predicate.createAnd(createPredicate(type), createPredicate(type));
            case or:
                return Predicate.createOr(createPredicate(type), createPredicate(type));
            default:
                throw new AssertionError();
        }
    }

    public NeighborTraversalOperation createVertexNeighbor(String type) {
        switch (Randomly.fromOptions(NeighborTraversalOperation.NeighborV.values())) {
            case out:
                NeighborTraversalOperation.Out out = NeighborTraversalOperation.createOut(getEdgeLabelList());
                out.setStartStep(type);
                out.setEndStep("vertex");
                out.setTraversalType("neighbor");
                return out;
            case in:
                NeighborTraversalOperation.In in = NeighborTraversalOperation.createIn(getEdgeLabelList());
                in.setStartStep(type);
                in.setEndStep("vertex");
                in.setTraversalType("neighbor");
                return in;
            case both:
                NeighborTraversalOperation.Both both = NeighborTraversalOperation.createBoth(getEdgeLabelList());
                both.setStartStep(type);
                both.setEndStep("vertex");
                both.setTraversalType("neighbor");
                return both;
            case outE:
                NeighborTraversalOperation.OutE outE = NeighborTraversalOperation.createOutE(getEdgeLabelList());
                outE.setStartStep(type);
                outE.setEndStep("edge");
                outE.setTraversalType("neighbor");
                return outE;
            case inE:
                NeighborTraversalOperation.InE inE = NeighborTraversalOperation.createInE(getEdgeLabelList());
                inE.setStartStep(type);
                inE.setEndStep("edge");
                inE.setTraversalType("neighbor");
                return inE;
            case bothE:
                NeighborTraversalOperation.BothE bothE = NeighborTraversalOperation.createBothE(getEdgeLabelList());
                bothE.setStartStep(type);
                bothE.setEndStep("edge");
                bothE.setTraversalType("neighbor");
                return bothE;
            default:
                throw new AssertionError();
        }
    }

    public NeighborTraversalOperation createEdgeNeighbor(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        switch (Randomly.fromOptions(NeighborTraversalOperation.NeighborE.values())) {
            case outV:
                NeighborTraversalOperation.OutV outV = NeighborTraversalOperation.createOutV();
                outV.setStartStep(type);
                outV.setEndStep("vertex");
                outV.setTraversalType("neighbor");
                return outV;
            case inV:
                NeighborTraversalOperation.InV inV = NeighborTraversalOperation.createInV();
                inV.setStartStep(type);
                inV.setEndStep("vertex");
                inV.setTraversalType("neighbor");
                return inV;
            case bothV:
                NeighborTraversalOperation.BothV bothV = NeighborTraversalOperation.createBothV();
                bothV.setStartStep(type);
                bothV.setEndStep("vertex");
                bothV.setTraversalType("neighbor");
                return bothV;
//            case otherV:
//                // g.E().otherV() is not true
//                if(startTraversal.getTraversalType().equals("E")) {
//                    return createNeighborTraversalOperation(startTraversal);
//                }
//                NeighborTraversalOperation.OtherV otherV = NeighborTraversalOperation.createOtherV();
//                otherV.setStartStep(type);
//                otherV.setEndStep("vertex");
//                otherV.setTraversalType("neighbor");
//                return otherV;
            default:
                throw new AssertionError();
        }
    }

    public NeighborTraversalOperation createNeighborTraversalOperation(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (type.equals("vertex")) {
            return createVertexNeighbor(type);
        } else if (type.equals("edge")) {
            return createEdgeNeighbor(startTraversal);
        } else {
            return null;
        }
    }

    public List<GraphSchema.GraphVertexLabel> getVertexLabels() {
        return Randomly.nonEmptySubList(state.getSchema().getVertexList());
    }

    public List<GraphSchema.GraphRelationship> getEdgeLabels() {
        return Randomly.nonEmptySubList(state.getSchema().getEdgeList());
    }

    public List<String> getVertexLabelList() {
        return Randomly.nonEmptySubList(state.getGraphData().getVertexLabels());
    }

    public List<String> getEdgeLabelList() {
        return Randomly.nonEmptySubList(state.getGraphData().getEdgeLabels());
    }

    public List<Object> getVertexIds() {
        return Randomly.nonEmptySubList(state.getConnection().getG().V().id().toList());
    }

    public List<Object> getEdgeIds() {
        return Randomly.nonEmptySubList(state.getConnection().getG().E().id().toList());
    }

    @Override
    public GraphExpression generateExpression(int depth) {
        if (depth >= state.getGenerateDepth()) return generateLeafNode();
        // randomly generate actions
        switch (Randomly.fromOptions(Actions.values())) {
            //TODO: edge_property
            case VERTEX_PROPERTY:
                System.out.println("VERTEX_PROPERTY : " + depth);
                return generateProperties();
            case LITERAL:
                System.out.println("LITERAL : " + depth);
                GraphExpression d = generateConstant();
                System.out.println(d.toString());
                return d;
            /*case UNARY_PREFIX:
                System.out.println("UNARY_PREFIX : " + depth);
                UnaryPrefixOperation a = new UnaryPrefixOperation(generateExpression(depth + 1), UnaryPrefixOperation.EmunUnaryPrefixOperator.getRandom());
                System.out.println(a.toString());
                return a;*/
            case BINARY_COMPARISON:
                System.out.println("BINARY_COMPARISON : " + depth);
                BinaryComparisonOperation b = new BinaryComparisonOperation(null, generateExpression(depth + 1),
                        BinaryComparisonOperation.EnumBinaryComparisonOperator.getRandom());
                System.out.println(b.toString());
                return b;
            case BINARY_LOGICAL:
                System.out.println("BINARY_LOGICAL : " + depth);
                BinaryLogicalOperation c = new BinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                        BinaryLogicalOperation.EnumBinaryLogicalOperator.getRandom());
                System.out.println(c.toString());
                return c;
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected GraphExpression generateProperties() {
        return null;
    }

    public GraphConstant generateConstant(ConstantType type) {
        switch (type) {
            case INTEGER:
                int int_value = (int) state.getRandomly().getInteger();
                return GraphConstant.createIntConstant(int_value);
            /*case NULL:
                return GraphConstant.createNullConstant();*/
            case STRING:
//                String string_value = state.getRandomly().getString();
                String string_value = RandomStringUtils.randomAlphanumeric(10);
                return GraphConstant.createStringConstant(string_value);
            case DOUBLE:
                double double_value = state.getRandomly().getDouble();
                return GraphConstant.createDoubleConstant(double_value);
            case BOOLEAN:
                boolean boolean_value = state.getRandomly().getBoolean();
                return GraphConstant.createBooleanConstant(boolean_value);
            case FLOAT:
                return GraphConstant.createFloatConstant(state.getRandomly().getFloat());
            case LONG:
                return GraphConstant.createLongConstant(state.getRandomly().getLong());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public Traversal generateConstant() {
        ConstantType type = Randomly.fromOptions(ConstantType.values());
        return generateConstant(type);
    }


    public Traversal getVertexProperty(String type) {
        //randomly choose one property
        GraphSchema.GraphVertexProperty property = null;
        //get the value of this property
        VertexPropertyReference vertexProperty = null;
        while (vertexProperty == null) {
            property = Randomly.fromList(state.getSchema().getVertexProperties());
            vertexProperty = getVertexPropertyValue(property);
        }
        vertexProperty.setStartStep(type);
        vertexProperty.setEndStep(property.getDataType().toString());
        vertexProperty.setDataType(property.getDataType());
        vertexProperty.setTraversalType("property");
        return vertexProperty;
    }

    public Traversal getEdgeProperty(String type) {
        EdgePropertyReference edgeProperty = null;
        GraphSchema.GraphEdgeProperty property = null;
        while (edgeProperty == null) {
            property = Randomly.fromList(state.getSchema().getEdgeProperties());
            edgeProperty = getEdgePropertyValue(property);
        }
        edgeProperty.setStartStep(type);
        edgeProperty.setEndStep(property.getDataType().toString());
        edgeProperty.setDataType(property.getDataType());
        edgeProperty.setTraversalType("property");
        return edgeProperty;
    }

    /**
     * Generate property of a certain vertex label
     *
     * @return
     */
    protected Traversal createProperties(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (type.equals("vertex")) {
            return getVertexProperty(type);
        } else if (type.equals("edge")) {
            return getEdgeProperty(type);
        } else {
            return null;
        }
    }

    public EdgePropertyReference getEdgePropertyValue(GraphSchema.GraphEdgeProperty property) {
        ConstantType type = property.getDataType();
        List list = state.getGraphData().getEdgeProperties().get(property.getEdgePropertyName());
        if (list == null || list.size() == 0) return null;
        Object o = Randomly.fromList(list);
        switch (type) {
            case INTEGER:
                return EdgePropertyReference.create(property, new GraphConstant.GraphIntConstant(Long.valueOf(o.toString())));
            case DOUBLE:
                return EdgePropertyReference.create(property, new GraphConstant.GraphDoubleConstant(Double.valueOf(o.toString())));
            case BOOLEAN:
                return EdgePropertyReference.create(property, new GraphConstant.GraphBooleanConstant(Boolean.valueOf(o.toString())));
            case FLOAT:
                return EdgePropertyReference.create(property, new GraphConstant.GraphFloatConstant(Float.valueOf(o.toString())));
            case LONG:
                return EdgePropertyReference.create(property, new GraphConstant.GraphLongConstant(Long.valueOf(o.toString())));
            /*case NULL:
                return EdgePropertyReference.create(property, new GraphConstant.GraphNullConstant());*/
            default:
                return EdgePropertyReference.create(property, new GraphConstant.GraphStringConstant(o.toString()));
        }
    }

    //INTEGER, NULL, STRING, DOUBLE, BOOLEAN, FLOAT, LONG;
    public VertexPropertyReference getVertexPropertyValue(GraphSchema.GraphVertexProperty property) {
        ConstantType type = property.getDataType();
        List list = state.getGraphData().getVertexProperties().get(property.getVertexPropertyName());
        if (list == null || list.size() == 0) return null;
        Object o = Randomly.fromList(list);
        switch (type) {
            case INTEGER:
                return VertexPropertyReference.create(property, new GraphConstant.GraphIntConstant(Long.valueOf(o.toString())));
            case DOUBLE:
                return VertexPropertyReference.create(property, new GraphConstant.GraphDoubleConstant(Double.valueOf(o.toString())));
            case BOOLEAN:
                return VertexPropertyReference.create(property, new GraphConstant.GraphBooleanConstant(Boolean.valueOf(o.toString())));
            case FLOAT:
                return VertexPropertyReference.create(property, new GraphConstant.GraphFloatConstant(Float.valueOf(o.toString())));
            case LONG:
                return VertexPropertyReference.create(property, new GraphConstant.GraphLongConstant(Long.valueOf(o.toString())));
            /*case NULL:
                return VertexPropertyReference.create(property, new GraphConstant.GraphNullConstant());*/
            default:
                return VertexPropertyReference.create(property, new GraphConstant.GraphStringConstant(o.toString()));
        }
    }


    @Override
    public GraphExpression negatePredicate(GraphExpression predicate) {
        return null;
    }

    @Override
    public GraphExpression isNull(GraphExpression expr) {
        return null;
    }

    public Map<String, List<GraphSchema.GraphVertexLabel>> getOutVertexLabelMap() {
        return outVertexLabelMap;
    }

    public void setOutVertexLabelMap(Map<String, List<GraphSchema.GraphVertexLabel>> outVertexLabelMap) {
        this.outVertexLabelMap = outVertexLabelMap;
    }

    public Map<String, List<GraphSchema.GraphVertexLabel>> getInVertexLabelMap() {
        return inVertexLabelMap;
    }

    public void setInVertexLabelMap(Map<String, List<GraphSchema.GraphVertexLabel>> inVertexLabelMap) {
        this.inVertexLabelMap = inVertexLabelMap;
    }


}
