package org.gdbtesting.gremlin;

public class Strategies {

    // remove "ProductiveByStrategy", "LazyBarrierStrategy"
    public static String[] gremlinStrategy = {"AdjacentToIncidentStrategy", "ByModulatorOptimizationStrategy", "CountStrategy",
            "EarlyLimitStrategy", "FilterRankingStrategy", "IdentityRemovalStrategy", "IncidentToAdjacentStrategy",
            "InlineFilterStrategy", "MatchPredicateStrategy", "OrderLimitStrategy",
            "PathProcessorStrategy", "PathRetractionStrategy", "RepeatUnrollStrategy"};

    // HugeGraph unable to resolve "ByModulatorOptimizationStrategy"
    // HugeGraph disables IdentityRemovalStrategy, PathProcessorStrategy and OrderLimitStrategy by default
    public static String[] hugegraphStrategy = {"HugeGraphStepStrategy", "HugeCountStepStrategy", "HugeVertexStepStrategy"};

    // JanusGraph unable to resolve JanusGraphMixedIndexAggStrategy, AdjacentVertexOptimizerStrategy, so we remove them
    // JanusGraph disables OrderLimitStrategy and PathProcessorStrategy by default
    public static String[] janusgraphStrategy = {"AdjacentVertexFilterOptimizerStrategy", "AdjacentVertexHasIdOptimizerStrategy",
            "AdjacentVertexHasUniquePropertyOptimizerStrategy", "AdjacentVertexIsOptimizerStrategy",
            "JanusGraphIoRegistrationStrategy", "JanusGraphLocalQueryOptimizerStrategy",
            "JanusGraphMixedIndexCountStrategy", "JanusGraphMultiQueryStrategy", "JanusGraphStepStrategy"};

    // TinkerGraph disables OrderLimitStrategy, PathProcessorStrategy and ProductiveByStrategy by default
    public static String[] tinkergraphStrategy = {"TinkerGraphCountStrategy", "TinkerGraphStepStrategy"};

    // unable to resolve ByModulatorOptimizationStrategy and ProductiveByStrategy
    public static String[] orientdbStrategy = {"OrientGraphCountStrategy", "OrientGraphMatchStepStrategy", "OrientGraphStepStrategy"};

    // disable disables OrderLimitStrategy, PathProcessorStrategy and ProductiveByStrategy by default
    // ArcadeTraversalStrategy is acted as OptimizationStrategy in ArcadeDB
    public static String[] arcadedbStrategy = {"ArcadeIoRegistrationStrategy", "ArcadeTraversalStrategy"};

//    public static String[] neo4jStrategy = {"Neo4jGraphStepStrategy"};
    public static String[] neo4jStrategy = {};
}
