package org.gdbtesting.gremlin;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StrategyLoader {

    private List<String> tinkerClosedStrategy = new ArrayList<>();
    private List<String> janusClosedStrategy = new ArrayList<>();
    private List<String> hugeClosedStrategy = new ArrayList<>();
    private List<String> orientClosedStrategy = new ArrayList<>();
    private List<String> arcadeClosedStrategy = new ArrayList<>();
    private List<String> neo4jClosedStrategy = new ArrayList<>();

    public List<StrategyInfo> loadTinkerGraphStrategyInfo(Map<String, Integer> historyStrategies) {
        List<StrategyInfo> tinkergraphInfo = new ArrayList<>();
        for (int i = 0; i < Strategies.gremlinStrategy.length; i++) {
            String s = Strategies.gremlinStrategy[i];
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, null);
            if (s.equals("OrderLimitStrategy") || s.equals("PathProcessorStrategy") || s.equals("ProductiveByStrategy")) {
                strategyInfo.setOpen(0);
                tinkerClosedStrategy.add(s);
            }
            if (s.equals("RepeatUnrollStrategy") || s.equals("PathRetractionStrategy") || s.equals("InlineFilterStrategy")) {
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
            }
            tinkergraphInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        for (int i = 0; i < Strategies.tinkergraphStrategy.length; i++) {
            String s = Strategies.tinkergraphStrategy[i];
            String importClass = ImportClass.TINKER_STRATEGY_IMPORT + s + "; ";
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, importClass);
            tinkergraphInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        return tinkergraphInfo;
    }

    public List<StrategyInfo> loadNeo4jStrategyInfo(Map<String, Integer> historyStrategies) {
        List<StrategyInfo> neo4jInfo = new ArrayList<>();
        for (int i = 0; i < Strategies.gremlinStrategy.length; i++) {
            String s = Strategies.gremlinStrategy[i];
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, null);
            if (s.equals("OrderLimitStrategy") || s.equals("PathProcessorStrategy") || s.equals("ProductiveByStrategy")) {
                strategyInfo.setOpen(0);
                neo4jClosedStrategy.add(s);
            }
            if (s.equals("RepeatUnrollStrategy") || s.equals("PathRetractionStrategy") || s.equals("InlineFilterStrategy")) {
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
            }
            neo4jInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        return neo4jInfo;
    }

    public List<StrategyInfo> loadJanusGraphStrategyInfo(Map<String, Integer> historyStrategies) {
        List<StrategyInfo> janusgraphInfo = new ArrayList<>();
        for (int i = 0; i < Strategies.gremlinStrategy.length; i++) {
            String s = Strategies.gremlinStrategy[i];
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, null);
            if(s.equals("OrderLimitStrategy") || s.equals("PathProcessorStrategy")){
                strategyInfo.setOpen(0);
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
                janusClosedStrategy.add(s);
            }
            if(s.equals("RepeatUnrollStrategy") || s.equals("PathRetractionStrategy") || s.equals("InlineFilterStrategy")){
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
            }
            if(s.equals("ProductiveByStrategy")){
                strategyInfo.setOpen(-1);
            }
            janusgraphInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        for (int i = 0; i < Strategies.janusgraphStrategy.length; i++) {
            String s = Strategies.janusgraphStrategy[i];
            String importClass = ImportClass.JANUS_STRATEGY_IMPORT + s + "; ";
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, importClass);
            janusgraphInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        return janusgraphInfo;
    }

    public List<StrategyInfo> loadHugeGraphStrategyInfo(Map<String, Integer> historyStrategies) {
        List<StrategyInfo> hugegraphInfo = new ArrayList<>();
        for (int i = 0; i < Strategies.gremlinStrategy.length; i++) {
            String s = Strategies.gremlinStrategy[i];
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, null);
            if(s.equals("IdentityRemovalStrategy") || s.equals("PathProcessorStrategy") || s.equals("OrderLimitStrategy")){
                strategyInfo.setOpen(0);
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
                hugeClosedStrategy.add(s);
            }
            if(s.equals("RepeatUnrollStrategy") || s.equals("PathRetractionStrategy") || s.equals("InlineFilterStrategy")){
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
            }
            if(s.equals("ByModulatorOptimizationStrategy")){
                strategyInfo.setOpen(-1);
            }
            hugegraphInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        for (int i = 0; i < Strategies.hugegraphStrategy.length; i++) {
            String s = Strategies.hugegraphStrategy[i];
            String importClass = ImportClass.HUGE_STRATEGY_IMPORT + s + "; ";
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, importClass);
            hugegraphInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        return hugegraphInfo;
    }

    public List<StrategyInfo> loadOrientDBStrategyInfo(Map<String, Integer> historyStrategies) {
        List<StrategyInfo> orientdbInfo = new ArrayList<>();
        for (int i = 0; i < Strategies.gremlinStrategy.length; i++) {
            String s = Strategies.gremlinStrategy[i];
            if(s.equals("ByModulatorOptimizationStrategy")){
                continue;
            }
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, null);
            if(s.equals("IdentityRemovalStrategy") || s.equals("PathProcessorStrategy") || s.equals("OrderLimitStrategy")){
                strategyInfo.setOpen(0);
                orientClosedStrategy.add(s);
            }
            if(s.equals("RepeatUnrollStrategy") || s.equals("PathRetractionStrategy") || s.equals("InlineFilterStrategy")){
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
            }


            orientdbInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        for (int i = 0; i < Strategies.orientdbStrategy.length; i++) {
            String s = Strategies.orientdbStrategy[i];
            String importClass = ImportClass.ORIENT_STRATEGY_IMPORT + s + "; ";
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, importClass);
            orientdbInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        return orientdbInfo;
    }


    public List<StrategyInfo> loadArcadeDBStrategyInfo(Map<String, Integer> historyStrategies) {
        List<StrategyInfo> arcadedbInfo = new ArrayList<>();
        for (int i = 0; i < Strategies.gremlinStrategy.length; i++) {
            String s = Strategies.gremlinStrategy[i];
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, null);
            if(s.equals("IdentityRemovalStrategy") || s.equals("PathProcessorStrategy") || s.equals("OrderLimitStrategy")){
                strategyInfo.setOpen(0);
                arcadeClosedStrategy.add(s);
            }
            if(s.equals("RepeatUnrollStrategy") || s.equals("PathRetractionStrategy") || s.equals("InlineFilterStrategy")){
                strategyInfo.setImportClass(ImportClass.GREMLIN_STRATEGY_IMPORT + s + "; ");
            }
            arcadedbInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        for (int i = 0; i < Strategies.arcadedbStrategy.length; i++) {
            String s = Strategies.arcadedbStrategy[i];
            String importClass = ImportClass.ARCADE_STRATEGY_IMPORT + s + "; ";
            StrategyInfo strategyInfo = new StrategyInfo(s, 1, importClass);
            arcadedbInfo.add(strategyInfo);
            historyStrategies.put(s, 0);
        }
        return arcadedbInfo;
    }

    public List<String> getTinkerClosedStrategy() {
        return tinkerClosedStrategy;
    }

    public List<String> getJanusClosedStrategy() {
        return janusClosedStrategy;
    }

    public List<String> getHugeClosedStrategy() {
        return hugeClosedStrategy;
    }

    public List<String> getOrientClosedStrategy() {
        return orientClosedStrategy;
    }

    public List<String> getArcadeClosedStrategy() {
        return arcadeClosedStrategy;
    }

    public List<String> getNeo4jClosedStrategy() {
        return neo4jClosedStrategy;
    }
}
