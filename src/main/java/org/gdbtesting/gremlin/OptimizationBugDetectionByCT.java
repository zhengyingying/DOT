package org.gdbtesting.gremlin;

import afu.org.checkerframework.checker.oigj.qual.O;
import org.gdbtesting.Randomly;
import org.gdbtesting.connection.GremlinConnection;

import java.util.*;

public class OptimizationBugDetectionByCT {

    private Map<String, StrategyInfo> infos = null;
    private List<StrategyInfo> infoList = null;
    private Map<String, Integer> historyUsedStrategies = new HashMap<>();
    private Map<String, Double> pMap = new HashMap<>();

    private Map<String, Integer> usedStrategies = new HashMap<>();
    private Map<String, Integer> selectedStrategies = new HashMap<>();
    private Map<String, Integer> selectAndUsedStrategies = new HashMap<>();

    private int randM = 5;
    private int exitM = 1000;

    public List<String> getNewUsed() {
        return newUsed;
    }

    // whether we create a new database
    private List<String> newUsed = new ArrayList<>();
    private boolean newGDB = false;
    private int noNewCount = 0;

    public int getEffTestCount() {
        return effTestCount;
    }

    // statistic effective test cases
    private int effTestCount = 0;

    public boolean isNewGDB() {
        return newGDB;
    }

    public void setNewGDB(boolean newGDB) {
        this.newGDB = newGDB;
    }

    public Map<String, Integer> getUsedStrategies() {
        return usedStrategies;
    }

    public Map<String, Integer> getSelectedStrategies() {
        return selectedStrategies;
    }

    public Map<String, Integer> getSelectAndUsedStrategies() {
        return selectAndUsedStrategies;
    }

    private List<String> closedStrategy;

    private int usedNum = 0;

    public OptimizationBugDetectionByCT(){}

    public OptimizationBugDetectionByCT(GremlinConnection connection) {
        StrategyLoader loader = new StrategyLoader();
        if(connection.getDatabase().equals("TinkerGraph")){
            infoList = loader.loadTinkerGraphStrategyInfo(historyUsedStrategies);
            closedStrategy = loader.getTinkerClosedStrategy();
        } else if (connection.getDatabase().equals("JanusGraph")){
            infoList = loader.loadJanusGraphStrategyInfo(historyUsedStrategies);
            closedStrategy = loader.getJanusClosedStrategy();
        } else if (connection.getDatabase().equals("HugeGraph")){
            infoList = loader.loadHugeGraphStrategyInfo(historyUsedStrategies);
            closedStrategy = loader.getJanusClosedStrategy();
        } else if (connection.getDatabase().equals("OrientDB")){
            infoList = loader.loadOrientDBStrategyInfo(historyUsedStrategies);
            closedStrategy = loader.getOrientClosedStrategy();
        } else if (connection.getDatabase().equals("ArcadeDB")){
            infoList = loader.loadArcadeDBStrategyInfo(historyUsedStrategies);
            closedStrategy = loader.getArcadeClosedStrategy();
        } else if (connection.getDatabase().equals("Neo4j")){
            infoList = loader.loadNeo4jStrategyInfo(historyUsedStrategies);
            closedStrategy = loader.getNeo4jClosedStrategy();
        }
        infos = getStrategyInfoMap(infoList);
        for(String s : infos.keySet()){
            usedStrategies.put(s, 0);
            selectedStrategies.put(s, 0);
            selectAndUsedStrategies.put(s, 0);
        }
    }

    public int[] randomConfig(int length) {
        int[] configList = new int[length];
        for (int i = 0; i < configList.length; i++) {
            configList[i] = Randomly.getBoolean() ? 1 : 0;
        }
        return configList;
    }

    public List<String> getUsedOptCombination() {
        return usedOptCombination;
    }

    public void setUsedOptCombination(List<String> usedOptCombination) {
        this.usedOptCombination = usedOptCombination;
    }

    private List<String> usedOptCombination;

    public boolean newQuery(List<String> used){
        if(used.size() == 0) return false;
        StringBuilder s = new StringBuilder();
        for(String o : used){
            s.append(o).append(",");
        }
        if(usedOptCombination.contains(s.substring(0, s.length()-1))){
            return true;
        }else{
            usedOptCombination.add(s.substring(0,s.length()-1));
            return false;
        }
    }

    public void countUsed(int size){
        if(usedCountStatis.containsKey(size)){
            int c = usedCountStatis.get(size)+1;
            usedCountStatis.put(size, c);
        }else{
            usedCountStatis.put(size, 1);
        }
    }

    // random select random number strategy
    public List<String> pairwiseCombineUsedStrategy(GremlinConnection connection, String query, List<String> used, List<List<String>> selected){
//        System.out.println("=======================Query================================");
        List<String> configs = new ArrayList<>();
        List<String> importClass = new ArrayList<>();
        List<String> newQueries = new ArrayList<>();
        iterateOpenStrategy(closedStrategy, configs, importClass);
        String allopenQuery = getNewQuery(configs, importClass, query);
//        System.out.println("allopenQuery: " + allopenQuery);
        List<String> oneSelect;
        // get all used strategies
        used = getExecutionFeedback(connection, allopenQuery);
        // sort used strategies
        Collections.sort(used);
//        System.out.println(used);
        // count the number of used strategies
        countUsed(used.size());
//        System.out.println("all used strategies: ");
//        printStrategy(used);

        // test this query or generate a new query?
//        newQuery(used);
        if(newQuery(used)){
            return newQueries;
        }
         // generate a new gdb?
        if(noNewUsed(used)){
            newGDB = true;
            return newQueries;
        }

        if(used.size() ==0 ){
            // encounter exception, we random select a random number of optimization strategies
            int rand = randM;
            while(rand > 0){
                configs = new ArrayList<>();
                importClass = new ArrayList<>();
                oneSelect = new ArrayList<>();
                int selectCount = Randomly.getInteger(2,4);
                while(selectCount > 0 ){
                    int index = Randomly.getInteger(0, infoList.size());
                    if(!oneSelect.contains(infoList.get(index).getStrategy())){
                        oneSelect.add(infoList.get(index).getStrategy());
                        selectCount--;
                    }
                }
                selected.add(oneSelect);
                iterateStrategy(oneSelect, configs, importClass);
                String newQuery = getNewQuery(configs, importClass, query);
                newQueries.add(newQuery);
                rand--;
            }
        } else if(used.size() == 1){
            configs = new ArrayList<>();
            importClass = new ArrayList<>();
            oneSelect = new ArrayList<>();
            oneSelect.add(used.get(0));
            selected.add(oneSelect);
            iterateStrategy(oneSelect, configs, importClass);
            String newQuery = getNewQuery(configs, importClass, query);
            newQueries.add(newQuery);
        } else {
            // pairwise combination
            List<List<Integer>> cases = pairwise(used.size());
            for(List<Integer> one : cases){
                configs = new ArrayList<>();
                importClass = new ArrayList<>();
                oneSelect = new ArrayList<>();
                // add used strategy
                for(int i = 0; i < one.size(); i++){
                    if(one.get(i) == 1){
                        oneSelect.add(used.get(i));
                    }
                }
                selected.add(oneSelect);
                iterateStrategy(oneSelect, configs, importClass);
                String newQuery = getNewQuery(configs, importClass, query);
                newQueries.add(newQuery);
            }
        }
        return newQueries;
    }

    private Map<Integer, List<List<Integer>>> pairwiseCases = new HashMap<>();
    private Map<Integer, List<List<Integer>>> cartesianCases = new HashMap<>();

    // pairwise
    public List<List<Integer>> pairwise(int n){
        if(pairwiseCases.keySet().contains(n)){
            return pairwiseCases.get(n);
        }
        List<List<Integer>> cp = cartesianProduct(n);
        List<List<List<Integer>>> s = split(cp);

        List<Integer> delRow = new ArrayList<>();
        List<List<List<Integer>>> s2 = new ArrayList<>(s);
        for (int i = 0; i < s.size(); i++) {
            int t = 0;
            for (int j = 0; j < s.get(i).size(); j++) {
                boolean flag = false;
                for (int i2 : range(0, s2.size())) {
                    if (!s2.get(i2).equals(s.get(i)) && s.get(i).get(j).equals(s2.get(i2).get(j))) {
                        t += 1;
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    break;
                }
            }
            if (t == s.get(i).size()) {
                delRow.add(i);
                s2.remove(s.get(i));
            }
        }

        List<List<Integer>> res = new ArrayList<>();
        for (int i = 0; i < cp.size(); i++) {
            if (!delRow.contains(i)) {
                res.add(cp.get(i));
            }
        }
        pairwiseCases.put(n, res);
//        for(List<Integer> l : res){
//            System.out.println(l);
//        }
//        System.out.println(res.size());
        return res;
    }

    public List<List<List<Integer>>> split(List<List<Integer>> cp) {
        List<List<List<Integer>>> s = new ArrayList<>();
        for (int i = 0; i < cp.size(); i++) {
            List<List<Integer>> row = new ArrayList<>();
            for (int j = 0; j < cp.get(i).size(); j++) {
                for (int k = j + 1; k < cp.get(i).size(); k++) {
                    row.add(Arrays.asList(cp.get(i).get(j), cp.get(i).get(k)));
                }
            }
            s.add(row);
        }
        return s;
    }

    public int[] range(int start, int end) {
        int[] arr = new int[end - start];
        for (int i = start; i < end; i++) {
            arr[i - start] = i;
        }
        return arr;
    }

    public void cartesian(int[][] arr, int index, List<Integer> permutation, List<List<Integer>> permutations) {
        if (index == arr.length) {
            permutations.add(new ArrayList<>(permutation));
            return;
        }

        for (int i = 0; i < arr[index].length; i++) {
            permutation.add(arr[index][i]);
            cartesian(arr, index + 1, permutation, permutations);
            permutation.remove(permutation.size() - 1);
        }
    }

    // cartesian product
    public List<List<Integer>> cartesianProduct(int n) {
        if(cartesianCases.keySet().contains(n)){
            return cartesianCases.get(n);
        }
        int[][] option = new int[n][2];
        for(int i = 0; i < n; i++){
            option[i][0] = 0;
            option[i][1] = 1;
        }
        List<List<Integer>> cp = new ArrayList<>();
        cartesian(option, 0, new ArrayList<>(), cp);
        cartesianCases.put(n, cp);
        return cp;
    }

    public List<String> locateStrategy(String query, List<String> selected, List<String> used, List<List<String>> mutateStrategies){
//        System.out.println("locate");
        List<String> configs;
        List<String> importClass;
        List<String> oneSelect;
        List<String> newQueries = new ArrayList<>();
        List<String> target = new ArrayList<>();
        if(used.size() == 0){
            target = selected;
        }else{
            for(String s : selected){
                if(used.contains(s)){
                    target.add(s);
                }
            }
        }
        List<List<String>> ones = getCombinations(target);
        for(List<String> one : ones){
            configs = new ArrayList<>();
            importClass = new ArrayList<>();
            oneSelect = new ArrayList<>();
            for(String s : one){
                oneSelect.add(s);
            }
            mutateStrategies.add(oneSelect);
            iterateStrategy(oneSelect, configs, importClass);
            String newQuery = getNewQuery(configs, importClass, query);
//                System.out.println("new Query: " + newQuery);
            newQueries.add(newQuery);
        }
        return newQueries;
    }

    public List<List<String>> getCombinations(List<String> arr) {
        List<List<String>> result = new ArrayList<>();
        int n = arr.size();
        for (int i = 1; i <= n; i++) {
            List<String> tmp = new ArrayList<>();
            getCombinations(result, tmp, arr, i, 0);
        }

        return result;
    }

    public void getCombinations(List<List<String>> result, List<String> tmp, List<String> arr, int len, int start) {
        if (len == 0) {
            result.add(new ArrayList<>(tmp));
            return;
        }
        for (int i = start; i <= arr.size() - len; i++) {
            tmp.add(arr.get(i));
            getCombinations(result, tmp, arr, len - 1, i + 1);
            tmp.remove(tmp.size() - 1);
        }
    }

    public void statisticUsedCount(int size){
        if(usedCountStatis.containsKey(size)){
            int c = usedCountStatis.get(size)+1;
            usedCountStatis.put(size, c);
        }else{
            usedCountStatis.put(size, 1);
        }
    }

    public void iterateStrategy(List<String> selected, List<String> configs, List<String> importClass){
        for(String s : selected){
            if(!infos.containsKey(s)){
                continue;
            }
            StrategyInfo info = infos.get(s);
            if(info.getOpen() == 0){
                configs.add("withStrategies(" + info.getStrategy() + ")");
            } else if(info.getOpen() == 1){
                configs.add("withoutStrategies(" + info.getStrategy() + ")");
            }
            if(info.getImportClass() != null){
                importClass.add(info.getImportClass());
            }
        }
    }

    public List<String> getExecutionFeedback(GremlinConnection connection, String query){
        String executeQuery = query + ".explain()";
//        System.out.println("executeQuery: " + executeQuery);
        List<String> strategies = new ArrayList<>();
        Object result = connection.executeExplain(executeQuery);
        // AdjacentVertexFilterOptimizerStrategy            [P]   [JanusGraphStep(vertex,[]), VertexStep(BOTH,[el2, el1],edge)]
        if(connection.getDatabase().equals("JanusGraph") || connection.getDatabase().equals("OrientDB")){
            try{
                String[] traversals = result.toString().split("\n");
                // front is initialized to the original traversal
                String[] original = traversals[2].split("  ");
                String front = original[original.length - 1];
                // remove unused lines
                for(int i = 4; i < traversals.length - 2; i++){
                    String[] s = traversals[i].split("  ");
                    // {Strategy, Category, Traversal}
                    String[] traversal = {s[0], s[s.length-2], s[s.length-1]};
                    if(traversal[1].contains("O") || traversal[1].contains("P")){
                        if(!traversal[2].equals(front)){
                            strategies.add(traversal[0]);
                        }
                    }
                    front = traversal[2];
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }else if(connection.getDatabase().equals("TinkerGraph") || connection.getDatabase().equals("HugeGraph") || connection.getDatabase().equals("ArcadeDB") || connection.getDatabase().equals("Neo4j")){
            try{
                String[] traversals = result.toString().split("}, ");
                // front is initialized to the original traversal
                String[] original = traversals[0].split("], ");
                String front = original[0].substring(10) + "]";
                for(int i = 1; i < traversals.length - 1; i++){
                    String[] s = traversals[i].split("=");
                    // {Strategy, Category, Traversal}
                    String[] traversal = {s[2].substring(0, s[2].length()-10), s[3], s[1].substring(0, s[1].length()-10)};
                    if(traversal[1].contains("OptimizationStrategy") || traversal[1].contains("ProviderOptimizationStrategy")){
                        if(!traversal[2].equals(front)){
                            strategies.add(traversal[0]);
                        }
                    }
                    front = traversal[2];
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return strategies;
    }

    public Map<Integer, Integer> getUsedCountStatis() {
        return usedCountStatis;
    }

    private Map<Integer, Integer> usedCountStatis = new HashMap<>();



    public boolean noNewUsed(List<String> used){
        int num = 0;
        for(String s : used){
            if(!newUsed.contains(s)){
//                System.out.println("add a new used strategy : " + s);
                newUsed.add(s);
                noNewCount = 0;
            }else{
                num ++;
            }
        }
        if(num == used.size()){
//            System.out.println("no new used strategy.");
            noNewCount++;
        }
        if(noNewCount < exitM){
            return false;
        }else {
            return true;
        }
    }

    public Map<String, StrategyInfo> getStrategyInfoMap(List<StrategyInfo> strategyInfos){
        Map<String, StrategyInfo> infoMap = new HashMap<>();
        for(StrategyInfo info : strategyInfos){
            infoMap.put(info.getStrategy(), info);
        }
        return infoMap;
    }

    // open selected strategies
    public void iterateOpenStrategy(List<String> selected, List<String> configs, List<String> importClass){
        for(String s : selected){
            StrategyInfo info = infos.get(s);
            configs.add("withStrategies(" + info.getStrategy() + ")");
            if(info.getImportClass() != null){
                importClass.add(info.getImportClass());
            }
        }
    }


    public void printMat(){
        System.out.println("probability map: ");
        for(Map.Entry<String, Double> entry : pMap.entrySet()){
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("=========================================");
    }

    public void printStrategy(List<String> list){
        for(String s : list){
            System.out.print(s + ", ");
        }
        System.out.println();
    }

    public List<String> getSelectedList(int[] list){
        List<String> selected = new ArrayList<>();
        int count = 0;
        for(String s : infos.keySet()){
            if(infos.get(s).getOpen() != list[count++]){
                selected.add(s);
            }
        }
        return selected;
    }

    public String getNewQuery(List<String> configs, List<String> importClass, String query){
        StringBuilder newQuery = new StringBuilder();
        // add import
        for(String i : importClass){
            newQuery.append(i);
        }
        // start from a graph traversal source g
        newQuery.append("g.");
        for(String t : configs){
            newQuery.append(t).append(".");
        }
        return newQuery.substring(0, newQuery.length()-1) + query.substring(1);
    }

    public static void main(String[] args) {
        OptimizationBugDetectionByCT obd = new OptimizationBugDetectionByCT();
        obd.pairwise(3);

    }

}
