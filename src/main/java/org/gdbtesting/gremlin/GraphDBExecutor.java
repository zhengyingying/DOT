package org.gdbtesting.gremlin;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.OptimizationTestType;
import org.gdbtesting.Randomly;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.ast.GraphConstant;
import org.gdbtesting.gremlin.ast.Traversal;
import org.gdbtesting.gremlin.query.*;
import org.gdbtesting.hugegraph.HugeGraphConnection;
import org.gdbtesting.hugegraph.HugeGraphOptimizaiton;
import org.gdbtesting.janusgraph.JanusGraphConnection;
import org.gdbtesting.janusgraph.JanusGraphOptimization;
import org.gdbtesting.orientdb.OrientDBOptimization;
import org.gdbtesting.orientdb.OrientdbConnection;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;
import org.gdbtesting.tinkergraph.TinkerGraphOptimization;
import org.janusgraph.diskstorage.BackendException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class GraphDBExecutor {

    private List<GremlinConnection> connections;
    private GraphGlobalState state;
    private Map<String, String> commands;
    // map vertex and edge in different databases to a unified Id
    private Map<String, Map<String, String>> vertexIDMap;
    private Map<String, Map<String, String>> edgeIDMap;
    private List<String> queryList;
    // traversal list
    List<List<Traversal>> queries;
    private List<List<List<Object>>> resultList;
    private List<Map<String, Exception>> errorList;
    private final List<Thread> threadList = new ArrayList<>();

    JanusGraphOptimization janusGraphOptimization = null;
    TinkerGraphOptimization tinkerGraphOptimization = null;
    OrientDBOptimization orientDBOptimization = null;
    HugeGraphOptimizaiton hugeGraphOptimizaiton = null;

    public int getNotNull() {
        return notNull;
    }

    private int notNull;


    public GraphDBExecutor(List<GremlinConnection> connections, GraphGlobalState state) {
        this.connections = connections;
        this.state = state;
        janusGraphOptimization = null;
        tinkerGraphOptimization = null;
        orientDBOptimization = null;
        hugeGraphOptimizaiton = null;
        notNull = 0;
    }

    // generate query
    public void generateRandomQuery() {
        queries = new ArrayList<>();
        queryList = new ArrayList<>();
        resultList = new ArrayList<>((int) state.getQueryNum());
        errorList = new ArrayList<>((int) state.getQueryNum());
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        for (int i = 0; i < state.getQueryNum(); i++) {
            List<Traversal> list = gtg.generateRandomlyTraversal();
            queries.add(list);
            String query = traversalToString(list);
//            System.out.println(query);
            queryList.add(query);
            resultList.add(i, new ArrayList<>(connections.size()));
            errorList.add(i, new HashMap<>());
        }
    }

    public String traversalToString(List<Traversal> list) {
        StringBuilder query = new StringBuilder("g");
        for (Traversal t : list) {
            query.append(".").append(t.toString());
        }
        return query.toString();
    }

    public void executeQuery(GremlinConnection connection, int count) throws IOException {
        String cur = System.getProperty("period");
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0/" + connection.getDatabase() + "-" + state.getRepeatTimes() + ".log"));
        for (int i = 0; i < queryList.size(); i++) {
            try {
                out.write("========================Query " + i + "=======================\n");
                out.write(queryList.get(i) + "\n");
                long time = System.currentTimeMillis();
                List<Result> results;
                List<Object> list = new ArrayList<>();
                if (connection.getDatabase().equals("HugeGraph")) {
                    GremlinManager gremlin = connection.getHugespecial().gremlin();
                    org.apache.hugegraph.structure.gremlin.ResultSet hugeResult = gremlin.gremlin(queryList.get(i)).execute();
                    Iterator<org.apache.hugegraph.structure.gremlin.Result> huresult = hugeResult.iterator();
                    Long t = System.currentTimeMillis() - time;
                    System.out.println("query " + i + " in " + t + "ms");
                    huresult.forEachRemaining(result -> {
                        Object object = result.getObject();
                        if (object instanceof org.apache.hugegraph.structure.graph.Vertex) {
                            try {
                                out.write("v[" + ((org.apache.hugegraph.structure.graph.Vertex) object).id() + "]");
                                list.add(object);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else if (object instanceof org.apache.hugegraph.structure.graph.Edge) {
                            try {
                                out.write("e[" + ((org.apache.hugegraph.structure.graph.Edge) object).id() + "]");
                                list.add(object);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else if (object instanceof org.apache.hugegraph.structure.graph.Path) {
                            List<Object> elements = ((org.apache.hugegraph.structure.graph.Path) object).objects();
                            elements.forEach(element -> {
                                System.out.println(element.getClass());
                                System.out.println(element);
                            });
                        } else {
                            try {
                                out.write("n[" + object + "]");
                                list.add(object);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }
                    });
                } else {
                    results = connection.getClient().submit(queryList.get(i)).all().get();
                    System.out.println("query " + i + " in " + (System.currentTimeMillis() - time) + "ms");
                    for (Result r : results) {
                        String result = String.valueOf(r);
                        out.write(result);
                        out.newLine();
                        list.add(r);
                    }
                }
                resultList.get(i).add(count, list);
            } catch (Exception e) {
                out.write(e.toString());
                resultList.get(i).add(count, null);
                errorList.get(i).put(String.valueOf(count), e);
            }
        }
        out.close();
    }

    public void setupGraphAndDOT(List<GraphData.VertexObject> addV, List<GraphData.EdgeObject> addE, String type, int repeat) throws IOException {
        vertexIDMap = new HashMap<>();
        edgeIDMap = new HashMap<>();
        // for each graph db
        for (GremlinConnection connection : connections) {
            // setup database
            long start = System.currentTimeMillis();
            setupGraphDatabase(connection, addV, addE, repeat);
            System.out.println("setup " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) + "ms");
            pairwiseCombUsedTest(connection, repeat);
        }
        // record db map
        recordDBMap(repeat);
    }

    private List<Integer> errors = new ArrayList<>();
    List<List<String>> strategyTrigger = new ArrayList<>();
    private int errorCount = 0;

    // DOT: Pairwise Testing
    public void pairwiseCombUsedTest(GremlinConnection connection, int repeat) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0" + "/pairwise-check-" + repeat + ".log"));
        BufferedWriter resultOut = new BufferedWriter(new FileWriter(System.getProperty("user.dir")+ "/log-0" + "/pairwise-result-" + repeat + ".log"));
        BufferedWriter indexLog = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0" + "/pairwise-index-" + repeat + ".log"));
        BufferedWriter strategy = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0" + "/pairwise-strategy-" + repeat + ".log"));
        OptimizationBugDetectionByCT opc = new OptimizationBugDetectionByCT(connection);
        opc.setUsedOptCombination(state.getUniqueCombination());
        long start = System.currentTimeMillis();
        int count = 0;
        int queryNum = 0;
        errors = new ArrayList<>();
        List<String> locateStrategies = new ArrayList<>();
        for(int i = 0; i < queryList.size(); i++){
            if(opc.isNewGDB()){
                out.write("test " + (i+1) + " queries\n");
                out.write("Generate a new GDB now.\n");
                System.out.println("Generate a new GDB now.");
                break;
            }
            String query = mutateQuery(i);
            List<List<String>> selected = new ArrayList<>();
            boolean isBug = false;
            List<List<String>> mutateStrategies = new ArrayList<>();
            List<String> used = new ArrayList<>();
            // used optimization strategies should be ordered by its name
            List<String> newQueries = opc.pairwiseCombineUsedStrategy(connection, query, used, selected);
            // store used to skip some duplicate queries
            if(newQueries.size()==0){

            }else {
                queryNum ++;
                Boolean index = false;
                if (Randomly.getBoolean()) {
                    index = true;
                }
                for (int t = 0; t < newQueries.size(); t++) {
                    isBug = checkDifferencies(connection, query, newQueries.get(t), out, resultOut, indexLog, i, index);
                    if(isBug){
                        // locate optimization strategy
                        List<String> news = opc.locateStrategy(query, selected.get(t), used, mutateStrategies);
                        for(int j = 0; j < news.size(); j++){
//                            System.out.println(news.get(j));
                            isBug = checkDifferencies(connection, query, news.get(j), out, resultOut, indexLog, i, index);
                            if(isBug){
                                // mutateStrategy
                                strategyTrigger.add(mutateStrategies.get(j));
                                strategy.write("query " + i + ": " + mutateStrategies.get(j));
                                strategy.newLine();
                                // store located optimization strategies;
                                List<String> ms = mutateStrategies.get(j);
                                Collections.sort(ms);
                                String s = storeTriggerStrategy(ms);
                                if(!locateStrategies.contains(s)){
                                    locateStrategies.add(s);
                                }
                                break;
                            }
                        }
                        break;
                    }
                }
                count += newQueries.size();
            }
        }
        long testTime = System.currentTimeMillis() - start;
        out.write("test " + connection.getDatabase() + " in " + testTime + " ms\n");
        out.write("test " + queryNum + " new queries.\n");
        out.write(count + " mutations are generated\n");
        out.write("detect " + errors.size() + " unique bugs.\n");
        out.write("totally test " + opc.getNewUsed().size() + " strategies.\n");
        out.write("locate strategies: \n");
        for(String l : locateStrategies){
            out.write(l);
            out.newLine();
        }

        System.out.println("test " + connection.getDatabase() + " in " + testTime + " ms");
        System.out.println(count + " mutations are generated\n");
        // statistic the frequency of used strategies
//        recordStrategyMap(opc.getInfos(), strategy);
        recordUsedMap(opc.getUsedCountStatis(), strategy);
        out.close();
        resultOut.close();
        indexLog.close();
        strategy.close();
    }

    public String storeTriggerStrategy(List<String> news){
        StringBuilder s = new StringBuilder();
        for(String o : news){
            s.append(o).append(",");
        }
        return s.substring(0, s.length()-1);
    }

    public void recordUsedMap(Map<Integer, Integer> strategyMap, BufferedWriter writer){
        try {
            writer.newLine();
            writer.write("Strategy\tUsed Number\n");
            for(Integer s : strategyMap.keySet()){
                writer.write(s + "\t" + strategyMap.get(s));
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public String mutateQuery(int i){
        if(Randomly.getInteger(100) < 5){
            if(Randomly.getBoolean()){
                return rewriteMatch(queries.get(i));
            }else{
                return randomAddBarrier(queries.get(i));
            }
        }
        return queryList.get(i);
    }

    // rewrite a query to a match manner
    public String rewriteMatch(List<Traversal> query){
        StringBuilder newQuery = new StringBuilder("g.");
        newQuery.append(query.get(0).toString()).append(".").append("match(");
        int l = query.size();
        for(int i = 1; i < l; i++){
            String t = query.get(i).toString();
            newQuery.append("__.as('t" + (i-1) + "')").append(".").append(t).append(".as('t" + i + "')");
            if(i != l-1){
                newQuery.append(",");
            }
        }
        newQuery.append(")").append(".").append("select('t"+(l-1)+"')");
        return newQuery.toString();
    }

    // rewrite a query to a barrier manner
    public String randomAddBarrier(List<Traversal> query){
        StringBuilder newQuery = new StringBuilder("g.");
        for(Traversal t: query){
            if(Randomly.getInteger(100) < 10){
                newQuery.append(t.toString()).append(".").append("barrier()").append(".");
            }else{
                newQuery.append(t.toString()).append(".");
            }
        }
        return newQuery.substring(0, newQuery.length()-1);
    }

    public List<String> executeQuery(GremlinConnection connection, String query){
        ResultWrapper resultSet = new ResultWrapper(connection.execute(query));
        List<String> list = new ArrayList<>(resultSet.extractIds());
        return list;
    }

    public void createIndex(GremlinConnection connection, BufferedWriter writer) {
        if(connection.getDatabase().equals("JanusGraph")){
            if(janusGraphOptimization == null){
                janusGraphOptimization = new JanusGraphOptimization(state, (JanusGraphConnection) connection, writer);
            }
            janusGraphOptimization.createJanusGraphIndex((JanusGraphConnection) connection);
        } else if(connection.getDatabase().equals("TinkerGraph")){
            if(tinkerGraphOptimization == null){
                tinkerGraphOptimization = new TinkerGraphOptimization(state, (TinkerGraphConnection) connection, writer);
            }
            tinkerGraphOptimization.createTinkerGraphIndex();
        } else if(connection.getDatabase().equals("OrientDB")){
            if(orientDBOptimization == null){
                orientDBOptimization = new OrientDBOptimization(state, (OrientdbConnection) connection, writer);
            }
            orientDBOptimization.createOrientDBIndex();
        } else if(connection.getDatabase().equals("ArcadeDB")){

        } else if(connection.getDatabase().equals("HugeGraph")){
            if(hugeGraphOptimizaiton == null){
                hugeGraphOptimizaiton = new HugeGraphOptimizaiton(state, (HugeGraphConnection) connection, writer);
            }
            hugeGraphOptimizaiton.createHugeGraphIndex();
        } else if(connection.getDatabase().equals("Neo4j")){

        }
    }

    public void setupGraphAndTest(List<GraphData.VertexObject> addV, List<GraphData.EdgeObject> addE) throws IOException {
        vertexIDMap = new HashMap<>();
        edgeIDMap = new HashMap<>();
        int count = 0;
        // for each graph db
        for (GremlinConnection connection : connections) {
            // setup database
            long start = System.currentTimeMillis();
            setupGraphDatabase(connection, addV, addE, this.state.getRepeatTimes());
            System.out.println("setup " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) + "ms");
            // query database
            start = System.currentTimeMillis();
            executeQuery(connection, count);
            count++;
            System.out.println("query " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) + "ms");
        }
        // record db map
        recordDBMap(this.state.getRepeatTimes());
    }

    public void recordDBMap(int repeat) throws IOException {
        for (GremlinConnection connection : connections) {
            String cur = System.getProperty("period");
            BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0/" + connection.getDatabase() + "-Map-" + repeat + ".log"));
            Map<String, String> vMap = vertexIDMap.get(connection.getDatabase());
            out.write("Vertex");
            out.newLine();
            for (String id : vMap.keySet()) {
                out.write(vMap.get(id) + "->" + id);
                out.newLine();
            }
            Map<String, String> eMap = edgeIDMap.get(connection.getDatabase());
            out.write("Edge");
            out.newLine();
            for (String id : eMap.keySet()) {
                out.write(eMap.get(id) + "->" + id);
                out.newLine();
            }
            out.close();
        }
    }


    public void setupGraphDatabase(GremlinConnection connection, List<GraphData.VertexObject> addV, List<GraphData.EdgeObject> addE, int repeat) throws IOException {
        String cur = System.getProperty("period");
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0/" + connection.getDatabase() + "-graphdata-" + repeat + ".txt"));
        Map<String, String> vIDMap = new HashMap<>();
        Map<Integer, String> tempMap = new HashMap<>();
        Map<String, String> eIDMap = new HashMap<>();
        if (connection.getDatabase().equals("HugeGraph")) {
            HugeClient hc = connection.getHugespecial();
            GraphManager graph = hc.graph();
            Map<String, org.apache.hugegraph.structure.graph.Vertex> verticesMap = new HashMap<>();
            out.write("Vertex:");
            for (GraphData.VertexObject v : addV) {
                try {
                    String Label = v.getLabel();
                    org.apache.hugegraph.structure.graph.Vertex add = new org.apache.hugegraph.structure.graph.Vertex(Label);
                    Map<String, GraphConstant> map = v.getProperites();
                    out.newLine();
                    out.write("Label: " + add.label());
                    out.newLine();
                    out.write("Properties: ");
                    out.newLine();
                    for (String key : map.keySet()) {
                        Object value = getTransValue(map.get(key));
                        out.write("Key: " + key + " Value: " + value);
                        out.newLine();
                        add.property(key, value);
                    }
                    add = graph.addVertex(add);
                    vIDMap.put(add.id().toString(), String.valueOf(v.getId()));
                    tempMap.put(v.getId(), add.id().toString());
                    out.write("ID: " + add.id().toString());
                    out.newLine();
                    verticesMap.put(add.id().toString(), add);
                } catch (Exception e) {
                    out.write(e.toString());
                    out.newLine();
                    e.printStackTrace();
                    break;
                }
            }
            out.newLine();
            out.write("Edge:");
            out.newLine();
            try {
                for (GraphData.EdgeObject e : addE) {
                    org.apache.hugegraph.structure.graph.Vertex outVertex = verticesMap.get(tempMap.get(e.getOutVertex().getId()));
                    org.apache.hugegraph.structure.graph.Vertex inVertex = verticesMap.get(tempMap.get(e.getInVertex().getId()));
                    org.apache.hugegraph.structure.graph.Edge addEdge = new org.apache.hugegraph.structure.graph.Edge(e.getLabel()).source(outVertex).target(inVertex);
                    Map<String, GraphConstant> map = e.getProperites();
                    out.write("Label: " + addEdge.label());
                    out.newLine();
                    out.write("Out: " + e.getOutVertex().getId());
                    out.newLine();
                    out.write("In: " + e.getInVertex().getId());
                    out.newLine();
                    out.write("Properties: ");
                    out.newLine();
                    for (String key : map.keySet()) {
                        out.write("Key: " + key + "  Value: " + getTransValue(map.get(key)));
                        out.newLine();
                        addEdge.property(key, getTransValue(map.get(key)));
                    }
                    addEdge = graph.addEdge(addEdge);
                    eIDMap.put(addEdge.id(), String.valueOf(e.getId()));
                    out.write("ID: " + addEdge.id());
                    out.newLine();
                }
            } catch (Exception e) {
                out.write(e.toString());
                out.newLine();
                e.printStackTrace();
            }
        } else {
            GraphTraversalSource g = connection.getG();
            // reset
            g.E().drop().iterate();
            g.V().drop().iterate();
            // add vertices
            out.write("Vertex:");
            out.newLine();
            for (GraphData.VertexObject v : addV) {
                try {
                    Vertex vv = generateVertex(g, v, out);
                    vIDMap.put(vv.id().toString(), String.valueOf(v.getId()));
                    tempMap.put(v.getId(), vv.id().toString());
                } catch (Exception e) {
                    out.write(e.toString());
                    out.newLine();
                    e.printStackTrace();
                    break;
                }
            }
            // add edges
            out.newLine();
            out.write("Edge:");
            out.newLine();
            try {
                for (GraphData.EdgeObject e : addE) {
                    Vertex outVertex = g.V(tempMap.get(e.getOutVertex().getId())).next();
                    Vertex inVertex = g.V(tempMap.get(e.getInVertex().getId())).next();
                    Edge edge = g.addE(e.getLabel()).from(outVertex).to(inVertex).next();
                    out.write("ID: " + edge.id());
                    out.newLine();
                    out.write("Label: " + edge.label());
                    out.newLine();
                    out.write("Out: " + outVertex.id());
                    out.newLine();
                    out.write("In: " + inVertex.id());
                    out.newLine();
                    out.write("Properties: ");
                    out.newLine();
                    Map<String, GraphConstant> map = e.getProperites();
                    for (String key : map.keySet()) {
                        out.write("Key: " + key + "  Value: " + getTransValue(map.get(key)));
                        out.newLine();
                        g.E(edge.id()).property(key, getTransValue(map.get(key))).iterate();
                    }
                    out.newLine();
                    eIDMap.put(edge.id().toString(), String.valueOf(e.getId()));
                }
            } catch (Exception e) {
                out.write(e.toString());
                out.newLine();
                e.printStackTrace();
            }
        }

        out.close();
        vertexIDMap.put(connection.getDatabase(), vIDMap);
        edgeIDMap.put(connection.getDatabase(), eIDMap);

//        // add index
//        createIndex(connection);
    }

    public void createIndex(GremlinConnection connection){
        if (connection.getDatabase().equals("Neo4j")) {
            createNeo4jGraphIndex(connection);
        } else if (connection.getDatabase().equals("JanusGraph")) {
            try {
                janusGraphOptimization = new JanusGraphOptimization(state, (JanusGraphConnection) connection, null);
                janusGraphOptimization.createJanusGraphRandomIndex();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (connection.getDatabase().equals("HugeGraph")) {
            createHugeGraphIndex(connection);
        }
    }

    private void createHugeGraphIndex(GremlinConnection connection) {
        return;
    }

    public void createNeo4jGraphIndex(GremlinConnection connection) {
        for (GraphSchema.GraphVertexIndex index : state.getSchema().getVertexIndices()) {
            String vl = index.getVl().getLabelName();
            List<String> vpList = new ArrayList<>();
            for (GraphSchema.GraphVertexProperty vp : index.getVpList()) {
                vpList.add(vp.getVertexPropertyName());
            }
            StringBuilder vps = new StringBuilder();
            for (String s : vpList) {
                vps.append(s).append(",");
            }
            StringBuilder sb = new StringBuilder();
            String graph = "graph = g.getGraph(); ";
            String cypher = "graph.cypher('CREATE INDEX ON :" + vl + "(" + vps.toString().subSequence(0, vps.length() - 1) + ")');";
            String commit = "graph.tx().commit();";
            try {
                connection.getClient().submit(sb.append(graph).append(cypher).append(commit).toString()).all().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public void checkResult() throws IOException {
        String cur = System.getProperty("period");
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0/check-" + state.getRepeatTimes() + ".log"));
        BufferedWriter resultOut = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0/result-" + state.getRepeatTimes() + ".log"));
        // for each query
        for (int i = 0; i < resultList.size(); i++) {
            List<List<Object>> list = resultList.get(i);
            Map<String, Exception> elist = errorList.get(i);
            List<String> compare = new ArrayList<>();
            out.write("========================Query " + i + "=======================");
            out.newLine();
            // for each graph db
            for (int j = 0; j < list.size(); j++) {
                List<Object> elements = list.get(j);
                Exception errors = elist.get(String.valueOf(j));
                StringBuilder sb = new StringBuilder();
                if (elements == null || elements.size() == 0) {
                    if (errors == null)
                        sb.append("null");
                    else
                        sb.append(errors);
                } else {
                    List<String> idList = new ArrayList<>();
                    String view = elements.get(0).toString();
                    if (elements.get(0).toString().contains("e[")) {
                        for (Object e : elements) {
                            idList.add(edgeIDMap.get(connections.get(j).getDatabase()).get(((Result) e).getElement().id().toString()));
                        }
                        if (idList != null && idList.size() > 0) {
                            Collections.sort(idList);
                        }
                        sb.append("e:").append(idList.toString());
                    } else if (elements.get(0).toString().contains("v[")) {
                        for (Object e : elements) {
                            idList.add(vertexIDMap.get(connections.get(j).getDatabase()).get(((Result) e).getElement().id().toString()));
                        }
                        if (idList != null && idList.size() > 0) {
                            Collections.sort(idList);
                        }
                        sb.append("v:").append(idList.toString());
                    } else if (elements.get(0).toString().contains("java.lang.")) {
                        for (Object e : elements) {
                            String value = e.toString();
                            idList.add(value.substring(value.indexOf("object=") + 7, value.indexOf(" class=")));
                        }
                        if (idList != null && idList.size() > 0) {
                            Collections.sort(idList);
                        }
                        sb.append("n:").append(idList.toString());
                    } else {
                        if (elements.get(0) instanceof org.apache.hugegraph.structure.graph.Vertex) {
                            for (Object e : elements) {
                                idList.add(vertexIDMap.get(connections.get(j).getDatabase()).get(((org.apache.hugegraph.structure.graph.Vertex) e).id().toString()));
                            }
                            if (idList != null && idList.size() > 0) {
                                Collections.sort(idList);
                            }
                            sb.append("v:").append(idList.toString());
                        } else if (elements.get(0) instanceof org.apache.hugegraph.structure.graph.Edge) {
                            for (Object e : elements) {
                                idList.add(edgeIDMap.get(connections.get(j).getDatabase()).get(((org.apache.hugegraph.structure.graph.Edge) e).id().toString()));
                            }
                            if (idList != null && idList.size() > 0) {
                                Collections.sort(idList);
                            }
                            sb.append("e:").append(idList.toString());
                        } else {
                            for (Object e : elements) {
                                String value = e.toString();
                                idList.add(value);
                            }
                            if (idList != null && idList.size() > 0) {
                                Collections.sort(idList);
                            }
                            sb.append("n:").append(idList.toString());
                        }
                    }
                }
                compare.add(sb.toString());
                out.write("db" + j + ": " + sb.toString());
                out.newLine();
            }
            if (!compareResult(compare)) {
                resultOut.write("query" + i + ": false");
                resultOut.newLine();
            }
        }
        out.close();
        resultOut.close();
    }

    public boolean compareResult(List<String> compare) {
        if (compare == null || compare.size() == 0) {
            return true;
        }
        String begin = compare.get(0);
        for (int i = 1; i < compare.size(); i++) {
            if (!compare.get(i).equals(begin)) {
                return false;
            }
        }
        return true;
    }


    public Vertex generateVertex(GraphTraversalSource g, GraphData.VertexObject v, BufferedWriter out) throws IOException {
        Vertex vertex = g.addV(v.getLabel()).next();
        Map<String, GraphConstant> map = v.getProperites();
        out.newLine();
        out.write("ID: " + vertex.id().toString());
        out.newLine();
        out.write("Label: " + v.getLabel());
        out.newLine();
        out.write("Properties: ");
        out.newLine();
        for (String key : map.keySet()) {
            Object value = getTransValue(map.get(key));
            out.write("Key: " + key + " Value: " + value);
            out.newLine();
            g.V(vertex).property(key, value).iterate();
        }
        return vertex;
    }

    public Object getTransValue(GraphConstant value) {
        Object type = value.getType();
        if (Integer.class.equals(type)) {
            return Integer.valueOf(value.toString());
        } else if (String.class.equals(type)) {
            return value.toString();
        } else if (Double.class.equals(type)) {
            return Double.valueOf(value.toString());
        } else if (Boolean.class.equals(type)) {
            return Boolean.valueOf(value.toString());
        } else if (Float.class.equals(type)) {
            return Float.valueOf(value.toString());
        } else if (Long.class.equals(type)) {
            return Long.valueOf(value.toString());
        }
        throw new AssertionError();
    }

    public boolean checkDifferencies(GremlinConnection connection, String original, String mutation, BufferedWriter out, BufferedWriter resultOut, BufferedWriter indexLog, int count, boolean index) throws IOException {
        resultOut.write("==============================Query " + count + "==============================\n");
        resultOut.write("original: " + original + "\n");
        resultOut.write("mutation: " + mutation + "\n");
        Boolean oLexception = false;
        Boolean rLexception = false;
        Boolean err = false;
        // execute the original query
        List<String> oL = new ArrayList<>();
        try{
            oL = executeQuery(connection, original);
        }catch (Exception e){
            String error = e.getMessage();
            oL.add(error);
            oLexception = true;
        }
        resultOut.write("original:" + oL.toString() + "\n");
        // execute the mutate query
        List<String> rL = new ArrayList<>();
        if(index){
            try {
                indexLog.write("==============================Query " + count + "==============================\n");
                indexLog.write(mutation + "\n");
                createIndex(connection, indexLog);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try{
            rL = executeQuery(connection, mutation);
        }catch (Exception e){
            String error = e.getMessage();
            rL.add(error);
            rLexception = true;
        }
        resultOut.write("mutation:" + rL.toString() + "\n");
        try{
            if(oL.size() != rL.size()) {
                if(oLexception || rLexception){
                    out.write("query " + count + ": exception\n");
                }else{
                    out.write("query " + count + ": false\n");
                }
                if(!errors.contains(count)){
                    errors.add(count);
                }
                return true;
            }
            if((oLexception && !rLexception) || (!oLexception && rLexception)){
                out.write("query " + count + ": exception\n");
                if(!errors.contains(count)){
                    errors.add(count);
                }
                return true;
            }else if(oLexception && rLexception){
                return false;
//                try{
//                    if(oL.get(0).contains("Script") && rL.get(0).contains("Script")){
//                        return false;
//                    }
//                    if(oL.get(0).substring(0,50) == rL.get(0).substring(0,50)){
//                        return false;
//                    }
//                    out.write("query " + count + ": exception\n");
//                    if(!errors.contains(count)){
//                        errors.add(count);
//                    }
//                    return true;
//                }catch (Exception e){
////                    e.printStackTrace();
//                }
            }
            Collections.sort(oL);
            Collections.sort(rL);
            for (int j = 0; j < oL.size(); j++) {
                if (!oL.get(j).equals(rL.get(j))) {
                    out.write("query " + count + ": false\n");
                    if(!errors.contains(count)){
                        errors.add(count);
                    }
                    return true;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}

