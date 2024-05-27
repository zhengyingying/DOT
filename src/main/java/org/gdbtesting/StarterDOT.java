package org.gdbtesting;

import org.gdbtesting.arcadedb.ArcadedbConnection;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GremlinGraphProvider;
import org.gdbtesting.hugegraph.HugeGraphConnection;
import org.gdbtesting.janusgraph.JanusGraphConnection;
import org.gdbtesting.neo4j.Neo4jConnection;
import org.gdbtesting.orientdb.OrientdbConnection;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StarterDOT {

    public static void main(String[] args) {
        String targetGDB =  "TinkerGraph";

        if(args.length < 8 && args.length > 0){
            System.out.println("Error Parameters!, 1.QueryDepth, 2.VerMaxNum, 3.EdgeMaxNum, 4.EdgeLabelNum, 5.VerLabelNum, 6.QueryNum, 7.TimeBudget, 8. TargetGDB");
        }else{
            GraphGlobalState state = null;

            if(args.length ==0){
                // use default parameters
                state = new GraphGlobalState(10);
                state.setVerticesMaxNum(100);
                state.setEdgesMaxNum(200);
                state.setEdgeLabelNum(10);
                state.setVertexLabelNum(10);
                state.setQueryNum(2000);
                state.setTimeBudget(1);
            }else{
                state = new GraphGlobalState(Integer.parseInt(args[0]));
                state.setVerticesMaxNum(Integer.parseInt(args[1]));
                state.setEdgesMaxNum(Integer.parseInt(args[2]));
                state.setEdgeLabelNum(Integer.parseInt(args[3]));
                state.setVertexLabelNum(Integer.parseInt(args[4]));
                state.setQueryNum(Integer.parseInt(args[5]));
                state.setTimeBudget(Integer.parseInt(args[6]));
                targetGDB = args[7];
            }

            List<GremlinConnection> connections = null;

            switch(targetGDB){
                case "TinkerGraph" : connections = Arrays.asList(new TinkerGraphConnection("3.6.2", "conf/remote-tinkergraph.properties")); break;
                case "Neo4j" : connections = Arrays.asList(new Neo4jConnection("3.4.11", "conf/remote-neo4j.properties")); break;
                case "JanusGraph" : connections = Arrays.asList(new JanusGraphConnection("0.6.3", "conf/remote-janusgraph.properties")); break;
                case "HugeGraph" : connections = Arrays.asList(new HugeGraphConnection("1.0.0", "conf/remote-hugegraph.properties")); break;
                case "ArcadeDB" : connections = Arrays.asList(new ArcadedbConnection("23.2.1","conf/remote-arcade.properties")); break;
                case "OrientDB" : connections = Arrays.asList(new OrientdbConnection("3.2.16", "conf/remote-orient.properties")); break;
            }

            long start = System.currentTimeMillis();
//            GremlinGraphProvider provider = new GremlinGraphProvider(state, connections);
//            try {
//                provider.generateAndTestGDBWithDOT(state, 1);
//            } catch (Exception e) {
//                e.printStackTrace();
//                return;
//            }
            // iteratively generate graph database and test

            BufferedWriter out;
            Thread thread;
            try {
                 out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log-0/"+ connections.get(0).getDatabase()+"-unique.txt"));
                 thread = new Thread(new TimeRunner(state, out));
                 thread.start();
                long startTime = System.nanoTime();
//            long timeBudget = TimeUnit.HOURS.toNanos(1)/2;
                long timeBudget = TimeUnit.MINUTES.toNanos(state.getTimeBudget());
                int repeat = 0;
                while (true) {
                    repeat++;
                    GremlinGraphProvider provider = new GremlinGraphProvider(state, connections);
                    try {
                        provider.generateAndTestGDBWithDOT(state, repeat);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                    long currentTime = System.nanoTime();
                    // time budget is used up
                    if (currentTime - startTime >= timeBudget) {
                        System.out.println(currentTime-startTime);
                        break;
                    }
                }
                System.out.println("execute time: " + (System.currentTimeMillis() - start));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.exit(0);
    }

    private static class TimeRunner implements Runnable {
        GraphGlobalState state;
        BufferedWriter out;

        public TimeRunner(GraphGlobalState state, BufferedWriter out) {
            this.state = state;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                Long start = System.currentTimeMillis();
                out.write("unique combination:\n");
//                Thread.sleep(10000);
                while((System.currentTimeMillis()-start)<300000){
                    Thread.sleep(10000);
                    int size = state.getUniqueCombination().size();
                    out.write(size + ", ");
                    System.out.println("Unique combination:" + size);
                }
                out.close();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }

        }
    }
}
