//package org.gdbtesting;
//
//import org.gdbtesting.common.query.Query;
//import org.gdbtesting.common.query.SQLancerResultSet;
//import org.gdbtesting.common.schema.AbstractSchema;
//import org.gdbtesting.common.schema.AbstractTable;
//
//public abstract class GlobalState<O extends GDBMSSpecificOptions<?>, S extends AbstractSchema<?, ?>, C extends GraphDBConnection> {
//
//    protected C databaseConnection;
//    private static Randomly r;
//    private MainOptions options;
//    private O dmbsSpecificOptions;
//    private S schema;
//    private Main.StateLogger logger;
//    private StateToReproduce state;
//    private Main.QueryManager<C> manager;
//    private String databaseName;
//    private GraphDB dbType;
//
//
//    public String getDbVersion() {
//        return dbVersion;
//    }
//
//    public void setDbVersion(String dbVersion) {
//        this.dbVersion = dbVersion;
//    }
//
//    private String dbVersion;
//
//    public enum GraphDB{
//        TINKERGRAPH, HUGEGRAPH, JANUSGRAPH, NEO4J, ORIENTDB
//    }
//
//    public GraphDB getDbType() {
//        return dbType;
//    }
//
//    public void setDbType(GraphDB dbType) {
//        this.dbType = dbType;
//    }
//
//    public void setConnection(C con) {
//        this.databaseConnection = con;
//    }
//
//    public C getConnection() {
//        return databaseConnection;
//    }
//
//    @SuppressWarnings("unchecked")
//    public void setDmbsSpecificOptions(Object dmbsSpecificOptions) {
//        this.dmbsSpecificOptions = (O) dmbsSpecificOptions;
//    }
//
//    public O getDmbsSpecificOptions() {
//        return dmbsSpecificOptions;
//    }
//
//    public void setRandomly(org.gdbtesting.Randomly r) {
//        this.r = r;
//    }
//
//    public static org.gdbtesting.Randomly getRandomly() {
//        return r;
//    }
//
//    public MainOptions getOptions() {
//        return options;
//    }
//
//    public void setMainOptions(MainOptions options) {
//        this.options = options;
//    }
//
//    public void setStateLogger(org.gdbtesting.Main.StateLogger logger) {
//        this.logger = logger;
//    }
//
//    public org.gdbtesting.Main.StateLogger getLogger() {
//        return logger;
//    }
//
//    public void setState(org.gdbtesting.StateToReproduce state) {
//        this.state = state;
//    }
//
//    public org.gdbtesting.StateToReproduce getState() {
//        return state;
//    }
//
//    public org.gdbtesting.Main.QueryManager<C> getManager() {
//        return manager;
//    }
//
//    public void setManager(org.gdbtesting.Main.QueryManager<C> manager) {
//        this.manager = manager;
//    }
//
//    public String getDatabaseName() {
//        return databaseName;
//    }
//
//    public void setDatabaseName(String databaseName) {
//        this.databaseName = databaseName;
//    }
//
//    private ExecutionTimer executePrologue(Query<?> q) throws Exception {
//        boolean logExecutionTime = getOptions().logExecutionTime();
//        ExecutionTimer timer = null;
//        if (logExecutionTime) {
//            timer = new ExecutionTimer().start();
//        }
//        if (getOptions().printAllStatements()) {
//            System.out.println(q.getQueryString());
//        }
//        if (getOptions().logEachSelect()) {
//            if (logExecutionTime) {
//                getLogger().writeCurrentNoLineBreak(q.getQueryString());
//            } else {
//                getLogger().writeCurrent(q.getQueryString());
//            }
//        }
//        return timer;
//    }
//
//    protected abstract void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception;
//
//    public boolean executeStatement(Query<C> q, String... fills) throws Exception {
//        ExecutionTimer timer = executePrologue(q);
//        boolean success = manager.execute(q, fills);
//        executeEpilogue(q, success, timer);
//        return success;
//    }
//
//    public SQLancerResultSet executeStatementAndGet(Query<C> q, String... fills) throws Exception {
//        ExecutionTimer timer = executePrologue(q);
//        SQLancerResultSet result = manager.executeAndGet(q, fills);
//        boolean success = result != null;
//        if (success) {
//            result.registerEpilogue(() -> {
//                try {
//                    executeEpilogue(q, success, timer);
//                } catch (Exception e) {
//                    throw new AssertionError(e);
//                }
//            });
//        }
//        return result;
//    }
//
//    public S getSchema() {
//        if (schema == null) {
//            try {
//                updateSchema();
//            } catch (Exception e) {
//                throw new AssertionError();
//            }
//        }
//        return schema;
//    }
//
//    protected void setSchema(S schema) {
//        this.schema = schema;
//    }
//
//    public void updateSchema() throws Exception {
//        setSchema(readSchema());
//        for (AbstractTable<?, ?, ?> table : schema.getDatabaseTables()) {
//            table.recomputeCount();
//        }
//    }
//
//    protected abstract S readSchema() throws Exception;
//
//}
