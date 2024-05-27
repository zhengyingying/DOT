package org.gdbtesting.gremlin;

public class StrategyInfo {

    private String strategy;
    private int open;
    private String importClass = null;

    public StrategyInfo(String strategy, int open, String importClass){
        this.strategy = strategy;
        this.open = open;
        this.importClass = importClass;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public int getOpen() {
        return open;
    }

    public void setOpen(int open) {
        this.open = open;
    }

    public String getImportClass() {
        return importClass;
    }

    public void setImportClass(String importClass) {
        this.importClass = importClass;
    }


}
