package DP.Transformations;

import java.util.*;

public class Query {
    String originalQuery;
    String currentQuery;
    HashMap<Integer, List<Transformation>> queryTransforms;
    HashMap<Integer, Boolean> runs;
    int currentRunNumber;
    boolean changed;

    public Query(String originalQuery, String currentQuery) {
        this.originalQuery = originalQuery;
        this.currentQuery = currentQuery;
    }

    public int getCurrentRunNumber() {
        return currentRunNumber;
    }

    public void setCurrentRunNumber(int currentRunNumber) {
        this.currentRunNumber = currentRunNumber;
    }

    public HashMap<Integer, Boolean> getRuns() {
        return runs;
    }

    public void setRuns(HashMap<Integer, Boolean> runs) {
        this.runs = runs;
    }

    public void addRun(Integer numberOfRuns, Boolean changed) {
        if (this.runs == null) {
            this.runs = new HashMap<>();
        }
        runs.put(numberOfRuns, changed);
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public void setOriginalQuery(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public String getCurrentQuery() {
        return currentQuery;
    }

    public void setCurrentQuery(String currentQuery) {
        this.currentQuery = currentQuery;
    }

    public HashMap<Integer, List<Transformation>> getQueryTransforms() {
        return queryTransforms;
    }

    public void setQueryTransforms(HashMap<Integer, List<Transformation>> queryTransforms) {
        this.queryTransforms = queryTransforms;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    public void addTransform(Transformation transform) {
        addTransformAutomatically(getCurrentRunNumber(), transform);
    }

    private void addTransformAutomatically(Integer numberOfRuns, Transformation transform) {
        if (this.queryTransforms == null) {
            this.queryTransforms = new HashMap<>();
        }
        List<Transformation> transformations;
        if (this.queryTransforms.containsKey(numberOfRuns)) {
            transformations = this.queryTransforms.get(numberOfRuns);
        } else {
            transformations = new ArrayList<>();
        }
        transformations.add(transform);
        this.queryTransforms.put(numberOfRuns, transformations);
    }

    public Boolean isQueryChangedByRun(int numberOfRuns) {
        List<Transformation> transformationsByRun = this.getQueryTransforms().get(numberOfRuns);
        for (Transformation t: transformationsByRun) {
            if (t.isChanged()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Query{" +
                "originalQuery='" + originalQuery + '\'' +
                ", currentQuery='" + currentQuery + '\'' +
                ", runs=" + runs +
                ", currentRunNumber=" + currentRunNumber +
                ", changed=" + changed +
                '}';
    }
}
