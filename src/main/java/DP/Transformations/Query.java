package DP.Transformations;

import java.util.*;

public class Query {
    String originalQuery;
    String currentQuery;
    HashMap<Integer, List<Transformation>> queryTransformations;
    HashMap<Integer, Boolean> runs;
    int currentRunNumber;
    boolean changed = false;

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
        return queryTransformations;
    }

    public void setQueryTransforms(HashMap<Integer, List<Transformation>> queryTransforms) {
        this.queryTransformations = queryTransforms;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    public void addTransformation(Transformation transformation) {
        if (this.queryTransformations == null) {
            this.queryTransformations = new HashMap<>();
        }
        List<Transformation> transformations;
        if (this.queryTransformations.containsKey(getCurrentRunNumber())) {
            transformations = this.queryTransformations.get(getCurrentRunNumber());
        } else {
            transformations = new ArrayList<>();
        }
        transformations.add(transformation);
        this.queryTransformations.put(getCurrentRunNumber(), transformations);
        setCurrentQuery(getQueryTransforms().get(getCurrentRunNumber()).get(getQueryTransforms().get(getCurrentRunNumber()).size() - 1).getOutputQuery());
        if (transformation.isChanged()) {
            setChanged(true);
        }
    }

    public Boolean isQueryChangedByRun(int numberOfRuns) {
        List<Transformation> transformationsByRun = this.getQueryTransforms().get(numberOfRuns);
        if (transformationsByRun == null) {
            return false;
        }
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
