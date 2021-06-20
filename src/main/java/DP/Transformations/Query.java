package DP.Transformations;

import java.util.*;

public class Query {
    private String originalQuery;
    private String inputQuery;
    private String outputQuery;
    private HashMap<Integer, List<Transformation>> queryTransformations;
    private HashMap<Integer, Boolean> runs;
    private int currentRunNumber;
    private boolean changed = false;

    public Query(String originalQuery, String inputQuery, String outputQuery) {
        this.originalQuery = originalQuery;
        this.inputQuery = QueryHandler.restoreConstants(originalQuery, inputQuery);
        this.outputQuery = outputQuery;
    }

    public boolean ignoredOperatorExists(Action action, String find) {
        for (int i = 1; i <= currentRunNumber; i++) {
            if (queryTransformations == null || queryTransformations.get(i) == null) continue;
            for (Transformation t: queryTransformations.get(i)) {
                if (t.getOperatorTransformation() == null || !t.getAction().equals(action)) continue;
                if (t.getOperatorTransformation().isIgnored() && find.equals(t.getOperatorTransformation().getTo())) {
                    return true;
                }
            }
        }
        return false;
    }

    public String getInputQuery() {
        return inputQuery;
    }

    public void setInputQuery(String inputQuery) {
        this.inputQuery = inputQuery;
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

    public String getOutputQuery() {
        return outputQuery;
    }

    public void setOutputQuery(String outputQuery) {
        this.outputQuery = outputQuery;
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
        transformation.setInputQuery(QueryHandler.restoreConstants(getOriginalQuery(), transformation.getInputQuery()));
        transformation.setOutputQuery(QueryHandler.restoreConstants(getOriginalQuery(), transformation.getOutputQuery()));
        transformations.add(transformation);
        this.queryTransformations.put(getCurrentRunNumber(), transformations);
        setOutputQuery(getQueryTransforms().get(getCurrentRunNumber()).get(getQueryTransforms().get(getCurrentRunNumber()).size() - 1).getOutputQuery());
        if (transformation.isChanged()) {
            setChanged(true);
        }
    }

    public Boolean isQueryChangedByRun(int numberOfRuns) {
        if (this.getQueryTransforms() == null || this.getQueryTransforms().isEmpty()) {
            return false;
        }
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
                ", inputQuery='" + inputQuery + '\'' +
                ", outputQuery='" + outputQuery + '\'' +
                ", runs=" + runs +
                ", currentRunNumber=" + currentRunNumber +
                ", changed=" + changed +
                '}';
    }
}
