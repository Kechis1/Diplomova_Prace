package DP.Transformations;

import java.util.ArrayList;
import java.util.List;

public class Query {
    String originalQuery;
    String currentQuery;
    List<Transformation> queryTransforms;
    boolean changed;

    public Query(String originalQuery, String currentQuery) {
        this.originalQuery = originalQuery;
        this.currentQuery = currentQuery;
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

    public List<Transformation> getQueryTransforms() {
        return queryTransforms;
    }

    public void setQueryTransforms(List<Transformation> queryTransforms) {
        this.queryTransforms = queryTransforms;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    public void addTransform(Transformation transform) {
        if (this.queryTransforms == null) {
            this.queryTransforms = new ArrayList<>();
        }
        queryTransforms.add(transform);
    }

    @Override
    public String toString() {
        return "Query{" +
                "originalQuery='" + originalQuery + '\'' +
                ", currentQuery='" + currentQuery + '\'' +
                ", queryTransforms=" + queryTransforms +
                ", changed=" + changed +
                '}';
    }

}
