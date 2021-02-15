package DP.Transformations;

import DP.Database.DatabaseMetadata;

public class TransformationBuilder {
    private QueryHandler chain;

    public TransformationBuilder(final DatabaseMetadata databaseMetadata) {
        buildChain(databaseMetadata);
    }

    public TransformationBuilder(QueryHandler chain) {
        this.chain = chain;
    }

    private void buildChain(final DatabaseMetadata databaseMetadata) {
        this.chain = new ExistsTransformation(
                new GroupByTransformation(
                        new JoinTableTransformation(
                                new JoinConditionTransformation(
                                        new BetweenTransformation(
                                                new LikeTransformation(new WhereComparisonTransformation(
                                                        new SelectClauseTransformation(
                                                                null,
                                                                databaseMetadata),
                                                        databaseMetadata),
                                                        databaseMetadata),
                                                databaseMetadata),
                                        databaseMetadata),
                                databaseMetadata),
                        databaseMetadata),
                databaseMetadata);
    }

    public void makeQuery(Query query) {
        int numberOfRuns = 0;
        do {
            query.setCurrentRunNumber(++numberOfRuns);
            query.addRun(numberOfRuns, false);
            chain.handleQuery(query);
            query.getRuns().put(numberOfRuns, query.isQueryChangedByRun(numberOfRuns));
        } while (query.getRuns().get(numberOfRuns));
    }
}
