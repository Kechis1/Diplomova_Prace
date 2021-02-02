package DP.Transformations;

import DP.Database.DatabaseMetadata;

public class TransformationBuilder {
    private QueryHandler chain;

    public TransformationBuilder(final DatabaseMetadata databaseMetadata) {
        buildChain(databaseMetadata);
    }

    private void buildChain(final DatabaseMetadata databaseMetadata) {
        chain = new WhereComparisonTransformation(
                new SelectClauseTransformation(
                        new LikeTransformation(
                                new ExistsTransformation(
                                        new GroupByTransformation(
                                                new JoinTableTransformation(
                                                        new JoinConditionTransformation(null, databaseMetadata),
                                                        databaseMetadata),
                                                databaseMetadata),
                                        databaseMetadata),
                                databaseMetadata),
                        databaseMetadata),
                databaseMetadata);
    }

    public void makeQuery(Query query) {
        chain.handleQuery(query);
    }
}
