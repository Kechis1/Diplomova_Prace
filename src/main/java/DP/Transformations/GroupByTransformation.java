package DP.Transformations;

import DP.Database.AggregateItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;

public class GroupByTransformation extends QueryHandler {
    public GroupByTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("GROUP BY");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();

        final List<AggregateItem> allAggregateFunctions = TSqlParseWalker.findAllAggregateFunctions(select);
        final List<AggregateItem> aggregateFunctionsInSelect = TSqlParseWalker.findAggregateFunctionsInSelect(select);
        final List<String> columnsInGroupBy = TSqlParseWalker.findAllColumnsInGroupBy(select);
        final List<DatabaseTable> joinTables = TSqlParseWalker.findTablesList(metadata, select);

        DatabaseMetadata newMetadata = metadata.withTables(joinTables);

        if (allAggregateFunctions.isEmpty()) {
            if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys())) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery().substring(0, query.getCurrentQuery().indexOf("GROUP BY")).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY",
                        Action.GroupByTransformation,
                        true,
                        null
                ));
                return query;
            }
            query.addTransformation(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    "OK",
                    Action.GroupByTransformation,
                    false,
                    null
            ));
            return query;
        }

        if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys()) && !aggregateFunctionsInSelect.isEmpty()) {
            for (AggregateItem item : aggregateFunctionsInSelect) {
                Transformation transform;
                if (item.getFunctionName().equals("COUNT")) {
                    transform = new Transformation(query.getCurrentQuery(),
                            (query.getCurrentQuery().substring(0, item.getStartAt()) + "1" + query.getCurrentQuery().substring(item.getStopAt())).trim(),
                            item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1",
                            Action.GroupByTransformation,
                            true,
                            null
                    );
                } else {
                    transform = new Transformation(query.getCurrentQuery(),
                            (query.getCurrentQuery().substring(0, item.getStartAt()) + item.getFullColumnName() + query.getCurrentQuery().substring(item.getStopAt())).trim(),
                            item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " " + item.getFullColumnName(),
                            Action.GroupByTransformation,
                            true,
                            null
                    );
                }
                query.addTransformation(transform);
            }
            query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).getOutputQuery());
            query.setChanged(true);
            return query;
        }

        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.GroupByTransformation,
                false,
                null
        ));
        return query;
    }
}
