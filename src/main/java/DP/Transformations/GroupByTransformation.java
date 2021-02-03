package DP.Transformations;

import DP.Database.AggregateItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class GroupByTransformation extends QueryHandler {
    private final String action = "GroupByTransformation";

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

        final ArrayList<AggregateItem> allAggregateFunctions = new ArrayList<>();
        final List<AggregateItem> aggregateFunctionsInSelect = TSqlParseWalker.findAggregateFunctionsInSelect(select);
        final ArrayList<String> columnsInGroupBy = new ArrayList<>();
        final ArrayList<DatabaseTable> joinTables = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterGroup_by_item(@NotNull TSqlParser.Group_by_itemContext ctx) {
                if (ctx.expression().full_column_name() != null) {
                    columnsInGroupBy.add(ctx.expression().full_column_name().column_name.getText());
                }
            }

            @Override
            public void exitTable_source_item(@NotNull TSqlParser.Table_source_itemContext ctx) {
                joinTables.add(DatabaseTable.create(metadata, ctx));
            }

            @Override
            public void enterAggregate_windowed_function(@NotNull TSqlParser.Aggregate_windowed_functionContext ctx) {
                allAggregateFunctions.add(new AggregateItem(ctx.getStart().getStartIndex(),
                        ctx.getStop().getStopIndex() + 1,
                        ctx.getChild(2).getText(),
                        ctx.STAR() != null
                                ? "*"
                                : ctx.all_distinct_expression().expression().full_column_name().column_name.getText(),
                        ctx.getChild(0).getText()));
            }
        }, select);

        DatabaseMetadata newMetadata = metadata.withTables(joinTables);

        if (allAggregateFunctions.isEmpty()) {
            if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys())) {
                query.addTransform(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery().substring(0, query.getCurrentQuery().indexOf("GROUP BY")).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY",
                        action,
                        true
                ));
                query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().size() - 1).getOutputQuery());
                query.setChanged(true);
                return query;
            }
            query.addTransform(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    "OK",
                    action,
                    false
            ));
            query.setChanged(false);
            return query;
        }

        if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys()) && !aggregateFunctionsInSelect.isEmpty()) {
            for (AggregateItem item : aggregateFunctionsInSelect) {
                Transformation transform;
                if (item.getFunctionName().equals("COUNT")) {
                    transform = new Transformation(query.getCurrentQuery(),
                            (query.getCurrentQuery().substring(0, item.getStartAt()) + "1" + query.getCurrentQuery().substring(item.getStopAt() + 1)).trim(),
                            item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1",
                            action,
                            true
                    );
                } else {
                    transform = new Transformation(query.getCurrentQuery(),
                            (query.getCurrentQuery().substring(0, item.getStartAt()) + item.getFullColumnName() + query.getCurrentQuery().substring(item.getStopAt() + 1)).trim(),
                            item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " " + item.getFullColumnName(),
                            action,
                            true
                    );
                }
                query.addTransform(transform);
            }
            query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().size() - 1).getOutputQuery());
            query.setChanged(true);
            return query;
        }

        query.addTransform(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                action,
                false
        ));
        query.setChanged(false);
        return query;
    }
}
