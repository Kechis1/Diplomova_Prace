package DP.Transformations;

import DP.Database.*;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class LikeTransformation extends QueryHandler {
    private final String action = "LikeTransformation";

    public LikeTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("LIKE");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = new ArrayList<>();
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                if (ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE() != null) {
                    ConditionItem item = new ConditionItem(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getStart().getStartIndex(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getStop().getStopIndex() + 1,
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE().getText(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getText()
                    );

                    if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                        item.setLeftSideColumnItem(ColumnItem.findOrCreate(metadata, ctx.search_condition_and().get(0).search_condition_not().get(0), 0));
                        item.setRightSideColumnItem(ColumnItem.findOrCreate(metadata, ctx.search_condition_and().get(0).search_condition_not().get(0), 1));
                    }
                    conditions.add(item);
                }
            }
        }, select);

        DatabaseMetadata newMetadata = metadata.withTables(allTables);

        for (ConditionItem condition : conditions) {
            if (condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN) {
                if (SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue())) {
                    query.addTransform(new Transformation(query.getCurrentQuery(),
                            (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim(),
                            UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                            action,
                            true
                    ));
                    query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size()-1).getOutputQuery());
                    query.setChanged(true);
                    return query;
                } else {
                    query.addTransform(new Transformation(query.getCurrentQuery(),
                            query.getCurrentQuery(),
                            QueryHandler.restoreSpaces(query.getCurrentQuery().substring(condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt()), condition.getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                            action,
                            false
                    ));
                    query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).getOutputQuery());
                    query.setChanged(false);
                    return query;
                }
            } else if (condition.getLeftSideDataType() == ConditionDataType.COLUMN &&
                    (condition.getRightSideDataType() == ConditionDataType.COLUMN || condition.getRightSideDataType() == ConditionDataType.STRING)) {
                if (condition.getRightSideDataType() == ConditionDataType.STRING) {
                    if (condition.getRightSideValue().matches("^[%]+$")) {
                        query.addTransform(new Transformation(query.getCurrentQuery(),
                                (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim(),
                                UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                                action,
                                true
                        ));
                        query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size()-1).getOutputQuery());
                        query.setChanged(true);
                        return query;
                    }
                } else if (newMetadata.columnsEqual(condition.getLeftSideColumnItem(), condition.getRightSideColumnItem())) {
                    query.addTransform(new Transformation(query.getCurrentQuery(),
                            (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim(),
                            UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                            action,
                            true
                    ));
                    query.setCurrentQuery(query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size()-1).getOutputQuery());
                    query.setChanged(true);
                    return query;
                }
            }
        }
        query.addTransform(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                action,
                false
        ));
        return query;
    }
}
