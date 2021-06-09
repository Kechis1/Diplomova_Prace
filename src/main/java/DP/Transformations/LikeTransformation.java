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
        final Integer[] conditionSize = {0};

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                for (TSqlParser.Search_condition_andContext and : ctx.search_condition_and()) {
                    for (TSqlParser.Search_condition_notContext not : and.search_condition_not()) {
                        conditionSize[0] = conditionSize[0] + 1;
                    }
                }
                conditions.addAll(ConditionItem.createFromLike(ctx, metadata));
            }
        }, select);

        DatabaseMetadata newMetadata = metadata.withTables(allTables);
        System.out.println(conditionSize[0]);

        for (ConditionItem condition : conditions) {

            System.out.println(condition);
            if (condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN && !SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue())) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        restoreSpaces(query.getCurrentQuery().substring(condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt()), condition.getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                        Action.LikeTransformation,
                        false,
                        null
                ));
                return query;
            } else if ((condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN && SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue()))
                    || (condition.getLeftSideDataType() == ConditionDataType.COLUMN &&
                    (condition.getRightSideDataType() == ConditionDataType.COLUMN || condition.getRightSideDataType() == ConditionDataType.STRING))
                    && ((condition.getRightSideDataType() == ConditionDataType.STRING && condition.getRightSideValue().matches("^[%]+$")) || (newMetadata.columnsEqual(condition.getLeftSideColumnItem(), condition.getRightSideColumnItem())))) {

                if ((condition.getLeftLogicalOperator() != null && condition.getLeftLogicalOperator().equals("OR")) || (condition.getRightLogicalOperator() != null && condition.getRightLogicalOperator().equals("OR"))) {
                    query.addTransformation(new Transformation(query.getCurrentQuery(),
                            query.getCurrentQuery(),
                            "OK",
                            Action.LikeTransformation,
                            false,
                            null
                    ));
                    return query;
                }

                String newQuery;

                if (conditionSize[0] == 1) {
                    newQuery = (query.getCurrentQuery().substring(0, (query.getCurrentQuery().substring(0, condition.getStartAt()).lastIndexOf("WHERE"))) + query.getCurrentQuery().substring(condition.getStopAt()).trim()).trim();
                } else {
                    if (condition.getRightLogicalOperator() != null) {
                        newQuery = (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getRightLogicalOperatorStopAt() + 2)).trim();
                    } else if (condition.getLeftLogicalOperator() != null) {
                        newQuery = (query.getCurrentQuery().substring(0, condition.getLeftLogicalOperatorStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim();
                    } else {
                        newQuery = (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim();
                    }
                }

                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        newQuery,
                        UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                        Action.LikeTransformation,
                        true,
                        null
                ));
                return query;
            }
        }
        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.LikeTransformation,
                false,
                null
        ));
        return query;
    }
}
