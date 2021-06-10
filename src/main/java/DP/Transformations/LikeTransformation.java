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
        final List<ConditionItem> conditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ConditionItem> likeConditions = ConditionItem.filterByOperator(conditions, ConditionOperator.LIKE);

        DatabaseMetadata newMetadata = metadata.withTables(allTables);

        for (ConditionItem condition : likeConditions) {

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
                            "LIKE " + UnnecessaryStatementException.messageConditionIsAlwaysTrue,
                            Action.LikeTransformation,
                            false,
                            null
                    ));
                    return query;
                }

                String newQuery;

                if (conditions.size() == 1) {
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
