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
        return query.getOutputQuery().contains("LIKE");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getOutputQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ConditionItem> likeConditions = ConditionItem.filterByOperator(conditions, ConditionOperator.LIKE);

        DatabaseMetadata newMetadata = metadata.withTables(allTables);

        for (ConditionItem condition : likeConditions) {
            if (condition.isNot() || condition.getOperatorType().equals(ConditionOperator.SAMPLE)) continue;
            if (condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN && !SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue())) {
                query.addTransformation(new Transformation(query.getOutputQuery(),
                        query.getOutputQuery(),
                        restoreSpaces(query.getOutputQuery().substring(condition.getStartAt()) + query.getOutputQuery().substring(condition.getStopAt()), condition.getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                        Action.LikeTransformation,
                        false,
                        null
                ));
                return query;
            } else if ((condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN && SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue()))
                    || (condition.getLeftSideDataType() == ConditionDataType.COLUMN &&
                    (condition.getRightSideDataType() == ConditionDataType.COLUMN || condition.getRightSideDataType() == ConditionDataType.STRING))
                    && ((condition.getRightSideDataType() == ConditionDataType.STRING && condition.getRightSideValue().matches("^[%]+$")) || (newMetadata.columnsEqual(condition.getLeftSideColumnItem(), condition.getRightSideColumnItem())))) {

                return Transformation.addNewTransformationBasedOnLogicalOperator(query, condition, conditions.size(), Action.LikeTransformation, "LIKE");
            }
        }
        query.addTransformation(new Transformation(query.getOutputQuery(),
                query.getOutputQuery(),
                "OK",
                Action.LikeTransformation,
                false,
                null
        ));
        return query;
    }
}
