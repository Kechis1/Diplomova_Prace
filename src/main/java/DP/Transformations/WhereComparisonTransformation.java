package DP.Transformations;

import DP.Database.*;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Arrays;
import java.util.List;

public class WhereComparisonTransformation extends QueryHandler {
    public WhereComparisonTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("WHERE");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ConditionItem> whereConditions = ConditionItem.filterByOperator(conditions, ConditionOperator.AO);

        for (ConditionItem condition : whereConditions) {
            boolean isConditionNecessary = true;
            if ((condition.getLeftSideDataType() == ConditionDataType.BINARY && Arrays.asList(ConditionDataType.BINARY, ConditionDataType.DECIMAL).contains(condition.getRightSideDataType())) ||
                    (condition.getLeftSideDataType() == ConditionDataType.DECIMAL && (condition.getRightSideDataType().isNumeric || condition.getRightSideDataType() == ConditionDataType.STRING_DECIMAL)) ||
                    (condition.getLeftSideDataType() == ConditionDataType.FLOAT && ((condition.getRightSideDataType().isNumeric && condition.getRightSideDataType() != ConditionDataType.BINARY) || (Arrays.asList(ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT).contains(condition.getRightSideDataType())))) ||
                    (condition.getLeftSideDataType() == ConditionDataType.REAL && !Arrays.asList(ConditionDataType.BINARY, ConditionDataType.STRING_BINARY).contains(condition.getRightSideDataType())) ||
                    (condition.getLeftSideDataType() == ConditionDataType.STRING_DECIMAL && condition.getRightSideDataType() != ConditionDataType.BINARY) ||
                    (condition.getLeftSideDataType() == ConditionDataType.STRING_FLOAT && Arrays.asList(ConditionDataType.REAL, ConditionDataType.FLOAT).contains(condition.getRightSideDataType())) ||
                    (condition.getLeftSideDataType() == ConditionDataType.STRING_REAL && condition.getRightSideDataType() == ConditionDataType.REAL)) {
                isConditionNecessary = condition.compareNumberAgainstNumber();
            } else if (condition.getRightSideDataType() == ConditionDataType.STRING &&
                    Arrays.asList(ConditionDataType.STRING_BINARY, ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT, ConditionDataType.STRING_REAL, ConditionDataType.STRING)
                            .contains(condition.getLeftSideDataType())) {
                isConditionNecessary = condition.compareStringAgainstString();
            } else if (condition.getRightSideDataType() == ConditionDataType.COLUMN && condition.getRightSideDataType() == ConditionDataType.COLUMN) {
                DatabaseMetadata newMetadata = metadata.withTables(allTables);
                isConditionNecessary = condition.compareColumnAgainstColumn(newMetadata);
            }

            if (!isConditionNecessary) {
                return Transformation.addNewTransformationBasedOnLogicalOperator(query, condition, conditions.size(), Action.WhereComparisonTransformation, "WHERE CONDITION");
            } else if (condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        QueryHandler.restoreSpaces(query.getCurrentQuery().substring(condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt()), condition.getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                        Action.WhereComparisonTransformation,
                        false,
                        null
                ));
                return query;
            }
        }
        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.WhereComparisonTransformation,
                false,
                null
        ));
        return query;
    }
}
