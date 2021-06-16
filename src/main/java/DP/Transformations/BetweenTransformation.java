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
import java.util.Arrays;
import java.util.List;

public class BetweenTransformation extends QueryHandler {
    public BetweenTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("BETWEEN");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<ConditionItem> betweenConditions = ConditionItem.filterByOperator(conditions, ConditionOperator.BETWEEN);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);

        boolean currentNecessary;
        boolean currentColumn;
        for (ConditionItem condition: betweenConditions) {
            if (condition.isNot() || condition.getOperatorType().equals(ConditionOperator.SAMPLE)) continue;
            currentNecessary = false;
            currentColumn = false;
            for (ConditionItem betweenCondition: Arrays.asList(condition.getBetweenLeftCondition(), condition.getBetweenRightCondition())) {
                currentColumn |= betweenCondition.getLeftSideDataType() != ConditionDataType.COLUMN && betweenCondition.getRightSideDataType() != ConditionDataType.COLUMN;
                if ((betweenCondition.getLeftSideDataType() == ConditionDataType.BINARY && Arrays.asList(ConditionDataType.BINARY, ConditionDataType.DECIMAL).contains(betweenCondition.getRightSideDataType())) ||
                        (betweenCondition.getLeftSideDataType() == ConditionDataType.DECIMAL && (betweenCondition.getRightSideDataType().isNumeric || betweenCondition.getRightSideDataType() == ConditionDataType.STRING_DECIMAL)) ||
                        (betweenCondition.getLeftSideDataType() == ConditionDataType.FLOAT && ((betweenCondition.getRightSideDataType().isNumeric && betweenCondition.getRightSideDataType() != ConditionDataType.BINARY) || (Arrays.asList(ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT).contains(betweenCondition.getRightSideDataType())))) ||
                        (betweenCondition.getLeftSideDataType() == ConditionDataType.REAL && !Arrays.asList(ConditionDataType.BINARY, ConditionDataType.STRING_BINARY).contains(betweenCondition.getRightSideDataType())) ||
                        (betweenCondition.getLeftSideDataType() == ConditionDataType.STRING_DECIMAL && betweenCondition.getRightSideDataType() != ConditionDataType.BINARY) ||
                        (betweenCondition.getLeftSideDataType() == ConditionDataType.STRING_FLOAT && Arrays.asList(ConditionDataType.REAL, ConditionDataType.FLOAT).contains(betweenCondition.getRightSideDataType())) ||
                        (betweenCondition.getLeftSideDataType() == ConditionDataType.STRING_REAL && betweenCondition.getRightSideDataType() == ConditionDataType.REAL)) {
                    currentNecessary |= betweenCondition.compareNumberAgainstNumber();
                } else if (betweenCondition.getRightSideDataType() == ConditionDataType.STRING &&
                        Arrays.asList(ConditionDataType.STRING_BINARY, ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT, ConditionDataType.STRING_REAL, ConditionDataType.STRING)
                                .contains(betweenCondition.getLeftSideDataType())) {
                    currentNecessary |= betweenCondition.compareStringAgainstString();
                } else if (betweenCondition.getRightSideDataType() == ConditionDataType.COLUMN && betweenCondition.getRightSideDataType() == ConditionDataType.COLUMN) {
                    DatabaseMetadata newMetadata = metadata.withTables(allTables);
                    currentNecessary |= betweenCondition.compareColumnAgainstColumn(newMetadata);
                } else {
                    currentNecessary = true;
                }
            }

            if (currentNecessary && currentColumn) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        QueryHandler.restoreSpaces(query.getCurrentQuery().substring(condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt()), condition.getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                        Action.BetweenTransformation,
                        false,
                        null
                ));
                return query;
            } else if (!currentNecessary) {
                return Transformation.addNewTransformationBasedOnLogicalOperator(query, condition, conditions.size(), Action.BetweenTransformation, "BETWEEN");
            }
        }

        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.BetweenTransformation,
                false,
                null
        ));
        return query;
    }
}
