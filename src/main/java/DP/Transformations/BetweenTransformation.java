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
        final List<ConditionItem> betweenConditions = TSqlParseWalker.findBetweenConditions(metadata, select);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);

        boolean currentNecessary;
        boolean currentColumn;
        for (int i = 0; i < betweenConditions.size(); i += 2) {
            currentNecessary = false;
            currentColumn = false;
            for (int j = i; j <= i + 1; j++) {
                currentColumn |= betweenConditions.get(j).getLeftSideDataType() != ConditionDataType.COLUMN && betweenConditions.get(j).getRightSideDataType() != ConditionDataType.COLUMN;
                if ((betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.BINARY && Arrays.asList(ConditionDataType.BINARY, ConditionDataType.DECIMAL).contains(betweenConditions.get(j).getRightSideDataType())) ||
                        (betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.DECIMAL && (betweenConditions.get(j).getRightSideDataType().isNumeric || betweenConditions.get(j).getRightSideDataType() == ConditionDataType.STRING_DECIMAL)) ||
                        (betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.FLOAT && ((betweenConditions.get(j).getRightSideDataType().isNumeric && betweenConditions.get(j).getRightSideDataType() != ConditionDataType.BINARY) || (Arrays.asList(ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT).contains(betweenConditions.get(j).getRightSideDataType())))) ||
                        (betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.REAL && !Arrays.asList(ConditionDataType.BINARY, ConditionDataType.STRING_BINARY).contains(betweenConditions.get(j).getRightSideDataType())) ||
                        (betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.STRING_DECIMAL && betweenConditions.get(j).getRightSideDataType() != ConditionDataType.BINARY) ||
                        (betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.STRING_FLOAT && Arrays.asList(ConditionDataType.REAL, ConditionDataType.FLOAT).contains(betweenConditions.get(j).getRightSideDataType())) ||
                        (betweenConditions.get(j).getLeftSideDataType() == ConditionDataType.STRING_REAL && betweenConditions.get(j).getRightSideDataType() == ConditionDataType.REAL)) {
                    currentNecessary |= betweenConditions.get(j).compareNumberAgainstNumber();
                } else if (betweenConditions.get(j).getRightSideDataType() == ConditionDataType.STRING &&
                        Arrays.asList(ConditionDataType.STRING_BINARY, ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT, ConditionDataType.STRING_REAL, ConditionDataType.STRING)
                                .contains(betweenConditions.get(j).getLeftSideDataType())) {
                    currentNecessary |= betweenConditions.get(j).compareStringAgainstString();
                } else if (betweenConditions.get(j).getRightSideDataType() == ConditionDataType.COLUMN && betweenConditions.get(j).getRightSideDataType() == ConditionDataType.COLUMN) {
                    DatabaseMetadata newMetadata = metadata.withTables(allTables);
                    currentNecessary |= betweenConditions.get(j).compareColumnAgainstColumn(newMetadata);
                } else {
                    currentNecessary = true;
                }
            }
            if (!currentNecessary) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        (query.getCurrentQuery().substring(0, betweenConditions.get(i).getStartAt()) + query.getCurrentQuery().substring(betweenConditions.get(i).getStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN",
                        Action.BetweenTransformation,
                        true,
                        null
                ));
                return query;
            } else if (currentColumn) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        QueryHandler.restoreSpaces(query.getCurrentQuery().substring(betweenConditions.get(i).getStartAt()) + query.getCurrentQuery().substring(betweenConditions.get(i).getStopAt()), betweenConditions.get(i).getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                        Action.BetweenTransformation,
                        false,
                        null
                ));
                return query;
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
