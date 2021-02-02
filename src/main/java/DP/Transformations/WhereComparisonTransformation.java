package DP.Transformations;

import DP.Database.*;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Arrays;
import java.util.List;

public class WhereComparisonTransformation extends QueryHandler {

    public WhereComparisonTransformation(QueryHandler handler) {
        super(handler);
    }

    @Override
    public void handleQuery(Query query) {
        super.handleQuery(query);
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = TSqlParseWalker.findConditions(metadata, select);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);

        boolean isConditionNecessary = true;
        for (ConditionItem condition : conditions) {
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
                query.addTransform(new Transform(query.getCurrentQuery(),
                        (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION",
                        "runEqualConditionInComparisonOperators",
                        true
                ));
                query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size()-1).getOutputQuery());
                query.setChanged(true);
                return query;
            }
        }
        query.addTransform(new Transform(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                "runEqualConditionInComparisonOperators",
                false
        ));
        query.setChanged(false);
        return query;
    }
}
