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
        final List<ConditionItem> conditions = new ArrayList<>();
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                if (ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().BETWEEN() != null) {
                    ConditionItem item = new ConditionItem(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getStart().getStartIndex(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getStop().getStopIndex() + 1,
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                            ">=",
                            ConditionOperator.BETWEEN,
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getText(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0).getText(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1).getText()
                    );

                    if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                        item.setLeftSideColumnItem(ColumnItem.findOrCreate(metadata, ctx.search_condition_and().get(0).search_condition_not().get(0), 0));
                        item.setRightSideColumnItem(ColumnItem.findOrCreate(metadata, ctx.search_condition_and().get(0).search_condition_not().get(0), 1));
                    }

                    item.setStartAt(ctx.search_condition_and().get(0).search_condition_not().get(0).getStart().getStartIndex());
                    item.setStopAt(ctx.search_condition_and().get(0).search_condition_not().get(0).getStop().getStopIndex());

                    conditions.add(item);

                    item = new ConditionItem(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getStart().getStartIndex(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getStop().getStopIndex() + 1,
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(2)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(2)),
                            "<=",
                            ConditionOperator.BETWEEN,
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getText(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0).getText(),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(2).getText()
                    );

                    item.setStartAt(ctx.search_condition_and().get(0).search_condition_not().get(0).getStart().getStartIndex());
                    item.setStopAt(ctx.search_condition_and().get(0).search_condition_not().get(0).getStop().getStopIndex());

                    if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                        item.setLeftSideColumnItem(ColumnItem.findOrCreate(metadata, ctx.search_condition_and().get(0).search_condition_not().get(0), 0));
                        item.setRightSideColumnItem(ColumnItem.findOrCreate(metadata, ctx.search_condition_and().get(0).search_condition_not().get(0), 2));
                    }

                    conditions.add(item);
                }
            }
        }, select);

        boolean currentNecessary;
        boolean currentColumn;
        for (int i = 0; i < conditions.size(); i += 2) {
            currentNecessary = false;
            currentColumn = false;
            for (int j = i; j <= i + 1; j++) {
                currentColumn |= conditions.get(j).getLeftSideDataType() != ConditionDataType.COLUMN && conditions.get(j).getRightSideDataType() != ConditionDataType.COLUMN;
                if ((conditions.get(j).getLeftSideDataType() == ConditionDataType.BINARY && Arrays.asList(ConditionDataType.BINARY, ConditionDataType.DECIMAL).contains(conditions.get(j).getRightSideDataType())) ||
                        (conditions.get(j).getLeftSideDataType() == ConditionDataType.DECIMAL && (conditions.get(j).getRightSideDataType().isNumeric || conditions.get(j).getRightSideDataType() == ConditionDataType.STRING_DECIMAL)) ||
                        (conditions.get(j).getLeftSideDataType() == ConditionDataType.FLOAT && ((conditions.get(j).getRightSideDataType().isNumeric && conditions.get(j).getRightSideDataType() != ConditionDataType.BINARY) || (Arrays.asList(ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT).contains(conditions.get(j).getRightSideDataType())))) ||
                        (conditions.get(j).getLeftSideDataType() == ConditionDataType.REAL && !Arrays.asList(ConditionDataType.BINARY, ConditionDataType.STRING_BINARY).contains(conditions.get(j).getRightSideDataType())) ||
                        (conditions.get(j).getLeftSideDataType() == ConditionDataType.STRING_DECIMAL && conditions.get(j).getRightSideDataType() != ConditionDataType.BINARY) ||
                        (conditions.get(j).getLeftSideDataType() == ConditionDataType.STRING_FLOAT && Arrays.asList(ConditionDataType.REAL, ConditionDataType.FLOAT).contains(conditions.get(j).getRightSideDataType())) ||
                        (conditions.get(j).getLeftSideDataType() == ConditionDataType.STRING_REAL && conditions.get(j).getRightSideDataType() == ConditionDataType.REAL)) {
                    currentNecessary |= conditions.get(j).compareNumberAgainstNumber();
                } else if (conditions.get(j).getRightSideDataType() == ConditionDataType.STRING &&
                        Arrays.asList(ConditionDataType.STRING_BINARY, ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT, ConditionDataType.STRING_REAL, ConditionDataType.STRING)
                                .contains(conditions.get(j).getLeftSideDataType())) {
                    currentNecessary |= conditions.get(j).compareStringAgainstString();
                } else if (conditions.get(j).getRightSideDataType() == ConditionDataType.COLUMN && conditions.get(j).getRightSideDataType() == ConditionDataType.COLUMN) {
                    DatabaseMetadata newMetadata = metadata.withTables(allTables);
                    currentNecessary |= conditions.get(j).compareColumnAgainstColumn(newMetadata);
                } else {
                    currentNecessary = true;
                }
            }
            if (!currentNecessary) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        (query.getCurrentQuery().substring(0, conditions.get(i).getStartAt()) + query.getCurrentQuery().substring(conditions.get(i).getStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN",
                        Action.BetweenTransformation,
                        true,
                        null
                ));
                return query;
            } else if (currentColumn) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        QueryHandler.restoreSpaces(query.getCurrentQuery().substring(conditions.get(i).getStartAt()) + query.getCurrentQuery().substring(conditions.get(i).getStopAt()), conditions.get(i).getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
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
