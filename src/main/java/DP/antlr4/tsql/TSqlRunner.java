package DP.antlr4.tsql;

import DP.Database.*;
import DP.Database.Respond.Respond;
import DP.Database.Respond.Transform;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.jetbrains.annotations.NotNull;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.*;

import java.util.*;

public class TSqlRunner {
    public static Respond runGroupBy(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
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
                respond.addTransform(new Transform(respond.getCurrentQuery(),
                        respond.getCurrentQuery().substring(0, respond.getCurrentQuery().indexOf("GROUP BY")).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY",
                        "runGroupBy",
                        true
                ));
                respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                respond.setChanged(true);
                return respond;
            }
            respond.addTransform(new Transform(respond.getCurrentQuery(),
                    respond.getCurrentQuery(),
                    "OK",
                    "runGroupBy",
                    false
            ));
            respond.setChanged(false);
            return respond;
        }

        if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys()) && !aggregateFunctionsInSelect.isEmpty()) {
            for (AggregateItem item : aggregateFunctionsInSelect) {
                Transform transform;
                if (item.getFunctionName().equals("COUNT")) {
                    transform = new Transform(respond.getCurrentQuery(),
                            (respond.getCurrentQuery().substring(0, item.getStartAt()) + "1" + respond.getCurrentQuery().substring(item.getStopAt() + 1)).trim(),
                            item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1",
                            "runGroupBy",
                            true
                    );
                } else {
                    transform = new Transform(respond.getCurrentQuery(),
                            (respond.getCurrentQuery().substring(0, item.getStartAt()) + item.getFullColumnName() + respond.getCurrentQuery().substring(item.getStopAt() + 1)).trim(),
                            item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " " + item.getFullColumnName(),
                            "runGroupBy",
                            true
                    );
                }
                respond.addTransform(transform);
            }
            respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
            respond.setChanged(true);
            return respond;
        }

        respond.addTransform(new Transform(respond.getCurrentQuery(),
                respond.getCurrentQuery(),
                "OK",
                "runGroupBy",
                false
        ));
        respond.setChanged(false);
        return respond;
    }

    public static Respond runSelectClause(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ExistItem> foundExistsNotConstant = new ArrayList<>();
        final List<Boolean> foundUnion = new ArrayList<>();
        DatabaseMetadata metadataWithTables = metadata.withTables(allTables);
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition_and(TSqlParser.Search_condition_andContext ctx) {
                for (int i = 0; i < ctx.search_condition_not().size(); i++) {
                    if (ctx.search_condition_not(i).predicate() != null && ctx.search_condition_not(i).predicate().EXISTS() != null && ctx.search_condition_not(i).predicate().subquery() != null &&
                            (ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().select_list_elem().size() > 1 ||
                                    ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().select_list_elem().get(0).expression_elem() == null ||
                                    ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().select_list_elem().get(0).expression_elem().expression().primitive_expression().constant() == null)) {
                        foundExistsNotConstant.add(new ExistItem(ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().getStart().getStartIndex(),
                                ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().getStop().getStopIndex()));
                    }
                }
            }

            @Override
            public void enterSql_union(TSqlParser.Sql_unionContext ctx) {
                foundUnion.add(true);
            }
        }, select);

        if (!foundExistsNotConstant.isEmpty()) {
            respond.addTransform(new Transform(respond.getCurrentQuery(),
                    (respond.getCurrentQuery().substring(0, foundExistsNotConstant.get(0).getSelectListStartAt()) + "1" + respond.getCurrentQuery().substring(foundExistsNotConstant.get(0).getSelectListStopAt() + 1)).trim(),
                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                    "runSelectClause",
                    true
            ));
            respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
            respond.setChanged(true);
            return respond;
        }

        final List<ColumnItem> allColumnsInSelect = TSqlParseWalker.findColumnsInSelect(metadataWithTables, select);

        ColumnItem equals = ColumnItem.duplicatesExists(allColumnsInSelect);
        if (equals != null) {
            respond.addTransform(new Transform(respond.getCurrentQuery(),
                    (respond.getCurrentQuery().substring(0, equals.getStartAt()) + respond.getCurrentQuery().substring(equals.getStopAt() + 1)).trim(),
                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE",
                    "runSelectClause",
                    true
            ));
            respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size() - 1).getOutputQuery());
            respond.setChanged(true);
            return respond;
        }

        for (ColumnItem item : allColumnsInSelect) {
            if (item.isConstant() && foundUnion.isEmpty()) {
                respond.addTransform(new Transform(respond.getCurrentQuery(),
                        (respond.getCurrentQuery().substring(0, item.getStartAt()) + respond.getCurrentQuery().substring(item.getStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE",
                        "runSelectClause",
                        true
                ));
                respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                respond.setChanged(true);
                return respond;
            }
        }

        final List<ConditionItem> conditions = TSqlParseWalker.findConditions(metadataWithTables, select);
        final List<ConditionItem> uniqueAttributesConditions = ConditionItem.removeMultipleAttributeConditions(conditions);

        for (ConditionItem item : conditions) {
            if (item.getOperator().equals("=") && item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                int bothInSelect = -1;
                List<ColumnItem> inSelect = new ArrayList<>();
                ColumnItem columnInSelect = null;
                for (ColumnItem column : allColumnsInSelect) {
                    if (column.equals(item.getLeftSideColumnItem()) || column.equals(item.getRightSideColumnItem())) {
                        columnInSelect = column;
                        if (column.equals(item.getLeftSideColumnItem())) {
                            inSelect.add(item.getRightSideColumnItem());
                        } else {
                            inSelect.add(item.getLeftSideColumnItem());
                        }
                        bothInSelect++;
                    }
                    if (bothInSelect == 1) {
                        respond.addTransform(new Transform(respond.getCurrentQuery(),
                                (respond.getCurrentQuery().substring(0, column.getStartAt()) + respond.getCurrentQuery().substring(column.getStopAt() + 1)).trim(),
                                UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE",
                                "runSelectClause",
                                true
                        ));
                        respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                        respond.setChanged(true);
                        return respond;
                    }
                }
                if (bothInSelect == 0) {
                    for (ConditionItem condition : conditions) {
                        if (item.getOperator().equals("=") &&
                                ((condition.getLeftSideColumnItem().equals(inSelect.get(0)) && condition.getRightSideDataType() != ConditionDataType.COLUMN) ||
                                        (condition.getRightSideColumnItem().equals(inSelect.get(0)) && condition.getLeftSideDataType() != ConditionDataType.COLUMN))) {
                            String value = condition.getLeftSideDataType() != ConditionDataType.COLUMN ? condition.getLeftSideValue() : condition.getRightSideValue();
                            respond.addTransform(new Transform(respond.getCurrentQuery(),
                                    (respond.getCurrentQuery().substring(0, columnInSelect.getStartAt()) + value + " AS " + columnInSelect.getName() + respond.getCurrentQuery().substring(columnInSelect.getStopAt() + 1)).trim(),
                                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                                    "runSelectClause",
                                    true
                            ));
                            respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                            respond.setChanged(true);
                            return respond;
                        }
                    }
                }
            }
        }

        for (ConditionItem item : uniqueAttributesConditions) {
            if (item.getOperator().equals("=") && (item.getLeftSideDataType() == ConditionDataType.COLUMN || item.getRightSideDataType() == ConditionDataType.COLUMN)) {
                for (ColumnItem column : allColumnsInSelect) {
                    if ((item.getLeftSideDataType() == ConditionDataType.COLUMN && column.equals(item.getLeftSideColumnItem()) && item.getRightSideDataType() != ConditionDataType.COLUMN) ||
                            (item.getRightSideDataType() == ConditionDataType.COLUMN && column.equals(item.getRightSideColumnItem()) && item.getLeftSideDataType() != ConditionDataType.COLUMN)) {
                        String value = item.getLeftSideDataType() != ConditionDataType.COLUMN ? item.getLeftSideValue() : item.getRightSideValue();
                        respond.addTransform(new Transform(respond.getCurrentQuery(),
                                (respond.getCurrentQuery().substring(0, column.getStartAt()) + value + " AS " + column.getName() + respond.getCurrentQuery().substring(column.getStopAt() + 1)).trim(),
                                UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                                "runSelectClause",
                                true
                        ));
                        respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                        respond.setChanged(true);
                        return respond;
                    }
                }
            }
        }

        respond.addTransform(new Transform(respond.getCurrentQuery(),
                respond.getCurrentQuery(),
                "OK",
                "runSelectClause",
                false
        ));
        respond.setChanged(false);
        return respond;
    }

    public static Respond runEqualConditionInComparisonOperators(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
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
                isConditionNecessary &= condition.compareNumberAgainstNumber();
            } else if (condition.getRightSideDataType() == ConditionDataType.STRING &&
                    Arrays.asList(ConditionDataType.STRING_BINARY, ConditionDataType.STRING_DECIMAL, ConditionDataType.STRING_FLOAT, ConditionDataType.STRING_REAL, ConditionDataType.STRING)
                            .contains(condition.getLeftSideDataType())) {
                isConditionNecessary &= condition.compareStringAgainstString();
            } else if (condition.getRightSideDataType() == ConditionDataType.COLUMN && condition.getRightSideDataType() == ConditionDataType.COLUMN) {
                DatabaseMetadata newMetadata = metadata.withTables(allTables);
                isConditionNecessary &= condition.compareColumnAgainstColumn(newMetadata);
            }
        }

        if (isConditionNecessary) {
            System.out.println("OK");
        } else {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION");
        }
        respond.setChanged(!isConditionNecessary);
        return respond;
    }

    public static Respond runRedundantJoinConditions(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> leftJoinConditions = new ArrayList<>();
        final HashMap<Integer, List<ConditionItem>> rightJoinConditions = new HashMap<>();
        final HashMap<Integer, List<ConditionItem>> fullOuterJoinConditions = new HashMap<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();
        final List<ConditionItem> whereConditions = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterJoin_part(TSqlParser.Join_partContext ctx) {
                if (ctx.LEFT() != null) {
                    leftJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.RIGHT() != null) {
                    rightJoinConditions.put(rightJoinConditions.size(), (List<ConditionItem>) TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.FULL() != null && ctx.OUTER() != null) {
                    fullOuterJoinConditions.put(fullOuterJoinConditions.size(), (List<ConditionItem>) TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else {
                    innerConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                }
            }

            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.search_condition() != null) {
                    for (TSqlParser.Search_conditionContext sCtx : ctx.search_condition()) {
                        whereConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, sCtx));
                    }
                }
            }
        }, select);

        if (!(leftJoinConditions.size() != 0 || rightJoinConditions.size() != 0 || fullOuterJoinConditions.size() != 0)) {
            innerConditions.addAll(whereConditions);
        }
        boolean foundDuplicateCondition = ConditionItem.duplicatesExists(metadata, innerConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(metadata, leftJoinConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(metadata, rightJoinConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(metadata, fullOuterJoinConditions);

        if (!foundDuplicateCondition) {
            System.out.println("OK");
        }
        respond.setChanged(foundDuplicateCondition);
        return respond;
    }

    public static Respond runRedundantJoinTables(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ColumnItem> allColumnsInSelect = TSqlParseWalker.findColumnsInSelect(metadata, select);
        final List<Boolean> isDistinctInSelect = TSqlParseWalker.findDistinctInSelect(select);
        final Map<String, List<DatabaseTable>> joinTables = TSqlParseWalker.findJoinTablesList(metadata, select);
        final List<ConditionItem> fullOuterJoinConditions = new ArrayList<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();
        final List<DatabaseTable> fromTable = TSqlParseWalker.findFromTable(metadata, select);

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterJoin_part(TSqlParser.Join_partContext ctx) {
                if (ctx.FULL() != null && ctx.OUTER() != null) {
                    fullOuterJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.LEFT() == null && ctx.RIGHT() == null) {
                    innerConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                }
            }
        }, select);

        if (isDistinctInSelect.isEmpty()) {
            System.out.println("OK");
            respond.setChanged(false);
            return respond;
        }

        boolean foundRedundantJoin = DatabaseTable.redundantJoinExists("LEFT", joinTables.get("leftJoin"),
                fromTable.get(0).getTableAlias(), fromTable.get(0), allColumnsInSelect, false, null, false);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists("RIGHT", joinTables.get("rightJoin"),
                null, null, allColumnsInSelect, false, null, false);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists("FULL OUTER", joinTables.get("fullOuterJoin"), fromTable.get(0).getTableAlias(),
                fromTable.get(0), allColumnsInSelect, true, metadata.setNullableColumns(fullOuterJoinConditions), true);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists("INNER", joinTables.get("innerJoin"), null,
                null, allColumnsInSelect, true, metadata.setNullableColumns(innerConditions), true);

        if (!foundRedundantJoin) {
            System.out.println("OK");
        }
        respond.setChanged(foundRedundantJoin);
        return respond;
    }

    public static Respond runEqualConditionInOperatorBetween(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
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
                            ">="
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
                            "<="
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
        for (int i = 0; i < conditions.size(); i += 2) {
            currentNecessary = false;
            for (int j = i; j <= i + 1; j++) {
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
                respond.addTransform(new Transform(respond.getCurrentQuery(),
                        (respond.getCurrentQuery().substring(0, conditions.get(i).getStartAt()) + respond.getCurrentQuery().substring(conditions.get(i).getStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN",
                        "runEqualConditionInOperatorBetween",
                        true
                ));
                respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                respond.setChanged(true);
                return respond;
            }
        }

        respond.addTransform(new Transform(respond.getCurrentQuery(),
                respond.getCurrentQuery(),
                "OK",
                "runEqualConditionInOperatorBetween",
                false
        ));
        respond.setChanged(false);
        return respond;
    }

    public static Respond runEqualConditionInOperatorExists(final DatabaseMetadata metadata, Respond respond) {
        TSqlParser parser = runFromString(respond.getCurrentQuery());
        ParseTree select = parser.select_statement();
        DatabaseTable fromTable = TSqlParseWalker.findFromTable(metadata, select).get(0);
        TSqlParseWalker.findTablesList(metadata, select);
        final List<ExistItem> existTables = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition_and(TSqlParser.Search_condition_andContext ctx) {
                for (int i = 0; i < ctx.search_condition_not().size(); i++) {
                    if (ctx.search_condition_not(i).predicate() != null && ctx.search_condition_not(i).predicate().EXISTS() != null) {
                        ExistItem eItem = new ExistItem();
                        eItem.setNot(ctx.search_condition_not(i).NOT() != null);
                        eItem.setPredicateStartAt(ctx.search_condition_not(i).getStart().getStartIndex());
                        eItem.setPredicateStopAt(ctx.search_condition_not(i).getStop().getStopIndex());
                        TSqlParser.Query_specificationContext qSpecContext = ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification();
                        if (qSpecContext.FROM() != null && qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().table_name_with_hint() != null) {
                            eItem.setTable(metadata.findTable(qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText(),
                                    qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().as_table_alias() == null
                                            ? null
                                            : qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().as_table_alias().getText()));
                        }
                        if (qSpecContext.WHERE() != null) {
                            List<ConditionItem> eConditions = new ArrayList<>();
                            for (TSqlParser.Search_condition_notContext scnContext : qSpecContext.search_condition().get(0).search_condition_and().get(0).search_condition_not()) {
                                ConditionItem item = new ConditionItem(scnContext.predicate().getStart().getStartIndex(),
                                        scnContext.predicate().getStop().getStopIndex() + 1,
                                        ConditionItem.findDataType(scnContext.predicate().expression().get(0)),
                                        ConditionItem.findSideValue(scnContext.predicate().expression().get(0)),
                                        ConditionItem.findDataType(scnContext.predicate().expression().get(1)),
                                        ConditionItem.findSideValue(scnContext.predicate().expression().get(1)),
                                        scnContext.predicate().comparison_operator() != null
                                                ? scnContext.predicate().comparison_operator().getText()
                                                : null
                                );
                                if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                                    item.setLeftSideColumnItem(ColumnItem.findOrCreate(metadata, scnContext, 0));
                                    item.setRightSideColumnItem(ColumnItem.findOrCreate(metadata, scnContext, 1));
                                }
                                eConditions.add(item);
                            }
                            eItem.setConditions(eConditions);
                        }

                        existTables.add(eItem);
                    }
                }
            }
        }, select);

        for (ExistItem exist : existTables) {
            if ((exist.isNot() && exist.getTable() != null && exist.getTable().isEmpty()) ||
                    (!exist.isNot() && (exist.getTable() == null ||
                            (!exist.getTable().isEmpty() &&
                                    (exist.getConditions() == null || exist.getConditions().size() == 0 ||
                                            (exist.getConditions().size() == 1 && ConditionItem.isComparingForeignKey(fromTable, exist.getTable(), exist.getConditions().get(0)))))))) {
                respond.addTransform(new Transform(respond.getCurrentQuery(),
                        (respond.getCurrentQuery().substring(0, exist.getPredicateStartAt()) + respond.getCurrentQuery().substring(exist.getPredicateStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS",
                        "runEqualConditionInOperatorExists",
                        true
                ));
                respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                respond.setChanged(true);
                return respond;
            }
        }
        respond.addTransform(new Transform(respond.getCurrentQuery(),
                respond.getCurrentQuery(),
                "OK",
                "runEqualConditionInOperatorExists",
                false
        ));
        respond.setChanged(false);
        return respond;
    }

    public static Respond runEqualConditionInOperatorLike(final DatabaseMetadata metadata, Respond respond) {
        
        TSqlParser parser = runFromString(respond.getCurrentQuery());
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
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE().getText()
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
                    respond.addTransform(new Transform(respond.getCurrentQuery(),
                            (respond.getCurrentQuery().substring(0, condition.getStartAt()) + respond.getCurrentQuery().substring(condition.getStopAt())).trim(),
                            UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                            "runEqualConditionInOperatorLike",
                            true
                    ));
                    respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                    respond.setChanged(true);
                    return respond;
                }
            } else if (condition.getLeftSideDataType() == ConditionDataType.COLUMN &&
                    (condition.getRightSideDataType() == ConditionDataType.COLUMN || condition.getRightSideDataType() == ConditionDataType.STRING)) {
                if (condition.getRightSideDataType() == ConditionDataType.STRING) {
                    if (condition.getRightSideValue().matches("^[%]+$")) {
                        respond.addTransform(new Transform(respond.getCurrentQuery(),
                                (respond.getCurrentQuery().substring(0, condition.getStartAt()) + respond.getCurrentQuery().substring(condition.getStopAt())).trim(),
                                UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                                "runEqualConditionInOperatorLike",
                                true
                        ));
                        respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                        respond.setChanged(true);
                        return respond;
                    }
                } else if (newMetadata.columnsEqual(condition.getLeftSideColumnItem(), condition.getRightSideColumnItem())) {
                    respond.addTransform(new Transform(respond.getCurrentQuery(),
                            (respond.getCurrentQuery().substring(0, condition.getStartAt()) + respond.getCurrentQuery().substring(condition.getStopAt())).trim(),
                            UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE",
                            "runEqualConditionInOperatorLike",
                            true
                    ));
                    respond.setCurrentQuery(respond.getQueryTransforms().get(respond.getQueryTransforms().size()-1).getOutputQuery());
                    respond.setChanged(true);
                    return respond;
                }
            }
        }
        respond.addTransform(new Transform(respond.getCurrentQuery(),
                respond.getCurrentQuery(),
                "OK",
                "runEqualConditionInOperatorLike",
                false
        ));
        respond.setChanged(false);
        return respond;
    }

    public static TSqlParser runFromString(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

}
