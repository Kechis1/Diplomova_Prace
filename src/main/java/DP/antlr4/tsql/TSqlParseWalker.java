package DP.antlr4.tsql;

import DP.Database.*;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class TSqlParseWalker {
    public static List<AggregateItem> findAggregateFunctionsInSelect(ParseTree select) {
        final List<AggregateItem> aggregateFunctionsInSelect = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSelect_list_elem(@NotNull TSqlParser.Select_list_elemContext ctx) {
                if (ctx.expression_elem() != null && ctx.expression_elem().expression().function_call() != null) {
                    TSqlParser.AGGREGATE_WINDOWED_FUNCContext aggCtx =
                            (TSqlParser.AGGREGATE_WINDOWED_FUNCContext) ctx.expression_elem().expression().function_call().getRuleContext();
                    aggregateFunctionsInSelect.add(new AggregateItem(ctx.expression_elem().expression().function_call().getChild(0).getChild(2).getText(),
                            aggCtx.aggregate_windowed_function().STAR() != null
                                    ? "*"
                                    : aggCtx.aggregate_windowed_function().all_distinct_expression().expression().full_column_name().column_name.getText(),
                            ctx.expression_elem().expression().function_call().getChild(0).getChild(0).getText()));
                }
            }
        }, select);
        return aggregateFunctionsInSelect;
    }

    public static List<DatabaseTable> findTablesList(final DatabaseMetadata metadata, ParseTree select) {
        final List<DatabaseTable> allTables = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void exitTable_source_item(@NotNull TSqlParser.Table_source_itemContext ctx) {
                allTables.add(DatabaseTable.create(metadata, ctx));
            }
        }, select);
        return allTables;
    }

    public static Map<String, List<DatabaseTable>> findJoinTablesList(final DatabaseMetadata metadata, ParseTree select) {
        final List<DatabaseTable> outerJoinTables = new ArrayList<>();
        final List<DatabaseTable> innerJoinTables = new ArrayList<>();
        final List<DatabaseTable> leftJoinTables = new ArrayList<>();
        final List<DatabaseTable> rightJoinTables = new ArrayList<>();
        final List<DatabaseTable> fullOuterJoinTables = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterTable_source_item_joined(TSqlParser.Table_source_item_joinedContext ctx) {
                for (int i = 0; i < ctx.join_part().size(); i++) {
                    if (ctx.join_part().get(i).OUTER() != null || ctx.join_part().get(i).LEFT() != null || ctx.join_part().get(i).RIGHT() != null) {
                        outerJoinTables.add(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()));
                        if (ctx.join_part().get(i).LEFT() != null) {
                            leftJoinTables.add(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()));
                        } else if (ctx.join_part().get(i).RIGHT() != null) {
                            rightJoinTables.add(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()));
                        } else {
                            fullOuterJoinTables.add(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()));
                        }
                    } else {
                        innerJoinTables.add(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()));
                    }
                }
            }
        }, select);
        Map<String, List<DatabaseTable>> map = new HashMap<>();
        map.put("outerJoin", outerJoinTables);
        map.put("innerJoin", innerJoinTables);
        map.put("leftJoin", leftJoinTables);
        map.put("rightJoin", rightJoinTables);
        map.put("fullOuterJoin", fullOuterJoinTables);
        return map;
    }

    public static List<ConditionItem> findConditions(final DatabaseMetadata metadata, ParseTree select) {
        List<ConditionItem> conditions = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                ConditionItem item = new ConditionItem(ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                        ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                        ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                        ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                        ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().getChild(1).getText()
                );

                if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                    item.setLeftSideColumnItem(ColumnItem.create(metadata, ctx, 0));
                    item.setRightSideColumnItem(ColumnItem.create(metadata, ctx, 1));
                }

                conditions.add(item);
            }
        }, select);
        return conditions;
    }

    public static Collection<? extends ConditionItem> findConditionsFromSearchCtx(final DatabaseMetadata metadata, TSqlParser.Search_conditionContext ctx) {
        List<ConditionItem> conditions = new ArrayList<>();
        for (TSqlParser.Search_condition_andContext ctxAnd : ctx.search_condition_and()) {
            for (TSqlParser.Search_condition_notContext ctxNot : ctxAnd.search_condition_not()) {
                ConditionItem item = new ConditionItem(ConditionItem.findDataType(ctxNot.predicate().expression().get(0)),
                        ConditionItem.findSideValue(ctxNot.predicate().expression().get(0)),
                        ConditionItem.findDataType(ctxNot.predicate().expression().get(1)),
                        ConditionItem.findSideValue(ctxNot.predicate().expression().get(1)),
                        ctxNot.predicate().getChild(1).getText()
                );
                if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                    item.setLeftSideColumnItem(ColumnItem.create(metadata, ctx, 0));
                    item.setRightSideColumnItem(ColumnItem.create(metadata, ctx, 1));
                }
                conditions.add(item);
            }
        }
        return conditions;
    }

    public static List<Boolean> findDistinctInSelect(ParseTree select) {
        final List<Boolean> isDistinctFoundInSelect = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.DISTINCT() != null) {
                    isDistinctFoundInSelect.add(Boolean.TRUE);
                }
            }
        }, select);
        return isDistinctFoundInSelect;
    }

    public static List<ColumnItem> findColumnsInSelect(final DatabaseMetadata metadata, ParseTree select) {
        final List<ColumnItem> columns = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {

            @Override
            public void enterSelect_list_elem(TSqlParser.Select_list_elemContext ctx) {
                if (ctx.asterisk() != null) {
                    columns.add(new ColumnItem(null,
                            null,
                            DatabaseTable.create(metadata,
                                    null,
                                    ctx.asterisk().table_name() != null
                                            ? ctx.asterisk().table_name().getText()
                                            : null),
                            ctx.asterisk().STAR().getText())
                    );
                } else if (ctx.column_elem() != null) {
                    columns.add(new ColumnItem(null, null,
                            DatabaseTable.create(metadata,
                                    null,
                                    ctx.column_elem().table_name() != null
                                            ? ctx.column_elem().table_name().table.getText()
                                            : null),
                            ctx.column_elem().column_name.getText()
                    ));
                } else if (ctx.expression_elem() != null) {
                    columns.add(new ColumnItem(null,
                            null,
                            DatabaseTable.create(metadata,
                                    null,
                                    ctx.expression_elem().expression().full_column_name().table_name() != null
                                            ? ctx.expression_elem().expression().full_column_name().table_name().table.getText()
                                            : null),
                            ctx.expression_elem().expression().full_column_name().column_name.getText()
                    ));
                }
            }
        }, select);
        return columns;
    }
}
