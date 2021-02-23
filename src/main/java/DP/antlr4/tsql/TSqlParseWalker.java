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
                    aggregateFunctionsInSelect.add(new AggregateItem(ctx.expression_elem().expression().function_call().getStart().getStartIndex(),
                            ctx.expression_elem().expression().function_call().getStop().getStopIndex(),
                            ctx.expression_elem().expression().function_call().getChild(0).getChild(2).getText(),
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

    public static Map<String, List<JoinTable>> findJoinTablesList(final DatabaseMetadata metadata, ParseTree select) {
        final List<JoinTable> outerJoinTables = new ArrayList<>();
        final List<JoinTable> innerJoinTables = new ArrayList<>();
        final List<JoinTable> leftJoinTables = new ArrayList<>();
        final List<JoinTable> rightJoinTables = new ArrayList<>();
        final List<JoinTable> fullOuterJoinTables = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterTable_source_item_joined(TSqlParser.Table_source_item_joinedContext ctx) {
                for (int i = 0; i < ctx.join_part().size(); i++) {
                    if (ctx.join_part().get(i).OUTER() != null || ctx.join_part().get(i).LEFT() != null || ctx.join_part().get(i).RIGHT() != null) {
                        outerJoinTables.add(new JoinTable(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()),
                                ctx.join_part().get(i).getStart().getStartIndex(),
                                ctx.join_part().get(i).getStop().getStopIndex()));
                        if (ctx.join_part().get(i).LEFT() != null) {
                            leftJoinTables.add(new JoinTable(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()),
                                    ctx.join_part().get(i).getStart().getStartIndex(),
                                    ctx.join_part().get(i).getStop().getStopIndex()));
                        } else if (ctx.join_part().get(i).RIGHT() != null) {
                            rightJoinTables.add(new JoinTable(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()),
                                    ctx.join_part().get(i).getStart().getStartIndex(),
                                    ctx.join_part().get(i).getStop().getStopIndex()));
                        } else {
                            fullOuterJoinTables.add(new JoinTable(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()),
                                    ctx.join_part().get(i).getStart().getStartIndex(),
                                    ctx.join_part().get(i).getStop().getStopIndex()));
                        }
                    } else {
                        innerJoinTables.add(new JoinTable(DatabaseTable.create(metadata, ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item()),
                                ctx.join_part().get(i).getStart().getStartIndex(),
                                ctx.join_part().get(i).getStop().getStopIndex()));
                    }
                }
            }
        }, select);
        Map<String, List<JoinTable>> map = new HashMap<>();
        map.put("outerJoin", outerJoinTables);
        map.put("innerJoin", innerJoinTables);
        map.put("leftJoin", leftJoinTables);
        map.put("rightJoin", rightJoinTables);
        map.put("fullOuterJoin", fullOuterJoinTables);
        return map;
    }

    public static List<ConditionItem> findConditions(final DatabaseMetadata metadata, ParseTree select) {
        List<ConditionItem> conditions = new ArrayList<>();
        final int[] havingGroupStartsAt = {-1, -1};
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void exitSearch_condition(TSqlParser.Search_conditionContext ctx) {
                conditions.addAll(findConditions(metadata, ctx, havingGroupStartsAt[0], havingGroupStartsAt[1]));
            }

            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.HAVING() != null) {
                    havingGroupStartsAt[0] = ctx.HAVING().getSymbol().getStartIndex();
                }
                if (ctx.GROUP() != null) {
                    havingGroupStartsAt[1] = ctx.GROUP().getSymbol().getStartIndex();
                }
            }
        }, select);
        return conditions;
    }

    public static List<ConditionItem> findWhereConditions(final DatabaseMetadata metadata, ParseTree select) {
        List<ConditionItem> conditions = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.WHERE() != null) {
                    for (TSqlParser.Search_conditionContext t : ctx.search_condition()) {
                        conditions.addAll(findConditions(metadata, t, ctx.HAVING() == null ? -1 : ctx.HAVING().getSymbol().getStartIndex(), ctx.GROUP() == null ? -1 : ctx.GROUP().getSymbol().getStartIndex()));
                    }
                }
            }
        }, select);
        return conditions;
    }

    private static List<ConditionItem> findConditions(final DatabaseMetadata metadata, TSqlParser.Search_conditionContext t, int havingStartsAt, int groupByStartsAt) {
        List<ConditionItem> conditions = new ArrayList<>();
        if (!((havingStartsAt != -1 && t.getStart().getStartIndex() > havingStartsAt)
                || (groupByStartsAt != -1 && t.getStart().getStartIndex() > groupByStartsAt))) {
            conditions.addAll(findConditionsFromSearchCtx(metadata, t));
        }
        return conditions;
    }

    public static Collection<? extends ConditionItem> findConditionsFromSearchCtx(final DatabaseMetadata metadata, TSqlParser.Search_conditionContext ctx) {
        List<ConditionItem> conditions = new ArrayList<>();
        for (TSqlParser.Search_condition_andContext ctxAnd : ctx.search_condition_and()) {
            for (int i = 0; i < ctxAnd.search_condition_not().size(); i++) {
                if (ctxAnd.search_condition_not().get(i).predicate().EXISTS() == null) {
                    ConditionItem item = new ConditionItem(ctxAnd.search_condition_not().get(i).predicate().getStart().getStartIndex(),
                            ctxAnd.search_condition_not().get(i).predicate().getStop().getStopIndex() + 1,
                            ConditionItem.findDataType(ctxAnd.search_condition_not().get(i).predicate().expression().get(0)),
                            ConditionItem.findSideValue(ctxAnd.search_condition_not().get(i).predicate().expression().get(0)),
                            ConditionItem.findDataType(ctxAnd.search_condition_not().get(i).predicate().expression().get(1)),
                            ConditionItem.findSideValue(ctxAnd.search_condition_not().get(i).predicate().expression().get(1)),
                            ctxAnd.search_condition_not().get(i).predicate().getChild(1).getText(),
                            ctxAnd.search_condition_not().get(i).predicate().getText()
                    );
                    if (item.getLeftSideDataType() == ConditionDataType.COLUMN) {
                        item.setLeftSideColumnItem(ColumnItem.findOrCreate(metadata, ctxAnd.search_condition_not().get(i), 0));
                    }
                    if (item.getRightSideDataType() == ConditionDataType.COLUMN) {
                        item.setRightSideColumnItem(ColumnItem.findOrCreate(metadata, ctxAnd.search_condition_not().get(i), 1));
                    }
                    if (ctxAnd.AND() != null && !ctxAnd.AND().isEmpty()) {
                        if (i == 0) {
                            item.setRightLogicalOperatorStartAt(ctxAnd.AND(i).getSymbol().getStartIndex());
                            item.setRightLogicalOperatorStopAt(ctxAnd.AND(i).getSymbol().getStopIndex());
                        } else if (i == ctxAnd.search_condition_not().size() - 1) {
                            item.setLeftLogicalOperatorStartAt(ctxAnd.AND(ctxAnd.AND().size() - 1).getSymbol().getStartIndex());
                            item.setLeftLogicalOperatorStopAt(ctxAnd.AND(ctxAnd.AND().size() - 1).getSymbol().getStopIndex());
                        } else {
                            item.setLeftLogicalOperatorStartAt(ctxAnd.AND(i - 1).getSymbol().getStartIndex());
                            item.setLeftLogicalOperatorStopAt(ctxAnd.AND(i - 1).getSymbol().getStopIndex());
                            item.setRightLogicalOperatorStartAt(ctxAnd.AND(i).getSymbol().getStartIndex());
                            item.setRightLogicalOperatorStartAt(ctxAnd.AND(i).getSymbol().getStopIndex());
                        }
                    }
                    conditions.add(item);
                }
            }
        }
        return conditions;
    }

    public static List<DatabaseTable> findFromTable(final DatabaseMetadata metadata, ParseTree select) {
        List<DatabaseTable> fromTable = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.table_sources() != null && ctx.table_sources().table_source(0).table_source_item_joined().table_source_item().table_name_with_hint() != null) {
                    DatabaseTable found = metadata.findTable(ctx.table_sources().table_source(0).table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText(), null);
                    if (ctx.table_sources().table_source(0).table_source_item_joined().table_source_item().as_table_alias() != null) {
                        found.setTableAlias(ctx.table_sources().table_source(0).table_source_item_joined().table_source_item().as_table_alias().getText());
                    }
                    found.setFromTableStartAt(ctx.table_sources().table_source(0).table_source_item_joined().table_source_item().getStart().getStartIndex());
                    found.setFromTableStopAt(ctx.table_sources().table_source(0).table_source_item_joined().table_source_item().getStop().getStopIndex());
                    fromTable.add(found);
                }
            }
        }, select);

        return fromTable;
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
        final List<Boolean> foundSelect = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {

            @Override
            public void enterSelect_statement(TSqlParser.Select_statementContext mCtx) {
                if (foundSelect.isEmpty()) {
                    foundSelect.add(true);
                    for (TSqlParser.Select_list_elemContext ctx : mCtx.query_expression().query_specification().select_list().select_list_elem()) {
                        if (ctx.asterisk() != null) {
                            ColumnItem it = new ColumnItem(null,
                                    null,
                                    DatabaseTable.create(metadata,
                                            null,
                                            ctx.asterisk().table_name() != null
                                                    ? ctx.asterisk().table_name().getText()
                                                    : null),
                                    ctx.asterisk().STAR().getText());
                            it.setStartAt(ctx.getStart().getStartIndex());
                            it.setStopAt(ctx.getStop().getStopIndex());
                            columns.add(it);
                        } else if (ctx.column_elem() != null) {
                            columns.add(ColumnItem.findOrCreate(metadata, ctx));
                        } else if (ctx.expression_elem() != null) {
                            if (ctx.expression_elem().expression().primitive_expression() != null) {
                                ColumnItem it = new ColumnItem(null,
                                        null,
                                        null,
                                        ctx.expression_elem().as_column_alias() == null
                                                ? null
                                                : ctx.expression_elem().as_column_alias().getText(),
                                        false,
                                        null,
                                        null,
                                        null,
                                        null,
                                        false,
                                        true,
                                        ctx.expression_elem().expression().primitive_expression().constant().getText()
                                );
                                it.setStartAt(ctx.expression_elem().expression().primitive_expression().getStart().getStartIndex());
                                it.setStopAt(ctx.expression_elem().expression().primitive_expression().getStop().getStopIndex());
                                columns.add(it);
                            } else {
                                ColumnItem it = new ColumnItem(null,
                                        null,
                                        DatabaseTable.create(metadata,
                                                null,
                                                ctx.expression_elem().expression().full_column_name().table_name() != null
                                                        ? ctx.expression_elem().expression().full_column_name().table_name().table.getText()
                                                        : null),
                                        ctx.expression_elem().expression().full_column_name().column_name.getText()
                                );
                                it.setStartAt(ctx.expression_elem().expression().getStart().getStartIndex());
                                it.setStopAt(ctx.expression_elem().expression().getStop().getStopIndex());
                                columns.add(it);
                            }
                        }
                    }
                }
            }
        }, select);
        return columns;
    }
}
