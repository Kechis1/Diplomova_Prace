package DP.antlr4.tsql;

import DP.Database.*;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.jetbrains.annotations.NotNull;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TSqlRunner {
    private final static String DIR_QUERIES = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "DP" + File.separator + "antlr4" + File.separator + "tsql" + File.separator + "queries" + File.separator;


    public static boolean runGroupBy(DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);
        ParseTree select = parser.select_statement();

        final ArrayList<AggregateItem> allAggregateFunctions = new ArrayList<>();
        final ArrayList<AggregateItem> aggregateFunctionsInSelect = new ArrayList<>();
        final ArrayList<String> columnsInGroupBy = new ArrayList<>();
        final ArrayList<String> joinTables = new ArrayList<>();

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

            @Override
            public void enterGroup_by_item(@NotNull TSqlParser.Group_by_itemContext ctx) {
                if (ctx.expression().full_column_name() != null) {
                    columnsInGroupBy.add(ctx.expression().full_column_name().column_name.getText());
                }
            }

            @Override
            public void exitTable_source_item(@NotNull TSqlParser.Table_source_itemContext ctx) {
                joinTables.add(ctx.table_name_with_hint().table_name().table.getText());
            }

            @Override
            public void enterAggregate_windowed_function(@NotNull TSqlParser.Aggregate_windowed_functionContext ctx) {
                allAggregateFunctions.add(new AggregateItem(ctx.getChild(2).getText(),
                        ctx.STAR() != null
                                ? "*"
                                : ctx.all_distinct_expression().expression().full_column_name().column_name.getText(),
                        ctx.getChild(0).getText()));
            }
        }, select);

        metadata = metadata.withTables(joinTables);

        if (allAggregateFunctions.isEmpty()) {
            if (columnsInGroupBy.containsAll(metadata.getAllPrimaryKeys())) {
                System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY");
                return false;
            }
            System.out.println("OK");
            return true;
        }

        if (columnsInGroupBy.containsAll(metadata.getAllPrimaryKeys()) && !aggregateFunctionsInSelect.isEmpty()) {
            for (AggregateItem item: aggregateFunctionsInSelect) {
                if (item.getFunctionName().equals("COUNT")) {
                    System.out.println(item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1");
                } else {
                    System.out.println(item.getFullFunctionName() + " " + UnnecessaryStatementException.messageCanBeRewrittenTo + " " + item.getFullColumnName());
                }
            }
            return false;
        }

        System.out.println("OK");
        return true;
    }

    public static boolean runEqualConditionInComparisonOperators(DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);

        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = new ArrayList<>();
        final List<String> outerJoinTables = new ArrayList<>();
        final List<String> innerJoinTables = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                conditions.add(
                        new ConditionItem(ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                                ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                                ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                                ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                                ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().comparison_operator().getText()
                        )
                );
            }

            @Override
            public void enterTable_source_item_joined(TSqlParser.Table_source_item_joinedContext ctx) {
                for (int i = 0; i < ctx.join_part().size(); i++) {
                    if (ctx.join_part().get(i).OUTER() != null || ctx.join_part().get(i).LEFT() != null || ctx.join_part().get(i).RIGHT() != null) {
                        outerJoinTables.add(ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText());
                    } else {
                        innerJoinTables.add(ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText());
                    }
                }
            }
        }, select);

        /**
         * @TODO checkovat vsechna vnitrni porovnani (INNER JOIN a WHERE)
         * @TODO checkovat vsechna vnejsi porovnani (OUTER JOIN)
         */
        boolean isConditionNecessary = true;
        for (ConditionItem condition : conditions) {
            if (condition.getLeftSideDataType() == ConditionDataType.STRING && condition.getRightSideDataType() == ConditionDataType.STRING) {
                isConditionNecessary &= condition.compareStringAgainstString();
            } else if (condition.getLeftSideDataType().isNumeric && condition.getRightSideDataType().isNumeric) {
                isConditionNecessary &= condition.compareNumberAgainstNumber();
            } else if (condition.getLeftSideDataType() == ConditionDataType.STRING && condition.getRightSideDataType().isNumeric) {
                isConditionNecessary &= condition.compareStringAgainstNumber();
            } else if (condition.getLeftSideDataType().isNumeric && condition.getRightSideDataType() == ConditionDataType.STRING) {
                isConditionNecessary &= condition.compareNumberAgainstString();
            } else if (condition.getRightSideDataType() == ConditionDataType.COLUMN && condition.getRightSideDataType() == ConditionDataType.COLUMN) {

            }
        }

        System.out.println("conditions: " + conditions);
        System.out.println("innerJoinTables: " + innerJoinTables);
        System.out.println("outerJoinTables: " + outerJoinTables);

        return isConditionNecessary;
    }

    public static boolean runEqualConditionInOperatorAll(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorAny(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorBetween(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorExists(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorIn(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorLike(DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                conditions.add(
                        new ConditionItem(ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                                ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                                ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                                ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                                ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE().getText()
                        )
                );
            }
        }, select);

        for (ConditionItem condition : conditions) {
            if (condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN) {
                if (SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue())) {
                    System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
                    return false;
                }
            } else if (condition.getLeftSideDataType() == ConditionDataType.COLUMN &&
                    (condition.getRightSideDataType() == ConditionDataType.COLUMN || condition.getRightSideDataType() == ConditionDataType.STRING)) {
                if (condition.getRightSideDataType() == ConditionDataType.STRING) {
                    if (condition.getRightSideValue().matches("^[%]+$")) {
                        System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
                        return false;
                    }
                } else {

                }
            }
        }
        return true;
    }

    public static boolean runEqualConditionInOperatorSome(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorSubQuery(DatabaseMetadata metadata, String query) {
        return true;
    }

    public static TSqlParser runFromFile(String fileName) throws IOException {
        String fileContent = CharStreams.fromFileName(fileName).toString();
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(fileContent.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

    public static TSqlParser runFromString(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

}
