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

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TSqlRunner {
    // private final static String DIR_QUERIES = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "DP" + File.separator + "antlr4" + File.separator + "tsql" + File.separator + "queries" + File.separator;


    public static boolean runGroupBy(final DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);
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
                allAggregateFunctions.add(new AggregateItem(ctx.getChild(2).getText(),
                        ctx.STAR() != null
                                ? "*"
                                : ctx.all_distinct_expression().expression().full_column_name().column_name.getText(),
                        ctx.getChild(0).getText()));
            }
        }, select);

        DatabaseMetadata newMetadata = metadata.withTables(joinTables);

        if (allAggregateFunctions.isEmpty()) {
            if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys())) {
                System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY");
                return false;
            }
            System.out.println("OK");
            return true;
        }

        if (columnsInGroupBy.containsAll(newMetadata.getAllPrimaryKeys()) && !aggregateFunctionsInSelect.isEmpty()) {
            for (AggregateItem item : aggregateFunctionsInSelect) {
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

    /**
     * @TODO OR, AND
     */
    public static boolean runEqualConditionInComparisonOperators(final DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);

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
        }
        return isConditionNecessary;
    }

    public static boolean runRedundantJoinConditions(final DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);

        ParseTree select = parser.select_statement();
        final List<ConditionItem> leftJoinConditions = new ArrayList<>();
        final List<ConditionItem> rightJoinConditions = new ArrayList<>();
        final List<ConditionItem> fullOuterJoinConditions = new ArrayList<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterJoin_part(TSqlParser.Join_partContext ctx) {
                if (ctx.LEFT() != null) {
                    leftJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.RIGHT() != null) {
                    rightJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.FULL() != null && ctx.OUTER() != null) {
                    fullOuterJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else {
                    innerConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                }
            }

            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.search_condition() != null) {
                    for (TSqlParser.Search_conditionContext sCtx : ctx.search_condition()) {
                        innerConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, sCtx));
                    }
                }
            }
        }, select);

        boolean foundDuplicateCondition = ConditionItem.duplicatesExists(metadata, innerConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(metadata, leftJoinConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(metadata, rightJoinConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(metadata, fullOuterJoinConditions);

        if (!foundDuplicateCondition) {
            System.out.println("OK");
        }
        return !foundDuplicateCondition;
    }

    public static boolean runRedundantJoinTables(final DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);

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
            return true;
        }

        boolean foundRedundantJoin = DatabaseTable.redundantJoinExists("LEFT", joinTables.get("leftJoin"),
                fromTable.get(0).getTableAlias(), fromTable.get(0), allColumnsInSelect, false, null, false);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists("RIGHT", joinTables.get("rightJoin"),
                null, null, allColumnsInSelect, false, null, false);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists("FULL OUTER", joinTables.get("fullOuterJoin"), fromTable.get(0).getTableAlias(),
                fromTable.get(0), allColumnsInSelect, true, metadata.setNullableColumns(fullOuterJoinConditions), true);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists("INNER", joinTables.get("innerJoin"), null,
                null, allColumnsInSelect, true, metadata.setNullableColumns(innerConditions), false);

        if (!foundRedundantJoin) {
            System.out.println("OK");
        }
        return !foundRedundantJoin;
    }

    public static boolean runEqualConditionInOperatorAll(final DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorAny(final DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorBetween(final DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorExists(final DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorIn(final DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorLike(final DatabaseMetadata metadata, String query) {
        TSqlParser parser = runFromString(query);
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = new ArrayList<>();
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
                if (ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE() != null) {
                    ConditionItem item = new ConditionItem(ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0)),
                            ConditionItem.findDataType(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                            ConditionItem.findSideValue(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1)),
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE().getText()
                    );

                    if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                        item.setLeftSideColumnItem(ColumnItem.create(metadata, ctx, 0));
                        item.setRightSideColumnItem(ColumnItem.create(metadata, ctx, 1));
                    }

                    conditions.add(item);
                }
            }
        }, select);

        DatabaseMetadata newMetadata = metadata.withTables(allTables);

        for (ConditionItem condition : conditions) {
            if (condition.getLeftSideDataType() != ConditionDataType.COLUMN && condition.getRightSideDataType() != ConditionDataType.COLUMN) {
                if (SQLLogicalOperators.like(condition.getLeftSideValue(), condition.getRightSideValue())) {
                    System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE");
                    return false;
                }
            } else if (condition.getLeftSideDataType() == ConditionDataType.COLUMN &&
                    (condition.getRightSideDataType() == ConditionDataType.COLUMN || condition.getRightSideDataType() == ConditionDataType.STRING)) {
                if (condition.getRightSideDataType() == ConditionDataType.STRING) {
                    if (condition.getRightSideValue().matches("^[%]+$")) {
                        System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE");
                        return false;
                    }
                } else if (newMetadata.columnsEqual(condition.getLeftSideColumnItem(), condition.getRightSideColumnItem())) {
                    System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE");
                    return false;
                }
            }
        }
        System.out.println("OK");
        return true;
    }

    public static boolean runEqualConditionInOperatorSome(final DatabaseMetadata metadata, String query) {
        return true;
    }

    public static boolean runEqualConditionInOperatorSubQuery(final DatabaseMetadata metadata, String query) {
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
