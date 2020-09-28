package DP.antlr4.tsql;

import DP.Database.AggregateItem;
import DP.Database.ConditionItem;
import DP.Database.DatabaseMetadata;
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


    public static boolean RunGroupBy(DatabaseMetadata metadata, String query) {
        TSqlParser parser = RunFromString(query);
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

    public static boolean RunSameCondition(DatabaseMetadata metadata, String query) {
        TSqlParser parser = RunFromString(query);

        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = new ArrayList<>();
        final List<String> outerJoinTables = new ArrayList<>();
        final List<String> innerJoinTables = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(@NotNull TSqlParser.Search_conditionContext ctx) {
           /*     if (ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE() != null) {
                    conditions.add(
                            new ConditionItem(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0).full_column_name().getText(),
                                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().LIKE().getText(),
                                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1).primitive_expression().getText())
                    );
                } else {
                    conditions.add(
                            new ConditionItem(ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(0).primitive_expression().getText(),
                                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().comparison_operator().getText(),
                                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(1).primitive_expression().getText())
                    );
                }*/
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
         * @TODO pokud to budou konstanty tak zkusit provest operaci porovnani a dostat z toho boolean (1=1, 1>0, 1<2, 1<>0, atd.)
         * @TODO checkovat vsechna vnitrni porovnani (INNER JOIN a WHERE)
         * @TODO checkovat vsechna vnejsi porovnani (OUTER JOIN)
         */
        for (ConditionItem condition : conditions) {
            if ((condition.getOperator().equals("=") && condition.getLeftSide().equals(condition.getRightSide())) ||
                    (condition.getOperator().equals("<>") && !condition.getLeftSide().equals(condition.getRightSide())) ||
                    (condition.getOperator().equals("LIKE") && condition.getRightSide().equals("'%'"))) {
                System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
                return false;
            }
        }

        System.out.println("conditions: " + conditions);
        System.out.println("innerJoinTables: " + innerJoinTables);
        System.out.println("outerJoinTables: " + outerJoinTables);

        return true;
    }

    public static void RunTests() {
        // test use from file
        /*TSqlParser parser1 = TSqlRunner.RunFromFile( TSqlRunner.DIR_QUERIES + "dml_select.sql");
        System.out.println(parser1.use_statement().toStringTree(parser1));*/

        TSqlParser parser = TSqlRunner.RunFromString("SELECT pId, jmeno FROM dbo.predmet GROUP BY pId, jmeno");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString("SELECT * FROM dbo.predmet WHERE 1=1");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString("SELECT *\n" +
                "FROM dbo.student sdt\n" +
                "INNER JOIN dbo.studuje sde ON sdt.sID = sde.sID\n" +
                "INNER JOIN dbo.predmet pdt ON sde.pID = pdt.pID\n" +
                "WHERE sdt.sID = sde.sID\n" +
                "ORDER BY sdt.sID");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString("SELECT sdt.sId, sdt.jmeno\n" +
                "FROM dbo.student sdt\n" +
                "LEFT JOIN dbo.studuje sde ON sdt.sID = sde.sID\n" +
                "GROUP BY sdt.sID, sdt.jmeno");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString("SELECT *\n" +
                "FROM dbo.student\n" +
                "ORDER BY sID");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString("SELECT *\n" +
                "FROM predmet\n" +
                "WHERE jmeno LIKE '%'");
        System.out.println(parser.select_statement().toStringTree(parser));


        // test select from string
        /*TSqlParser parser2 = TSqlRunner.RunFromString( "SELECT DISTINCT Name FROM Production.Product WHERE ProductModelID;");
        System.out.println(parser2.select_statement().toStringTree(parser2));*/
    }


    public static TSqlParser RunFromFile(String fileName) throws IOException {
        String fileContent = CharStreams.fromFileName(fileName).toString();
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(fileContent.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

    public static TSqlParser RunFromString(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

}
