package DP.antlr4.tsql;

import DP.Database.AggregateItem;
import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import com.sun.istack.internal.NotNull;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TSqlRunner {
    private final static String DIR_QUERIES = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "DP" + File.separator + "antlr4" + File.separator + "tsql" + File.separator + "queries" + File.separator;


    public static boolean RunGroupBy(DatabaseMetadata metadata, String query) throws IOException {
        TSqlParser parser = RunFromString(query);
        ParseTree select = parser.select_statement();

        final ArrayList<AggregateItem> allAggregateFunctions = new ArrayList<>();
        final ArrayList<AggregateItem> aggregateFunctionsInSelect = new ArrayList<>();
        final ArrayList<String> columnsInGroupBy = new ArrayList<>();
        final ArrayList<String> joinTables = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {

            @Override
            public void enterSelect_list_elem(TSqlParser.Select_list_elemContext ctx) {
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
            public void exitTable_source_item(TSqlParser.Table_source_itemContext ctx) {
                joinTables.add(ctx.table_name_with_hint().table_name().table.getText());
            }

            @Override
            public void enterAggregate_windowed_function(TSqlParser.Aggregate_windowed_functionContext ctx) {
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

    public static boolean RunSameCondition(DatabaseMetadata metadata, String query) throws IOException {
        TSqlParser parser = RunFromString(query);

        ParseTree select = parser.select_statement();
        final List<String> conditions = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition(TSqlParser.Search_conditionContext ctx) {
                conditions.add(ctx.getText());
            }
        }, select);

        for (int i = 0; i < conditions.size(); i++) {
            for (int j = i + 1; j < conditions.size(); j++) {
                if (conditions.get(i).equals(conditions.get(j))) {
                    System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
                }
            }
        }

        return true;
    }

    public static void RunTests() throws IOException {
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

    public static TSqlParser RunFromString(String query) throws IOException {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

}
