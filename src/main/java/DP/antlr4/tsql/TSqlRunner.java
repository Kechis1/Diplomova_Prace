package DP.antlr4.tsql;

import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
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
import java.util.ArrayList;
import java.util.List;

public class TSqlRunner {
    private final static String DIR_QUERIES = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "DP" + File.separator + "antlr4" + File.separator + "tsql" + File.separator + "queries" + File.separator;

    /**
     * @param metadata
     * @throws IOException
     * @TODO agregacni funkce
     * @TODO prepsat COUNT(*) => 1, kdyz jsou vsechny primarni klice v group by
     * @TODO prepsat ostatni agregacni funkce => column_name, kdyz jsou vsechny primarni klice v group by
     */
    public static void RunGroupByWithPrimaryKey(DatabaseMetadata metadata, String query) throws IOException {
        TSqlParser parser = RunFromString(query);
        ParseTree select = parser.select_statement();

        final ArrayList<String> aggregateFunctions = new ArrayList<String>();
        final ArrayList<String> columnsInGroupBy = new ArrayList<String>();
        final ArrayList<String> joinTables = new ArrayList<String>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            /**
             * @TODO zjistit, jestli je agregacni funkce v group by => nesmi byt, vyhodit error
             * @param ctx
             */
            @Override
            public void enterGroup_by_item(@NotNull TSqlParser.Group_by_itemContext ctx) {
                if (ctx.expression().full_column_name() != null) {
                    columnsInGroupBy.add(ctx.expression().full_column_name().column_name.getText());
                } else if (ctx.expression().function_call() != null) {
                    String groupByItem = ctx.expression().function_call().getRuleContext().getText();

                }
            }

            @Override
            public void exitTable_source_item(TSqlParser.Table_source_itemContext ctx) {
                joinTables.add(ctx.table_name_with_hint().table_name().table.getText());
            }

            @Override
            public void enterAggregate_windowed_function(TSqlParser.Aggregate_windowed_functionContext ctx) {
                aggregateFunctions.add(ctx.all_distinct_expression().expression().full_column_name().column_name.getText());
            }
        }, select);

        System.out.println("aggregateFunctions: " + aggregateFunctions);
        System.out.println("columnsInGroupBy: " + columnsInGroupBy);
        System.out.println("joinTables: " + joinTables);

        metadata = metadata.withTables(joinTables);

        if (!columnsInGroupBy.isEmpty() && columnsInGroupBy.containsAll(metadata.getAllPrimaryKeys())) {
            System.out.println(UnnecessaryStatementException.message + "GROUP BY");
        }
    }

    public static void RunSameCondition() throws IOException {
        TSqlParser parser = RunFromString("SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                "WHERE SDT.SID = SDE.SID\n" +
                "ORDER BY SDT.SID");

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
                    System.out.println(UnnecessaryStatementException.message + "CONDITION");
                }
            }
        }
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
