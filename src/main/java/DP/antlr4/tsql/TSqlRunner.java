package DP.antlr4.tsql;

import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.File;
import java.io.IOException;

public class TSqlRunner {
    private final static String DIR_QUERIES = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "DP" + File.separator + "antlr4" + File.separator + "tsql" + File.separator  + "queries" + File.separator;

    public static void Run() throws IOException {
        // test use from file
        /*TSqlParser parser1 = TSqlRunner.RunFromFile( TSqlRunner.DIR_QUERIES + "dml_select.sql");
        System.out.println(parser1.use_statement().toStringTree(parser1));*/

        TSqlParser parser = TSqlRunner.RunFromString( "SELECT pId, jmeno FROM dbo.predmet GROUP BY pId, jmeno");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString( "SELECT * FROM dbo.predmet WHERE 1=1");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString( "SELECT *\n" +
                "FROM dbo.student sdt\n" +
                "INNER JOIN dbo.studuje sde ON sdt.sID = sde.sID\n" +
                "INNER JOIN dbo.predmet pdt ON sde.pID = pdt.pID\n" +
                "WHERE sdt.sID = sde.sID\n" +
                "ORDER BY sdt.sID");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString( "SELECT sdt.sId, sdt.jmeno\n" +
                "FROM dbo.student sdt\n" +
                "LEFT JOIN dbo.studuje sde ON sdt.sID = sde.sID\n" +
                "GROUP BY sdt.sID, sdt.jmeno");
        System.out.println(parser.select_statement().toStringTree(parser));

        parser = TSqlRunner.RunFromString( "SELECT *\n" +
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
