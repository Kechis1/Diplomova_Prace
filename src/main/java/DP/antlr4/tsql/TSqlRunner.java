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
        TSqlParser parser1 = TSqlRunner.RunFromFile( TSqlRunner.DIR_QUERIES + "dml_select.sql");
        System.out.println(parser1.use_statement().toStringTree(parser1));

        // test select from string
        TSqlParser parser2 = TSqlRunner.RunFromString( "SELECT DISTINCT Name FROM Production.Product WHERE ProductModelID;");
        System.out.println(parser2.select_statement().toStringTree(parser2));
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
