package DP.antlr4.tsql;

import DP.Main;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class TSqlRunner {
    private final static String DIR_QUERIES = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "DP" + File.separator + "antlr4" + File.separator + "tsql" + File.separator  + "queries" + File.separator;

    public static void Run() throws IOException {
        // test select from file
        TSqlParser parser1 = TSqlRunner.RunFromFile( TSqlRunner.DIR_QUERIES + "dml_select.sql");

        // test use from string
        TSqlRunner.RunFromString( "SELECT users.id, user_products.id FROM users JOIN user_products ON products.user_id = users.id;");

    }


    public static TSqlParser RunFromFile(String fileName) throws IOException {
        String fileContent = CharStreams.fromFileName(fileName).toString();
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(fileContent.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }

    public static void RunFromString(String query) throws IOException {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        TSqlParser parser = new TSqlParser(new CommonTokenStream(lexer));
        System.out.println(parser.select_statement().toStringTree());

    }

}
