package DP;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.MetadataException;
import DP.Transformations.*;
import DP.antlr4.tsql.parser.TSqlParser;

import java.io.InputStream;
import java.util.Locale;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static final String pathToMetadata = "databases/db_student_studuje_predmet.json";

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String inOriginalQuery;
        String inTransformedQuery;
        String inPathToMetadata;
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is;

        System.out.println("Enter query");
        inOriginalQuery = in.nextLine().trim();
        inTransformedQuery = inOriginalQuery.toUpperCase();
        TSqlParser parser = QueryHandler.parseQuery(inTransformedQuery);
        parser.select_statement();

        do {
            System.out.println("Enter path to database metadata (optional)");
            inPathToMetadata = in.nextLine().trim();
            is = classloader.getResourceAsStream(inPathToMetadata);
        } while (!inPathToMetadata.isEmpty() && is == null);

        try {
            long start = System.nanoTime();
            if (inPathToMetadata.isEmpty()) {
                runExample(inOriginalQuery, inTransformedQuery, pathToMetadata);
            } else {
                runExample(inOriginalQuery, inTransformedQuery, inPathToMetadata);
            }
            long finish = System.nanoTime();
            long timeElapsed = (finish - start) / 1000000;
            System.out.println("Time: " + timeElapsed + " ms");
        }  catch (MetadataException exception) {
            System.out.println(exception.getMessage());
        }
    }

    private static void runExample(String originalQuery, String requestQuery, String requestMetadata) throws MetadataException {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson(requestMetadata);
        Query query = new Query(originalQuery, requestQuery, requestQuery);

        runChain(metadata, query);
    }

    private static void runChain(DatabaseMetadata metadata, Query query) {
        TransformationBuilder builder = new TransformationBuilder(metadata);
        builder.makeQuery(query);

        printRuns(query);

        System.out.println("---");
        System.out.println("Result query: " + query.getOutputQuery());
    }

    private static void runChain(Query query, QueryHandler chain) {
        TransformationBuilder builder = new TransformationBuilder(chain);
        builder.makeQuery(query);

        printRuns(query);

        System.out.println("---");
        System.out.println("Result query: " + query.getOutputQuery());
    }

    private static void printRuns(Query query) {
        if (query.getQueryTransforms() == null) {
            return;
        }
        for (int i = 1; i <= query.getCurrentRunNumber(); i++) {
            System.out.println("Run (" + i + "): ");
            if (query.getQueryTransforms().get(i) != null) {
                for (Transformation r : query.getQueryTransforms().get(i)) {
                    System.out.println(r);
                }
            } else {
                System.out.println("--");
            }
        }
    }
}
