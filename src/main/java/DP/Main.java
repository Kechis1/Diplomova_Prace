package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.*;
import DP.antlr4.tsql.parser.TSqlParser;

import java.io.InputStream;
import java.util.Scanner;

public class Main {
    private static final String pathToMetadata = "databases/db_student_studuje_predmet.json";

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String inQuery;
        String inPathToMetadata;
        boolean repeat;
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is;

        do {
            System.out.println("Enter query");
            inQuery = in.nextLine().toUpperCase().trim();
            if (inQuery.isEmpty()) {
                repeat = true;
            } else {
                TSqlParser parser = QueryHandler.parseQuery(inQuery);
                parser.select_statement();
                repeat = parser.getErrorHandler().inErrorRecoveryMode(parser);
            }
        } while (repeat);

        do {
            System.out.println("Enter path to database metadata (optional)");
            inPathToMetadata = in.nextLine().trim();
            is = classloader.getResourceAsStream(inPathToMetadata);
        } while (!inPathToMetadata.isEmpty() && is == null);

        if (inPathToMetadata.isEmpty()) {
            runExample(inQuery, pathToMetadata);
        } else {
            runExample(inQuery, inPathToMetadata);
        }
    }

    private static void runExample(String requestQuery, String requestMetadata) {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson(requestMetadata);
        Query query = new Query(requestQuery, requestQuery);

        runChain(metadata, query);
    }

    private static void runChain(DatabaseMetadata metadata, Query query) {
        TransformationBuilder builder = new TransformationBuilder(metadata);
        builder.makeQuery(query);

        printRuns(query);

        System.out.println("---");
        System.out.println("Result query: " + query.getCurrentQuery());
    }

    private static void runChain(Query query, QueryHandler chain) {
        TransformationBuilder builder = new TransformationBuilder(chain);
        builder.makeQuery(query);

        printRuns(query);

        System.out.println("---");
        System.out.println("Result query: " + query.getCurrentQuery());
    }

    private static void printRuns(Query query) {
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
