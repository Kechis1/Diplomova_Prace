package DP;

import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Transformations.TransformationBuilder;
import com.google.common.io.CharStreams;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PerformanceTestMain {
    private static final String pathToMetadata = "databases/db_student_studuje_predmet.json";

    public static void main(String[] args) {
        try {
            // InputStream is = loadQueryFile("queries/performance_test_queries_old.sql");
            InputStream is = loadQueryFile("queries/performance_test_queries_old.sql");
            DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson(pathToMetadata);
            List<Long> times = new ArrayList<>();
            String[] queries = splitQueries(is);
            runTest(is, queries, times, metadata, 4);
            printTime(times);
        } catch (Exception exception) {
            System.out.println("An error occurred while running a performance test");
            exception.printStackTrace();
        }
    }

    private static String[] splitQueries(InputStream is) throws Exception {
        String queriesText = null;
        try (final Reader reader = new InputStreamReader(is)) {
            queriesText = CharStreams.toString(reader);
        }

        return queriesText.split(";");
    }

    private static void runTest(InputStream is, String[] queries, List<Long> times, DatabaseMetadata metadata, int skipQueries) {
        for (int i = 0; i < queries.length; i++) {
            String queryText = queries[i].replaceAll("\\s", " ").trim().toUpperCase();

            Query query = new Query(queryText, queryText, queryText);

            TransformationBuilder builder = new TransformationBuilder(metadata);

            long start = System.nanoTime();

            builder.makeQuery(query);

            if (i > skipQueries) {
                long finish = System.nanoTime();
                long timeElapsed = (finish - start) / 1000000;
                times.add(timeElapsed);

                System.out.println("#Q" + (i-skipQueries) + ": " + queryText);
                System.out.println(timeElapsed + " ms");
                for (int k = 1; k <= query.getCurrentRunNumber(); k++) {
                    System.out.println("Run (" + k + "): ");
                    if (query.getQueryTransforms().get(k) != null) {
                        for (Transformation r : query.getQueryTransforms().get(k)) {
                            System.out.println(r);
                        }
                    } else {
                        System.out.println("--");
                    }
                }
                System.out.println("\n");
            }
        }
    }

    private static InputStream loadQueryFile(String path) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        return classloader.getResourceAsStream(path);
    }

    private static void printTime(List<Long> times) {
        System.out.println("STATS:\n");
        System.out.println("Total time: " + (times.stream().reduce(0L, Long::sum)) + " ms");
        System.out.println("Max time: " + (Collections.max(times)) + " ms");
        System.out.println("Min time: " + (Collections.min(times)) + " ms");
        System.out.println("Avg time: " + (times.stream().mapToInt(Math::toIntExact).average().orElse(0.0)) + " ms");
    }

}
