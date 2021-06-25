package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Transformations.TransformationBuilder;
import com.google.common.io.CharStreams;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PerformanceTestMain {
    private static final String pathToMetadata = "databases/db_student_studuje_predmet.json";

    public static void main(String[] args) {
        List<Long> times = new ArrayList<>();
        try {
            DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson(pathToMetadata);
            // runOperatorsTest(times, metadata);
            runLongTest(times, metadata);
        } catch (Exception exception) {
            System.out.println("An error occurred while running a performance test");
            exception.printStackTrace();
        } finally {
            printTime(times);
        }
    }

    private static void runOperatorsTest(List<Long> times, DatabaseMetadata metadata) throws Exception {
        PrintWriter out = new PrintWriter("results.txt");
        InputStream is = loadQueryFile("queries/performance_text_queries_operators.sql");
        Matcher queries = splitQueries(is, "(.*);");
        runTest(out, queries, times, metadata, 4);
        printTime(times);
        out.close();
    }

    private static void runLongTest(List<Long> times, DatabaseMetadata metadata) throws Exception {
        PrintWriter out = new PrintWriter("results4.txt");
        InputStream is = loadQueryFile("queries/performance_test_queries_new4.txt");
        Matcher queries = splitQueries(is, "[0-9]+_[0-9]+_(.+)");
        runTest(out, queries, times, metadata, -1);
        printTime(times);
        out.close();
    }

    private static Matcher splitQueries(InputStream is, String splitRegex) throws Exception {
        String queriesText = null;
        try (final Reader reader = new InputStreamReader(is)) {
            queriesText = CharStreams.toString(reader);
        }
        return Pattern.compile(splitRegex).matcher(queriesText);
    }

    private static void runTest(PrintWriter out, Matcher queries, List<Long> times, DatabaseMetadata metadata, int skipQueries) {
        int i = 0;
        String queryText;
        Query query = new Query();
        TransformationBuilder builder = new TransformationBuilder(metadata);
        long start;
        long finish;
        long timeElapsed;
        while (queries.find()) {
            if (skipQueries < i) {
                queryText = queries.group(1).replaceAll("\\s", " ").trim().toUpperCase();
                query.setQuery(queryText, queryText, queryText);

                start = System.nanoTime();

                builder.makeQuery(query);

                finish = System.nanoTime();
                timeElapsed = (finish - start) / 1000000;
                times.add(timeElapsed);
                saveTransformation(out, i, skipQueries, queries.group(0), timeElapsed, query);
                if (i % 5000 == 0) {
                    saveTime(out, times, i);
                }
            }
            i++;
        }
    }

    private static void saveTransformation(PrintWriter out, int i, int skipQueries, String queryText, long timeElapsed, Query query) {
        out.println("#Q" + (i - skipQueries) + ": " + queryText);
        out.println(timeElapsed + " ms");
        for (int k = 1; k <= query.getCurrentRunNumber(); k++) {
            out.println("Run (" + k + "): ");
            if (query.getQueryTransforms() != null && query.getQueryTransforms().get(k) != null) {
                for (Transformation r : query.getQueryTransforms().get(k)) {
                    out.println(r.toString());
                }
            } else {
                out.println("--");
            }
        }
        out.println("\n");
    }

    private static void saveTime(PrintWriter out, List<Long> times, int count) {
        try {
            boolean close = false;
            if (out == null) {
                out = new PrintWriter("results.txt");
                close = true;
            }
            out.println("Count: " + count);
            out.println("STATS:\n");
            out.println("Total time: " + (times.stream().reduce(0L, Long::sum)) + " ms");
            out.println("Max time: " + (Collections.max(times)) + " ms");
            out.println("Min time: " + (Collections.min(times)) + " ms");
            out.println("Avg time: " + (times.stream().mapToInt(Math::toIntExact).average().orElse(0.0)) + " ms\n");
            if (close) {
                out.close();
            }
        } catch (FileNotFoundException ignored) {

        }
    }

    private static InputStream loadQueryFile(String path) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        return classloader.getResourceAsStream(path);
    }

    private static void printTime(List<Long> times) {
        if (times.size() > 1) {
            System.out.println("STATS:\n");
            System.out.println("Total time: " + (times.stream().reduce(0L, Long::sum)) + " ms");
            System.out.println("Max time: " + (Collections.max(times)) + " ms");
            System.out.println("Min time: " + (Collections.min(times)) + " ms");
            System.out.println("Avg time: " + (times.stream().mapToInt(Math::toIntExact).average().orElse(0.0)) + " ms");
        }
    }

}
