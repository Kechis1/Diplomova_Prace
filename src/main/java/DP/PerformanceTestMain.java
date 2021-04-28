package DP;

import DP.Database.DatabaseMetadata;
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

    public static void main(String[] args) throws Exception {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("queries/transformation_test_queries.sql");
        String queriesText = null;
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson(pathToMetadata);

        try (final Reader reader = new InputStreamReader(is)) {
            queriesText = CharStreams.toString(reader);
        }

        String[] queries = queriesText.split(";");
        List<Long> times = new ArrayList<>();

        for (int i = 0; i < queries.length; i++) {
            String queryText = queries[i].replaceAll("\\s", " ").trim().toUpperCase();

            Query query = new Query(queryText, queryText);

            TransformationBuilder builder = new TransformationBuilder(metadata);

            long start = System.nanoTime();

            builder.makeQuery(query);

            if (i > 2) {
                long finish = System.nanoTime();
                long timeElapsed = (finish - start) / 1000000;
                times.add(timeElapsed);

                System.out.println("#Q" + (i-2) + ": " + queryText);
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

        System.out.println("STATS:\n");
        System.out.println("Total time: " + (times.stream().reduce(0L, Long::sum)) + " ms");
        System.out.println("Max time: " + (Collections.max(times)) + " ms");
        System.out.println("Min time: " + (Collections.min(times)) + " ms");
        System.out.println("Avg time: " + (times.stream().mapToInt(Math::toIntExact).average().orElse(0.0)) + " ms");
    }

}
