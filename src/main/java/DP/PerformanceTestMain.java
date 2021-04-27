package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Transformations.TransformationBuilder;
import com.google.common.io.CharStreams;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

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

        for (String queryText : queries) {
            queryText = queryText.replaceAll("\\s", " ").trim().toUpperCase();
            System.out.println(queryText);
            Query query = new Query(queryText, queryText);

            TransformationBuilder builder = new TransformationBuilder(metadata);

            long start = System.nanoTime();

            builder.makeQuery(query);

            long finish = System.nanoTime();
            long timeElapsed = (finish - start)/1000000;

            System.out.println(timeElapsed + " ms\n");
        }
    }
}
