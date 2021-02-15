package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.*;


public class Main {
    public static void main(String[] args) {
        // init data
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        String requestQuery = "SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID";
        Query query = new Query(requestQuery, requestQuery);

        runChain(metadata, query);
      //   printRuns(query);
    }

    private static void runChain(DatabaseMetadata metadata, Query query) {
        TransformationBuilder builder = new TransformationBuilder(metadata);
        builder.makeQuery(query);

        printRuns(query);

        System.out.println("---");
        System.out.println(query);
    }

    private static void runChain(Query query, QueryHandler chain) {
        TransformationBuilder builder = new TransformationBuilder(chain);
        builder.makeQuery(query);

        printRuns(query);

       System.out.println("---");
        System.out.println(query);
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
