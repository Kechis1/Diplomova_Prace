package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.*;


public class Main {
    public static void main(String[] args) {
        // init data
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        String requestQuery = "SELECT pr.pId, stt.sID, ste.sID, ste.pID FROM dbo.student stt JOIN dbo.studuje ste ON stt.sID = ste.sID JOIN dbo.predmet pr ON ste.pID = pr.pID GROUP BY pr.pID, stt.sID, ste.pID, ste.sID, ste.rok";
        Query query = new Query(requestQuery, requestQuery);

        runChain(metadata, query);
        printRuns(query);
    }

    private static void runChain(DatabaseMetadata metadata, Query query) {
        TransformationBuilder builder = new TransformationBuilder(metadata);
        builder.makeQuery(query);

        printRuns(query);

        System.out.println(query);
    }

    private static void runChain(DatabaseMetadata metadata, Query query, QueryHandler chain) {
        TransformationBuilder builder = new TransformationBuilder(metadata, chain);
        builder.makeQuery(query);

        printRuns(query);

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
