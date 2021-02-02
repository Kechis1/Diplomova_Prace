package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Transformations.TransformationBuilder;
import DP.antlr4.tsql.TSqlRunner;



public class Main {
    public static void main(String[] args) {
        // init data
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        String requestQuery = "SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT t1.a FROM (SELECT 1 as a) t1)";
        Query query = new Query(requestQuery, requestQuery);

        runChain(metadata, query);
    }

    private static void runRunnerVersion(DatabaseMetadata metadata, Query query) {
        query = TSqlRunner.runEqualConditionInOperatorExists(metadata, query);

        for (Transformation r : query.getQueryTransforms()) {
            System.out.println(r.getOutputQuery());
        }

        System.out.println(query);
    }

    private static void runChain(DatabaseMetadata metadata, Query query) {
        TransformationBuilder builder = new TransformationBuilder(metadata);
        builder.makeQuery(query);

        for (Transformation r : query.getQueryTransforms()) {
            System.out.println(r.getOutputQuery());
        }

        System.out.println(query);
    }
}
