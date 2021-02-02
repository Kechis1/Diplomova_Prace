package DP;

import DP.Database.DatabaseMetadata;
import DP.Transformations.Query;
import DP.Transformations.Transform;
import DP.antlr4.tsql.TSqlRunner;



public class Main {
    public static void main(String[] args) {
        // init data
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        String requestQuery = "SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT t1.a FROM (SELECT 1 as a) t1)";
        Query query = new Query(requestQuery, requestQuery);

        // run methods
        query = TSqlRunner.runEqualConditionInOperatorExists(metadata, query);

        for (Transform r : query.getQueryTransforms()) {
            System.out.println(r.getOutputQuery());
        }

        System.out.println(query);
    }
}
