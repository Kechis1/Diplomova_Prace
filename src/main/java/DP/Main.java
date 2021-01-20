package DP;

import DP.Database.DatabaseMetadata;
import DP.Database.Respond.Respond;
import DP.Database.Respond.Transform;
import DP.antlr4.tsql.TSqlRunner;



public class Main {
    public static void main(String[] args) {
        // init data
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        String query = "SELECT PID, JMENO, JMENO FROM DBO.PREDMET";
        Respond respond = new Respond(query, query);

        // run methods
        respond = TSqlRunner.runSelectClause(metadata, respond);

        for (Transform r : respond.getQueryTransforms()) {
            System.out.println(r.getOutputQuery());
        }

        System.out.println(respond);
    }
}
