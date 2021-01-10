package DP;

import DP.Database.DatabaseMetadata;
import DP.Database.Respond.Respond;
import DP.antlr4.tsql.TSqlRunner;


public class Main {
    public static void main(String[] args) {
        // init data
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        String query = "SELECT pId, jmeno, sum(pid) FROM dbo.predmet GROUP BY pid, jmeno";
        Respond respond = new Respond(query);

        // run methods
        respond = TSqlRunner.runGroupBy(metadata, query, respond);


        System.out.println(respond);
    }
}
