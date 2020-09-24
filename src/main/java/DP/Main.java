package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;

import java.io.IOException;
import java.util.ArrayList;


public class Main {
    public static void main(String[] args) throws IOException {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pId, jmeno\n" +
                "FROM dbo.predmet\n" +
                "GROUP BY pid, jmeno\n" +
                "HAVING sum(pid) > 3");


        // runner.RunSameCondition();
    }
}
