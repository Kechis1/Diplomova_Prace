package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        // TSqlRunner.RunGroupBy(metadata, "SELECT pId, jmeno, sum(pId) FROM dbo.predmet GROUP BY pid, jmeno");

        TSqlRunner.RunSameCondition(metadata, "SELECT *\n" +
                "FROM DBO.PREDMET\n" +
                "WHERE 1 = 1");
    }
}
