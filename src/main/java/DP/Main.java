package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;


public class Main {
    public static void main(String[] args) {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        // TSqlRunner.RunGroupBy(metadata, "SELECT pId, jmeno, sum(pId) FROM dbo.predmet GROUP BY pid, jmeno");

        TSqlRunner.RunSameCondition(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                "JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                "WHERE SDE.PID = PDT.PID\n" +
                "ORDER BY SDT.SID");
    }
}
