package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        // TSqlRunner.RunGroupBy(metadata, "SELECT pId, jmeno, sum(pId) FROM dbo.predmet GROUP BY pid, jmeno");

        TSqlRunner.RunSameCondition(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                "WHERE SDT.SID = SDE.SID\n" +
                "ORDER BY SDT.SID");
    }
}
