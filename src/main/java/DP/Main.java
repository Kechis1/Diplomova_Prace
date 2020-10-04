package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;


public class Main {
    public static void main(String[] args) {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        // TSqlRunner.runGroupBy(metadata, "SELECT pId, jmeno, sum(pId) FROM dbo.predmet GROUP BY pid, jmeno");

  /*      TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "WHERE (('A' = 'a' AND 'b' > 'a') OR 'c' > 'b') AND 1 = 1");*/

        System.out.println("1");
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "WHERE STD.jmeno = jmeno");
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "         INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                "         INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                "WHERE SDT.SID = SDE.SID");
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT distinct SDT.SID, SDT.JMENO\n" +
                "FROM DBO.STUDENT SDT\n" +
                "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID");
    }
}
