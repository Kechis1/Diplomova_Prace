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
        TSqlRunner.runEqualOuterConditions(metadata, "SELECT distinct SDT.SID, SDT.JMENO " +
                "FROM DBO.STUDENT SDT " +
                "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID");
        System.out.println("2");
        TSqlRunner.runEqualOuterConditions(metadata, "SELECT * " +
                "FROM DBO.STUDENT SDT " +
                "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                "LEFT JOIN DBO.PREDMET PRT ON SDE.PID = PRT.PID AND SDE.PID = PRT.PID");
    }
}
