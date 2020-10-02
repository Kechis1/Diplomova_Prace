package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;


public class Main {
    public static void main(String[] args) {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        // TSqlRunner.runGroupBy(metadata, "SELECT pId, jmeno, sum(pId) FROM dbo.predmet GROUP BY pid, jmeno");

       /*TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "WHERE '11' > '10'");
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "WHERE 9 < '10'");
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "WHERE '3.22E+1' < 3.52E+1");*/
        /*TSqlRunner.runEqualConditionInComparisonOperators(metadata, "SELECT *\n" +
                "FROM DBO.STUDENT SDT\n" +
                "WHERE 0xA < 0xA");*/

        Integer.valueOf("0xA");


    }
}
