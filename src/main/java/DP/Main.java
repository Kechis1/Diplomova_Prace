package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;


public class Main {
    public static void main(String[] args) {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        TSqlRunner.runEqualConditionInOperatorExists(metadata, "SELECT * " +
                "FROM DBO.PREDMET " +
                "WHERE EXISTS (SELECT t1.a FROM (SELECT 1 as a) t1)");
    }
}
