package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;


public class Main {
    public static void main(String[] args) {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        TSqlRunner.runSelectClause(metadata, "SELECT PID, JMENO, JMENO " +
                "FROM DBO.PREDMET " +
                "WHERE JMENO = 'DAIS'");
    }
}
