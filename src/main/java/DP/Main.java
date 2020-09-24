package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");


        // TSqlRunner.RunTests();


        TSqlRunner.RunGroupByWithPrimaryKey();
        // TSqlRunner.RunSameCondition();
    }
}
