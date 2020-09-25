package DP;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlRunner;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException {
        DatabaseMetadata metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");

        System.out.println("OK");

        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pt.pId, pt.jmeno, count(*), sum(pt.pid)\n" +
                "    FROM dbo.predmet pt\n" +
                "    GROUP BY pt.pid, pt.jmeno");

       /* TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pId, jmeno, sum(pid)\n" +
                "FROM dbo.predmet\n" +
                "GROUP BY pid, jmeno");

        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pr.pId, stt.sID\n" +
                "    FROM dbo.student stt\n" +
                "             JOIN dbo.studuje ste ON stt.sID = ste.sID\n" +
                "             JOIN dbo.predmet pr ON ste.pID = pr.pID\n" +
                "    GROUP BY pr.pID, stt.sID");


        System.out.println("NOT OK");

        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT PID, JMENO\n" +
                "FROM DBO.PREDMET\n" +
                "GROUP BY PID, JMENO");

        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pr.pId, stt.sID, ste.sID, ste.pID\n" +
                "    FROM dbo.student stt\n" +
                "             JOIN dbo.studuje ste ON stt.sID = ste.sID\n" +
                "             JOIN dbo.predmet pr ON ste.pID = pr.pID\n" +
                "    GROUP BY pr.pID, stt.sID, ste.pID, ste.sID, ste.rok");

        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pId, jmeno, count(*)\n" +
                "    FROM dbo.predmet\n" +
                "    GROUP BY pid, sqrt(jmeno), jmeno");


        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pId, jmeno, count(*)\n" +
                "    FROM dbo.predmet\n" +
                "    GROUP BY pid, jmeno");


        TSqlRunner.RunGroupByWithPrimaryKey(metadata, "SELECT pId, jmeno, sum(pId)\n" +
                "    FROM dbo.predmet\n" +
                "    GROUP BY pid, jmeno");
*/


        // runner.RunSameCondition();
    }
}
