package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.BetweenTransformation;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Transformations.TransformationBuilder;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class RandomTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindUnnecessaryBetweenFullRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryBetweenSource")
    void doFindUnnecessaryBetweenFullRunTest(String requestQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        for (int i = 1; i <= query.getCurrentRunNumber(); i++) {
            System.out.println("Run (" + i + "): ");
            if (query.getQueryTransforms().get(i) != null) {
                for (Transformation r : query.getQueryTransforms().get(i)) {
                    System.out.println(r);
                }
            } else {
                System.out.println("--");
            }
        }
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
    }

    public static Stream<Arguments> doFindUnnecessaryBetweenSource() {
        return Stream.of(
                // WhereTransformation

                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = TRUE",
                        "SELECT TRUE as jmeno FROM STUDENT where jmeno = TRUE"),

                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID = stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 = 1 AND ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 = 1 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 OR JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno FROM STUDENT where 1 = 1 or jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT * FROM STUDENT STT JOIN STUDUJE SDE ON STT.SID = STT.SID WHERE SDE.SID LIKE STT.SID AND 1 = 1 HAVING sum(SID) > 3 ORDER BY STT.SID",
                        "SELECT * FROM STUDENT STT JOIN STUDUJE SDE ON STT.SID = STT.SID WHERE SDE.SID LIKE STT.SID HAVING sum(SID) > 3 ORDER BY STT.SID"),


                // BetweenTransformation
                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID BETWEEN stt.sID AND stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 BETWEEN 0 AND 2 AND ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 OR JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno FROM STUDENT where 1 BETWEEN 0 AND 2 or jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND 1 BETWEEN 0 AND 2 HAVING sum(STT.SID) > 3 ORDER BY stt.SID",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID HAVING sum(STT.SID) > 3 ORDER BY stt.SID"),



                // LikeTransformation + SelectClauseTransformation
               Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS' HAVING sum(STE.SID) > 3 ORDER BY STE.SID",
                        "SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS' HAVING sum(STE.SID) > 3 ORDER BY STE.SID"),
                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 LIKE 1 AND ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 like 1 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND JMENO = 'PETR' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND JMENO = 'PETR' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO, JMENO FROM STUDENT WHERE JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno, 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO, JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno, jmeno FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
               Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 OR JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno FROM STUDENT where 1 LIKE 1 or jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT 'AHOJ' FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'ahoj' FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT 1 AS A FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 1 AS a FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT 1 FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 1 FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'Petr' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'Petr' as jmeno FROM STUDENT WHERE jmeno = 'Petr' HAVING sum(SID) > 3 ORDER BY SID"),



                //  JoinTableTransformation
               Arguments.arguments("SELECT DISTINCT predmet.jmeno FROM student JOIN (predmet LEFT JOIN studuje ON predmet.pID = studuje.pID) ON student.sID = studuje.sID",
                        "SELECT DISTINCT predmet.jmeno FROM student JOIN (predmet LEFT JOIN studuje ON predmet.pID = studuje.pID) ON student.sID = studuje.sID"),
                    Arguments.arguments("SELECT DISTINCT STUDENT.SID FROM (STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID) INNER JOIN PREDMET ON STUDUJE.PID = PREDMET.PID",
                        "SELECT DISTINCT STUDENT.SID FROM (STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID) INNER JOIN PREDMET ON STUDUJE.PID = PREDMET.PID"),
                Arguments.arguments("SELECT DISTINCT PREDMET.JMENO, SUM(PREDMET.PID) FROM STUDENT JOIN PREDMET ON STUDENT.SID = PREDMET.PID WHERE PREDMET.PID > 3 AND STUDENT.SID > 3 OR STUDENT.JMENO = 'ADAM' GROUP BY PREDMET.PID HAVING MIN(STUDENT.SID) > 3 ORDER BY PREDMET.PID, STUDENT.SID",
                        "SELECT DISTINCT PREDMET.JMENO, SUM(PREDMET.PID) FROM STUDENT JOIN PREDMET ON STUDENT.SID = PREDMET.PID WHERE PREDMET.PID > 3 AND STUDENT.SID > 3 OR STUDENT.JMENO = 'ADAM' GROUP BY PREDMET.PID HAVING MIN(STUDENT.SID) > 3 ORDER BY PREDMET.PID, STUDENT.SID"),
              Arguments.arguments("SELECT DISTINCT STUDENT.SID FROM (STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID) INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID",
                        "SELECT DISTINCT STUDENT.SID FROM (STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID) INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID"),
                Arguments.arguments("SELECT DISTINCT STUDENT.SID FROM STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID INNER JOIN PREDMET ON STUDUJE.PID = PREDMET.PID",
                        "SELECT DISTINCT STUDENT.SID FROM STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID INNER JOIN PREDMET ON STUDUJE.PID = PREDMET.PID"),
                Arguments.arguments("SELECT DISTINCT STUDENT.SID FROM STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID",
                        "SELECT DISTINCT STUDENT.SID FROM STUDENT INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID"),
                Arguments.arguments("SELECT DISTINCT STUDENT.* " +
                                "FROM STUDENT " +
                                "LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID " +
                                "INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID " +
                                "LEFT JOIN STUDUJE s1 ON STUDENT.SID = 3",
                        "SELECT DISTINCT STUDENT.* " +
                                "FROM STUDENT " +
                                "INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID"),



                // ExistsTransformation
                Arguments.arguments("SELECT * FROM DBO.STUDUJE SDT WHERE EXISTS (SELECT * FROM DBO.PREDMET PDT WHERE SDT.PID = PDT.PID) ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDUJE SDT ORDER BY SDT.SID"),
                Arguments.arguments("SELECT * FROM student WHERE 1 = DATEADD(DAY, 1, ROK_NAROZENI)",
                        "SELECT * FROM student WHERE 1 = DATEADD(DAY, 1, ROK_NAROZENI)"),
                Arguments.arguments("SELECT * FROM student x WHERE EXISTS(SELECT * FROM studuje y WHERE x.sID + 1 = y.sID + 1) ORDER BY x.SID",
                        "SELECT * FROM student x WHERE EXISTS(SELECT 1 FROM studuje y WHERE x.sID + 1 = y.sID + 1) ORDER BY x.SID"),
                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE EXISTS(SELECT 1) GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND EXISTS(SELECT 1) HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND EXISTS(SELECT 1) AND ROK_NAROZENI = 2000 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND EXISTS(SELECT 1) ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil' ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR EXISTS(SELECT 1) ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or EXISTS(SELECT 1) ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) OR JMENO = 'ADAM' ORDER BY STUDENT.SID",
                        "SELECT jmeno FROM STUDENT where EXISTS(SELECT 1) or jmeno = 'adam' ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND EXISTS(SELECT 1) ORDER BY SSTT.SID",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID ORDER BY STT.SID"),


                // JoinConditionTransformation
                Arguments.arguments("SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) HAVING sum(SID) > 3 ORDER BY STUDENT.SID",
                        "SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) HAVING sum(SID) > 3 ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID",
                        "SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT * FROM student x JOIN studuje y ON x.sID + 1 = y.sID + 1 HAVING sum(x.SID) > 3 ORDER BY x.SID",
                        "SELECT * FROM student x JOIN studuje y ON x.sID + 1 = y.sID + 1 HAVING sum(x.SID) > 3 ORDER BY x.SID"),
                Arguments.arguments("SELECT STT.SID FROM STUDENT STT JOIN STUDUJE SDE ON 1 = 1 WHERE ROK_NAROZENI = JMENO GROUP BY STT.SID HAVING SUM(STT.SID) > 3 ORDER BY STT.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON 1 = 1 WHERE ROK_NAROZENI = JMENO GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID AND 1 = 1 WHERE JMENO = 'adam' ORDER BY STUDUJE.SID",
                        "SELECT 'adam' as jmeno FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID where jmeno = 'adam' ORDER BY STUDUJE.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' ORDER BY STUDENT.SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID AND 1 = 1 AND ROK_NAROZENI = '33' WHERE JMENO = 'ADAM' AND ROK_NAROZENI = 2000 ORDER BY STUDUJE.SID",
                        "SELECT 'adam' as jmeno FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID AND ROK_NAROZENI = '33' where jmeno = 'adam' and ROK_NAROZENI = 2000 ORDER BY STUDUJE.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR STUDENT.JMENO = 'Emil' AND 1 = 1 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR STUDENT.JMENO = 'Emil' ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON 1 = 1 AND JMENO = 'ADAM' ORDER BY SID",
                        "SELECT 'ADAM' as jmeno FROM STUDENT JOIN STUDUJE ON JMENO = 'ADAM' ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON 1 = 1 OR JMENO = 'ADAM' HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID",
                        "SELECT jmeno FROM STUDENT JOIN STUDUJE ON 1 = 1 OR JMENO = 'ADAM' HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID"),


                // GroupByTransformation
                Arguments.arguments("SELECT SID FROM STUDENT GROUP BY SID HAVING sum(SID) > 3",
                        "SELECT SID FROM STUDENT GROUP BY SID HAVING sum(SID) > 3"),
                Arguments.arguments("SELECT SID FROM STUDENT GROUP BY SID HAVING SUM(SID) > 1",
                        "SELECT SID FROM STUDENT GROUP BY SID HAVING SUM(SID) > 1"),
                Arguments.arguments("SELECT * FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID GROUP BY STUDENT.SID, STUDUJE.SID, STUDUJE.ROK, STUDUJE.PID",
                        "SELECT * FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID"),
                Arguments.arguments("SELECT SID, (SELECT 1 FROM STUDUJE WHERE 1 = 0) FROM STUDENT GROUP BY SID",
                        "SELECT SID, (SELECT 1 FROM STUDUJE WHERE 1 = 0) FROM STUDENT GROUP BY SID"),
                Arguments.arguments("SELECT SID, SUM(1 + 1) FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + 1 FROM STUDENT"),
                Arguments.arguments("SELECT SID, (SELECT SUM(SID) FROM STUDUJE WHERE 1 = 0 GROUP BY SID) FROM STUDENT GROUP BY SID",
                        "SELECT SID, (SELECT SUM(SID) FROM STUDUJE WHERE 1 = 0 GROUP BY SID) FROM STUDENT GROUP BY SID"),
                Arguments.arguments("SELECT SID, 1 + 1 FROM STUDENT",
                        "SELECT SID, 1 + 1 FROM STUDENT"),
                Arguments.arguments("SELECT SID, 1 + SUM(1) FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + 1 FROM STUDENT"),
                Arguments.arguments("SELECT SID, DATEADD(DAY, SUM(SID), SID) FROM STUDENT GROUP BY SID",
                        "SELECT SID, DATEADD(DAY, SID, SID) FROM STUDENT"),
                Arguments.arguments("SELECT SID, 1 + SUM(SID) FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID, SUM(SID) + 1 FROM STUDENT GROUP BY SID",
                        "SELECT SID, SID + 1 FROM STUDENT"),
                Arguments.arguments("SELECT SID, SUM(1) + SID FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID, AVG(1) + SID FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID, MIN(1) + SID FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID, MAX(1) + SID FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID, COUNT(SID + 1) + SID FROM STUDENT GROUP BY SID",
                        "SELECT SID, 1 + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID, SUM(SID * ROK_NAROZENI) FROM STUDENT GROUP BY SID",
                        "SELECT SID, SID * ROK_NAROZENI FROM STUDENT"),
                Arguments.arguments("SELECT SID, CAST('ROK_NAROZENI' as numeric(9,2)) as [ROK_NAROZENI] FROM STUDENT GROUP BY SID",
                        "SELECT SID, CAST('ROK_NAROZENI' as numeric(9,2)) as [ROK_NAROZENI] FROM STUDENT"),



                // union + union all
                // any
                // all
                // select into
                // case

                Arguments.arguments("SELECT COUNT(DISTINCT JMENO) FROM STUDENT",
                        "SELECT COUNT(DISTINCT JMENO) FROM STUDENT"),
                Arguments.arguments("SELECT COUNT(DISTINCT JMENO) FROM STUDENT",
                        "SELECT COUNT(DISTINCT JMENO) FROM STUDENT"),
                Arguments.arguments("SELECT SID * SID + SID FROM STUDENT",
                        "SELECT SID * SID + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID * (SID + ISNULL(SID, 0)) FROM STUDENT",
                        "SELECT SID * (SID + ISNULL(SID, 0)) FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO LIKE 'Ahoj'",
                        "SELECT 'Ahoj' as JMENO FROM STUDENT WHERE JMENO LIKE 'Ahoj'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO IN ('Ahoj')",
                        "SELECT JMENO FROM STUDENT WHERE JMENO IN ('Ahoj')"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE SID BETWEEN #07/01/1996# AND #07/31/1996#",
                        "SELECT SID FROM STUDENT WHERE SID BETWEEN #07/01/1996# AND #07/31/1996#"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE #07/01/1996# BETWEEN #07/01/1996# AND #07/31/1996#",
                        "SELECT SID FROM STUDENT"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 1 AND 2",
                        "SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 1 AND 2"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 3 AND 4",
                        "SELECT SID FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO NOT LIKE 'Ahoj'",
                        "SELECT JMENO FROM STUDENT WHERE JMENO NOT LIKE 'Ahoj'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO IS NULL",
                        "SELECT NULL AS JMENO FROM STUDENT WHERE JMENO IS NULL"),
                Arguments.arguments("SELECT TOP 100 JMENO FROM STUDENT",
                        "SELECT TOP 100 JMENO FROM STUDENT"),
                Arguments.arguments("SELECT TOP 100 PERCENT JMENO FROM STUDENT",
                        "SELECT TOP 100 PERCENT JMENO FROM STUDENT"),
                Arguments.arguments("SELECT MAX(SID) FROM STUDENT",
                        "SELECT MAX(SID) FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO IS NOT NULL",
                        "SELECT JMENO FROM STUDENT WHERE JMENO IS NOT NULL"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE NOT JMENO = 'ADAM'",
                        "SELECT JMENO FROM STUDENT WHERE NOT JMENO = 'ADAM'"),
                Arguments.arguments("INSERT INTO STUDENT (SID, JMENO) VALUES (1, 'Adam')",
                        "INSERT INTO STUDENT (SID, JMENO) VALUES (1, 'Adam')"),
                Arguments.arguments("SELECT COUNT(*) AS DISTINCT_JMENO FROM (SELECT DISTINCT JMENO FROM STUDENT)",
                        "SELECT COUNT(*) AS DISTINCT_JMENO FROM (SELECT DISTINCT JMENO FROM STUDENT)")
        );
    }
}
