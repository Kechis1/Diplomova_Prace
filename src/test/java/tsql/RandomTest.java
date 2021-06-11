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
                /*Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID = stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 = 1",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1",
                        "SELECT JMENO FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 = 1 AND ROK_NAROZENI = 2000",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 = 1",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 = 1",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 = 1"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 AND JMENO = 'ADAM'",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 OR JMENO = 'ADAM'",
                        "SELECT jmeno FROM STUDENT where 1 = 1 or jmeno = 'adam'"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND 1 = 1",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID")*/


                // BetweenTransformation
               /* Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID BETWEEN stt.sID AND stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 BETWEEN 0 AND 2",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2",
                        "SELECT JMENO FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 BETWEEN 0 AND 2 AND ROK_NAROZENI = 2000",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 BETWEEN 0 AND 2",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 BETWEEN 0 AND 2",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 BETWEEN 0 AND 2"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 AND JMENO = 'ADAM'",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 OR JMENO = 'ADAM'",
                        "SELECT jmeno FROM STUDENT where 1 BETWEEN 0 AND 2 or jmeno = 'adam'"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND 1 BETWEEN 0 AND 2",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID")*/


                // LikeTransformation + SelectClauseTransformation
              /* Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'",
                        "SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'"),
                /*Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 LIKE 1",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1",
                        "SELECT JMENO FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 LIKE 1 AND ROK_NAROZENI = 2000",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 LIKE 1",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 LIKE 1",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 like 1"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND JMENO = 'PETR'",
                        "SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND JMENO = 'PETR'"),
                Arguments.arguments("SELECT JMENO, JMENO FROM STUDENT WHERE JMENO = 'ADAM'",
                        "SELECT 'adam' as jmeno, 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO, JMENO FROM STUDENT",
                        "SELECT jmeno, jmeno FROM STUDENT"),
               Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 AND JMENO = 'ADAM'",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 OR JMENO = 'ADAM'",
                        "SELECT jmeno FROM STUDENT where 1 LIKE 1 or jmeno = 'adam'"),
                Arguments.arguments("SELECT 'AHOJ' FROM STUDENT",
                        "SELECT 'ahoj' FROM STUDENT"),
                Arguments.arguments("SELECT 1 AS A FROM DBO.STUDENT",
                        "SELECT 1 AS a FROM DBO.STUDENT"),
                Arguments.arguments("SELECT 1 FROM DBO.STUDENT",
                        "SELECT 1 FROM DBO.STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'Petr'",
                        "SELECT 'Petr' as jmeno FROM STUDENT WHERE jmeno = 'Petr'"),*/


                //  JoinTableTransformation
                /*Arguments.arguments("SELECT DISTINCT predmet.jmeno FROM student JOIN (predmet LEFT JOIN studuje ON predmet.pID = studuje.pID) ON student.sID = studuje.sID",
                        "SELECT DISTINCT predmet.jmeno FROM student JOIN (predmet LEFT JOIN studuje ON predmet.pID = studuje.pID) ON student.sID = studuje.sID"),*/

                // ExistsTransformation
                Arguments.arguments("SELECT * FROM student x WHERE EXISTS(SELECT * FROM studuje y WHERE x.sID + 1 = y.sID + 1)",
                        "SELECT * FROM student x WHERE EXISTS(SELECT 1 FROM studuje y WHERE x.sID + 1 = y.sID + 1)"),
                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE EXISTS(SELECT 1) GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND EXISTS(SELECT 1)",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1)",
                        "SELECT JMENO FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND EXISTS(SELECT 1) AND ROK_NAROZENI = 2000",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND EXISTS(SELECT 1)",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR EXISTS(SELECT 1)",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or EXISTS(SELECT 1)"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) AND JMENO = 'ADAM'",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) OR JMENO = 'ADAM'",
                        "SELECT jmeno FROM STUDENT where EXISTS(SELECT 1) or jmeno = 'adam'"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND EXISTS(SELECT 1)",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID")
        );
    }
}
