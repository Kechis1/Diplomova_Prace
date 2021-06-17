package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.JoinConditionTransformation;
import DP.Transformations.Query;
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
public class JoinConditionTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private JoinConditionTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new JoinConditionTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryJoinConditionOneRunTest {index} query = {0}")
    @MethodSource("doFindNecessaryJoinConditionSource")
    void doFindNecessaryJoinConditionOneRunTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryJoinConditionOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryJoinConditionSource")
    void doFindUnnecessaryJoinConditionOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryJoinConditionFullRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryJoinConditionSource")
    void doFindUnnecessaryJoinConditionFullRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 4);
        assertEquals(query.getQueryTransforms().get(2).size(), 3);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION", query.getQueryTransforms().get(1).get(1).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doJoinConditionRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doJoinConditionRandomSource")
    void doJoinConditionRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doJoinConditionRandomSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) HAVING sum(SID) > 3 ORDER BY STUDENT.SID",
                    "SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) HAVING sum(SID) > 3 ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID",
                        "SELECT * FROM student JOIN STUDUJE ON 1 = DATEADD(DAY, 1, ROK_NAROZENI) OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT * FROM student x JOIN studuje y ON x.sID + 1 = y.sID + 1 HAVING sum(x.SID) > 3 ORDER BY x.SID",
                        "SELECT * FROM student x JOIN studuje y ON x.sID + 1 = y.sID + 1 HAVING sum(x.SID) > 3 ORDER BY x.SID"),
                Arguments.arguments("SELECT STT.SID FROM STUDENT STT JOIN STUDUJE SDE ON 1 = 1 WHERE ROK_NAROZENI = JMENO GROUP BY STT.SID HAVING SUM(STT.SID) > 3 ORDER BY STT.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON 1 = 1 WHERE ROK_NAROZENI = JMENO GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID AND 1 = 1 WHERE JMENO = 'adam' ORDER BY STUDUJE.SID",
                        "SELECT 'adam' as jmeno FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID  where jmeno = 'adam' ORDER BY STUDUJE.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' ORDER BY STUDENT.SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID AND 1 = 1 AND ROK_NAROZENI = '33' WHERE JMENO = 'ADAM' AND ROK_NAROZENI = 2000 ORDER BY STUDUJE.SID",
                        "SELECT 'adam' as jmeno FROM STUDENT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID AND ROK_NAROZENI = '33' where jmeno = 'adam' and ROK_NAROZENI = 2000 ORDER BY STUDUJE.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR STUDENT.JMENO = 'Emil' AND 1 = 1 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR STUDENT.JMENO = 'Emil'  ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT JOIN STUDUJE ON STUDENT.JMENO = 'ADAM' OR 1 = 1 HAVING sum(STUDENT.SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON 1 = 1 AND JMENO = 'ADAM' ORDER BY SID",
                        "SELECT 'ADAM' as jmeno FROM STUDENT JOIN STUDUJE ON JMENO = 'ADAM' ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT JOIN STUDUJE ON 1 = 1 OR JMENO = 'ADAM' HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID",
                        "SELECT jmeno FROM STUDENT JOIN STUDUJE ON 1 = 1 OR JMENO = 'ADAM' HAVING sum(STUDENT.SID) > 3 ORDER BY STUDENT.SID")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryJoinConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDT.SID = SDE.SID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON  SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID")
        );
    }

    public static Stream<Arguments> doFindNecessaryJoinConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * FROM DBO.student SDT " +
                        "LEFT JOIN DBO.studuje SDE ON SDT.sID = SDE.sID or jmeno = 'Petr' " +
                        "LEFT JOIN DBO.predmet PDT ON SDE.pID= PDT.pID and SDT.sID = SDE.sID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID")
        );
    }
}
