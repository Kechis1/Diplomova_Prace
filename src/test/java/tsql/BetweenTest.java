package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.BetweenTransformation;
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
public class BetweenTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private BetweenTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new BetweenTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryBetweenOneRunTest {index} query = {0}")
    @MethodSource("doFindNecessaryBetweenSource")
    void doFindNecessaryBetweenOneRunTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doBetweenWhereResultIsEmptySetTest {index} query = {0}, condition = {1}")
    @MethodSource("doBetweenWhereResultIsEmptySetSource")
    void doBetweenWhereResultIsEmptySetTest(String requestQuery, String condition) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(condition + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet, query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryBetweenOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryBetweenSource")
    void doFindUnnecessaryBetweenOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " BETWEEN CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryBetweenFullRunTest {index} query = {0}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryBetweenSource")
    void doFindUnnecessaryBetweenFullRunTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 3);
        assertEquals(query.getQueryTransforms().get(2).size(), 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " BETWEEN CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doBetweenRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doBetweenRandomSource")
    void doBetweenRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doBetweenRandomSource() {
        return Stream.of(Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID BETWEEN stt.sID AND stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 BETWEEN 0 AND 2 AND ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 BETWEEN 0 AND 2 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 BETWEEN 0 AND 2 OR JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno FROM STUDENT where 1 BETWEEN 0 AND 2 or jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND 1 BETWEEN 0 AND 2 HAVING sum(STT.SID) > 3 ORDER BY stt.SID",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID  HAVING sum(STT.SID) > 3 ORDER BY stt.SID")
                );
    }

    public static Stream<Arguments> doFindUnnecessaryBetweenSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'b' BETWEEN 'a' AND 'c'",
                        "SELECT * FROM DBO.STUDENT",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'abc' BETWEEN 'aaa' AND 'abc'",
                        "SELECT * FROM DBO.STUDENT",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1 BETWEEN 0 AND 2",
                        "SELECT * FROM DBO.STUDENT",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1.5 BETWEEN 1.2 AND 1.7",
                        "SELECT * FROM DBO.STUDENT",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE jmeno BETWEEN jmeno AND jmeno",
                        "SELECT * FROM DBO.STUDENT",
                        "SELECT * FROM DBO.STUDENT")
        );
    }

    public static Stream<Arguments> doBetweenWhereResultIsEmptySetSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 'b' BETWEEN 'c' AND 'd'",
                        "'B' BETWEEN 'C' AND 'D'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 'abc' BETWEEN 'abc' AND 'aaa'",
                        "'ABC' BETWEEN 'ABC' AND 'AAA'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 1 BETWEEN 2 AND 5",
                        "1 BETWEEN 2 AND 5"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 1.5 BETWEEN -1.7 AND -1.5",
                        "1.5 BETWEEN -1.7 AND -1.5")
        );
    }

    public static Stream<Arguments> doFindNecessaryBetweenSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT STT " +
                        "INNER JOIN DBO.STUDUJE SDE ON STT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDE.SID BETWEEN 5 AND 10"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT STT " +
                        "INNER JOIN DBO.STUDUJE SDE ON STT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDE.SID BETWEEN STT.SID AND PDT.SID")
        );
    }
}
