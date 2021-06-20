package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.MetadataException;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
import DP.Transformations.WhereComparisonTransformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;

import org.junit.jupiter.api.BeforeEach;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class WhereComparisonTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private WhereComparisonTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        try {
            metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
            transformation = new WhereComparisonTransformation(null, metadata);
            transformationBuilder = new TransformationBuilder(metadata);
        } catch (MetadataException exception) {
            Assertions.fail(exception.getMessage());
        }
    }

    @ParameterizedTest(name = "doFindNecessaryWhereComparisonTest {index} query = {0}")
    @MethodSource("doFindNecessaryWhereComparisonSource")
    void doFindNecessaryWhereComparisonTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryWhereComparisonOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryWhereComparisonSource")
    void doFindUnnecessaryWhereComparisonOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryWhereComparisonFullRunTest {index} query = {0}, resultQuery = {1}, transformationsInFirstRun = {2}, transformationsSecondInRun = {3}")
    @MethodSource("doFindUnnecessaryWhereComparisonSource")
    void doFindUnnecessaryWhereComparisonFullRunTest(String requestQuery, String resultQuery, int transformationsInFirstRun, int transformationsInSecondRun) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), transformationsInFirstRun);
        assertEquals(query.getQueryTransforms().get(2).size(), transformationsInSecondRun);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION", query.getQueryTransforms().get(1).get(transformationsInFirstRun - 3).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doWhereComparisonWhereResultIsEmptySetTest {index} query = {0}, condition = {1}")
    @MethodSource("doWhereComparisonWhereResultIsEmptySetSource")
    void doWhereComparisonWhereResultIsEmptySetTest(String requestQuery, String condition) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(condition + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet, query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doWhereComparisonWhereRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doWhereComparisonWhereRandomSource")
    void doWhereComparisonWhereRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doWhereComparisonWhereRandomSource() {
        return Stream.of(Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID = stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 = 1 AND ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 = 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 = 1 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 = 1 OR JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno FROM STUDENT where 1 = 1 or jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT * FROM STUDENT STT JOIN STUDUJE SDE ON STT.SID = STT.SID WHERE SDE.SID LIKE STT.SID AND 1 = 1 HAVING sum(SID) > 3 ORDER BY STT.SID",
                        "SELECT * FROM STUDENT STT JOIN STUDUJE SDE ON STT.SID = STT.SID WHERE SDE.SID LIKE STT.SID  HAVING sum(SID) > 3 ORDER BY STT.SID")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryWhereComparisonSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = 1 GROUP BY PID HAVING SUM(PID) > 3 ORDER BY PID",
                "SELECT * FROM DBO.PREDMET GROUP BY PID HAVING SUM(PID) > 3 ORDER BY PID",
                4,
                2),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 >= 0 ORDER BY PID",
                        "SELECT * FROM DBO.PREDMET ORDER BY PID",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 > 0 ORDER BY PID",
                        "SELECT * FROM DBO.PREDMET ORDER BY PID",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 <= 1",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 < 1",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 <> 0",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = '1'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' = 1",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'a' = 'A'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' >= 'aa'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' > 'aa'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ab' <= 'ac'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' < 'ab'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' <> 'ab'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDT.SID ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID ORDER BY SDT.SID",
                        5,
                        3)
        );
    }

    public static Stream<Arguments> doWhereComparisonWhereResultIsEmptySetSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = 2",
                "1 = 2"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 0 >= 1",
                        "0 >= 1"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 1 > 2",
                        "1 > 2"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 2 <= 1",
                        "2 <= 1"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 2 < 1",
                        "2 < 1"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 1 <> 1",
                        "1 <> 1"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 1 = '2'",
                        "1 = '2'"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE '1' = 2",
                        "'1' = 2"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 'a' = 'B'",
                        "'A' = 'B'"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 'ba' >= 'ca'",
                        "'BA' >= 'CA'"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 'ba' > 'ca'",
                        "'BA' > 'CA'"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 'ab' <= 'aa'",
                        "'AB' <= 'AA'"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 'ac' < 'ab'",
                        "'AC' < 'AB'"),
                Arguments.arguments("SELECT * " +
                                "FROM DBO.PREDMET " +
                                "WHERE 'aa' <> 'aa'",
                        "'AA' <> 'AA'")
        );
    }

    public static Stream<Arguments> doFindNecessaryWhereComparisonSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDT.JMENO " +
                        "ORDER BY SDT.SID")
        );
    }
}
