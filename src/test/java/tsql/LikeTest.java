package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.MetadataException;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.LikeTransformation;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Transformations.TransformationBuilder;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class LikeTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private LikeTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        try {
            metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
            transformation = new LikeTransformation(null, metadata);
            transformationBuilder = new TransformationBuilder(metadata);
        } catch (MetadataException exception) {
            Assertions.fail(exception.getMessage());
        }
    }

    @ParameterizedTest(name = "doFindNecessaryLikeTest {index} query = {0}")
    @MethodSource("doFindNecessaryLikeSource")
    void doFindNecessaryLikeTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doLikeWhereResultIsEmptySetTest {index} query = {0}, condition = {1}")
    @MethodSource("doLikeWhereResultIsEmptySetSource")
    void doExistsWhereResultIsEmptySetTest(String requestQuery, String condition) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(condition + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet, query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryLikeOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryLikeSource")
    void doFindUnnecessaryLikeOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " LIKE CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryLikeFullRunTest {index} query = {0}, resultQuery = {1}, transformationsInFirstRun = {2}, transformationsSecondInRun = {3}")
    @MethodSource("doFindUnnecessaryLikeSource")
    void doFindUnnecessaryLikeFullRunTest(String requestQuery, String resultQuery, int transformationsInFirstRun, int transformationsInSecondRun) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), transformationsInFirstRun);
        assertEquals(query.getQueryTransforms().get(2).size(), transformationsInSecondRun);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " LIKE CONDITION", query.getQueryTransforms().get(1).get(transformationsInFirstRun - 3).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doLikeRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doLikeRandomSource")
    void doLikeRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doLikeRandomSource() {
        return Stream.of(
                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND 1 LIKE 1 AND ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR 1 LIKE 1 HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or 1 like 1 HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE 1 LIKE 1 OR JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno FROM STUDENT where 1 LIKE 1 or jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryLikeSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE 1",
                "SELECT * FROM DBO.PREDMET",
                3,
                1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '1' ORDER BY PID",
                        "SELECT * FROM DBO.PREDMET ORDER BY PID",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '%1'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '%1%'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '1%'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' LIKE '1'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'string' LIKE 'str%'",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID",
                        5,
                        3)
        );
    }

    public static Stream<Arguments> doFindNecessaryLikeSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM student stt " +
                        "JOIN studuje sde ON stt.sID = stt.sID " +
                        "WHERE sde.sID LIKE stt.sID"),
                Arguments.arguments("SELECT * " +
                        "FROM student stt " +
                        "WHERE stt.prijmeni LIKE stt.jmeno"),
                Arguments.arguments("SELECT * " +
                        "FROM student stt " +
                        "WHERE stt.prijmeni LIKE '%ová'")
        );
    }

    public static Stream<Arguments> doLikeWhereResultIsEmptySetSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                                "FROM student stt " +
                                "WHERE 'a' LIKE 'b'",
                        "'A' LIKE 'B'"),
                Arguments.arguments("SELECT * " +
                                "FROM student stt " +
                                "WHERE 'a' LIKE 2",
                        "'A' LIKE 2"),
                Arguments.arguments("SELECT * " +
                                "FROM student stt " +
                                "WHERE 'a' LIKE 'b%'",
                        "'A' LIKE 'B%'")
        );
    }
}
