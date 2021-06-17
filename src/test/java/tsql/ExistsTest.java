package tsql;

import DP.Database.ColumnItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.ExistsTransformation;
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
public class ExistsTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private ExistsTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new ExistsTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryExistsTest {index} query = {0}")
    @MethodSource("doFindNecessaryExistsSource")
    void doFindNecessaryExistsTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryExistsOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryExistsSource")
    void doFindUnnecessaryExistsOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryExistsFullRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryExistsSource")
    void doFindUnnecessaryExistsFullRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 3);
        assertEquals(query.getQueryTransforms().get(2).size(), 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryExistsBasedOnRecordsCountTest {index} query = {0}, recordsCount = {1}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryExistsBasedOnRecordsCountSource")
    void doFindUnnecessaryExistsBasedOnRecordsCountTest(String requestQuery, int recordsCount, String resultQuery) {
        DatabaseTable table = metadata.findTable("STUDUJE", "SDT");
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        table.setRecordsCount(recordsCount);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryExistsBasedOnNullableForeignKeyTest {index} query = {0}")
    @MethodSource("doFindNecessaryExistsBasedOnNullableForeignKeySource")
    void doFindUnnecessaryExistsBasedOnNullableForeignKeyTest(String requestQuery) {
        DatabaseTable table = metadata.findTable("STUDUJE", "SDT");
        ColumnItem column = table.findColumn("PID");
        column.setNullable(true);
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doExistsWhereResultIsEmptySetTest {index} query = {0}, condition = {1}")
    @MethodSource("doExistsWhereResultIsEmptySetSource")
    void doExistsWhereResultIsEmptySetTest(String requestQuery, String condition) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(condition + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet, query.getQueryTransforms().get(1).get(0).getMessage());
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doExistsRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doExistsRandomSource")
    void doExistsRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doExistsRandomSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDUJE SDT WHERE EXISTS (SELECT * FROM DBO.PREDMET PDT WHERE SDT.PID = PDT.PID) ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDUJE SDT ORDER BY SDT.SID"),
                Arguments.arguments("SELECT * FROM student WHERE 1 = DATEADD(DAY, 1, ROK_NAROZENI)",
                        "SELECT * FROM student WHERE 1 = DATEADD(DAY, 1, ROK_NAROZENI)"),
                Arguments.arguments("SELECT * FROM student x WHERE EXISTS(SELECT * FROM studuje y WHERE x.sID + 1 = y.sID + 1) ORDER BY x.SID",
                        "SELECT * FROM student x WHERE EXISTS(SELECT 1 FROM studuje y WHERE x.sID + 1 = y.sID + 1) ORDER BY x.SID"),
                Arguments.arguments("SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE EXISTS(SELECT 1) GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID",
                        "SELECT stt.sid FROM student stt JOIN studuje sde ON stt.sID = sde.sID GROUP BY stt.sid HAVING sum(stt.sid) > 3 ORDER BY stt.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'adam' AND EXISTS(SELECT 1) HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam'  HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND EXISTS(SELECT 1) AND ROK_NAROZENI = 2000 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' and ROK_NAROZENI = 2000 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR JMENO = 'Emil' AND EXISTS(SELECT 1) ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' OR JMENO = 'Emil'  ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' OR EXISTS(SELECT 1) ORDER BY SID",
                        "SELECT JMENO FROM STUDENT where jmeno = 'adam' or EXISTS(SELECT 1) ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) AND JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE EXISTS(SELECT 1) OR JMENO = 'ADAM' ORDER BY STUDENT.SID",
                        "SELECT jmeno FROM STUDENT where EXISTS(SELECT 1) or jmeno = 'adam' ORDER BY STUDENT.SID"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID AND EXISTS(SELECT 1) ORDER BY STT.SID",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID  ORDER BY STT.SID")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryExistsBasedOnRecordsCountSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET PDT WHERE NOT EXISTS (SELECT * FROM STUDUJE SDT WHERE PDT.PID = SDT.PID)",
                0,
                "SELECT * FROM DBO.PREDMET PDT"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT * FROM STUDUJE)",
                        46,
                        "SELECT * FROM DBO.PREDMET")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryExistsSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT 1)",
                "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT 0)",
                        "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT null)",
                        "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT t1.a FROM (SELECT 1 as a) t1)",
                        "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.STUDUJE SDT WHERE EXISTS (SELECT * FROM DBO.PREDMET PDT WHERE SDT.PID = PDT.PID)",
                        "SELECT * FROM DBO.STUDUJE SDT")
        );
    }

    public static Stream<Arguments> doFindNecessaryExistsBasedOnNullableForeignKeySource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                "FROM DBO.STUDUJE SDT " +
                "WHERE EXISTS (SELECT * " +
                "FROM DBO.PREDMET PDT " +
                "WHERE SDT.PID = PDT.PID " +
                ")")
        );
    }

    public static Stream<Arguments> doFindNecessaryExistsSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET PDT " +
                        "WHERE EXISTS (SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE PDT.PID = SDT.PID " +
                        ")"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE EXISTS (SELECT * " +
                        "FROM DBO.PREDMET PDT " +
                        "WHERE SDT.PID = PDT.PID AND PDT.JMENO = 'DAIS' " +
                        ")")
        );
    }

    public static Stream<Arguments> doExistsWhereResultIsEmptySetSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE NOT EXISTS (SELECT 1)",
                        "NOT EXISTS (SELECT 1)")
        );
    }
}
