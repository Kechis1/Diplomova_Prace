package tsql;

import DP.Database.ColumnItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Exceptions.MetadataException;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.JoinTableTransformation;
import DP.Transformations.Query;
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
public class JoinTableTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private JoinTableTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        try {
            metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
            transformation = new JoinTableTransformation(null, metadata);
            transformationBuilder = new TransformationBuilder(metadata);
        } catch (MetadataException exception) {
            Assertions.fail(exception.getMessage());
        }
    }

    @ParameterizedTest(name = "doFindNecessaryJoinTableWithNullableTest {index} query = {0}")
    @MethodSource("doFindNecessaryJoinTableWithNullableSource")
    void doFindNecessaryJoinTableWithNullableTest(String requestQuery) {
        DatabaseTable table = metadata.findTable("STUDENT", null);
        ColumnItem sId = table.findColumn("SID");
        sId.setNullable(true);
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryJoinTableOneRunTest {index} query = {0}")
    @MethodSource("doFindNecessaryJoinTableSource")
    void doFindNecessaryJoinTableOneRunTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryJoinTableOneRunTest {index} query = {0}, resultQuery = {1}, message = {2}")
    @MethodSource("doFindUnnecessaryJoinTableSource")
    void doFindUnnecessaryJoinTableOneRunTest(String requestQuery, String resultQuery, String message) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " " + message, query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryJoinTableFullRunTest {index} query = {0}, resultQuery = {1}, message = {2}")
    @MethodSource("doFindUnnecessaryJoinTableSource")
    void doFindUnnecessaryJoinTableFullRunTest(String requestQuery, String resultQuery, String message) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 3);
        assertEquals(query.getQueryTransforms().get(2).size(), 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " " + message, query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doJoinTableRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doJoinTableRandomSource")
    void doJoinTableRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doJoinTableRandomSource() {
        return Stream.of(
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
                Arguments.arguments("SELECT DISTINCT STUDENT.* FROM STUDENT LEFT JOIN STUDUJE ON STUDENT.SID = STUDUJE.SID INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID LEFT JOIN STUDUJE s1 ON STUDENT.SID = 3",
                        "SELECT DISTINCT STUDENT.* FROM STUDENT INNER JOIN PREDMET ON STUDENT.SID = PREDMET.PID")
        );
    }

    public static Stream<Arguments> doFindNecessaryJoinTableWithNullableSource() {
        return Stream.of(
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDT.* " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDE.SID, SDE.body " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDE.* " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryJoinTableSource() {
        return Stream.of(
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID HAVING SUM(SDT.SID) > 3 ORDER BY SDT.SID",
                        "SELECT distinct SDT.SID, SDT.JMENO FROM DBO.STUDENT SDT HAVING SUM(SDT.SID) > 3 ORDER BY SDT.SID",
                        "LEFT JOIN"),
                Arguments.arguments("SELECT distinct SDT.SID FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDT.SID FROM DBO.STUDENT SDT",
                        "LEFT JOIN"),
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDT.SID, SDT.JMENO FROM DBO.STUDENT SDT",
                        "FULL OUTER JOIN"),
                Arguments.arguments("SELECT distinct SDT.SID FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDT.SID FROM DBO.STUDENT SDT",
                        "FULL OUTER JOIN"),
                Arguments.arguments("SELECT DISTINCT SDE.SID, SDE.BODY FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDE.SID, SDE.body FROM dbo.STUDUJE SDE",
                        "RIGHT JOIN"),
                Arguments.arguments("SELECT DISTINCT SDE.SID FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDE.SID FROM dbo.STUDUJE SDE",
                        "RIGHT JOIN"),
                Arguments.arguments("SELECT distinct SDE.SID, SDE.body FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDE.SID, SDE.body FROM DBO.STUDUJE SDE",
                        "INNER JOIN"),
                Arguments.arguments("SELECT distinct SDE.SID FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID",
                        "SELECT distinct SDE.SID FROM DBO.STUDUJE SDE",
                        "INNER JOIN"
                )
        );
    }

    public static Stream<Arguments> doFindNecessaryJoinTableSource() {
        return Stream.of(
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDT.* " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDE.* " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID")
        );
    }
}
