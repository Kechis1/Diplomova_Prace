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
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryBetweenOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryBetweenSource")
    void doFindUnnecessaryBetweenOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryBetweenFullRunTest {index} query = {0}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryBetweenSource")
    void doFindUnnecessaryBetweenFullRunTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 3);
        assertEquals(query.getQueryTransforms().get(2).size(), 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryBetweenSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'b' BETWEEN 'a' AND 'c'",
                        "SELECT * FROM DBO.STUDENT WHERE",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'abc' BETWEEN 'aaa' AND 'abc'",
                        "SELECT * FROM DBO.STUDENT WHERE",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1 BETWEEN 0 AND 2",
                        "SELECT * FROM DBO.STUDENT WHERE",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1.5 BETWEEN 1.2 AND 1.7",
                        "SELECT * FROM DBO.STUDENT WHERE",
                        "SELECT * FROM DBO.STUDENT"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE jmeno BETWEEN jmeno AND jmeno",
                        "SELECT * FROM DBO.STUDENT WHERE",
                        "SELECT * FROM DBO.STUDENT")
        );
    }

    public static Stream<Arguments> doFindNecessaryBetweenSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 'b' BETWEEN 'c' AND 'd'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 'abc' BETWEEN 'abc' AND 'aaa'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 1 BETWEEN 2 AND 5"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 1.5 BETWEEN -1.7 AND -1.5"),
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
