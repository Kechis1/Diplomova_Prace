package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
import DP.Transformations.SelectClauseTransformation;
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
public class SelectClauseTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private SelectClauseTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new SelectClauseTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessarySelectClauseTest {index} query = {0}")
    @MethodSource("doFindNecessarySelectClauseSource")
    void doFindNecessarySelectClauseTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessarySelectClauseOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessarySelectClauseSource")
    void doFindUnnecessarySelectClauseOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessarySelectClauseFullRunTest {index} query = {0}, resultQuery = {2}, transformationsInFirstRun = {3}, transformationsSecondInRun = {4}")
    @MethodSource("doFindUnnecessarySelectClauseSource")
    void doFindUnnecessarySelectClauseFullRunTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery, int transformationsInFirstRun, int transformationsInSecondRun) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), transformationsInFirstRun);
        assertEquals(query.getQueryTransforms().get(2).size(), transformationsInSecondRun);
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE", query.getQueryTransforms().get(1).get(transformationsInFirstRun-2).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource")
    void doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    public static Stream<Arguments> doFindUnnecessarySelectClauseSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, JMENO FROM DBO.PREDMET",
                        "SELECT PID, JMENO,  FROM DBO.PREDMET",
                        "SELECT PID,JMENO  FROM DBO.PREDMET",
                        2,
                        1),
                Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'",
                        "SELECT PDT.PID, , PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'",
                        "SELECT PDT.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'",
                        1,
                        1),
                Arguments.arguments("SELECT PID, JMENO, 2 FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO,  FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID,JMENO  FROM DBO.PREDMET WHERE ROCNIK = 2",
                        3,
                        2)
        );
    }

    public static Stream<Arguments> doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, ROCNIK FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO, 2 AS ROCNIK FROM DBO.PREDMET WHERE ROCNIK = 2"),
                Arguments.arguments("SELECT SID, ROK, BODY FROM DBO.STUDUJE WHERE PID = 2 AND SID = PID",
                        "SELECT 2 AS SID, ROK, BODY FROM DBO.STUDUJE WHERE PID = 2 AND SID = PID"),
                Arguments.arguments("SELECT SID, PID, BODY FROM STUDUJE WHERE EXISTS (SELECT * FROM STUDENT WHERE STUDUJE.SID = STUDENT.SID)",
                        "SELECT SID, PID, BODY FROM STUDUJE WHERE EXISTS (SELECT 1 FROM STUDENT WHERE STUDUJE.SID = STUDENT.SID)")
        );
    }

    public static Stream<Arguments> doFindNecessarySelectClauseSource() {
        return Stream.of(
                Arguments.arguments("SELECT PDT.PID, PDT.JMENO, STD.JMENO " +
                        "FROM DBO.PREDMET PDT " +
                        "JOIN DBO.STUDUJE STE ON PDT.PID = STE.PID " +
                        "JOIN DBO.STUDENT SDT ON STE.SID = SDT.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE EXISTS (SELECT 1 FROM STUDUJE WHERE PREDMET.PID = STUDUJE.PID)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO <> 'DAIS'"),
                Arguments.arguments("SELECT PID, SID, ROK, BODY " +
                        "FROM DBO.STUDUJE " +
                        "WHERE ROK = 2010 OR ROK = 2011"),
                Arguments.arguments("SELECT *, 1" +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'UDBS' " +
                        "UNION " +
                        "SELECT *, 2" +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'DAIS'")
        );
    }
}

