package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.MetadataException;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
import DP.Transformations.SelectClauseTransformation;
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
public class SelectClauseTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private SelectClauseTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        try {
            metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
            transformation = new SelectClauseTransformation(null, metadata);
            transformationBuilder = new TransformationBuilder(metadata);
        } catch (MetadataException exception) {
            Assertions.fail(exception.getMessage());
        }
    }

    @ParameterizedTest(name = "doFindNecessarySelectClauseTest {index} query = {0}")
    @MethodSource("doFindNecessarySelectClauseSource")
    void doFindNecessarySelectClauseTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindConstantsInSelectClauseOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindConstantsInSelectClauseSource")
    void doFindConstantsInSelectClauseOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms().get(1).get(0).getMessage().contains(UnnecessaryStatementException.messageConstant));
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindDuplicatesInSelectClauseOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindDuplicatesInSelectClauseSource")
    void doFindDuplicatesInSelectClauseOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms().get(1).get(0).getMessage().contains(UnnecessaryStatementException.messageDuplicateAttribute));
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource")
    void doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getOutputQuery().toUpperCase(), query.getInputQuery().toUpperCase());
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doSelectClauseRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doSelectClauseRandomSource")
    void doSelectClauseRandomTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    public static Stream<Arguments> doSelectClauseRandomSource() {
        return Stream.of(
                Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS' HAVING sum(STE.SID) > 3 ORDER BY STE.SID",
                        "SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS' HAVING sum(STE.SID) > 3 ORDER BY STE.SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND JMENO = 'PETR' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT JMENO FROM STUDENT WHERE JMENO = 'ADAM' AND JMENO = 'PETR' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO, JMENO FROM STUDENT WHERE JMENO = 'ADAM' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'adam' as jmeno, 'adam' as jmeno FROM STUDENT where jmeno = 'adam' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO, JMENO FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT jmeno, jmeno FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT 'AHOJ' FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'ahoj' FROM STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT 1 AS A FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 1 AS a FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT 1 FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 1 FROM DBO.STUDENT HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO = 'Petr' HAVING sum(SID) > 3 ORDER BY SID",
                        "SELECT 'Petr' as jmeno FROM STUDENT WHERE jmeno = 'Petr' HAVING sum(SID) > 3 ORDER BY SID"),
                Arguments.arguments("SELECT * FROM STUDENT WHERE JMENO = 'Adam'",
                        "SELECT * FROM STUDENT WHERE JMENO = 'Adam'")
        );
    }

    public static Stream<Arguments> doFindDuplicatesInSelectClauseSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, JMENO FROM DBO.PREDMET",
                        "SELECT PID, JMENO, JMENO FROM DBO.PREDMET"),
                Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'",
                        "SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'")
        );
    }

    public static Stream<Arguments> doFindConstantsInSelectClauseSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, 2 FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO, 2 FROM DBO.PREDMET WHERE ROCNIK = 2"),
                Arguments.arguments("SELECT PID, JMENO, 2 AS A FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO, 2 AS A FROM DBO.PREDMET WHERE ROCNIK = 2"),
                Arguments.arguments("SELECT PID, JMENO, 'Ahoj' AS A FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO, 'Ahoj' AS A FROM DBO.PREDMET WHERE ROCNIK = 2")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, ROCNIK FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO, 2 AS ROCNIK FROM DBO.PREDMET WHERE ROCNIK = 2"),
                Arguments.arguments("SELECT SID, ROK, BODY FROM DBO.STUDUJE WHERE PID = 2 AND DBO.STUDUJE.SID = PID",
                        "SELECT 2 AS SID, ROK, BODY FROM DBO.STUDUJE WHERE PID = 2 AND DBO.STUDUJE.SID = 2"),
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

