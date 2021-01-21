package tsql;

import DP.Database.DatabaseMetadata;
import DP.Database.Respond.Respond;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlRunner;
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

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
    }

    @ParameterizedTest(name = "doFindUnnecessaryConditionTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String query, String resultQuery) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runSelectClause(metadata, respond);
        assertNotEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(respond.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE", respond.getQueryTransforms().get(0).getMessage());
        assertTrue(respond.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource")
    void doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest(String query, String resultQuery) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runSelectClause(metadata, respond);
        assertNotEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertEquals(respond.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                respond.getQueryTransforms().get(0).getMessage());
        assertTrue(respond.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runSelectClause(metadata, respond);
        assertEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals("OK", respond.getQueryTransforms().get(0).getMessage());
        assertFalse(respond.isChanged());
    }

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, JMENO FROM DBO.PREDMET",
                        "SELECT PID, JMENO,  FROM DBO.PREDMET"),
                Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'",
                        "SELECT PDT.PID, , PDT.JMENO FROM PREDMET PDT INNER JOIN STUDUJE STE ON PDT.PID = STE.PID WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'"),
                Arguments.arguments("SELECT PID, JMENO, 2 FROM DBO.PREDMET WHERE ROCNIK = 2",
                        "SELECT PID, JMENO,  FROM DBO.PREDMET WHERE ROCNIK = 2")
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

    public static Stream<Arguments> doFindNecessaryConditionSource() {
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

