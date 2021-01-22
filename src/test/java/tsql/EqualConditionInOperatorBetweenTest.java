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
public class EqualConditionInOperatorBetweenTest {
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
        TSqlRunner.runEqualConditionInOperatorBetween(metadata, respond);
        assertNotEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertEquals(respond.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN", respond.getQueryTransforms().get(0).getMessage());
        assertTrue(respond.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runEqualConditionInOperatorBetween(metadata, respond);
        assertEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals("OK", respond.getQueryTransforms().get(0).getMessage());
        assertFalse(respond.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'b' BETWEEN 'a' AND 'c'",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'abc' BETWEEN 'aaa' AND 'abc'",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1 BETWEEN 0 AND 2",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1.5 BETWEEN 1.2 AND 1.7",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE jmeno BETWEEN jmeno AND jmeno",
                        "SELECT * FROM DBO.STUDENT WHERE")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
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
