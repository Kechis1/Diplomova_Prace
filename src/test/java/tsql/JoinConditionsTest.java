package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
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
public class JoinConditionsTest {
    @Mock
    private DatabaseMetadata metadata;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
    }

    @ParameterizedTest(name = "doFindUnnecessaryConditionTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        TSqlRunner.runRedundantJoinConditions(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION", query.getQueryTransforms().get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        TSqlRunner.runRedundantJoinConditions(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(0).getMessage());
        assertFalse(query.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID")
        );
    }
}
