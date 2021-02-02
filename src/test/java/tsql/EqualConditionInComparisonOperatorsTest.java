package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
import DP.antlr4.tsql.TSqlRunner;
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
public class EqualConditionInComparisonOperatorsTest {
    @Mock
    private DatabaseMetadata metadata;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
    }

    @ParameterizedTest(name="doFindUnnecessaryConditionTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION", query.getQueryTransforms().get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name="doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(0).getMessage());
        assertFalse(query.isChanged());
    }

/*
    @ParameterizedTest(name="doFindInconsistentConditionTest {index} query = {0}")
    @MethodSource("doFindInconsistentConditionSource")
    void doFindInconsistentConditionTest(String query) {
        boolean result = TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " INCONSISTENT CONDITION", this.consoleContent.toString().trim());
        assertFalse(result);
    }

    @ParameterizedTest(name="doFindNotInconsistentConditionTest {index} query = {0}")
    @MethodSource("doFindNotInconsistentConditionSource")
    void doFindNotInconsistentConditionTest(String query) {
        boolean result = TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(result);
    }

    public static Stream<Arguments> doFindInconsistentConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'DAIS' AND JMENO = 'UDBS'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE ROCNIK = 2 AND ROCNIK > 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE ROCNIK = 2 AND PID < ROCNIK AND PID > 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE ROCNIK = 1 AND PID = 2 AND PID = ROCNIK"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' < 'ba' OR 'bc' > 'bb'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 0 OR 1 < 0"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' > 'aa' AND 'bb' >= 'ba'")
        );
    }

    public static Stream<Arguments> doFindNotInconsistentConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'DAIS' OR JMENO = 'UDBS'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'DAIS' AND ROCNIK = 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE PID = 1 AND ROCNIK = PID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 < 0 OR 1 > 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' < 'ba' OR 'bc' > 'db'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 2 AND 0 >= 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' > 'ad' AND 'bb' >= 'bd'")
        );
    }
*/

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = 1",
                "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 >= 0",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 > 0",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 <= 1",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 < 1",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 <> 0",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = '1'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' = 1",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'a' = 'A'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' >= 'aa'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' > 'aa'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ab' <= 'ac'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' < 'ab'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' <> 'ab'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDT.SID ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE  ORDER BY SDT.SID")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 0 >= 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 2 <= 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 2 < 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 <> 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = '2'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE '1' = 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'a' = 'B'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' >= 'ca'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' > 'ca'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' <= 'aa'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ac' < 'ab'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'aa' <> 'aa'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDT.JMENO " +
                        "ORDER BY SDT.SID")
        );
    }
}
