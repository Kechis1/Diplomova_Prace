package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;

import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class EqualConditionInComparisonOperatorsTest {
    @Mock
    private DatabaseMetadata metadata;

    private final PrintStream originalStdOut = System.out;
    private ByteArrayOutputStream consoleContent = new ByteArrayOutputStream();


    @AfterEach
    public void restoreStreams() {
        System.setOut(this.originalStdOut);
        this.consoleContent = new ByteArrayOutputStream();
    }

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        System.setOut(new PrintStream(this.consoleContent));
    }

    @ParameterizedTest(name="doFindUnnecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION", this.consoleContent.toString().trim());
        assertFalse(result);
    }

    @ParameterizedTest(name="doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(result);
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 0 >= 0"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 0"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 0 <= 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 0 < 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 <> 0"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = '1'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE '1' = 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 0 AND 0 >= 0"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'a' = 'A'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' >= 'aa'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' > 'aa'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' <= 'ac'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'aa' < 'ab'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'aa' <> 'ab'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' > 'aa' AND 'bb' >= 'ba'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDT.SID " +
                        "ORDER BY SDT.SID")
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
                        "WHERE 'ba' < 'ba' OR 'bc' > 'bb'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 0 OR 1 < 0"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 < 0 OR 1 > 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 2 AND 0 >= 2"),
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
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' < 'ba' OR 'bc' > 'db'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' > 'ad' AND 'bb' >= 'bd'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDT.JMENO " +
                        "ORDER BY SDT.SID")
        );
    }
}
