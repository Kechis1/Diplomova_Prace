package tsql;

import DP.Database.ColumnItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlRunner;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class InconsistentConditionTest {
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

    @ParameterizedTest(name = "doFindUnnecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runInconsistentCondition(metadata, query);
        assertEquals(UnnecessaryStatementException.messageInconsistentCondition, this.consoleContent.toString().trim());
        assertFalse(result);
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runInconsistentCondition(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(result);
    }

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
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
                        "WHERE ROCNIK = 1 AND PID = 2 AND PID = ROCNIK")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'DAIS' OR JMENO = 'UDBS'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE JMENO = 'DAIS' AND ROCNIK = 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE PID = 1 AND ROCNIK = PID")
        );
    }
}

