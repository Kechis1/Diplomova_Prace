package tsql;

import DP.Database.DatabaseMetadata;
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
public class EqualConditionInOperatorLikeTest {
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
        boolean result = TSqlRunner.runEqualConditionInOperatorLike(metadata, query);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE", this.consoleContent.toString().trim());
        assertFalse(result);
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runEqualConditionInOperatorLike(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(result);
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE 1 LIKE 1"),
                Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE 1 LIKE '1'"),
                Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE 1 LIKE '%1'"),
                Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE 1 LIKE '%1%'"),
                Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE 1 LIKE '1%'"),
                Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE '1' LIKE '1'"),
                Arguments.arguments("SELECT *\n" +
                        "FROM DBO.PREDMET\n" +
                        "WHERE 'string' LIKE 'str%'"),
                Arguments.arguments("SELECT *\n" +
                        "FROM student stt\n" +
                        "JOIN studuje sde ON stt.sID = stt.sID\n" +
                        "WHERE stt.sID LIKE stt.sID")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT *\n" +
                        "FROM student stt\n" +
                        "JOIN studuje sde ON stt.sID = stt.sID\n" +
                        "WHERE sde.sID LIKE stt.sID"),
                Arguments.arguments("SELECT *\n" +
                        "FROM student stt\n" +
                        "WHERE stt.prijmeni LIKE stt.jmeno"),
                Arguments.arguments("SELECT *\n" +
                        "FROM student stt\n" +
                        "WHERE stt.prijmeni LIKE '%ová'")
        );
    }
}