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
public class SelectClauseTest {
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
        boolean result = TSqlRunner.runSelectClause(metadata, query);
        assertEquals(UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE", this.consoleContent.toString().trim());
        assertFalse(result);
    }

    @ParameterizedTest(name = "doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest {index} query = {0}")
    @MethodSource("doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource")
    void doFindUnnecessaryAttributeInSelectThatCanBeRewrittenTest(String query) {
        boolean result = TSqlRunner.runSelectClause(metadata, query);
        assertEquals( UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                this.consoleContent.toString().trim());
        assertFalse(result);
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runSelectClause(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(result);
    }

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, JMENO " +
                        "FROM DBO.PREDMET"),
                Arguments.arguments("SELECT PDT.PID, STE.PID, PDT.JMENO " +
                        "FROM PREDMET PDT " +
                        "INNER JOIN STUDUJE STE ON PDT.PID = STE.PID " +
                        "WHERE PDT.JMENO = 'DAIS' OR PDT.JMENO = 'UDBS'"),
                Arguments.arguments("SELECT PID, JMENO, 2 " +
                        "FROM DBO.PREDMET " +
                        "WHERE ROCNIK = 2")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryAttributeInSelectThatCanBeRewrittenSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID, JMENO, ROCNIK " +
                        "FROM DBO.PREDMET " +
                        "WHERE ROCNIK = 2"),
                Arguments.arguments("SELECT SID, ROK, BODY " +
                        "FROM DBO.STUDUJE " +
                        "WHERE PID = 2 AND SID = PID"),
                Arguments.arguments("SELECT SID, PID, BODY " +
                        "FROM STUDUJE " +
                        "WHERE EXISTS (SELECT * FROM STUDENT WHERE STUDUJE.SID = STUDENT.SID)")
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

