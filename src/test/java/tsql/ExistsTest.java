package tsql;

import DP.Database.ColumnItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Database.Respond;
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
public class ExistsTest {
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
        Respond respond = TSqlRunner.runEqualConditionInOperatorExists(metadata, query);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS" , this.consoleContent.toString().trim());
        assertFalse(respond.isUnnecessaryStatement());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        Respond respond = TSqlRunner.runEqualConditionInOperatorExists(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(respond.isUnnecessaryStatement());
    }

    @ParameterizedTest(name = "doFindUnnecessaryConditionBasedOnRecordsCountTest {index} query = {0}, recordsCount = {1}")
    @MethodSource("doFindUnnecessaryConditionBasedOnRecordsCountSource")
    void doFindUnnecessaryConditionBasedOnRecordsCountTest(String query, int recordsCount) {
        DatabaseTable table = metadata.findTable("STUDUJE", "SDT");
        table.setRecordsCount(recordsCount);
        Respond respond = TSqlRunner.runEqualConditionInOperatorExists(metadata, query);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS", this.consoleContent.toString().trim());
        assertFalse(respond.isUnnecessaryStatement());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionBasedOnNullableForeignKeyTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionBasedOnNullableForeignKeySource")
    void doFindUnnecessaryConditionBasedOnNullableForeignKeyTest(String query) {
        DatabaseTable table = metadata.findTable("STUDUJE", "SDT");
        ColumnItem column = table.findColumn("PID");
        column.setNullable(true);
        Respond respond = TSqlRunner.runEqualConditionInOperatorExists(metadata, query);
        assertEquals("OK", this.consoleContent.toString().trim());
        assertTrue(respond.isUnnecessaryStatement());
    }

    public static Stream<Arguments> doFindUnnecessaryConditionBasedOnRecordsCountSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET PDT " +
                        "WHERE NOT EXISTS (" +
                            "SELECT * " +
                            "FROM STUDUJE SDT " +
                            "WHERE PDT.PID = SDT.PID" +
                        ")", 0),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE EXISTS (" +
                            "SELECT * " +
                            "FROM STUDUJE " +
                        ")", 46)
        );
    }

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE EXISTS (SELECT 1)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE EXISTS (SELECT 0)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE EXISTS (SELECT null)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE EXISTS (SELECT t1.a FROM (SELECT 1 as a) t1)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE EXISTS (SELECT * " +
                            "FROM DBO.PREDMET PDT " +
                            "WHERE SDT.PID = PDT.PID " +
                        ")")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionBasedOnNullableForeignKeySource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE EXISTS (SELECT * " +
                            "FROM DBO.PREDMET PDT " +
                            "WHERE SDT.PID = PDT.PID " +
                        ")")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET PDT " +
                        "WHERE EXISTS (SELECT * " +
                            "FROM DBO.STUDUJE SDT " +
                            "WHERE PDT.PID = SDT.PID "+
                        ")"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE NOT EXISTS (SELECT 1)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE EXISTS (SELECT * " +
                            "FROM DBO.PREDMET PDT " +
                            "WHERE SDT.PID = PDT.PID AND PDT.JMENO = 'DAIS' " +
                        ")")
        );
    }
}
