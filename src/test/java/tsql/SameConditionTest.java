package tsql;

import DP.Database.DatabaseMetadata;
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
import java.io.IOException;
import java.io.PrintStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@ExtendWith(MockitoExtension.class)
public class SameConditionTest {
    @Mock
    private DatabaseMetadata metadata;

    private final PrintStream originalStdOut = System.out;
    private ByteArrayOutputStream consoleContent = new ByteArrayOutputStream();


    @AfterEach
    public void restoreStreams() {
        System.setOut(this.originalStdOut);
        // System.out.println(this.consoleContent.toString());
        this.consoleContent = new ByteArrayOutputStream();
    }

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        System.setOut(new PrintStream(this.consoleContent));
    }

    @ParameterizedTest(name="doFindUnnecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String query) throws IOException {
        boolean result = TSqlRunner.RunSameCondition(metadata, query);
        assertFalse(result);
    }

    @ParameterizedTest(name="doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) throws IOException {
        boolean result = TSqlRunner.RunSameCondition(metadata, query);
        assertFalse(result);
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 = 1"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 > 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 < 2"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 <> 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.STUDENT SDT\n" +
                        "\t\tINNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                        "\t\tINNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                        "\tWHERE SDT.SID = SDE.SID\n" +
                        "\tORDER BY SDT.SID"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM PREDMET\n" +
                        "\tWHERE JMENO LIKE '%'")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT *\n" +
                "\tFROM DBO.STUDENT SDT\n" +
                "\t\tLEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                "\t\tINNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                "\tWHERE SDT.SID = SDE.SID\n" +
                "\tORDER BY SDT.SID")
        );
    }
}
