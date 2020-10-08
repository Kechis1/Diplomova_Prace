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
import java.io.PrintStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@ExtendWith(MockitoExtension.class)
public class EqualConditionInComparisonOperators {
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
        assertFalse(result);
    }

    @ParameterizedTest(name="doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        boolean result = TSqlRunner.runEqualConditionInComparisonOperators(metadata, query);
        assertTrue(result);
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 = 1"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 0 >= 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 > 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 0 <= 1"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 0 < 1"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 <> 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 = '1'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE '1' = 1"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 < 0 OR 1 > 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 1 > 0 AND 0 >= 0"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'a' = 'A'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'ba' >= 'aa'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'ba' > 'aa'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'ab' <= 'ac'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'aa' < 'ab'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'aa' <> 'ab'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'ba' < 'ba' OR 'bc' > 'bb'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.PREDMET\n" +
                        "\tWHERE 'ab' > 'aa' AND 'bb' >= 'ba'"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.STUDENT SDT\n" +
                        "\t\tINNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                        "\t\tINNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                        "\tWHERE SDT.SID = SDE.SID\n" +
                        "\tORDER BY SDT.SID"),
                Arguments.arguments("SELECT *\n" +
                        "\tFROM DBO.STUDENT SDT\n" +
                        "\t\tINNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID\n" +
                        "\t\tINNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID\n" +
                        "\tWHERE SDT.SID = SDT.SID\n" +
                        "\tORDER BY SDT.SID")
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
