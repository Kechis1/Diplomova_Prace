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
public class EqualConditionInOperatorLikeTest {
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
        TSqlRunner.runEqualConditionInOperatorLike(metadata, respond);

        assertNotEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(respond.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE", respond.getQueryTransforms().get(0).getMessage());
        assertTrue(respond.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runEqualConditionInOperatorLike(metadata, respond);

        assertEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals("OK", respond.getQueryTransforms().get(0).getMessage());
        assertFalse(respond.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE 1", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '1'", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '%1'", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '%1%'", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '1%'", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' LIKE '1'", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'string' LIKE 'str%'", "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE stt.sID LIKE stt.sID", "SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM student stt " +
                        "JOIN studuje sde ON stt.sID = stt.sID " +
                        "WHERE sde.sID LIKE stt.sID"),
                Arguments.arguments("SELECT * " +
                        "FROM student stt " +
                        "WHERE stt.prijmeni LIKE stt.jmeno"),
                Arguments.arguments("SELECT * " +
                        "FROM student stt " +
                        "WHERE stt.prijmeni LIKE '%ová'")
        );
    }
}
