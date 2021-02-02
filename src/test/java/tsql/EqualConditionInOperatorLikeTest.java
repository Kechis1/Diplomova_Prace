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
public class EqualConditionInOperatorLikeTest {
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
        TSqlRunner.runEqualConditionInOperatorLike(metadata, query);

        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().size() == 1);
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE", query.getQueryTransforms().get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        TSqlRunner.runEqualConditionInOperatorLike(metadata, query);

        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(0).getMessage());
        assertFalse(query.isChanged());
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
