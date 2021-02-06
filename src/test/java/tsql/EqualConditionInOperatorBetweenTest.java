package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.BetweenTransformation;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
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
public class EqualConditionInOperatorBetweenTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private BetweenTransformation transformation;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new BetweenTransformation(null, metadata);
    }

    @ParameterizedTest(name = "doFindUnnecessaryConditionTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION BETWEEN", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'b' BETWEEN 'a' AND 'c'",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 'abc' BETWEEN 'aaa' AND 'abc'",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1 BETWEEN 0 AND 2",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE 1.5 BETWEEN 1.2 AND 1.7",
                        "SELECT * FROM DBO.STUDENT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT WHERE jmeno BETWEEN jmeno AND jmeno",
                        "SELECT * FROM DBO.STUDENT WHERE")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 'b' BETWEEN 'c' AND 'd'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 'abc' BETWEEN 'abc' AND 'aaa'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 1 BETWEEN 2 AND 5"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT " +
                        "WHERE 1.5 BETWEEN -1.7 AND -1.5"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT STT " +
                        "INNER JOIN DBO.STUDUJE SDE ON STT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDE.SID BETWEEN 5 AND 10"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT STT " +
                        "INNER JOIN DBO.STUDUJE SDE ON STT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDE.SID BETWEEN STT.SID AND PDT.SID")
        );
    }
}
