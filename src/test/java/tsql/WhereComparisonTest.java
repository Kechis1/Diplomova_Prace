package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
import DP.Transformations.WhereComparisonTransformation;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;

import org.junit.jupiter.api.BeforeEach;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class WhereComparisonTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private WhereComparisonTransformation transformation;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new WhereComparisonTransformation(null, metadata);
    }

    @ParameterizedTest(name="doFindUnnecessaryConditionTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name="doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK",  query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = 1",
                "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 >= 0",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 > 0",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 <= 1",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 < 1",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 <> 0",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = '1'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' = 1",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'a' = 'A'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' >= 'aa'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' > 'aa'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ab' <= 'ac'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' < 'ab'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' <> 'ab'",
                        "SELECT * FROM DBO.PREDMET WHERE"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDT.SID ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE  ORDER BY SDT.SID")
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
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDT.JMENO " +
                        "ORDER BY SDT.SID")
        );
    }
}