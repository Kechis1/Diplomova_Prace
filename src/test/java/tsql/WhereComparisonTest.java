package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
import DP.Transformations.WhereComparisonTransformation;
import org.jetbrains.annotations.NotNull;
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
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new WhereComparisonTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryWhereComparisonTest {index} query = {0}")
    @MethodSource("doFindNecessaryWhereComparisonSource")
    void doFindNecessaryWhereComparisonTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryWhereComparisonTest {index} query = {0}, resultQuery = {2}, transformationsInFirstRun = {3}, transformationsSecondInRun = {4}")
    @MethodSource("doFindUnnecessaryWhereComparisonSource")
    void doFindUnnecessaryWhereComparisonTest(String requestQuery, String oneRunResultQuery, @NotNull String fullRunResultQuery, int transformationsInFirstRun, int transformationsInSecondRun) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), transformationsInFirstRun);
        assertEquals(query.getQueryTransforms().get(2).size(), transformationsInSecondRun);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " WHERE CONDITION", query.getQueryTransforms().get(1).get(transformationsInFirstRun - 3).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doWhereComparisonWhereResultIsEmptySetTest {index} query = {0}, condition = {1}")
    @MethodSource("doWhereComparisonWhereResultIsEmptySetSource")
    void doWhereComparisonWhereResultIsEmptySetTest(String requestQuery, String condition) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(condition + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet, query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    public static Stream<Arguments> doFindUnnecessaryWhereComparisonSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = 1",
                "SELECT * FROM DBO.PREDMET WHERE",
                "SELECT * FROM DBO.PREDMET",
                3,
                1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 >= 0",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 > 0",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 <= 1",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 0 < 1",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 <> 0",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 = '1'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' = 1",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'a' = 'A'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' >= 'aa'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ba' > 'aa'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'ab' <= 'ac'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' < 'ab'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'aa' <> 'ab'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDT.SID ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE  ORDER BY SDT.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID ORDER BY SDT.SID",
                        5,
                        3)
        );
    }

    public static Stream<Arguments> doWhereComparisonWhereResultIsEmptySetSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = 2",
                "1 = 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 0 >= 1",
                        "0 >= 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 > 2",
                        "1 > 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 2 <= 1",
                        "2 <= 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 2 < 1",
                        "2 < 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 <> 1",
                        "1 <> 1"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 1 = '2'",
                        "1 = '2'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE '1' = 2",
                        "'1' = 2"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'a' = 'B'",
                        "'A' = 'B'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' >= 'ca'",
                        "'BA' >= 'CA'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ba' > 'ca'",
                        "'BA' > 'CA'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ab' <= 'aa'",
                        "'AB' <= 'AA'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'ac' < 'ab'",
                        "'AC' < 'AB'"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE 'aa' <> 'aa'",
                        "'AA' <> 'AA'")
        );
    }

    public static Stream<Arguments> doFindNecessaryWhereComparisonSource() {
        return Stream.of(
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDT.JMENO " +
                        "ORDER BY SDT.SID")
        );
    }
}
