package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.LikeTransformation;
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
public class LikeTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private LikeTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new LikeTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryLikeTest {index} query = {0}")
    @MethodSource("doFindNecessaryLikeSource")
    void doFindNecessaryLikeTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryLikeOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryLikeSource")
    void doFindUnnecessaryLikeOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryLikeFullRunTest {index} query = {0}, resultQuery = {2}, transformationsInFirstRun = {3}, transformationsSecondInRun = {4}")
    @MethodSource("doFindUnnecessaryLikeSource")
    void doFindUnnecessaryLikeFullRunTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery, int transformationsInFirstRun, int transformationsInSecondRun) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), transformationsInFirstRun);
        assertEquals(query.getQueryTransforms().get(2).size(), transformationsInSecondRun);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION LIKE", query.getQueryTransforms().get(1).get(transformationsInFirstRun - 3).getMessage());
        assertTrue(query.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryLikeSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE 1",
                "SELECT * FROM DBO.PREDMET WHERE",
                "SELECT * FROM DBO.PREDMET",
                3,
                1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '1'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '%1'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '%1%'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 1 LIKE '1%'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE '1' LIKE '1'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE 'string' LIKE 'str%'",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET",
                        3,
                        1),
                Arguments.arguments("SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE",
                        "SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID",
                        5,
                        3)
        );
    }

    public static Stream<Arguments> doFindNecessaryLikeSource() {
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
