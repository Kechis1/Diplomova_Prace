package DP.Transformations;

import DP.Database.*;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.List;

public class ExistsTransformation extends QueryHandler {
    public ExistsTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("EXISTS");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        DatabaseTable fromTable = TSqlParseWalker.findFromTable(metadata, select).get(0);
        TSqlParseWalker.findTablesList(metadata, select);
        final List<ConditionItem> conditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<ConditionItem> existConditions = ConditionItem.filterByOperator(conditions, ConditionOperator.EXISTS);

        for (ConditionItem condition : existConditions) {
            if (condition.getOperatorType().equals(ConditionOperator.SAMPLE)) continue;
            if ((condition.getExistItem().isNot() && condition.getExistItem().getTable() != null && condition.getExistItem().getTable().isEmpty()) ||
                    (!condition.getExistItem().isNot() && (condition.getExistItem().getTable() == null ||
                            (!condition.getExistItem().getTable().isEmpty() &&
                                    (condition.getExistItem().getConditions() == null || condition.getExistItem().getConditions().size() == 0 ||
                                            (condition.getExistItem().getConditions().size() == 1 && ConditionItem.isComparingForeignKey(fromTable, condition.getExistItem().getTable(), condition.getExistItem().getConditions().get(0)))))))) {

                return Transformation.addNewTransformationBasedOnLogicalOperator(query, condition, conditions.size(), Action.ExistsTransformation, "EXISTS");
            } else if (condition.getExistItem().isNot() && condition.getExistItem().getTable() == null) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        QueryHandler.restoreSpaces(query.getCurrentQuery().substring(condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt()), condition.getFullCondition()) + ": " + UnnecessaryStatementException.messageAlwaysReturnsEmptySet,
                        Action.ExistsTransformation,
                        false,
                        null
                ));
                return query;
            }
        }
        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.ExistsTransformation,
                false,
                null
        ));
        return query;
    }
}
