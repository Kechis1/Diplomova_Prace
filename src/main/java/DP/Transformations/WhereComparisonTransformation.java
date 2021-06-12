package DP.Transformations;

import DP.Database.*;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;

public class WhereComparisonTransformation extends QueryHandler {
    public WhereComparisonTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("WHERE");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ConditionItem> conditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ConditionItem> whereConditions = ConditionItem.filterByOperator(conditions, ConditionOperator.AO);

        boolean foundError = ConditionItem.findAndProcessErrorInConditions("WHERE", Action.WhereComparisonTransformation, whereConditions, conditions, metadata, query, allTables);
        if (foundError) {
            return query;
        }
        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.WhereComparisonTransformation,
                false,
                null
        ));
        return query;
    }
}
