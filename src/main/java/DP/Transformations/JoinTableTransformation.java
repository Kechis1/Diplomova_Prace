package DP.Transformations;

import DP.Database.*;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JoinTableTransformation extends QueryHandler {
    public JoinTableTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getOutputQuery().contains("JOIN");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getOutputQuery());
        ParseTree select = parser.select_statement();
        final List<ColumnItem> allColumnsInSelect = TSqlParseWalker.findColumnsInSelect(metadata, select);
        final List<Boolean> isDistinctInSelect = TSqlParseWalker.findDistinctInSelect(select);
        final Map<JoinType, List<JoinItem>> joins = TSqlParseWalker.findJoinsList(metadata, select);
        final List<ConditionItem> fullOuterJoinConditions = new ArrayList<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();
        final List<DatabaseTable> fromTable = TSqlParseWalker.findFromTable(metadata, select);
        final Map<String, ColumnItem> columnItems = TSqlParseWalker.findAllColumns(metadata, select);

        for (JoinItem join : joins.get(JoinType.FULL_OUTER)) {
            fullOuterJoinConditions.addAll(join.getConditions());
        }
        for (JoinItem join : joins.get(JoinType.INNER)) {
            innerConditions.addAll(join.getConditions());
        }

        if (isDistinctInSelect.isEmpty()) {
            query.addTransformation(new Transformation(query.getOutputQuery(),
                    query.getOutputQuery(),
                    "OK",
                    Action.JoinTableTransformation,
                    false,
                    null
            ));
            return query;
        }

        boolean foundRedundantJoin = DatabaseTable.redundantJoinExists(columnItems, query, JoinType.LEFT, joins.get(JoinType.LEFT),
                fromTable.get(0).getTableAlias(), fromTable.get(0), allColumnsInSelect, false, null, false, null);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists(columnItems, query, JoinType.RIGHT, joins.get(JoinType.RIGHT),
                null, null, allColumnsInSelect, false, null, false, fromTable.get(0));
        foundRedundantJoin |= DatabaseTable.redundantJoinExists(columnItems, query, JoinType.FULL_OUTER, joins.get(JoinType.FULL_OUTER), fromTable.get(0).getTableAlias(),
                fromTable.get(0), allColumnsInSelect, true, metadata.setNullableColumns(fullOuterJoinConditions), true, null);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists(columnItems, query, JoinType.INNER, joins.get(JoinType.INNER), null, null,
                allColumnsInSelect, true, metadata.setNullableColumns(innerConditions), true, fromTable.get(0));

        if (!foundRedundantJoin) {
            query.addTransformation(new Transformation(query.getOutputQuery(),
                    query.getOutputQuery(),
                    "OK",
                    Action.JoinTableTransformation,
                    false,
                    null
            ));
        }
        return query;
    }
}
