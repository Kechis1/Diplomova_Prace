package DP.Transformations;

import DP.Database.*;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JoinTableTransformation extends QueryHandler {
    public static final String action = "JoinTableTransformation";

    public JoinTableTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("JOIN");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<ColumnItem> allColumnsInSelect = TSqlParseWalker.findColumnsInSelect(metadata, select);
        final List<Boolean> isDistinctInSelect = TSqlParseWalker.findDistinctInSelect(select);
        final Map<String, List<JoinTable>> joinTables = TSqlParseWalker.findJoinTablesList(metadata, select);
        final List<ConditionItem> fullOuterJoinConditions = new ArrayList<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();
        final List<DatabaseTable> fromTable = TSqlParseWalker.findFromTable(metadata, select);

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterJoin_part(TSqlParser.Join_partContext ctx) {
                if (ctx.FULL() != null && ctx.OUTER() != null) {
                    fullOuterJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.LEFT() == null && ctx.RIGHT() == null) {
                    innerConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                }
            }
        }, select);

        if (isDistinctInSelect.isEmpty()) {
            query.addTransform(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    "OK",
                    action,
                    false
            ));
            query.setChanged(false);
            return query;
        }

        boolean foundRedundantJoin = DatabaseTable.redundantJoinExists(query, "LEFT", joinTables.get("leftJoin"),
                fromTable.get(0).getTableAlias(), fromTable.get(0), allColumnsInSelect, false, null, false, null);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists(query, "RIGHT", joinTables.get("rightJoin"),
                null, null, allColumnsInSelect, false, null, false, fromTable.get(0));
        foundRedundantJoin |= DatabaseTable.redundantJoinExists(query, "FULL OUTER", joinTables.get("fullOuterJoin"), fromTable.get(0).getTableAlias(),
                fromTable.get(0), allColumnsInSelect, true, metadata.setNullableColumns(fullOuterJoinConditions), true, null);
        foundRedundantJoin |= DatabaseTable.redundantJoinExists(query, "INNER", joinTables.get("innerJoin"), null, null,
                allColumnsInSelect, true, metadata.setNullableColumns(innerConditions), true, fromTable.get(0));

        if (!foundRedundantJoin) {
            query.addTransform(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    "OK",
                    action,
                    false
            ));
            query.setChanged(false);
        }
        return query;
    }
}
