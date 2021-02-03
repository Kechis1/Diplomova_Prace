package DP.Transformations;

import DP.Database.ConditionItem;
import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JoinConditionTransformation extends QueryHandler {
    public static final String action = "JoinConditionTransformation";

    public JoinConditionTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
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
        final List<ConditionItem> leftJoinConditions = new ArrayList<>();
        final HashMap<Integer, List<ConditionItem>> rightJoinConditions = new HashMap<>();
        final HashMap<Integer, List<ConditionItem>> fullOuterJoinConditions = new HashMap<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();
        final List<ConditionItem> whereConditions = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterJoin_part(TSqlParser.Join_partContext ctx) {
                if (ctx.LEFT() != null) {
                    leftJoinConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.RIGHT() != null) {
                    rightJoinConditions.put(rightJoinConditions.size(), (List<ConditionItem>) TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else if (ctx.FULL() != null && ctx.OUTER() != null) {
                    fullOuterJoinConditions.put(fullOuterJoinConditions.size(), (List<ConditionItem>) TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                } else {
                    innerConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, ctx.search_condition()));
                }
            }

            @Override
            public void enterQuery_specification(TSqlParser.Query_specificationContext ctx) {
                if (ctx.search_condition() != null) {
                    for (TSqlParser.Search_conditionContext sCtx : ctx.search_condition()) {
                        whereConditions.addAll(TSqlParseWalker.findConditionsFromSearchCtx(metadata, sCtx));
                    }
                }
            }
        }, select);

        if (!(leftJoinConditions.size() != 0 || rightJoinConditions.size() != 0 || fullOuterJoinConditions.size() != 0)) {
            innerConditions.addAll(whereConditions);
        }
        boolean foundDuplicateCondition = ConditionItem.duplicatesExists(query, metadata, innerConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(query, metadata, leftJoinConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(query, metadata, rightJoinConditions);
        foundDuplicateCondition |= ConditionItem.duplicatesExists(query, metadata, fullOuterJoinConditions);

        if (!foundDuplicateCondition) {
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
