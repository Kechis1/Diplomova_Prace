package DP.Transformations;

import DP.Database.*;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JoinConditionTransformation extends QueryHandler {
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
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final HashMap<Integer, List<ConditionItem>> rightJoinConditions = new HashMap<>();
        final HashMap<Integer, List<ConditionItem>> fullOuterJoinConditions = new HashMap<>();
        final List<ConditionItem> innerConditions = new ArrayList<>();
        final List<ConditionItem> whereConditions = TSqlParseWalker.findWhereConditions(metadata, select);
        final List<JoinItem> joins = TSqlParseWalker.findJoinConditions(metadata, select, leftJoinConditions, rightJoinConditions, fullOuterJoinConditions, innerConditions);

        if (!(leftJoinConditions.size() != 0 || rightJoinConditions.size() != 0 || fullOuterJoinConditions.size() != 0)) {
            innerConditions.addAll(whereConditions);
        }
        boolean foundOrInQueryCondition = false;
        boolean foundDuplicateCondition = false;
        List<ConditionItem> allConditions = new ArrayList<>();
        if (!leftJoinConditions.isEmpty()) {
            allConditions.addAll(leftJoinConditions);
        }
        if (!fullOuterJoinConditions.isEmpty()) {
            for (int i = 0; i < fullOuterJoinConditions.size(); i++) {
                allConditions.addAll(fullOuterJoinConditions.get(i));
            }
        }
        if (!rightJoinConditions.isEmpty()) {
            for (int i = 0; i < rightJoinConditions.size(); i++) {
                allConditions.addAll(rightJoinConditions.get(i));
            }
        }

        for (JoinItem join : joins) {
            if (join.getConditions().size() > 1) {
                boolean foundError = ConditionItem.findAndProcessErrorInConditions("JOIN", Action.JoinConditionTransformation, join.getConditions(), join.getConditions(), metadata, query, allTables);
                if (foundError) {
                    return query;
                }
            }
        }

        for (ConditionItem condition: allConditions) {
            if (condition.getLeftLogicalOperator() != null && condition.getLeftLogicalOperator().equals("OR")) {
                foundOrInQueryCondition = true;
                break;
            }
        }

        if (!foundOrInQueryCondition) {
            foundDuplicateCondition = ConditionItem.duplicatesExists(query, metadata, innerConditions);
            foundDuplicateCondition |= ConditionItem.duplicatesExists(query, metadata, leftJoinConditions);
            foundDuplicateCondition |= ConditionItem.duplicatesExists(query, metadata, rightJoinConditions);
            foundDuplicateCondition |= ConditionItem.duplicatesExists(query, metadata, fullOuterJoinConditions);
        }

        if (!foundDuplicateCondition) {
            query.addTransformation(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    "OK",
                    Action.JoinConditionTransformation,
                    false,
                    null
            ));
        }
        return query;
    }
}
