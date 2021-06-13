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

public class SelectClauseTransformation extends QueryHandler {
    public SelectClauseTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return true;
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ExistsItem> foundExistsNotConstant = TSqlParseWalker.findExistsNotConstant(select);
        boolean foundUnion = TSqlParseWalker.findUnion(select);
        DatabaseMetadata metadataWithTables = metadata.withTables(allTables);

        if (!foundExistsNotConstant.isEmpty()) {
            query.addTransformation(new Transformation(query.getCurrentQuery(),
                    (query.getCurrentQuery().substring(0, foundExistsNotConstant.get(0).getSelectListStartAt()) + "1" + query.getCurrentQuery().substring(foundExistsNotConstant.get(0).getSelectListStopAt() + 1)).trim(),
                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                    Action.SelectClauseTransformation,
                    true,
                    null
            ));
            return query;
        }

        final List<ColumnItem> allColumnsInSelect = TSqlParseWalker.findColumnsInSelect(metadataWithTables, select);

        final List<ConditionItem> conditions = TSqlParseWalker.findConditions(metadataWithTables, select);
        final List<ConditionItem> uniqueAttributesConditions = ConditionItem.removeMultipleAttributeConditions(conditions);

        for (ConditionItem item : conditions) {
            if (item.getOperatorType().equals(ConditionOperator.SAMPLE)) continue;
            if (item.getOperator().equals("=") && item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                int bothInSelect = -1;
                List<ColumnItem> inSelect = new ArrayList<>();
                ColumnItem columnInSelect = null;
                for (ColumnItem column : allColumnsInSelect) {
                    if (column.equals(item.getLeftSideColumnItem()) || column.equals(item.getRightSideColumnItem())) {
                        columnInSelect = column;
                        if (column.equals(item.getLeftSideColumnItem())) {
                            inSelect.add(item.getRightSideColumnItem());
                        } else {
                            inSelect.add(item.getLeftSideColumnItem());
                        }
                        bothInSelect++;
                    }
                    if (bothInSelect == 1) {
                        query.addTransformation(new Transformation(query.getCurrentQuery(),
                                query.getCurrentQuery(),
                                UnnecessaryStatementException.messageDuplicateAttribute,
                                Action.SelectClauseTransformation,
                                false,
                                null
                        ));
                        return query;
                    }
                }
                if (bothInSelect == 0) {
                    for (ConditionItem condition : conditions) {
                        if (item.getOperator().equals("=") &&
                                ((condition.getLeftSideColumnItem() != null && condition.getLeftSideColumnItem().equals(inSelect.get(0)) && condition.getRightSideDataType() != ConditionDataType.COLUMN) ||
                                        (condition.getRightSideColumnItem() != null && condition.getRightSideColumnItem().equals(inSelect.get(0)) && condition.getLeftSideDataType() != ConditionDataType.COLUMN))) {
                            String value = condition.getLeftSideDataType() != ConditionDataType.COLUMN ? condition.getLeftSideValue() : condition.getRightSideValue();
                            String newCurrentQuery;
                            if (item.getLeftSideColumnItem().equals(inSelect.get(0))) {
                                newCurrentQuery = (query.getCurrentQuery().substring(0, columnInSelect.getStartAt()) + value + " AS " + columnInSelect.getName() + query.getCurrentQuery().substring(columnInSelect.getStopAt() + 1, item.getStartAt())
                                    + item.getRightSideFullCondition() + " " + item.getOperator() + " " + value + query.getCurrentQuery().substring(item.getStopAt())).trim();
                            } else {
                                newCurrentQuery = (query.getCurrentQuery().substring(0, columnInSelect.getStartAt()) + value + " AS " + columnInSelect.getName() + query.getCurrentQuery().substring(columnInSelect.getStopAt() + 1, item.getStartAt())
                                        + item.getLeftSideFullCondition() + " " + item.getOperator() + " " + value + query.getCurrentQuery().substring(item.getStopAt())).trim();
                            }

                            query.addTransformation(new Transformation(query.getCurrentQuery(),
                                    newCurrentQuery,
                                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                                    Action.SelectClauseTransformation,
                                    true,
                                    new OperatorTransformation(true, columnInSelect.getName(), value + " AS " + columnInSelect.getName())
                            ));
                            return query;
                        }
                    }
                }
            }
        }

        boolean isOrUsed = ConditionItem.isOrUsed(conditions);

        if (!isOrUsed) {
            for (ConditionItem item : uniqueAttributesConditions) {
                if (item.getOperatorType().equals(ConditionOperator.SAMPLE)) continue;
                if (item.getOperator().equals("=") && (item.getLeftSideDataType() == ConditionDataType.COLUMN || item.getRightSideDataType() == ConditionDataType.COLUMN)) {
                    for (ColumnItem column : allColumnsInSelect) {
                        if ((item.getLeftSideDataType() == ConditionDataType.COLUMN && column.equals(item.getLeftSideColumnItem()) && item.getRightSideDataType() != ConditionDataType.COLUMN) ||
                                (item.getRightSideDataType() == ConditionDataType.COLUMN && column.equals(item.getRightSideColumnItem()) && item.getLeftSideDataType() != ConditionDataType.COLUMN)) {
                            String value;
                            if (item.getLeftSideDataType() != ConditionDataType.COLUMN) {
                                value = item.getLeftSideDataType() == ConditionDataType.STRING ? "'" + item.getLeftSideValue() + "'" : item.getLeftSideValue();
                            } else {
                                value = item.getRightSideDataType() == ConditionDataType.STRING ? "'" + item.getRightSideValue() + "'" : item.getRightSideValue();
                            }

                            query.addTransformation(new Transformation(query.getCurrentQuery(),
                                    (query.getCurrentQuery().substring(0, column.getStartAt()) + value + " AS " + column.getName() + query.getCurrentQuery().substring(column.getStopAt() + 1)).trim(),
                                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                                    Action.SelectClauseTransformation,
                                    true,
                                    new OperatorTransformation(true, column.getName(), value + " AS " + column.getName())
                            ));
                            return query;
                        }
                    }
                }
            }
        }

        ColumnItem equals = ColumnItem.duplicatesExists(allColumnsInSelect);
        if (equals != null) {
            query.addTransformation(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    UnnecessaryStatementException.messageDuplicateAttribute,
                    Action.SelectClauseTransformation,
                    false,
                    null
            ));
            return query;
        }
        for (ColumnItem item : allColumnsInSelect) {
            if (item.isConstant() && !foundUnion && !query.IgnoredOperatorExists(Action.SelectClauseTransformation, item.getValue() + " AS " + item.getName())) {
                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        query.getCurrentQuery(),
                        UnnecessaryStatementException.messageConstant + " " + (item.getValue() + (item.getName() == null ? "" : " AS " + item.getName())),
                        Action.SelectClauseTransformation,
                        false,
                        null
                ));
                return query;
            }
        }

        query.addTransformation(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                Action.SelectClauseTransformation,
                false,
                null
        ));
        return query;
    }
}
