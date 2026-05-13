package edu.sustech.cs307.optimizer;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.*;
import edu.sustech.cs307.physicalOperator.*;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.IndexMeta;
import edu.sustech.cs307.meta.TableMeta;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Values;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PhysicalPlanner {
    public static PhysicalOperator generateOperator(DBManager dbManager, LogicalOperator logicalOp) throws DBException {
        if (logicalOp instanceof LogicalTableScanOperator tableScanOperator) {
            return handleTableScan(dbManager, tableScanOperator);
        } else if (logicalOp instanceof LogicalFilterOperator filterOperator) {
            return handleFilter(dbManager, filterOperator);
        } else if (logicalOp instanceof LogicalSubqueryFilterOperator subqueryFilterOperator) {
            return handleSubqueryFilter(dbManager, subqueryFilterOperator);
        } else if (logicalOp instanceof LogicalJoinOperator joinOperator) {
            return handleJoin(dbManager, joinOperator);
        } else if (logicalOp instanceof LogicalProjectOperator projectOperator) {
            return handleProject(dbManager, projectOperator);
        } else if (logicalOp instanceof LogicalInsertOperator insertOperator) {
            return handleInsert(dbManager, insertOperator);
        } else if (logicalOp instanceof LogicalUpdateOperator updateOperator) {
            return handleUpdate(dbManager, updateOperator);
        } else if (logicalOp instanceof LogicalDeleteOperator deleteOperator) {
            return handleDelete(dbManager, deleteOperator);
        } else if (logicalOp instanceof LogicalOrderOperator orderOperator) {
            return handleOrder(dbManager, orderOperator);
        } else if (logicalOp instanceof LogicalAggregateOperator aggregateOperator) {
            return handleAggregate(dbManager, aggregateOperator);
        }

        else {
            throw new DBException(ExceptionTypes.UnsupportedOperator(logicalOp.getClass().getSimpleName()));
        }
    }

    private static PhysicalOperator handleTableScan(DBManager dbManager, LogicalTableScanOperator logicalTableScanOp) {
        return new SeqScanOperator(logicalTableScanOp.getTableName(), dbManager);
    }

    private static PhysicalOperator handleFilter(DBManager dbManager, LogicalFilterOperator logicalFilterOp)
            throws DBException {
        if (logicalFilterOp.getChild() instanceof LogicalTableScanOperator tableScanOperator) {
            IndexedPredicate indexedPredicate = extractIndexedPredicate(dbManager, tableScanOperator.getTableName(),
                    logicalFilterOp.getWhereExpr());
            if (indexedPredicate != null) {
                return new IndexScanOperator(dbManager, tableScanOperator.getTableName(), indexedPredicate.indexMeta,
                        indexedPredicate.predicateType, indexedPredicate.value);
            }
        }
        PhysicalOperator inputOp = generateOperator(dbManager, logicalFilterOp.getChild());
        return new FilterOperator(inputOp, logicalFilterOp.getWhereExpr());
    }

    private static PhysicalOperator handleSubqueryFilter(DBManager dbManager,
                                                         LogicalSubqueryFilterOperator logicalFilterOp)
            throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalFilterOp.getChild());
        return new SubqueryFilterOperator(dbManager, inputOp, logicalFilterOp.getWhereExpr());
    }

    private static PhysicalOperator handleJoin(DBManager dbManager, LogicalJoinOperator logicalJoinOp)
            throws DBException {
        PhysicalOperator leftOp = generateOperator(dbManager, logicalJoinOp.getLeftInput());
        PhysicalOperator rightOp = generateOperator(dbManager, logicalJoinOp.getRightInput());
        return new NestedLoopJoinOperator(leftOp, rightOp, logicalJoinOp.getJoinExprs());
    }

    private static PhysicalOperator handleProject(DBManager dbManager, LogicalProjectOperator logicalProjectOp)
            throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalProjectOp.getChild());
        if (logicalProjectOp.isCountAll()) {
            return new CountOperator(inputOp);
        }
        return new ProjectOperator(inputOp, logicalProjectOp.getOutputSchema());
    }

    private record IndexedPredicate(IndexMeta indexMeta, IndexScanOperator.PredicateType predicateType, Value value) {
    }

    private static IndexedPredicate extractIndexedPredicate(DBManager dbManager, String tableName, Expression expression)
            throws DBException {
        expression = unwrapParenthesis(expression);
        if (!(expression instanceof BinaryExpression binaryExpression)) {
            return null;
        }
        String operator = binaryExpression.getStringExpression();
        if (!operator.equals("=") && !operator.equals(">") && !operator.equals(">=")
                && !operator.equals("<") && !operator.equals("<=")) {
            return null;
        }
        Expression left = unwrapParenthesis(binaryExpression.getLeftExpression());
        Expression right = unwrapParenthesis(binaryExpression.getRightExpression());
        boolean columnOnLeft = left instanceof Column && constantValue(right) != null;
        boolean columnOnRight = right instanceof Column && constantValue(left) != null;
        if (!columnOnLeft && !columnOnRight) {
            return null;
        }
        Column column = (Column) (columnOnLeft ? left : right);
        String predicateTable = column.getTableName();
        if (predicateTable != null && !predicateTable.isBlank() && !predicateTable.equalsIgnoreCase(tableName)) {
            return null;
        }
        Value value = constantValue(columnOnLeft ? right : left);
        IndexMeta indexMeta = dbManager.findIndexOnColumn(tableName, column.getColumnName());
        if (indexMeta == null) {
            return null;
        }
        return new IndexedPredicate(indexMeta, predicateType(operator, columnOnLeft), value);
    }

    private static Expression unwrapParenthesis(Expression expression) {
        while (expression instanceof Parenthesis parenthesis) {
            expression = parenthesis.getExpression();
        }
        return expression;
    }

    private static Value constantValue(Expression expression) {
        expression = unwrapParenthesis(expression);
        if (expression instanceof StringValue stringValue) {
            return new Value(stringValue.getValue(), ValueType.CHAR);
        }
        if (expression instanceof DoubleValue doubleValue) {
            return new Value(doubleValue.getValue(), ValueType.FLOAT);
        }
        if (expression instanceof LongValue longValue) {
            return new Value(longValue.getValue(), ValueType.INTEGER);
        }
        return null;
    }

    private static IndexScanOperator.PredicateType predicateType(String operator, boolean columnOnLeft) {
        if (operator.equals("=")) {
            return IndexScanOperator.PredicateType.EQUAL;
        }
        if (columnOnLeft) {
            return switch (operator) {
                case ">" -> IndexScanOperator.PredicateType.GREATER_THAN;
                case ">=" -> IndexScanOperator.PredicateType.GREATER_THAN_OR_EQUAL;
                case "<" -> IndexScanOperator.PredicateType.LESS_THAN;
                case "<=" -> IndexScanOperator.PredicateType.LESS_THAN_OR_EQUAL;
                default -> throw new IllegalArgumentException(operator);
            };
        }
        return switch (operator) {
            case ">" -> IndexScanOperator.PredicateType.LESS_THAN;
            case ">=" -> IndexScanOperator.PredicateType.LESS_THAN_OR_EQUAL;
            case "<" -> IndexScanOperator.PredicateType.GREATER_THAN;
            case "<=" -> IndexScanOperator.PredicateType.GREATER_THAN_OR_EQUAL;
            default -> throw new IllegalArgumentException(operator);
        };
    }

    /**
     * 处理将逻辑插入操作转换为物理插入运算符的过程
     * 
     * @param dbManager       提供数据库操作访问的数据库管理器实例
     * @param logicalInsertOp 需要被转换的逻辑插入运算符
     * @return 准备好执行的物理插入运算符
     * @throws DBException 如果存在列不匹配、类型不匹配或无效SQL语法时抛出
     */
    @SuppressWarnings("deprecation") // for ExpressionList<?>::getExpressions
    private static PhysicalOperator handleInsert(DBManager dbManager, LogicalInsertOperator logicalInsertOp)
            throws DBException {
        var tableMeta = dbManager.getMetaManager().getTable(logicalInsertOp.tableName);
        // Process columns
        List<String> columns = new ArrayList<>();
        if (logicalInsertOp.columns != null) {
            // the length must equal to the number of columns in the table
            if (tableMeta.columns.size() != logicalInsertOp.columns.size()) {
                throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
            }
            for (int i = 0; i < logicalInsertOp.columns.size(); i++) {
                String colName = logicalInsertOp.columns.get(i).getColumnName();
                if (tableMeta.getColumnMeta(colName) == null) {
                    throw new DBException(ExceptionTypes.ColumnDoesNotExist(colName));
                }
                if (!tableMeta.columns_list.get(i).name.equals(colName)) {
                    throw new DBException(ExceptionTypes.InsertColumnNameMismatch());
                }
                columns.add(colName);
            }

        } else {
            // If no columns specified, use all table columns in order
            for (ColumnMeta columnMeta : tableMeta.columns_list) {
                columns.add(columnMeta.name);
            }
        }
        if (!(logicalInsertOp.values instanceof Values)) {
            throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Values must be an expression list"));
        }
        ExpressionList<?> valuesList = ((Values) logicalInsertOp.values).getExpressions();
        if (columns.size() != valuesList.size()) {
            var element = valuesList.get(0);
            if (element instanceof ParenthesedExpressionList<?> parenthesed) {
                // check the children reexpressions
                for (Expression expr : valuesList) {
                    if (expr instanceof ParenthesedExpressionList<?> expressionList) {
                        if (expressionList.getExpressions().size() != columns.size()) {
                            throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
                        }
                    } else {
                        throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
                    }
                }
            } else {
                throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
            }
        }

        List<Value> values = new ArrayList<>();
        parseValue(values, valuesList, tableMeta);
        // will always be same size tuple

        // check the

        return new InsertOperator(logicalInsertOp.tableName, columns,
                values, dbManager);
    }

    @SuppressWarnings("deprecation")
    private static void parseValue(List<Value> values, ExpressionList<?> valuesList, TableMeta tableMeta)
            throws DBException {
        for (int i = 0; i < valuesList.size(); i++) {
            var expr = valuesList.getExpressions().get(i);
            if (expr instanceof StringValue string_value) {
                if (tableMeta.columns_list.get(i).type != ValueType.CHAR) {
                    throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
                }
                String value_str = string_value.getValue();
                if (value_str.length() > 64) {
                    value_str = value_str.substring(0, 64);
                }
                values.add(new Value(value_str));
            } else if (expr instanceof DoubleValue float_value) {
                if (tableMeta.columns_list.get(i).type != ValueType.FLOAT) {
                    throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
                }
                values.add(new Value(float_value.getValue()));
            } else if (expr instanceof LongValue long_value) {
                if (tableMeta.columns_list.get(i).type != ValueType.INTEGER) {
                    throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
                }
                values.add(new Value(long_value.getValue()));
            } else if (expr instanceof ParenthesedExpressionList<?> expressionList) {
                parseValue(values, expressionList, tableMeta);
            } else {
                throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Unsupported value type in VALUES clause"));
            }
        }
    }


    private static PhysicalOperator handleUpdate(DBManager dbManager, LogicalUpdateOperator logicalUpdateOp) throws DBException {
        // TODO: Implement handleUpdate
        PhysicalOperator scanner = generateOperator(dbManager, logicalUpdateOp.getChild());
        if (logicalUpdateOp.getColumns().size() != 1 ) {
            throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Unsupported expression list"));
        }
        return new UpdateOperator(scanner, logicalUpdateOp.getTableName(), logicalUpdateOp.getColumns().get(0), logicalUpdateOp.getExpression());
    }

    private static PhysicalOperator handleDelete(DBManager dbManager, LogicalDeleteOperator logicalDeleteOp) throws DBException {
        PhysicalOperator scanner = generateOperator(dbManager, logicalDeleteOp.getChild());
        return new DeleteOperator(scanner, logicalDeleteOp.getWhereExpr());
    }

    private static PhysicalOperator handleOrder(DBManager dbManager, LogicalOrderOperator logicalOrderOp) throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalOrderOp.getChild());
        return new OrderByOperator(inputOp, logicalOrderOp.getOrderByElements());
    }

    private static PhysicalOperator handleAggregate(DBManager dbManager, LogicalAggregateOperator logicalAggregateOp) throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalAggregateOp.getChild());
        return new AggregateOperator(inputOp, logicalAggregateOp.getSelectItems(), logicalAggregateOp.getGroupByExpressions());
    }
}
