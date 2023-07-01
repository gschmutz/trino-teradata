/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcJoinPushdownUtil;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition.Operator;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TeradataClient
        extends BaseJdbcClient
{
    private static final Set<String> INERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("dbc")     // dbc contains the data dictionary
            .build();
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    @Inject
    public TeradataClient(
            BaseJdbcConfig config,
            TeradataConfig teradataConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        requireNonNull(teradataConfig, "teradataConfig is null");

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(TeradataClient::toTypeHandle))
                        .build());
    }

    /*
    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle) {
        try {
            Connection connection = this.connectionFactory.openConnection(session);

            Map var5;
            try {
                Handle handle = Jdbi.open(connection);

                try {
                    var5 = TeradataIndices.getTablePropertiesForIndices(tableHandle, handle);
                } catch (Throwable var9) {
                    if (handle != null) {
                        try {
                            handle.close();
                        } catch (Throwable var8) {
                            var9.addSuppressed(var8);
                        }
                    }

                    throw var9;
                }

                if (handle != null) {
                    handle.close();
                }
            } catch (Throwable var10) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Throwable var7) {
                        var10.addSuppressed(var7);
                    }
                }

                throw var10;
            }

            if (connection != null) {
                connection.close();
            }

            return var5;
        } catch (SQLException var11) {
            throw new TrinoException(JdbcErrorCode.JDBC_ERROR, var11);
        }
    }
    */

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        return Optional.empty();
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle) throws SQLException
    {
        Connection connection = this.connectionFactory.openConnection(session);

        try {
            connection.setReadOnly(true);
            return connection;
        }
        catch (SQLException var6) {
            connection.close();
            throw var6;
        }
    }

    @Override
    public PreparedQuery applyQueryTransformations(JdbcTableHandle tableHandle, PreparedQuery query)
    {
        return super.applyQueryTransformations(tableHandle, query);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return this.aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return this.connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (INERNAL_SCHEMAS.contains(schemaName.toLowerCase(ENGLISH))) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        // This is a heuristic, not exact science. A better formula can perhaps be found with measurements.
        // Column count is not known for non-SELECT queries. Not setting fetch size for these.
        if (columnCount.isPresent()) {
            statement.setFetchSize(max(100_000 / columnCount.get(), 1_000));
        }
        return statement;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(typeHandle);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }
        switch (jdbcTypeName) {
        }

        switch (typeHandle.getJdbcType()) {
//            case Types.TINYINT:
//                return Optional.of(new ColumnMapping(TinyintType.TINYINT, ResultSet::getByte, writeTypedNull(typeHandle.getJdbcType(), StandardColumnMappings.tinyintWriteFunction()), FULL_PUSHDOWN));

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.TIMESTAMP:
                if (typeHandle.getJdbcTypeName().isPresent() && !((String) typeHandle.getJdbcTypeName().get()).contains("WITH TIME ZONE")) {
                    int decimalDigits = typeHandle.getRequiredDecimalDigits();
                    TimestampType timestampType = TimestampType.createTimestampType(decimalDigits);
                    //return Optional.of(timestampColumnMapping(timestampType));
                }
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                FULL_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                FULL_PUSHDOWN);
    }

    /*
    public static ColumnMapping timestampColumnMapping(TimestampType type)
    {
        LongWriteFunction writeFunction = writeTypedNull(Types.TIMESTAMP, (LongWriteFunction) StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp(type));
        return new ColumnMapping(type, (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            return StandardColumnMappings.toTrinoTimestamp(type, timestamp.toLocalDateTime());
        }, writeFunction, FULL_PUSHDOWN);
    }
    */

    private static LongWriteFunction writeTypedNull(int type, LongWriteFunction delegate)
    {
        return new TeradataLongWriteFunction(delegate, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT TOP %d * FROM (%s) o", limit, sql));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((sql, sortItems, limit) -> {
            String start = "SELECT ";
            verify(sql.startsWith(start), "Expected query to start with %s but query was %s", start, sql);
            String orderBy = (String) sortItems.stream().map((sortItem) -> {
                String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                return String.format("%s %s %s", this.quoted(sortItem.getColumn().getColumnName()), ordering, nullsHandling);
            }).collect(Collectors.joining(", "));
            return format("SELECT TOP %d * FROM (%s) o ORDER BY %s ", limit, sql, orderBy);
        });
    }

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session, JoinType joinType, PreparedQuery leftSource, PreparedQuery rightSource, List<JdbcJoinCondition> joinConditions, Map<JdbcColumnHandle, String> rightAssignments, Map<JdbcColumnHandle, String> leftAssignments, JoinStatistics statistics)
    {
        return JdbcJoinPushdownUtil.implementJoinCostAware(session, joinType, leftSource, rightSource, statistics, () -> {
            return super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
        });
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return joinCondition.getOperator() == Operator.IS_DISTINCT_FROM ? false : Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn()).map(JdbcColumnHandle::getColumnType).noneMatch((type) -> {
            return type instanceof CharType || type instanceof VarcharType;
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    private static class TeradataLongWriteFunction
            implements LongWriteFunction
    {
        private final LongWriteFunction delegate;
        private final int jdbcType;

        public TeradataLongWriteFunction(LongWriteFunction delegate, int jdbcType)
        {
            this.delegate = (LongWriteFunction) requireNonNull(delegate, "delegate is null");
            this.jdbcType = jdbcType;
        }

        public void set(PreparedStatement statement, int index, long value) throws SQLException
        {
            this.delegate.set(statement, index, value);
        }

        public String getBindExpression()
        {
            return this.delegate.getBindExpression();
        }

        public void setNull(PreparedStatement statement, int index) throws SQLException
        {
            statement.setNull(index, this.jdbcType);
        }
    }
}
