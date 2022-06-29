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
package io.prestosql.plugin.postgresql;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import org.postgresql.util.PGobject;

import javax.inject.Inject;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.postgresql.TypeUtils.getArrayElementPgTypeName;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class PostgreSqlClient
        extends BasePostgreSqlClient
{
    @Inject
    public PostgreSqlClient(BaseJdbcConfig config, PostgreSqlConfig postgresqlConfig,
                            ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, postgresqlConfig, connectionFactory, typeManager);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (TIMESTAMP.equals(type)) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunction(session));
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("timestamp with time zone", timestampWithTimeZoneWriteFunction());
        }
        if (TinyintType.TINYINT.equals(type)) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.JSON)) {
            return WriteMapping.sliceMapping("jsonb", typedVarcharWriteFunction("json"));
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.UUID)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        if (type instanceof ArrayType && supportArrays) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.blockMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }
        return super.toWriteMapping(session, type);
    }

    @Override
    protected ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                typedVarcharWriteFunction("json"),
                DISABLE_PUSHDOWN);
    }

    @Override
    protected ColumnMapping typedVarcharColumnMapping(String jdbcTypeName)
    {
        return ColumnMapping.sliceMapping(
                VarcharType.VARCHAR,
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                typedVarcharWriteFunction(jdbcTypeName));
    }

    private static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName)
    {
        return (statement, index, value) -> {
            PGobject pgObject = new PGobject();
            pgObject.setType(jdbcTypeName);
            pgObject.setValue(value.toStringUtf8());
            statement.setObject(index, pgObject);
        };
    }
}
