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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class BaseTestTrinoCatalog
{
    private static final Logger LOG = Logger.get(BaseTestTrinoCatalog.class);

    protected abstract TrinoCatalog createTrinoCatalog();

    @Test
    public void testCreateNamespaceWithLocation()
    {
        TrinoCatalog catalog = createTrinoCatalog();

        String namespace = "test_create_namespace_with_location_" + randomTableSuffix();
        catalog.createNamespace(SESSION, namespace, ImmutableMap.of(LOCATION_PROPERTY, "/a/path/"), new TrinoPrincipal(PrincipalType.USER, "admin"));
        assertThat(catalog.listNamespaces(SESSION)).contains(namespace);
        assertEquals(catalog.loadNamespaceMetadata(SESSION, namespace), ImmutableMap.of(LOCATION_PROPERTY, "/a/path/"));
        assertEquals(catalog.defaultTableLocation(SESSION, new SchemaTableName(namespace, "table")), "/a/path/table");
        catalog.dropNamespace(SESSION, namespace);
        assertThat(catalog.listNamespaces(SESSION))
                .doesNotContain(namespace);
    }

    @Test
    public void testCreateTable()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog();
        Path tmpDirectory = Files.createTempDirectory("glue_catalog_test_create_table_");
        tmpDirectory.toFile().deleteOnExit();

        String namespace = "test_create_table_" + randomTableSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        try {
            catalog.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, "admin"));
            catalog.newCreateTableTransaction(
                    SESSION,
                    schemaTableName,
                    new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                    PartitionSpec.unpartitioned(),
                    tmpDirectory.toAbsolutePath().toString(),
                    ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty())).contains(schemaTableName);

            Table icebergTable = catalog.loadTable(SESSION, schemaTableName);
            assertEquals(icebergTable.name(), quotedTableName(schemaTableName));
            assertEquals(icebergTable.schema().columns().size(), 1);
            assertEquals(icebergTable.schema().columns().get(0).name(), "col1");
            assertEquals(icebergTable.schema().columns().get(0).type(), Types.LongType.get());
            assertEquals(icebergTable.location(), tmpDirectory.toAbsolutePath().toString());
            assertEquals(icebergTable.properties(), ImmutableMap.of());

            catalog.dropTable(SESSION, schemaTableName, true);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace)))
                    .doesNotContain(schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty()))
                    .doesNotContain(schemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: " + namespace);
            }
        }
    }
}
