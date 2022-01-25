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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class GlueIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private static final Logger log = Logger.get(GlueIcebergTableOperations.class);

    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;
    private final Optional<String> catalogId;

    protected GlueIcebergTableOperations(
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            Optional<String> catalogId,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
    }

    @Override
    protected String getRefreshedLocation()
    {
        Table table = getTable();

        if (isPrestoView(table) && isHiveOrPrestoView(table)) {
            // this is a Presto Hive view, hence not a table
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION, getSchemaTableName()));
        }
        return metadataLocation;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version == -1, "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);
        Map<String, String> parameters = ImmutableMap.of(
                TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH),
                METADATA_LOCATION, newMetadataLocation);
        TableInput tableInput = new TableInput()
                .withName(tableName)
                .withTableType(EXTERNAL_TABLE.name())
                .withOwner(owner.orElse(null))
                .withParameters(parameters);

        try {
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput);
            catalogId.ifPresent(createTableRequest::setCatalogId);
            stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
        }
        catch (Exception e) {
            try {
                cleanupMetadataLocation(newMetadataLocation);
            }
            catch (Exception cleanupException) {
                if (!cleanupException.equals(e)) {
                    e.addSuppressed(cleanupException);
                }
                throw e;
            }
        }

        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version + 1);
        Map<String, String> parameters = ImmutableMap.of(
                TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH),
                METADATA_LOCATION, newMetadataLocation,
                PREVIOUS_METADATA_LOCATION, currentMetadataLocation);
        TableInput tableInput = new TableInput()
                .withName(tableName)
                .withTableType(EXTERNAL_TABLE.name())
                .withOwner(owner.orElse(null))
                .withParameters(parameters);

        try {
            Table table = getTable();

            checkState(currentMetadataLocation != null, "No current metadata location for existing table");
            String metadataLocation = table.getParameters().get(METADATA_LOCATION);
            if (!currentMetadataLocation.equals(metadataLocation)) {
                throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                        currentMetadataLocation, metadataLocation, getSchemaTableName());
            }

            TableInput tableInputToUpdate = tableInput
                    .withDescription(table.getDescription())
                    .withTargetTable(table.getTargetTable())
                    .withLastAccessTime(table.getLastAccessTime())
                    .withLastAnalyzedTime(table.getLastAnalyzedTime())
                    .withPartitionKeys(table.getPartitionKeys())
                    .withRetention(table.getRetention())
                    .withStorageDescriptor(table.getStorageDescriptor())
                    .withViewExpandedText(table.getViewExpandedText())
                    .withViewOriginalText(table.getViewOriginalText());

            UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInputToUpdate);
            catalogId.ifPresent(updateTableRequest::setCatalogId);
            stats.getUpdateTable().call(() -> glueClient.updateTable(updateTableRequest));
        }
        catch (Exception e) {
            try {
                cleanupMetadataLocation(newMetadataLocation);
            }
            catch (Exception cleanupException) {
                if (!cleanupException.equals(e)) {
                    e.addSuppressed(cleanupException);
                }
            }
            throw e;
        }
        shouldRefresh = true;
    }

    private boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    private boolean isHiveOrPrestoView(Table table)
    {
        return table.getTableType().equals(TableType.VIRTUAL_VIEW.name());
    }

    private boolean isIcebergTable(Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    private Table getTable()
    {
        try {
            GetTableRequest getTableRequest = new GetTableRequest()
                    .withDatabaseName(database)
                    .withName(tableName);
            catalogId.ifPresent(getTableRequest::setCatalogId);
            return stats.getGetTable().call(() -> glueClient.getTable(getTableRequest).getTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(getSchemaTableName());
        }
    }

    private void cleanupMetadataLocation(String metadataLocation)
    {
        try {
            io().deleteFile(metadataLocation);
        }
        catch (RuntimeException e) {
            log.error(e, "Fail to cleanup metadata file at " + metadataLocation);
            throw e;
        }
    }
}
