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

import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.glue.GlueIcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.glue.TrinoGlueCatalog;

import java.util.Optional;

public class TestTrinoGlueCatalog
        extends BaseTestTrinoCatalog
{
    @Override
    protected TrinoCatalog createTrinoCatalog()
    {
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig(),
                        ImmutableSet.of()),
                ImmutableSet.of()),
                new HdfsConfig(),
                new NoHdfsAuthentication());
        return new TrinoGlueCatalog(
                hdfsEnvironment,
                new GlueIcebergTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)),
                AWSGlueAsyncClientBuilder.defaultClient(),
                new GlueMetastoreStats(),
                Optional.empty(),
                Optional.empty(),
                false);
    }
}
