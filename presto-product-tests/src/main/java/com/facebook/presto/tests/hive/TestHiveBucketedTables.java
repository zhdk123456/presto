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
package com.facebook.presto.tests.hive;

import com.google.inject.name.Named;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.Requirements;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.TableDefinitionsRepository;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;
import com.teradata.tempto.hadoop.hdfs.HdfsClient;
import com.teradata.tempto.query.QueryExecutionException;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.Optional;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.TableDefinitionUtils.mutableTableInstanceOf;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestHiveBucketedTables
        extends ProductTest
        implements RequirementsProvider
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectoryPath;

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_TABLE = bucketTableDefinition("bucket_table", false);
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_EMPTY_TABLE = bucketTableDefinition("bucket_empty_table", false);
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_PARTITIONED_TABLE = bucketTableDefinition("bucket_partition_table", true);

    private static final String BUCKET_0_CONTENTS = "0|a\n2|c\n4|e\n6|g\n";
    private static final String BUCKET_1_CONTENTS = "1|b\n3|d\n5|f\n";

    private String getTableLocation(String tableName, Optional<String> partitionName)
    {
        String location = warehouseDirectoryPath + '/' + tableName;
        if (partitionName.isPresent()) {
            location += "/" + partitionName.get();
        }
        return location;
    }

    private static HiveTableDefinition bucketTableDefinition(String tableName, boolean partitioned)
    {
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                        "a     BIGINT, " +
                        "b     STRING) " +
                        (partitioned ? "PARTITIONED BY (c BIGINT) " : " ") +
                        "CLUSTERED BY (a) " +
                        "INTO 2 BUCKETS " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
                )
                .setNoData()
                .build();
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return Requirements.compose(
                MutableTableRequirement.builder(BUCKETED_TABLE).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_EMPTY_TABLE).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_PARTITIONED_TABLE).withState(CREATED).build());
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectStar()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_TABLE).getNameInDatabase();
        populateDataToHiveTable(tableName, Optional.empty(), Optional.empty());

        assertThat(query(format("SELECT * FROM %s WHERE a < 4", tableName))).containsOnly(
                row(0, "a"),
                row(1, "b"),
                row(2, "c"),
                row(3, "d")
        );
        assertThat(query(format("SELECT count(*) FROM %s WHERE a = 1", tableName)))
                .containsExactly(row(1));
        assertThat(query(format("SELECT count(*) FROM %s t1 JOIN %s t2 ON t1.a = t2.a", tableName, tableName)))
                .containsExactly(row(7));
    }

    @Test(groups = {HIVE_CONNECTOR},
            expectedExceptions = QueryExecutionException.class,
            expectedExceptionsMessageRegExp = ".*does not match the declared bucket count.*")
    public void testSelectAfterMultipleInsertsMultiBucketDisabled()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_TABLE).getNameInDatabase();
        populateDataToHiveTable(tableName, Optional.empty(), Optional.empty());
        populateDataToHiveTable(tableName, Optional.empty(), Optional.of("_copy_0"));

        query(format("SELECT count(*) FROM %s WHERE a = 1", tableName));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAfterMultipleInserts()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_TABLE).getNameInDatabase();
        populateDataToHiveTable(tableName, Optional.empty(), Optional.empty());
        populateDataToHiveTable(tableName, Optional.empty(), Optional.of("_copy_0"));

        enableMultiFileBucketing();

        assertThat(query(format("SELECT count(*) FROM %s WHERE a = 1", tableName)))
                .containsExactly(row(2));
        assertThat(query(format("SELECT count(*) FROM %s WHERE a < 4", tableName)))
                .containsExactly(row(8));
        assertThat(query(format("SELECT a, count(*) FROM %s GROUP BY a", tableName)))
                .containsOnly(row(0, 2), row(1, 2), row(2, 2), row(3, 2), row(4, 2), row(5, 2), row(6, 2));
        assertThat(query(format("SELECT count(*) FROM %s 1 JOIN %s t1 ON t1.a = t2.a", tableName, tableName)))
                .containsExactly(row(28));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAfterMultipleInsertsForPartitionedTable()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_PARTITIONED_TABLE).getNameInDatabase();

        onHive().executeQuery(format("ALTER TABLE %s ADD PARTITION (c=1)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s ADD PARTITION (c=2)", tableName));

        populateDataToHiveTable(tableName, Optional.of("c=1"), Optional.empty());
        populateDataToHiveTable(tableName, Optional.of("c=2"), Optional.empty());
        populateDataToHiveTable(tableName, Optional.of("c=1"), Optional.of("_copy_0"));
        populateDataToHiveTable(tableName, Optional.of("c=2"), Optional.of("_copy_0"));

        enableMultiFileBucketing();

        assertThat(query(format("SELECT count(*) FROM %s WHERE a = 1", tableName)))
                .containsExactly(row(4));
        assertThat(query(format("SELECT count(*) FROM %s WHERE b = 'a'", tableName)))
                .containsExactly(row(4));
        assertThat(query(format("SELECT count(*) FROM %s WHERE a = 1 AND c = 1", tableName)))
                .containsExactly(row(2));
        assertThat(query(format("SELECT a, count(*) FROM %s WHERE c = 2 GROUP BY a", tableName)))
                .containsOnly(row(0, 2), row(1, 2), row(2, 2), row(3, 2), row(4, 2), row(5, 2), row(6, 2));
        assertThat(query(format("SELECT count(*) FROM %s t1 JOIN %s t2 ON t1.a = t2.a", tableName, tableName)))
                .containsExactly(row(4 * 4 * 7));
        assertThat(query(format("SELECT count(*) FROM %s t1 JOIN %s t2 ON t1.a = t2.a WHERE t1.c = 1", tableName, tableName)))
                .containsExactly(row(4 * 4 * 7 / 2));
    }

    @Test(groups = {HIVE_CONNECTOR},
            expectedExceptions = QueryExecutionException.class,
            expectedExceptionsMessageRegExp = ".*\\(0\\) does not match the declared bucket count.*")
    public void testSelectFromEmptyBucketedTableEmptyTablesNotAllowed()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_EMPTY_TABLE).getNameInDatabase();
        query(format("SELECT count(*) FROM %s", tableName));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectFromEmptyBucketedTableEmptyTablesAllowed()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_EMPTY_TABLE).getNameInDatabase();
        enableEmptyBucketedPartitions();
        assertThat(query(format("SELECT count(*) FROM %s", tableName)))
                .containsExactly(row(0));
    }

    private static void enableMultiFileBucketing()
            throws SQLException
    {
        setSessionProperty(defaultQueryExecutor().getConnection(), "hive.multi_file_bucketing_enabled", "true");
    }

    private static void enableEmptyBucketedPartitions()
            throws SQLException
    {
        setSessionProperty(defaultQueryExecutor().getConnection(), "hive.empty_bucketed_partitions_enabled", "true");
    }

    private void populateDataToHiveTable(String tableName, Optional<String> partitionName, Optional<String> fileNameSuffix)
    {
        hdfsClient.saveFile(getTableLocation(tableName, partitionName) + "/000000_0" + fileNameSuffix.orElse(""), BUCKET_0_CONTENTS);
        hdfsClient.saveFile(getTableLocation(tableName, partitionName) + "/000001_0" + fileNameSuffix.orElse(""), BUCKET_1_CONTENTS);
    }
}
