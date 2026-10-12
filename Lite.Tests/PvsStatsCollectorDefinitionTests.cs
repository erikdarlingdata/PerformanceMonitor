/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the cross-SKU parity contract of the shared ADR persistent version store collector (#1951):
/// the 2019 version gate, the ONE 2022-only column, the Azure-vs-on-prem query fork, the shape that
/// guarantees one row per database, and the payload/read/write column alignment.
/// </summary>
public sealed class PvsStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        int sqlMajorVersion = 15,
        bool isAzureSqlDb = false,
        bool isAzureManagedInstance = false,
        string[]? excludedDatabases = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                SqlMajorVersion = sqlMajorVersion,
                IsAzureSqlDb = isAzureSqlDb,
                IsAzureManagedInstance = isAzureManagedInstance,
            },
            ExcludedDatabases = excludedDatabases ?? Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pvs_stats", PvsStatsCollector.Instance.Name);
        Assert.Equal("pvs_stats", PvsStatsCollector.Instance.TargetTable);

        /* Point-in-time snapshot of a DMV that reports current state — no watermark of either kind,
           and no probe-failure result set (the query is one statement against one instance-wide DMV,
           not a per-database enumeration). */
        Assert.Null(PvsStatsCollector.Instance.WatermarkColumn);
        Assert.Null(PvsStatsCollector.Instance.NumericWatermarkColumn);
        Assert.False(PvsStatsCollector.Instance.EmitsProbeFailures);
    }

    [Fact]
    public void AppliesTo_GatesOnSqlServer2019_AndNeverGatesAzure()
    {
        /* sys.dm_tran_persistent_version_store_stats is SQL Server 2019 (15.x)+. Below that the object
           does not exist, so the collector must not run at all — the repo supports 2016 as its floor. */
        Assert.False(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 13 }));
        Assert.False(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 14 }));

        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 15 }));
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 16 }));
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 17 }));

        /* Unknown version assumes newest, matching query_store and database_config. */
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 0 }));
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo()));

        /* Azure SQL DB and MI report a low ProductMajorVersion yet ship the DMV, and ADR is ALWAYS ON
           there — so they are never version-gated. A version gate that caught them would silently drop
           the platform where this telemetry matters most. */
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 12, IsAzureSqlDb = true }));
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 12, IsAzureManagedInstance = true }));

        /* RDS is just SQL Server: it follows the version gate, and needs no msdb access. */
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 15, IsAwsRds = true }));
        Assert.False(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 13, IsAwsRds = true }));
        Assert.True(PvsStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 15, HasMsdbAccess = false }));
    }

    [Fact]
    public void NeverRunsPerDatabase()
    {
        /* The database grain comes from the DMV's own rows on box/MI, and from the connection's own
           database on Azure SQL DB. Enumerating would connect to master for nothing (#1631). */
        Assert.False(PvsStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(PvsStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(PvsStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
    }

    [Theory]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(0)]
    public void SkippedOldestAbortedColumn_IsTheRealColumn_On2022AndLater(int majorVersion)
    {
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(sqlMajorVersion: majorVersion)).Text;

        Assert.Contains(
            "pvs_off_row_page_skipped_oldest_aborted_xdesid =\r\n        pvss.pvs_off_row_page_skipped_oldest_aborted_xdesid",
            text.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("\n", "\r\n", StringComparison.Ordinal),
            StringComparison.Ordinal);
        Assert.DoesNotContain("CONVERT(bigint, NULL)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void SkippedOldestAbortedColumn_IsTypedNull_On2019()
    {
        /* MS documents this ONE column as "Applies to: SQL Server 2022 (16.x) and later versions".
           Selecting it on 2019 fails the whole cycle; OMITTING it would fork the payload shape by
           version and make a mixed 2019/2022 fleet unstorable in one table. So it is a typed NULL. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(sqlMajorVersion: 15)).Text;

        Assert.Contains("CONVERT(bigint, NULL)", text, StringComparison.Ordinal);
        Assert.DoesNotContain("pvss.pvs_off_row_page_skipped_oldest_aborted_xdesid", text, StringComparison.Ordinal);

        /* The payload shape is version-INDEPENDENT: same 23 columns either way. */
        Assert.Equal(23, PvsStatsCollector.Instance.PayloadColumns.Count);
    }

    [Fact]
    public void AzureSqlDb_IsDatabaseScoped_AndUsesDatabaseFiles()
    {
        var query = PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true));

        /* Database-scoped: MS's Azure variant filters on DB_ID() because the DMV is not instance-wide
           there. tempdb (database_id 2) is deliberately NOT collected — it is not the customer's to size. */
        Assert.Contains("WHERE pvss.database_id = DB_ID()", query.Text, StringComparison.Ordinal);
        Assert.Contains("database_name =\r\n        DB_NAME()".Replace("\r\n", "\n", StringComparison.Ordinal),
            query.Text.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* sys.master_files is not readable on Azure SQL DB — MS ships two query variants for this reason. */
        Assert.Contains("sys.database_files", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.master_files", query.Text, StringComparison.Ordinal);

        /* No exclusion splice on the Azure path: the host already chose the database it connected to. */
        Assert.Empty(query.Parameters);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPrem_IsInstanceWide_AndUsesMasterFiles()
    {
        var query = PvsStatsCollector.Instance.BuildQuery(MakeContext());

        Assert.Contains("sys.master_files", query.Text, StringComparison.Ordinal);
        /* Only the tempdb-qualified form (#5644 W5, below) may appear; the unqualified view is the Azure path's. */
        Assert.DoesNotContain("sys.database_files", query.Text.Replace("tempdb.sys.database_files", "", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("DB_ID()", query.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPrem_SizesTempdbFromItsLiveFiles_NotTheStartupSizeInMasterFiles()
    {
        /* #5644 W5: sys.master_files carries tempdb's size at startup (32 GB against a live 44 GB), so the
           denominator for database_id 2 comes from tempdb.sys.database_files. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("FROM tempdb.sys.database_files AS tdbf", text, StringComparison.Ordinal);
        Assert.Equal(1, CountOccurrences(text, "OUTER APPLY"));
        Assert.Contains("AND   mf.database_id <> 2", text, StringComparison.Ordinal);
        Assert.Contains("WHERE pvss.database_id = 2", text, StringComparison.Ordinal);
        Assert.DoesNotContain("tempdb.sys.database_files", PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true)).Text, StringComparison.Ordinal);
    }

    [Fact]
    public void ManagedInstance_TakesTheOnPremPath()
    {
        /* MI is a full instance: instance-wide sys.databases and sys.master_files both work, so it must
           NOT take the single-database Azure fork or it would report one database out of many. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureManagedInstance: true)).Text;

        Assert.Contains("sys.master_files", text, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE pvss.database_id = DB_ID()", text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPrem_SplicesExclusionFilterOnce_WithParameters()
    {
        var query = PvsStatsCollector.Instance.BuildQuery(
            MakeContext(excludedDatabases: new[] { "ScratchDb", "OtherDb" }));

        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);
        Assert.Contains("AND d.name NOT IN (@excl_db_0, @excl_db_1)", query.Text, StringComparison.Ordinal);

        /* Parameterized, never interpolated — and bound once, not once per splice site. */
        Assert.Equal(2, query.Parameters.Count);
        Assert.Equal(new[] { "@excl_db_0", "@excl_db_1" }, query.Parameters.Select(p => p.Name).ToArray());
        Assert.Equal(new object?[] { "ScratchDb", "OtherDb" }, query.Parameters.Select(p => p.Value).ToArray());
        Assert.All(query.Parameters, p => Assert.Equal(CollectorParameterType.NVarChar128, p.Type));
    }

    [Fact]
    public void NoExclusions_LeavesNoResidue()
    {
        var query = PvsStatsCollector.Instance.BuildQuery(MakeContext());

        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT IN", query.Text, StringComparison.Ordinal);
        Assert.Empty(query.Parameters);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UnsoundMicrosoftJoins_AreNotPresent_AndRowsCannotMultiply(bool isAzureSqlDb)
    {
        /* THE correctness guard, and the one most likely to be "helpfully" undone by someone comparing
           this query to Microsoft's published diagnostic. MS resolves a begin time and a session id with
           two LEFT JOINs; both were measured against a live ADR database (SQL Server 2025) with a
           genuinely open transaction and a genuinely open snapshot scan, and both returned NULL --
           oldest_active_transaction_id lives in a different ID space than
           sys.dm_tran_database_transactions.transaction_id, and min_transaction_timestamp is a cleanup
           low-water mark rather than any live transaction's sequence number. Re-adding either would ship
           columns that are structurally always NULL under headers promising the opposite; MS's snapshot
           join would additionally MULTIPLY the row (it is an OR across two timestamps), and a doubled
           per-database row doubles every aggregate downstream. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: isAzureSqlDb)).Text;

        Assert.DoesNotContain("sys.dm_tran_database_transactions", text, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.dm_tran_active_snapshot_database_transactions", text, StringComparison.Ordinal);
        Assert.DoesNotContain("database_transaction_begin_time", text, StringComparison.Ordinal);
        Assert.DoesNotContain("transaction_sequence_num", text, StringComparison.Ordinal);

        /* What remains cannot multiply: one aggregate OUTER APPLY for the size denominator, and on the
           on-prem path one INNER JOIN to sys.databases, which is one row per database_id. The Azure path
           has no join at all (see AzureSqlDb_ReadsTheAdrFlagByName_NotByDatabaseId) and its scalar
           subquery is one row by construction. */
        Assert.Equal(1, CountOccurrences(text, "OUTER APPLY"));
        Assert.Equal(0, CountOccurrences(text, "LEFT JOIN"));
        Assert.Equal(isAzureSqlDb ? 0 : 1, CountOccurrences(text, "JOIN sys.databases"));
    }

    [Fact]
    public void AzureSqlDb_ReadsTheAdrFlagByName_NotByDatabaseId()
    {
        /* REGRESSION GUARD for a silent-zero-rows defect. On Azure SQL Database the two database_id
           spaces are NOT the same one: MS documents that sys.databases.database_id is unique within the
           LOGICAL SERVER, while DB_ID() and the database_id of every other system view -- this DMV
           included -- are unique only within the database or elastic pool, and that "DB_ID may not return
           the same value as the database_id column in sys.databases". So `JOIN sys.databases AS d ON
           d.database_id = pvss.database_id` matches nothing on Azure and the INNER JOIN eats the row:
           no exception, no log line, an empty result set every hour, forever. Worse, it would match on
           some databases and not others, so a fleet fails intermittently.

           MS's own Azure variant of this diagnostic never joins sys.databases for exactly this reason.
           The flag is read by NAME instead, which is deterministic: in a user database sys.databases
           returns only the current database and master, so d.name = DB_NAME() is exactly one row. The
           on-prem/MI path keeps the id join -- there both ids are instance-scoped and it is correct. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true)).Text;

        Assert.DoesNotContain("d.database_id = pvss.database_id", text, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN sys.databases", text, StringComparison.Ordinal);
        Assert.Contains("WHERE d.name = DB_NAME()", text, StringComparison.Ordinal);
        Assert.Contains("d.is_accelerated_database_recovery_on", text, StringComparison.Ordinal);

        /* The on-prem text is the one that DOES join by id, so the two paths cannot be collapsed. */
        var onPrem = PvsStatsCollector.Instance.BuildQuery(MakeContext()).Text;
        Assert.Contains("ON d.database_id = pvss.database_id", onPrem, StringComparison.Ordinal);
        Assert.DoesNotContain("DB_NAME()", onPrem, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SysDatabasesJoinIsInner_SoTheRowSetDoesNotDependOnConfiguration(bool withExclusions)
    {
        /* Measured on SQL Server 2025: the DMV returns rows for hidden internal databases (32761
           model_msdb, 32762 model_replicatedmaster, 32767 unnamed) that sys.databases does not expose.
           Under a LEFT JOIN they appeared with a NULL name -- and then VANISHED the moment any database
           was excluded, because the exclusion compares d.name and NULL NOT IN (...) is UNKNOWN. An inner
           join makes the row set the same either way, which is what this pins. */
        var text = PvsStatsCollector.Instance.BuildQuery(
            MakeContext(excludedDatabases: withExclusions ? new[] { "ScratchDb" } : null)).Text;

        Assert.DoesNotContain("LEFT JOIN sys.databases", text, StringComparison.Ordinal);
        Assert.Contains("JOIN sys.databases AS d", text, StringComparison.Ordinal);

        /* And the name comes straight from the catalog, so it is never null. */
        Assert.DoesNotContain("COALESCE(d.name", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void QueryNeverUsesSelectStar_AndAlwaysRecompiles(bool isAzureSqlDb)
    {
        /* MS: "Don't use the syntax SELECT * FROM dynamic_management_view_name in production code
           because the number of columns returned might change and break your application." */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: isAzureSqlDb)).Text;

        Assert.DoesNotContain("SELECT *", text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE);", text, StringComparison.Ordinal);
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SizesAreConvertedFromTheDmvsKilobytes(bool isAzureSqlDb)
    {
        /* The DMV reports KB; the FinOps surface is MB everywhere else. A dropped conversion would
           overstate every PVS by 1024x and still look plausible on a small database. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: isAzureSqlDb)).Text;

        Assert.Contains("CONVERT(decimal(19,2), pvss.persistent_version_store_size_kb / 1024.0)", text, StringComparison.Ordinal);
        Assert.Contains("CONVERT(decimal(19,2), pvss.online_index_version_store_size_kb / 1024.0)", text, StringComparison.Ordinal);
        Assert.Contains("CONVERT(decimal(19,2), df.total_db_size_kb / 1024.0)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AdrFlagIsCollected_AndCollectionIsNotGatedOnIt()
    {
        /* The issue asks for enablement as inventory. It must NOT become a filter: MS's own
           PVS-filegroup-move procedure disables ADR and then watches this DMV until the size reaches
           zero, so rows outlive the setting — and a draining PVS is exactly when someone is watching. */
        var text = PvsStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("is_accelerated_database_recovery_on =\n        d.is_accelerated_database_recovery_on",
            text.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("is_accelerated_database_recovery_on = 1", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder_WithTheDeclaredTypes()
    {
        var columns = PvsStatsCollector.Instance.PayloadColumns;

        Assert.Equal(
            new[]
            {
                "database_name",
                "database_id",
                "is_accelerated_database_recovery_on",
                "pvs_filegroup_id",
                "persistent_version_store_size_mb",
                "online_index_version_store_size_mb",
                "database_data_size_mb",
                "current_aborted_transaction_count",
                "oldest_active_transaction_id",
                "oldest_aborted_transaction_id",
                "min_transaction_timestamp",
                "online_index_min_transaction_timestamp",
                "secondary_low_water_mark",
                "offrow_version_cleaner_start_time",
                "offrow_version_cleaner_end_time",
                "aborted_version_cleaner_start_time",
                "aborted_version_cleaner_end_time",
                "pvs_off_row_page_skipped_low_water_mark",
                "pvs_off_row_page_skipped_transaction_not_cleaned",
                "pvs_off_row_page_skipped_oldest_active_xdesid",
                "pvs_off_row_page_skipped_min_useful_xts",
                "pvs_off_row_page_skipped_oldest_snapshot",
                "pvs_off_row_page_skipped_oldest_aborted_xdesid",
            },
            columns.Select(c => c.Name).ToArray());

        /* Sizes carry explicit precision — the Postgres generator throws without it. */
        foreach (var size in new[] { "persistent_version_store_size_mb", "online_index_version_store_size_mb", "database_data_size_mb" })
        {
            var column = columns.Single(c => c.Name == size);
            Assert.Equal(CollectorColumnType.Decimal, column.Type);
            Assert.Equal(19, column.Precision);
            Assert.Equal(2, column.Scale);
        }

        /* pvs_filegroup_id is smallint in the DMV, and stored as one rather than widened. */
        Assert.Equal(CollectorColumnType.SmallInt, columns.Single(c => c.Name == "pvs_filegroup_id").Type);
        Assert.Equal(CollectorColumnType.Boolean, columns.Single(c => c.Name == "is_accelerated_database_recovery_on").Type);
        Assert.Equal(CollectorColumnType.Integer, columns.Single(c => c.Name == "database_id").Type);

        /* The four cleaner timestamps are datetime2 in the DMV, and their NULLs are SEMANTIC:
           start set + end NULL means cleanup is in progress. */
        foreach (var time in new[]
                 {
                     "offrow_version_cleaner_start_time",
                     "offrow_version_cleaner_end_time",
                     "aborted_version_cleaner_start_time",
                     "aborted_version_cleaner_end_time",
                 })
        {
            Assert.Equal(CollectorColumnType.Timestamp, columns.Single(c => c.Name == time).Type);
        }

        /* Transaction IDs and the skipped-page counters are all bigint — never int, which the DMV's
           transaction identifiers overflow. */
        foreach (var big in new[]
                 {
                     "current_aborted_transaction_count",
                     "oldest_active_transaction_id",
                     "oldest_aborted_transaction_id",
                     "min_transaction_timestamp",
                     "online_index_min_transaction_timestamp",
                     "secondary_low_water_mark",
                     "pvs_off_row_page_skipped_low_water_mark",
                     "pvs_off_row_page_skipped_transaction_not_cleaned",
                     "pvs_off_row_page_skipped_oldest_active_xdesid",
                     "pvs_off_row_page_skipped_min_useful_xts",
                     "pvs_off_row_page_skipped_oldest_snapshot",
                     "pvs_off_row_page_skipped_oldest_aborted_xdesid",
                 })
        {
            Assert.Equal(CollectorColumnType.BigInt, columns.Single(c => c.Name == big).Type);
        }
    }

    [Fact]
    public async Task ReadAsync_MapsEveryColumn_ByOrdinal()
    {
        var cleanerStart = new DateTime(2026, 8, 1, 11, 0, 0, DateTimeKind.Unspecified);
        var cleanerEnd = new DateTime(2026, 8, 1, 11, 5, 0, DateTimeKind.Unspecified);

        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "AdrDb", 7, true, (short)1,
                4096.50m, 12.25m, 20480.00m,
                42L, 1234567L, 1234000L, 999L, 888L, 777L,
                cleanerStart, cleanerEnd, cleanerStart, cleanerEnd,
                10L, 20L, 30L, 40L, 50L, 60L,
            });

        var rows = await PvsStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("AdrDb", row.DatabaseName);
        Assert.Equal(7, row.DatabaseId);
        Assert.True(row.IsAcceleratedDatabaseRecoveryOn);
        Assert.Equal((short)1, row.PvsFilegroupId);
        Assert.Equal(4096.50m, row.PersistentVersionStoreSizeMb);
        Assert.Equal(12.25m, row.OnlineIndexVersionStoreSizeMb);
        Assert.Equal(20480.00m, row.DatabaseDataSizeMb);
        Assert.Equal(42L, row.CurrentAbortedTransactionCount);
        Assert.Equal(1234567L, row.OldestActiveTransactionId);
        Assert.Equal(1234000L, row.OldestAbortedTransactionId);
        Assert.Equal(999L, row.MinTransactionTimestamp);
        Assert.Equal(888L, row.OnlineIndexMinTransactionTimestamp);
        Assert.Equal(777L, row.SecondaryLowWaterMark);
        Assert.Equal(cleanerStart, row.OffrowVersionCleanerStartTime);
        Assert.Equal(cleanerEnd, row.OffrowVersionCleanerEndTime);
        Assert.Equal(cleanerStart, row.AbortedVersionCleanerStartTime);
        Assert.Equal(cleanerEnd, row.AbortedVersionCleanerEndTime);
        Assert.Equal(10L, row.PvsOffRowPageSkippedLowWaterMark);
        Assert.Equal(20L, row.PvsOffRowPageSkippedTransactionNotCleaned);
        Assert.Equal(30L, row.PvsOffRowPageSkippedOldestActiveXdesid);
        Assert.Equal(40L, row.PvsOffRowPageSkippedMinUsefulXts);
        Assert.Equal(50L, row.PvsOffRowPageSkippedOldestSnapshot);
        Assert.Equal(60L, row.PvsOffRowPageSkippedOldestAbortedXdesid);
    }

    [Fact]
    public async Task ReadAsync_ToleratesNulls_IncludingTheOngoingCleanupShape()
    {
        /* A 2019 server returns NULL for the 2022-only column; a quiet database has no oldest
           transaction and no snapshot scan; and a cleanup IN PROGRESS reports a start with a NULL end —
           MS's documented "cleanup is ongoing" signal, which must survive the read as a NULL rather
           than being coalesced away. */
        var cleanerStart = new DateTime(2026, 8, 1, 11, 0, 0, DateTimeKind.Unspecified);

        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "QuietDb", 9, false, DBNull.Value,
                0.00m, 0.00m, DBNull.Value,
                0L, 0L, 0L, 0L, 0L, 0L,
                cleanerStart, DBNull.Value, DBNull.Value, DBNull.Value,
                0L, 0L, 0L, 0L, 0L, DBNull.Value,
            });

        var rows = await PvsStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.False(row.IsAcceleratedDatabaseRecoveryOn);
        Assert.Null(row.PvsFilegroupId);
        Assert.Null(row.DatabaseDataSizeMb);
        Assert.Equal(cleanerStart, row.OffrowVersionCleanerStartTime);
        Assert.Null(row.OffrowVersionCleanerEndTime);
        Assert.Null(row.PvsOffRowPageSkippedOldestAbortedXdesid);

        /* Zeroes are DATA, not absence — a database with a real zero PVS still reports it. */
        Assert.Equal(0.00m, row.PersistentVersionStoreSizeMb);
        Assert.Equal(0L, row.CurrentAbortedTransactionCount);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_AndTakesNoDeltas()
    {
        var begin = new DateTime(2026, 8, 1, 9, 30, 0, DateTimeKind.Unspecified);
        var row = new PvsStatsCollector.Row(
            "AdrDb", 7, true, 1,
            4096.50m, 12.25m, 20480.00m,
            42L, 1234567L, 1234000L, 999L, 888L, 777L,
            begin, begin, begin, begin,
            10L, 20L, 30L, 40L, 50L, 60L);

        var writer = new RecordingCollectorRowWriter();

        PvsStatsCollector.Instance.WritePayload(row, writer, MakeContext());

        /* The DuckDB appender and the Npgsql binary COPY are both positional — a writer that emits a
           different count than PayloadColumns declares corrupts every subsequent column silently. */
        Assert.Equal(PvsStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal(
            new object?[]
            {
                "AdrDb", 7, true, (short)1,
                4096.50m, 12.25m, 20480.00m,
                42L, 1234567L, 1234000L, 999L, 888L, 777L,
                begin, begin, begin, begin,
                10L, 20L, 30L, 40L, 50L, 60L,
            },
            writer.Values.ToArray());

        /* Pure snapshot: every column is current state, so nothing goes through the delta framework.
           s_deltas is the calculator MakeContext binds, so this observes what WritePayload actually did -
           a fresh local here would be vacuous. */
        Assert.Empty(s_deltas.Calls);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        int count = 0;
        int index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
