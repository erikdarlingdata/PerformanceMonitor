/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the query_store_health definition (#2319) — database_scoped_config's
/// enumeration shape applied to <c>sys.database_query_store_options</c>: database-list selection
/// (AG-primary filter on-prem, plain list on Azure), the [db].sys.sp_executesql per-database query with
/// bracket escaping, the 2016+ <c>AppliesTo</c> gate, and the 10-column payload — including the
/// ordinal-mapped <see cref="QueryStoreHealthCollector.ReadItemAsync"/> and the
/// <see cref="QueryStoreHealthCollector.WritePayload"/> order, the two paths most likely to silently
/// drift on a future column reorder.
/// </summary>
public sealed class QueryStoreHealthCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void EnumerationQuery_OnPrem_FiltersToAgPrimaries_AndSplicesExclusions()
    {
        var plan = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "SO" },
        });

        Assert.NotNull(plan);
        Assert.Contains("sys.dm_hadr_database_replica_states", plan!.Text, StringComparison.Ordinal);
        Assert.Contains("is_primary_replica = 1", plan.Text, StringComparison.Ordinal);
        /* #1823: a least-privilege login without per-db access must be filtered out up front, the
           same self-skip the sibling per-database collectors carry. */
        Assert.Contains("HAS_DBACCESS(d.name) = 1", plan.Text, StringComparison.Ordinal);
        Assert.Contains("AND d.name NOT IN (@excl_db_0)", plan.Text, StringComparison.Ordinal);
        Assert.Equal("SO", Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void EnumerationQuery_Azure_ListsAllOnline_NoAgFilter()
    {
        var plan = QueryStoreHealthCollector.Instance.BuildEnumerationQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.NotNull(plan);
        Assert.DoesNotContain("dm_hadr_database_replica_states", plan!.Text, StringComparison.Ordinal);
        Assert.Contains("state_desc = N'ONLINE'", plan.Text, StringComparison.Ordinal);
        /* The asymmetry is deliberate: from master on Azure SQL DB, HAS_DBACCESS() returns 0 for
           every user database, so the on-prem filter here would silently enumerate nothing. */
        Assert.DoesNotContain("HAS_DBACCESS", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PerItemQuery_EscapesClosingBrackets_InDatabaseNames()
    {
        var plan = QueryStoreHealthCollector.Instance.BuildPerItemQuery("we]rd db", CollectorTestContext.Make(s_deltas));

        Assert.Contains("EXECUTE [we]]rd db].sys.sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.database_query_store_options", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    /// <summary>Query Store shipped in 2016 (v13); without the gate a pre-2016 target errors once per
    /// database per hour. Same condition as QueryStoreCollector, so both SKUs skip identically.</summary>
    [Theory]
    [InlineData(11, false)]
    [InlineData(12, false)]
    [InlineData(13, true)]
    [InlineData(0, true)]     /* version unknown = assume newest */
    public void AppliesTo_GatesOnQueryStoresExistence(int majorVersion, bool applies)
        => Assert.Equal(applies, QueryStoreHealthCollector.Instance.AppliesTo(
            new CollectorTargetInfo { SqlMajorVersion = majorVersion }));

    [Fact]
    public async Task ReadItemAsync_AccumulatesAcrossItems_TaggedWithDatabase_AndNullsCoalesceHonestly()
    {
        var rows = new List<QueryStoreHealthCollector.Row>();
        var context = CollectorTestContext.Make(s_deltas);

        /* A healthy READ_WRITE database with a cap. */
        using (var reader = new FakeCollectorDataReader(new object[]
               { "READ_WRITE", "READ_WRITE", 0, 512L, 1000L, "AUTO", 30L, 200L, 60L }))
        {
            await QueryStoreHealthCollector.Instance.ReadItemAsync("db1", reader, rows, context, CancellationToken.None);
        }

        /* The cap-hit shape this collector exists for: desired READ_WRITE, actual READ_ONLY, reason
           65536 — plus DBNulls exercising every coalesce arm. */
        using (var reader = new FakeCollectorDataReader(new object[]
               { "READ_ONLY", "READ_WRITE", 65536, 1000L, 1000L, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value }))
        {
            await QueryStoreHealthCollector.Instance.ReadItemAsync("db2", reader, rows, context, CancellationToken.None);
        }

        Assert.Equal(2, rows.Count);

        Assert.Equal("db1", rows[0].DbName);
        Assert.Equal("READ_WRITE", rows[0].ActualState);
        Assert.Equal("READ_WRITE", rows[0].DesiredState);
        Assert.Equal(0, rows[0].ReadonlyReason);
        Assert.Equal(512L, rows[0].CurrentStorageMb);
        Assert.Equal(1000L, rows[0].MaxStorageMb);
        Assert.Equal("AUTO", rows[0].SizeBasedCleanupMode);
        Assert.Equal(30L, rows[0].StaleQueryThresholdDays);
        Assert.Equal(200L, rows[0].MaxPlansPerQuery);
        Assert.Equal(60L, rows[0].IntervalLengthMinutes);

        Assert.Equal("db2", rows[1].DbName);
        Assert.Equal("READ_ONLY", rows[1].ActualState);
        Assert.Equal("READ_WRITE", rows[1].DesiredState);
        Assert.Equal(65536, rows[1].ReadonlyReason);
        Assert.Null(rows[1].SizeBasedCleanupMode);
        Assert.Equal(0L, rows[1].StaleQueryThresholdDays);
        Assert.Equal(0L, rows[1].MaxPlansPerQuery);
        Assert.Equal(0L, rows[1].IntervalLengthMinutes);
    }

    [Fact]
    public void PayloadColumns_MatchSchema_AndWriteOrder()
    {
        Assert.Equal(
            new[]
            {
                "database_name", "actual_state", "desired_state", "readonly_reason",
                "current_storage_size_mb", "max_storage_size_mb", "size_based_cleanup_mode",
                "stale_query_threshold_days", "max_plans_per_query", "interval_length_minutes",
            },
            QueryStoreHealthCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());

        var writer = new RecordingCollectorRowWriter();
        QueryStoreHealthCollector.Instance.WritePayload(
            new QueryStoreHealthCollector.Row
            {
                DbName = "db1",
                ActualState = "READ_WRITE",
                DesiredState = "READ_WRITE",
                ReadonlyReason = 0,
                CurrentStorageMb = 512L,
                MaxStorageMb = 1000L,
                SizeBasedCleanupMode = "AUTO",
                StaleQueryThresholdDays = 30L,
                MaxPlansPerQuery = 200L,
                IntervalLengthMinutes = 60L,
            },
            writer, CollectorTestContext.Make(s_deltas));

        Assert.Equal(
            new object?[] { "db1", "READ_WRITE", "READ_WRITE", 0, 512L, 1000L, "AUTO", 30L, 200L, 60L },
            writer.Values);
    }
}
