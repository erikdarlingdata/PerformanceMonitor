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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the query_store_health definition (#2319) — database_scoped_config's
/// shape applied to <c>sys.database_query_store_options</c>: on-prem database-list selection (the
/// AG-primary filter), the [db].sys.sp_executesql per-database query with bracket escaping, the 2016+
/// <c>AppliesTo</c> gate, the 10-column payload — including the ordinal-mapped
/// <see cref="QueryStoreHealthCollector.ReadItemAsync"/> and the
/// <see cref="QueryStoreHealthCollector.WritePayload"/> order, the two paths most likely to silently
/// drift on a future column reorder — and since #3764 the second execution shape: on Azure SQL DB the
/// definition runs per database on the host's per-database connection (<c>RunsPerDatabase</c>) with the
/// payload body bare, because the three-part reference the on-prem idiom nests is rejected there for every
/// database that is not the connection's own. The two shapes are pinned to ONE body so they cannot drift
/// apart.
/// </summary>
public sealed class QueryStoreHealthCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    /// <summary>
    /// The exact shape Azure SQL DB rejects (#3764, the #3755 defect carried here): a bracket-closed database
    /// name, then <c>.sys.sp_executesql</c>. Matched as a regex rather than by <c>Contains("sp_executesql")</c>
    /// so the pin names the three-part reference specifically — a future body that legitimately mentioned
    /// sp_executesql in a comment would not be the bug this guards against.
    /// </summary>
    private static readonly Regex s_threePartReference = new(@"\]\.sys\.sp_executesql", RegexOptions.CultureInvariant);

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
        /* The system-database screen the per-database body repeats (see the Azure pin below): user
           databases plus tempdb, never master / model / msdb. */
        Assert.Contains("(d.database_id > 4 OR d.database_id = 2)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPrem_ManagedInstance_AndRds_KeepTheEnumerationShape_AndBuildQueryThrows()
    {
        /* Every target that honors a cross-database three-part reference keeps the single connection and
           the enumeration/per-item pair; BuildQuery is the Azure branch and throws off it rather than
           silently collecting the connection's own catalog as if it were the instance. */
        foreach (var target in new[]
        {
            new CollectorTargetInfo(),
            new CollectorTargetInfo { IsAzureManagedInstance = true },
            new CollectorTargetInfo { IsAwsRds = true },
        })
        {
            Assert.False(QueryStoreHealthCollector.Instance.RunsPerDatabase(target));

            var context = new CollectorContext
            {
                ServerId = 42,
                ServerName = "test-server",
                CollectionTime = DateTime.UtcNow,
                Deltas = s_deltas,
                Target = target,
            };
            Assert.NotNull(QueryStoreHealthCollector.Instance.BuildEnumerationQuery(context));
            Assert.Throws<NotSupportedException>(() => QueryStoreHealthCollector.Instance.BuildQuery(context));
        }
    }

    [Fact]
    public void Azure_TakesThePerDatabaseConnectionBranch_InsteadOfEnumerating()
    {
        /* #3764: the field failure, identical to database_scoped_config's (#3755). From a logical-server
           registration the connection sits in master and every enumerated database is some other database,
           so EXECUTE [db].sys.sp_executesql was rejected for each of them ("Reference to database and/or
           server name in 'xedb1.sys.sp_executesql' is not supported in this version of SQL Server") and the
           collector stored nothing while reading HEALTHY — which for THIS collector means the READ_ONLY
           transition it exists to catch (#2319) was never observed on such a registration. Both hosts test
           RunsPerDatabase BEFORE asking for an enumeration, so true here is what routes Azure onto the
           per-database connection loop; null from BuildEnumerationQuery says the same thing from the
           definition's side, and is what the enumerator censuses in both suites read. */
        var azure = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);

        Assert.True(QueryStoreHealthCollector.Instance.RunsPerDatabase(azure.Target));
        Assert.Null(QueryStoreHealthCollector.Instance.BuildEnumerationQuery(azure));

        /* Already connected to the database, so the payload body runs bare — no three-part name, no
           sp_executesql at all, and no parameters to map. */
        var plan = QueryStoreHealthCollector.Instance.BuildQuery(azure);
        Assert.Contains("FROM sys.database_query_store_options AS qso", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotMatch(s_threePartReference, plan.Text);
        Assert.DoesNotContain("sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void Azure_PerDatabaseBody_CarriesTheSystemDatabaseScreen_SoMasterStaysOut()
    {
        /* On the per-database branch the HOST owns the database list, and a logical-server registration's
           list is `database_id > 0` from master — master included. The on-prem enumeration never admits
           master (the `database_id > 4 OR database_id = 2` screen pinned above), so the row set on Azure
           would otherwise differ from on-prem by exactly master's Query Store row. The body carries the
           same screen on DB_ID(); this is the definition-side replacement for the deleted Azure list
           query's master exclusion. */
        var azure = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);

        var plan = QueryStoreHealthCollector.Instance.BuildQuery(azure);
        Assert.Contains("WHERE (DB_ID() > 4 OR DB_ID() = 2)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PerItemQuery_EscapesClosingBrackets_InDatabaseNames()
    {
        var plan = QueryStoreHealthCollector.Instance.BuildPerItemQuery("we]rd db", CollectorTestContext.Make(s_deltas));

        Assert.Contains("EXECUTE [we]]rd db].sys.sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Matches(s_threePartReference, plan.Text);
        Assert.Contains("sys.database_query_store_options", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void TheTwoExecutionShapes_RunOneBody_QuoteDoubledForNesting()
    {
        /* The drift pin. The on-prem per-item text must contain the Azure body verbatim after the ONE
           transformation nesting requires — doubling single quotes — so a column added to one path
           cannot be missing from the other and the shared row loop cannot mis-read an ordinal. Asserted
           through the two public builders rather than a shared constant, so the pin holds even if the
           constant is renamed or split. */
        var azureBody = QueryStoreHealthCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)).Text;
        var onPremText = QueryStoreHealthCollector.Instance.BuildPerItemQuery("db1", CollectorTestContext.Make(s_deltas)).Text;

        Assert.Contains("N'" + azureBody.Replace("'", "''", StringComparison.Ordinal) + "'", onPremText, StringComparison.Ordinal);

        /* And the nine reader ordinals, in ReadRowsAsync's order, on both. */
        var ordinals = new[]
        {
            "actual_state = qso.actual_state_desc",
            "desired_state = qso.desired_state_desc",
            "readonly_reason = qso.readonly_reason",
            "current_storage_size_mb = qso.current_storage_size_mb",
            "max_storage_size_mb = qso.max_storage_size_mb",
            "size_based_cleanup_mode = qso.size_based_cleanup_mode_desc",
            "stale_query_threshold_days = qso.stale_query_threshold_days",
            "max_plans_per_query = qso.max_plans_per_query",
            "interval_length_minutes = qso.interval_length_minutes",
        };

        foreach (var text in new[] { azureBody, onPremText })
        {
            var positions = ordinals.Select(o => text.IndexOf(o, StringComparison.Ordinal)).ToArray();

            Assert.All(positions, p => Assert.True(p >= 0, "every reader ordinal must be selected on both paths"));
            Assert.True(positions.SequenceEqual(positions.OrderBy(p => p)),
                "the payload must select actual_state, desired_state, readonly_reason, current_storage_size_mb, "
                + "max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, "
                + "interval_length_minutes in that order — ReadRowsAsync reads ordinals 0 through 8");
        }
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
    public async Task ReadAsync_Azure_TagsRowsWithTheConnectedDatabase_AndRefusesToRunUntagged()
    {
        /* The Azure path's row has no database column of its own — the body is the same bare SELECT on
           every database — so the name comes from CollectorContext.CurrentDatabaseName, which the host's
           per-database loop sets before each read. Same Row shape, same ordinals, same coalesce arms, as
           the enumerated path. */
        var context = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);
        context.CurrentDatabaseName = "xedb1";

        List<QueryStoreHealthCollector.Row> rows;
        using (var reader = new FakeCollectorDataReader(new object[]
               { "READ_ONLY", "READ_WRITE", 65536, 1000L, 1000L, DBNull.Value, 30L, 200L, 60L }))
        {
            rows = await QueryStoreHealthCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        }

        var row = Assert.Single(rows);
        Assert.Equal("xedb1", row.DbName);
        Assert.Equal("READ_ONLY", row.ActualState);
        Assert.Equal("READ_WRITE", row.DesiredState);
        Assert.Equal(65536, row.ReadonlyReason);
        Assert.Equal(1000L, row.CurrentStorageMb);
        Assert.Equal(1000L, row.MaxStorageMb);
        Assert.Null(row.SizeBasedCleanupMode);
        Assert.Equal(30L, row.StaleQueryThresholdDays);
        Assert.Equal(200L, row.MaxPlansPerQuery);
        Assert.Equal(60L, row.IntervalLengthMinutes);

        /* A host that forgot to set the name gets one loud failure, not rows filed under a blank
           database — the query_store rule, for the same reason: the latest-snapshot readers group and
           filter by database_name, and blank rows from N databases would collapse into one, so a
           READ_ONLY transition on one database could not be told from a healthy row on another. */
        var untagged = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);
        using var untaggedReader = new FakeCollectorDataReader(new object[]
            { "READ_WRITE", "READ_WRITE", 0, 512L, 1000L, "AUTO", 30L, 200L, 60L });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await QueryStoreHealthCollector.Instance.ReadAsync(untaggedReader, untagged, CancellationToken.None));
        Assert.Contains("CurrentDatabaseName", ex.Message, StringComparison.Ordinal);
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
