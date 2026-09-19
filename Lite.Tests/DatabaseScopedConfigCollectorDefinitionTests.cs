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
/// Pins the parity contract of the extracted database_scoped_config definition — the first
/// enumeration-shape collector: on-prem database-list selection (the AG-primary filter), the
/// [db].sys.sp_executesql per-database query with bracket escaping, the 4-column payload, and since
/// #3755 the second execution shape: on Azure SQL DB the definition runs per database on the host's
/// per-database connection (<c>RunsPerDatabase</c>) with the payload body bare, because the three-part
/// reference the on-prem idiom nests is rejected there for every database that is not the connection's
/// own. The two shapes are pinned to ONE body so they cannot drift apart.
/// </summary>
public sealed class DatabaseScopedConfigCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    /// <summary>
    /// The exact shape Azure SQL DB rejects (#3755): a bracket-closed database name, then
    /// <c>.sys.sp_executesql</c>. Matched as a regex rather than by <c>Contains("sp_executesql")</c> so
    /// the pin names the three-part reference specifically — a future body that legitimately mentioned
    /// sp_executesql in a comment would not be the bug this guards against.
    /// </summary>
    private static readonly Regex s_threePartReference = new(@"\]\.sys\.sp_executesql", RegexOptions.CultureInvariant);

    [Fact]
    public void EnumerationQuery_OnPrem_FiltersToAgPrimaries_AndSplicesExclusions()
    {
        var plan = DatabaseScopedConfigCollector.Instance.BuildEnumerationQuery(new CollectorContext
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
           same self-skip index_object_stats and database_size_stats already do. */
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
            Assert.False(DatabaseScopedConfigCollector.Instance.RunsPerDatabase(target));

            var context = new CollectorContext
            {
                ServerId = 42,
                ServerName = "test-server",
                CollectionTime = DateTime.UtcNow,
                Deltas = s_deltas,
                Target = target,
            };
            Assert.NotNull(DatabaseScopedConfigCollector.Instance.BuildEnumerationQuery(context));
            Assert.Throws<NotSupportedException>(() => DatabaseScopedConfigCollector.Instance.BuildQuery(context));
        }
    }

    [Fact]
    public void Azure_TakesThePerDatabaseConnectionBranch_InsteadOfEnumerating()
    {
        /* #3755: the field failure. From a logical-server registration the connection sits in master and
           every enumerated database is some other database, so EXECUTE [db].sys.sp_executesql was rejected
           for each of them ("Reference to database and/or server name in 'xedb1.sys.sp_executesql' is not
           supported in this version of SQL Server") and the collector stored nothing while reading
           HEALTHY. Both hosts test RunsPerDatabase BEFORE asking for an enumeration, so true here is what
           routes Azure onto the per-database connection loop; null from BuildEnumerationQuery says the
           same thing from the definition's side, and is what the enumerator censuses in both suites read. */
        var azure = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);

        Assert.True(DatabaseScopedConfigCollector.Instance.RunsPerDatabase(azure.Target));
        Assert.Null(DatabaseScopedConfigCollector.Instance.BuildEnumerationQuery(azure));

        /* Already connected to the database, so the payload body runs bare — no three-part name, no
           sp_executesql at all, and no parameters to map. */
        var plan = DatabaseScopedConfigCollector.Instance.BuildQuery(azure);
        Assert.Contains("FROM sys.database_scoped_configurations AS dsc", plan.Text, StringComparison.Ordinal);
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
           would otherwise differ from on-prem by exactly master's read-only defaults. The body carries the
           same screen on DB_ID(); this is the definition-side replacement for the deleted Azure list
           query's master exclusion. */
        var azure = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);

        var plan = DatabaseScopedConfigCollector.Instance.BuildQuery(azure);
        Assert.Contains("WHERE (DB_ID() > 4 OR DB_ID() = 2)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PerItemQuery_EscapesClosingBrackets_InDatabaseNames()
    {
        var plan = DatabaseScopedConfigCollector.Instance.BuildPerItemQuery("we]rd db", CollectorTestContext.Make(s_deltas));

        Assert.Contains("EXECUTE [we]]rd db].sys.sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Matches(s_threePartReference, plan.Text);
        Assert.Contains("sys.database_scoped_configurations", plan.Text, StringComparison.Ordinal);
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
        var azureBody = DatabaseScopedConfigCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)).Text;
        var onPremText = DatabaseScopedConfigCollector.Instance.BuildPerItemQuery("db1", CollectorTestContext.Make(s_deltas)).Text;

        Assert.Contains("N'" + azureBody.Replace("'", "''", StringComparison.Ordinal) + "'", onPremText, StringComparison.Ordinal);

        /* And the three reader ordinals, in ReadRowsAsync's order, on both. */
        foreach (var text in new[] { azureBody, onPremText })
        {
            var name = text.IndexOf("configuration_name = dsc.name", StringComparison.Ordinal);
            var value = text.IndexOf("value = CONVERT(nvarchar(256), dsc.value)", StringComparison.Ordinal);
            var secondary = text.IndexOf("value_for_secondary = CONVERT(nvarchar(256), dsc.value_for_secondary)", StringComparison.Ordinal);

            Assert.True(name >= 0 && name < value && value < secondary,
                "the payload must select configuration_name, value, value_for_secondary in that order — ReadRowsAsync reads ordinals 0, 1, 2");
        }
    }

    [Fact]
    public async Task ReadItemAsync_AccumulatesAcrossItems_TaggedWithDatabase()
    {
        var rows = new List<DatabaseScopedConfigCollector.Row>();
        var context = CollectorTestContext.Make(s_deltas);

        using (var reader = new FakeCollectorDataReader(new object[] { "MAXDOP", "8", "0" }))
        {
            await DatabaseScopedConfigCollector.Instance.ReadItemAsync("db1", reader, rows, context, CancellationToken.None);
        }

        using (var reader = new FakeCollectorDataReader(new object[] { "MAXDOP", "4", DBNull.Value }))
        {
            await DatabaseScopedConfigCollector.Instance.ReadItemAsync("db2", reader, rows, context, CancellationToken.None);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(new DatabaseScopedConfigCollector.Row("db1", "MAXDOP", "8", "0"), rows[0]);
        Assert.Equal(new DatabaseScopedConfigCollector.Row("db2", "MAXDOP", "4", null), rows[1]);
    }

    [Fact]
    public async Task ReadAsync_Azure_TagsRowsWithTheConnectedDatabase_AndRefusesToRunUntagged()
    {
        /* The Azure path's rows have no database column of their own — the body is the same bare SELECT
           on every database — so the name comes from CollectorContext.CurrentDatabaseName, which the
           host's per-database loop sets before each read. Same Row shape, same ordinals, as the
           enumerated path. */
        var context = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);
        context.CurrentDatabaseName = "xedb1";

        List<DatabaseScopedConfigCollector.Row> rows;
        using (var reader = new FakeCollectorDataReader(
            new object[] { "LEGACY_CARDINALITY_ESTIMATION", "0", "0" },
            new object[] { "MAXDOP", "8", DBNull.Value }))
        {
            rows = await DatabaseScopedConfigCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        }

        Assert.Equal(
            new[]
            {
                new DatabaseScopedConfigCollector.Row("xedb1", "LEGACY_CARDINALITY_ESTIMATION", "0", "0"),
                new DatabaseScopedConfigCollector.Row("xedb1", "MAXDOP", "8", null),
            },
            rows);

        /* A host that forgot to set the name gets one loud failure, not rows filed under a blank
           database — the query_store rule, for the same reason: the latest-snapshot readers group and
           filter by database_name, and blank rows from N databases would collapse into one. */
        var untagged = CollectorTestContext.Make(s_deltas, isAzureSqlDb: true);
        using var untaggedReader = new FakeCollectorDataReader(new object[] { "MAXDOP", "8", "0" });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DatabaseScopedConfigCollector.Instance.ReadAsync(untaggedReader, untagged, CancellationToken.None));
        Assert.Contains("CurrentDatabaseName", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_MatchSchema_AndWriteOrder()
    {
        Assert.Equal(
            new[] { "database_name", "configuration_name", "value", "value_for_secondary" },
            DatabaseScopedConfigCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());

        var writer = new RecordingCollectorRowWriter();
        DatabaseScopedConfigCollector.Instance.WritePayload(
            new DatabaseScopedConfigCollector.Row("db1", "MAXDOP", "8", null), writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { "db1", "MAXDOP", "8", null }, writer.Values);
    }
}
