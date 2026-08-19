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
/// Pins the freeze-headroom collector: it runs on any PostgreSQL target (not just Aurora), it collects
/// MultiXact age as a first-class second counter, and its percentages use the right denominators.
/// </summary>
public class PgWraparoundStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(bool isAurora = true)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                IsAurora = isAurora,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_wraparound_stats", PgWraparoundStatsCollector.Instance.Name);
        Assert.Equal("pg_wraparound_stats", PgWraparoundStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgWraparoundStatsCollector.Instance.TargetEngine);
    }

    /// <summary>
    /// Unlike the wait and statement collectors, this one reads only core catalog surfaces, so it must
    /// apply to stock PostgreSQL as well as Aurora. Gating it on Aurora would silently drop the single
    /// most consequential PostgreSQL signal on every non-Aurora target.
    /// </summary>
    [Fact]
    public void AppliesToAnyPostgresTargetIncludingNonAurora()
    {
        Assert.True(PgWraparoundStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true }));
        Assert.True(PgWraparoundStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = false }));
    }

    /// <summary>But the engine gate still keeps it off SQL Server.</summary>
    [Fact]
    public void NeverDispatchesAtASqlServerTarget()
    {
        Assert.False(CollectorCatalog.AppliesTo(
            PgWraparoundStatsCollector.Instance, new CollectorTargetInfo()));
    }

    /// <summary>
    /// MultiXact is a separate counter and separately fatal, and it is the one most monitoring misses.
    /// Both must be in the query or the collector gives false comfort.
    /// </summary>
    [Fact]
    public void CollectsBothTransactionIdAndMultiXactAge()
    {
        var sql = PgWraparoundStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("age(d.datfrozenxid)", sql, StringComparison.Ordinal);
        Assert.Contains("mxid_age(d.datminmxid)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// age()/mxid_age() rather than arithmetic on the raw xid: both handle the modular wrap that makes a
    /// naive subtraction wrong precisely near the boundary, which is the only region that matters.
    /// </summary>
    [Fact]
    public void UsesAgeFunctionsRatherThanRawSubtraction()
    {
        var sql = PgWraparoundStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotContain("- d.datfrozenxid", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("txid_current()", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Reads the SHARED catalog and INCLUDES templates — the opposite of the per-database fan-out, and the
    /// contrast is the point.
    /// <para>The cluster-wide stop limit derives from the oldest <c>datfrozenxid</c> anywhere in
    /// <c>pg_database</c>, so excluding template0/template1 would understate cluster risk by exactly the
    /// amount that matters; template0 aging without ever being vacuumed is a documented route there, usually
    /// after a major upgrade. This query needs no connection — <c>pg_database</c> is shared, and template0's
    /// row reads fine despite <c>datallowconn = false</c> (verified on live Aurora 17.7).</para>
    /// <para>The per-database enumeration in <c>ITargetProvider.BuildDatabaseListPlan</c> DOES exclude
    /// templates, and must: it opens a connection per database and template0 refuses them. Two queries, two
    /// correct answers — so do NOT "fix" the inconsistency by aligning them. That counterpart is pinned by
    /// <c>Darling.Tests.TargetProviderTests.PostgresDatabaseList_SkipsTemplatesAndClosedDatabases</c>;
    /// it cannot be asserted here because the provider lives in the Darling service project.</para>
    /// </summary>
    [Fact]
    public void ReadsTheSharedCatalogAndIncludesTemplates()
    {
        var sql = PgWraparoundStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("FROM pg_database", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT d.datistemplate", sql, StringComparison.Ordinal);
        Assert.False(PgWraparoundStatsCollector.Instance.RunsPerDatabase(MakeContext().Target));

    }

    [Fact]
    public void PayloadColumns_OrderAndTypes_Pinned()
    {
        var expected = new (string Name, CollectorColumnType Type)[]
        {
            ("database_name", CollectorColumnType.Varchar),
            ("frozen_xid_age", CollectorColumnType.BigInt),
            ("min_multixid_age", CollectorColumnType.BigInt),
            ("autovacuum_freeze_max_age", CollectorColumnType.BigInt),
            ("autovacuum_multixact_freeze_max_age", CollectorColumnType.BigInt),
            ("pct_toward_emergency_vacuum", CollectorColumnType.Double),
            ("pct_toward_wraparound", CollectorColumnType.Double),
            ("pct_toward_multixact_emergency", CollectorColumnType.Double),
            ("pct_toward_multixact_wraparound", CollectorColumnType.Double),
            ("xids_remaining", CollectorColumnType.BigInt),
            ("multixids_remaining", CollectorColumnType.BigInt),
            ("allows_connections", CollectorColumnType.Boolean),
        };

        var actual = PgWraparoundStatsCollector.Instance.PayloadColumns;
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Name, actual[i].Name);
            Assert.Equal(expected[i].Type, actual[i].Type);
        }
    }

    /// <summary>
    /// The two percentages answer different questions against different denominators: emergency-vacuum
    /// percentage is against autovacuum_freeze_max_age (200M by default), wraparound percentage against
    /// the ~2^31 ceiling. Conflating them would make a routine anti-wraparound vacuum look like an
    /// imminent outage, roughly a tenfold overstatement at defaults.
    /// </summary>
    [Fact]
    public void PercentagesUseTheirOwnDenominators()
    {
        var writer = new RecordingCollectorRowWriter();
        PgWraparoundStatsCollector.Instance.WritePayload(
            new PgWraparoundStatsCollector.Row(
                DatabaseName: "app",
                FrozenXidAge: 200_000_000,
                MinMultiXidAge: 100_000_000,
                AutovacuumFreezeMaxAge: 200_000_000,
                AutovacuumMultixactFreezeMaxAge: 400_000_000,
                AllowsConnections: true),
            writer,
            MakeContext());

        /* At exactly autovacuum_freeze_max_age: 100% of the way to an emergency vacuum, but only
           ~9.3% of the way to the wraparound ceiling. */
        Assert.Equal(100.0, (double)writer.Values[5]!, precision: 6);
        Assert.Equal(200_000_000d / PgWraparoundStatsCollector.WraparoundCeiling * 100, (double)writer.Values[6]!, precision: 6);

        /* MultiXact has its own, larger default ceiling: 100M against 400M is 25%. */
        Assert.Equal(25.0, (double)writer.Values[7]!, precision: 6);
    }

    /// <summary>An unreadable setting must not manufacture a percentage — or an alert.</summary>
    [Fact]
    public void UnknownDenominatorYieldsZeroNotInfinity()
    {
        var writer = new RecordingCollectorRowWriter();
        PgWraparoundStatsCollector.Instance.WritePayload(
            new PgWraparoundStatsCollector.Row("app", 500, 500, 0, 0, true),
            writer,
            MakeContext());

        Assert.Equal(0.0, (double)writer.Values[5]!);
        Assert.Equal(0.0, (double)writer.Values[7]!);
    }

    /// <summary>
    /// Remaining headroom counts down to the point where writes STOP, not to the raw 2^31 ceiling.
    /// <para>PostgreSQL refuses new write transactions with roughly <see cref="PgWraparoundStatsCollector.StopMargin"/>
    /// ids still unconsumed, so counting to the ceiling overstated the runway by that margin — and disagreed
    /// with the MCP tool's own 99.86%-of-space figure, which already accounts for it. This assertion changed
    /// with that fix and the change IS the fix.</para>
    /// </summary>
    [Fact]
    public void RemainingHeadroomCountsDownToWhereWritesStop()
    {
        var writer = new RecordingCollectorRowWriter();
        PgWraparoundStatsCollector.Instance.WritePayload(
            new PgWraparoundStatsCollector.Row("app", 1_000_000_000, 2_000_000_000, 200_000_000, 400_000_000, true),
            writer,
            MakeContext());

        var stopPoint = PgWraparoundStatsCollector.WraparoundCeiling - PgWraparoundStatsCollector.StopMargin;
        Assert.Equal(stopPoint - 1_000_000_000, writer.Values[9]);
        Assert.Equal(stopPoint - 2_000_000_000, writer.Values[10]);

        /* And the margin is real, not zero — otherwise this test would pass against the old behaviour. */
        Assert.True(PgWraparoundStatsCollector.StopMargin > 0);
    }

    /// <summary>
    /// Past the stop point, headroom clamps at 0 rather than going negative. A negative "ids remaining" is
    /// nonsense to render and worse to compare against a threshold.
    /// </summary>
    [Fact]
    public void RemainingHeadroomClampsAtZeroPastTheStopPoint()
    {
        var writer = new RecordingCollectorRowWriter();
        PgWraparoundStatsCollector.Instance.WritePayload(
            new PgWraparoundStatsCollector.Row("app", 2_147_000_000, 2_147_000_000, 200_000_000, 400_000_000, true),
            writer,
            MakeContext());

        Assert.Equal(0L, writer.Values[9]);
        Assert.Equal(0L, writer.Values[10]);
    }

    /// <summary>Age is a distance from a wall, not accumulated work, so no deltas are taken.</summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            ExcludedDatabases = Array.Empty<string>(),
        };

        PgWraparoundStatsCollector.Instance.WritePayload(
            new PgWraparoundStatsCollector.Row("app", 1, 1, 200_000_000, 400_000_000, true),
            new RecordingCollectorRowWriter(),
            context);

        Assert.Empty(deltas.Calls);
    }

    [Fact]
    public async Task ReadAsync_MapsEveryDatabaseRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { "app", 1_500_000_000L, 90_000_000L, 200_000_000L, 400_000_000L, true },
            new object[] { "rdsadmin", 12_345L, 0L, 200_000_000L, 400_000_000L, false });

        var rows = await PgWraparoundStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal("app", rows[0].DatabaseName);
        Assert.Equal(1_500_000_000L, rows[0].FrozenXidAge);
        Assert.True(rows[0].AllowsConnections);
        Assert.False(rows[1].AllowsConnections);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_wraparound_stats");

        var schedule = CollectorScheduleDefaults.All["pg_wraparound_stats"];
        Assert.Equal(5, schedule.FrequencyMinutes);
        Assert.Equal(90, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }
}
