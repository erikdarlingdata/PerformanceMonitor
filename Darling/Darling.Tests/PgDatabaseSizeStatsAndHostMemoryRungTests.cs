/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V136 (#3691): the two PostgreSQL-target series the analysis engine had no source for —
/// <c>collect.pg_database_size_stats</c>, the hourly per-database size series (a NEW table, generated from
/// <see cref="PgDatabaseSizeStatsCollector"/>), and the host's memory from Performance Insights as SIX
/// COLUMNS on <c>collect.pg_cpu_utilization</c> (not a <c>pg_host_memory</c> table — the rung doc and the CPU
/// collector's say why: same call, same minute, same watermark; and a 73rd hypertable would have pushed the
/// compression band past what the hour can pay). The shape is <see cref="PgNumbackendsAndSampledMsRungTests"/>'s
/// (V133) for the column half and the V129 log-events rung's for the table half.
///
/// <para>The "I am the top rung" claims this file carried when it landed (moved here off
/// <c>LongRunningQueryExclusionKnobRungTests</c>, V135) moved on again to <c>QsCaptureModeRouteKnobToastRungTests</c>
/// when V137 (#3796 / #3712 / #3783 — the Query Store capture modes, the route knob's store column and the plan
/// dimension's TOAST bytes) landed on top of it. What stays is everything true of this rung wherever it sits:
/// its name, its DDL, its probe sentinel at its own ordinal, and that a store which stopped here maps to
/// exactly 136.</para>
///
/// <para>The collector's own behaviour (the two privilege tests, templates collected, the NULL-total rule) is
/// <c>Lite.Tests.PgDatabaseSizeStatsCollectorDefinitionTests</c>; the ingestor's memory arithmetic and the
/// three-step fallback are <see cref="PgCpuCapacityHeadroomTests"/>. This file is the RUNG: the ladder, the DDL,
/// the probe, the catalog/dispatch/schedule wiring, the "nothing reads them yet" exit criterion, and the live
/// round trip.</para>
/// </summary>
public sealed class PgDatabaseSizeStatsAndHostMemoryRungTests
{
    private const int RungVersion = 136;
    private const int PreviousVersion = 135;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V137 appended its
    /// own — so this is a position within the signature rather than its end.</summary>
    private const int ProbeOrdinal = 111;

    private const string SizeTable = "pg_database_size_stats";
    private const string CpuTable = "pg_cpu_utilization";

    private static readonly string[] MemoryColumns =
    {
        "memory_total_bytes", "memory_free_bytes", "memory_cached_bytes", "memory_buffers_bytes", "memory_active_bytes",
        "configured_memory_bytes",
    };

    private static PgMigrations.Migration V136 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-database-size-and-host-memory", V136.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped being
           true when V137 landed. The invariant that outlives the handoff is that the LADDER's top and the
           declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung is one CREATE TABLE + one CREATE INDEX (the generator's, pinned identical by
    /// <c>PgSchemaGeneratorTests</c>) and ONE ALTER adding the six memory columns — nullable, no DEFAULT, no
    /// backfill, no view, nothing else. The six are rendered from the collector's declaration so a type here
    /// that differed from <c>PayloadColumns</c> would fail rather than ship two populations.
    /// </summary>
    [Fact]
    public void TheRungCreatesTheSizeTable_AddsSixNullableBigintColumnsToTheCpuTable_AndNothingElse()
    {
        var sql = V136.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"CREATE TABLE IF NOT EXISTS collect.{SizeTable} (", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE INDEX IF NOT EXISTS idx_{SizeTable}_time", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE TABLE"));
        Assert.Single(Regex.Matches(sql, "CREATE INDEX"));

        Assert.Contains($"ALTER TABLE collect.{CpuTable}\n", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "ALTER TABLE"));
        Assert.Equal(6, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);

        var declared = PgCpuUtilizationCollector.Instance.PayloadColumns.TakeLast(6).ToList();
        Assert.Equal(MemoryColumns, declared.Select(c => c.Name).ToArray());
        foreach (var column in declared)
        {
            var rendered = PgSchemaGenerator.TypeFor(column);
            Assert.Equal("bigint", rendered);
            Assert.Contains($"ADD COLUMN IF NOT EXISTS {column.Name} {rendered}", sql, StringComparison.Ordinal);
        }

        /* The ALTER comes AFTER the CREATE, so PgSchemaGeneratorTests' CreateHalf comparison holds the whole
           generated table and nothing of the ALTER. */
        Assert.True(sql.IndexOf("CREATE TABLE", StringComparison.Ordinal) < sql.IndexOf("ALTER TABLE", StringComparison.Ordinal));

        /* Neither table has a passthrough, so there is nothing to refresh — and a CREATE OR REPLACE VIEW here
           would CREATE one the generator does not know about. */
        Assert.DoesNotContain("VIEW", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain($"v_{SizeTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{CpuTable}", PgSchemaGenerator.AllPassthroughViews);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP ", sql, StringComparison.Ordinal);
        /* NOT NULL appears exactly four times: the generated prefix columns of the new table, never on a
           value column and never on the ALTER. */
        Assert.Equal(4, Regex.Matches(sql, "NOT NULL").Count);
        Assert.DoesNotContain("NOT NULL", sql[sql.IndexOf("ALTER TABLE", StringComparison.Ordinal)..], StringComparison.Ordinal);

        /* The V101 rule: V106's CREATE carries the six for the fresh population, LAST and in the same order, so
           the fresh store and the upgraded store agree with the positional COPY. */
        var v106 = PgMigrations.Scripts.Single(m => m.Version == 106).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains(
            "    max_configured_acu double precision,\n    memory_total_bytes bigint,\n    memory_free_bytes bigint,\n    memory_cached_bytes bigint,\n"
            + "    memory_buffers_bytes bigint,\n    memory_active_bytes bigint,\n    configured_memory_bytes bigint\n);",
            v106, StringComparison.Ordinal);
        AssertGeneratedTail(PgCpuUtilizationCollector.Instance, MemoryColumns, "max_configured_acu ");
        Assert.EndsWith($", {string.Join(", ", MemoryColumns)}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(PgCpuUtilizationCollector.Instance), StringComparison.Ordinal);

        /* The rung doc carries the argument in the words the next reader will look for. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V136 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V136Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the V136 rung doc is missing or sits after its constant");
        var doc = source[start..end];
        foreach (var phrase in new[]
        {
            "#3691", "pg_host_memory", "73rd hypertable", "896 s recorded ceiling", "scheduling decision", "72 / 3 = 24",
            "V101 rule", "Nullable, no DEFAULT, no backfill", "NULL where the role may not size", "a sum over the databases this role can see",
            "BYTES", "2 GiB per ACU", "no <c>pg_cpu_utilization</c> row at all", "<c>unavailable</c>", "Nothing reads",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        /* One table, one collector: the censuses moved by exactly one; the CPU payload grew by six. */
        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(71, CollectorCatalog.All.Count);
        Assert.Equal(5, PgDatabaseSizeStatsCollector.Instance.PayloadColumns.Count);
        Assert.Equal(11, PgCpuUtilizationCollector.Instance.PayloadColumns.Count);
    }

    private static void AssertGeneratedTail(ICollectorSchemaInfo definition, string[] columns, string expectedBefore)
    {
        var generated = PgSchemaGenerator.CreateTable(definition);
        var lines = generated.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, generated);
        for (var i = 0; i < columns.Length; i++)
        {
            Assert.Equal($"{columns[i]} bigint", lines[closing - columns.Length + i]);
        }

        Assert.StartsWith(expectedBefore, lines[closing - columns.Length - 1], StringComparison.Ordinal);
    }

    /// <summary>
    /// The new collector is wired everywhere a collector has to be: the catalog (which is what generates its
    /// table, whitelists it for analysis reads and makes it a hypertable), the worker's dispatch, the schedule
    /// (hourly, a year — the existing 365-day tier), and it gates on nothing (every PostgreSQL target). The
    /// memory series has NO collector, dispatch or schedule of its own, and that is pinned too: a
    /// <c>pg_host_memory</c> entry anywhere would be a second PI call or a table the grid cannot pay for.
    /// </summary>
    [Fact]
    public void TheSizeCollectorIsCatalogued_Dispatched_AndScheduledHourlyForAYear_AndMemoryHasNoCollectorOfItsOwn()
    {
        var size = Assert.Single(CollectorCatalog.All, c => c.Name == SizeTable);
        Assert.Same(PgDatabaseSizeStatsCollector.Instance, size);
        Assert.Equal(CollectorTargetEngine.PostgreSql, size.TargetEngine);
        Assert.Equal(SizeTable, size.TargetTable);
        Assert.Contains(SizeTable, DarlingWorker.DispatchedCollectorNames);

        var schedule = CollectorScheduleDefaults.All[SizeTable];
        Assert.Equal(60, schedule.FrequencyMinutes);
        Assert.Equal(365, schedule.RetentionDays);
        /* The existing year tier, not a new one — the README's horizon table lists distinct values. */
        Assert.Contains(CollectorScheduleDefaults.All.Where(kv => kv.Key != SizeTable), kv => kv.Value.RetentionDays == 365);

        Assert.True(CollectorCatalog.AppliesTo(size, new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 14, IsAurora = false, IsInRecovery = true }));
        Assert.True(CollectorCatalog.AppliesTo(size, new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = true }));
        Assert.False(CollectorCatalog.AppliesTo(size, new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer }));

        Assert.DoesNotContain(CollectorCatalog.All, c => c.Name == "pg_host_memory" || c.TargetTable == "pg_host_memory");
        Assert.DoesNotContain("pg_host_memory", DarlingWorker.DispatchedCollectorNames);
        Assert.False(CollectorScheduleDefaults.All.ContainsKey("pg_host_memory"));
        Assert.DoesNotContain("pg_host_memory", V136.Sql, StringComparison.Ordinal);

        /* The CPU collector's Aurora gate is the memory series' gate, unchanged: a stock target dispatches
           neither the collector nor, at runtime, the ingestor's AWS call (RdsCpuIngestorTests). */
        Assert.False(PgCpuUtilizationCollector.Instance.AppliesTo(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = false }));
        Assert.True(PgCpuUtilizationCollector.Instance.AppliesTo(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = true }));
    }

    [Fact]
    public void LiteStoresNeitherSeries_SoTheRungHasNoDuckDbTwin()
    {
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgDatabaseSizeStatsCollector.Instance.TargetEngine);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgCpuUtilizationCollector.Instance.TargetEngine);
    }

    /* ---- the probe (three sites) --------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and a store that stopped here maps to it. The
    /// probe asks the question, the caller reads the answer, the map has the parameter — a sentinel present at
    /// only some of them shifts every LATER ordinal onto the wrong column. The top-arm claims (last argument,
    /// returns the build's version) moved to <c>QsCaptureModeRouteKnobToastRungTests</c> with V137.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains(
            $"EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = '{SizeTable}')",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgDatabaseSizeStatsAndHostMemory", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V137 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this rung's own literal — not the
           build's version: the "returns StorageVersion.SchemaVersion" half of the top-arm claim moved to V137's
           test with the top. */
        var thisArm = viewer.IndexOf("if (hasPgDatabaseSizeStatsAndHostMemory)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasLrqExclusionKnob)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V136 sentinel arm — a store that stopped here would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V136 arm sits below the previous rung's, so a V136 store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose. */
        Assert.DoesNotContain(SizeTable, viewer[thisArm..previousArm], StringComparison.Ordinal);
        Assert.DoesNotContain(CpuTable, viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /* ---- nothing reads them yet -------------------------------------------------------------------- */

    /// <summary>
    /// The exit criterion's last clause, pinned so a consumer lane has to move this deliberately: no product read
    /// names the new table, and the six memory columns are read by EXACTLY the consumers that were promised. The CPU
    /// reader's two ALERT reads still select what they selected before V136, and no viewer reader names either.
    /// <b>Re-shaped deliberately for the memory half by lane 32 (#3691 §4b) into a positive census, and widened to a
    /// TWO-MEMBER roster by the third between-waves batch (#3809):</b> the readers of the six columns are
    /// <c>PgTargetFactCollector.Memory.cs</c> (its host read MUST name all six — the facts) and
    /// <c>DarlingPgCpuUtilizationReader.HistorySql</c> (MUST name all six — the served read behind
    /// <c>get_pg_cpu_utilization</c>, which the memory facts' tool rows point an operator at; until this batch that
    /// tool carried no memory, so the rows sent them to a read that could not answer). <c>DarlingMcpPgCpuUtilizationTools.cs</c>
    /// names them as the payload keys of that projection, and <c>PgTargetAdvice.Memory.cs</c> in PROSE (a string
    /// literal in an advice block is not a read). Every other file still may not name them: a third reader fails
    /// here until it is named deliberately.
    /// <b>The size-table half was re-shaped the same way by lane 38 (#3691,
    /// object growth):</b> the ONLY analysis readers of <c>pg_database_size_stats</c> are the growth family's three reads —
    /// the trend read in <c>PgTargetFactCollector.Growth.cs</c>, the baseline arm in <c>PgTargetBaselineProvider.Growth.cs</c>
    /// and the window read in <c>PgTargetAnomalyDetector.Growth.cs</c> — each of which MUST name the table; the
    /// retention floor in <c>DailySummaryHorizon.cs</c> names it as a collector name (a purge floor, not a read); and
    /// <c>PgTargetAdvice.Growth.cs</c> names it in prose. No MCP tool, viewer reader or web read names it yet —
    /// <c>ServerPageTabsTests.KnownUnreadable</c> and the <c>ViewerCollectorCoverageTests</c> allow-list entry are about
    /// SERVED and VIEWER reads and stand until one lands; an analysis fact is neither.
    /// </summary>
    [Fact]
    public void NoReaderNamesTheSizeTableOrTheMemoryColumnsYet()
    {
        /* The promised consumer landed: the memory family's host read names every one of the six. */
        foreach (var column in MemoryColumns)
        {
            Assert.Contains(column, PerformanceMonitor.Darling.Analysis.PgTargetFactCollector.PgTargetMemoryHostSql, StringComparison.Ordinal);
        }

        /* The size table's promised consumer landed too (lane 38): the growth family's three reads name it, and no other
           analysis read does. */
        Assert.Contains("FROM " + SizeTable, PerformanceMonitor.Darling.Analysis.PgTargetFactCollector.PgTargetDatabaseGrowthSql, StringComparison.Ordinal);
        Assert.Contains("FROM " + SizeTable, PerformanceMonitor.Darling.Analysis.PgTargetBaselineProvider.GetPgTargetBaselineQuery(PerformanceMonitor.Analysis.Baselines.MetricNames.PgDatabaseGrowthBytesPerDay)!, StringComparison.Ordinal);
        Assert.Contains("FROM " + SizeTable, PerformanceMonitor.Darling.Analysis.PgTargetAnomalyDetector.DatabaseGrowthWindowSql, StringComparison.Ordinal);

        /* The second promised consumer (#3809): the served read names every one of the six; the two alert-gate
           reads name none — the High CPU gate is a sub-second alert-path read with no memory question. */
        foreach (var column in MemoryColumns)
        {
            Assert.Contains(column, DarlingPgCpuUtilizationReader.HistorySql, StringComparison.Ordinal);
        }

        foreach (var sql in new[] { DarlingPgCpuUtilizationReader.LatestCpuSql, DarlingPgCpuUtilizationReader.SamplesSinceSql })
        {
            foreach (var column in MemoryColumns)
            {
                Assert.DoesNotContain(column, sql, StringComparison.Ordinal);
            }
        }

        foreach (var project in new[]
        {
            RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Analysis"),
            RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service"),
            RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Storage"),
            RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Viewer"),
            RepoFile.PathTo("PerformanceMonitor.Analysis"),
        })
        {
            foreach (var file in System.IO.Directory.EnumerateFiles(project, "*.cs", System.IO.SearchOption.AllDirectories)
                .Where(f => !f.Contains($"{System.IO.Path.DirectorySeparatorChar}obj{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                         && !f.Contains($"{System.IO.Path.DirectorySeparatorChar}bin{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal)))
            {
                /* CODE only: a doc comment that names the column to say it is NOT yet read (the v3 plumbing's
                   memory stub does exactly that) is the opposite of a consumer. Block and line comments are
                   stripped before the scan; SQL string constants are code and stay in. */
                var text = WithoutComments(System.IO.File.ReadAllText(file));
                var name = System.IO.Path.GetFileName(file);
                if (name is "PgMigrations.cs" or "ViewerDataService.cs" or "DarlingWorker.cs" or "RdsCpuIngestor.cs"
                    or "PgTargetToolRecommendations.cs" or "ViewerPostgresTabs.cs")
                {
                    /* The rung, its probe line, the worker's dispatch and the WRITER name them by construction; the
                       tool-recommendation registry names the column in the prose that tells an agent where the
                       figure WILL live (the v3 plumbing's memory family), and the viewer tab registry PLACES the
                       table on the Storage tab with a note that it is not yet drawn — pointers, not reads. */
                    continue;
                }

                /* Lane 38: the growth family's three reads ARE the readers of the size table (asserted positively above);
                   the retention floor names it as a collector name; the advice names it in prose. No other file is on this
                   list — and these five still may not name a memory column (checked below). */
                var sizeTableReader = name is "PgTargetFactCollector.Growth.cs" or "PgTargetBaselineProvider.Growth.cs" or "PgTargetAnomalyDetector.Growth.cs"
                    or "DailySummaryHorizon.cs" or "PgTargetAdvice.Growth.cs";
                if (!sizeTableReader)
                {
                    Assert.False(text.Contains(SizeTable, StringComparison.Ordinal), $"{file} names {SizeTable}: a second analysis reader of the size table landed — the only readers are the growth family's three (PgTargetFactCollector / PgTargetBaselineProvider / PgTargetAnomalyDetector .Growth.cs); name it here deliberately, or read the fact through get_analysis_facts instead. A SERVED read (an MCP tool) lowers ServerPageTabsTests.KnownUnreadable; a VIEWER read drops the ViewerCollectorCoverageTests allow-list entry");
                }
                if (name is "PgTargetFactCollector.Memory.cs" or "PgTargetAdvice.Memory.cs"
                    or "DarlingPgCpuUtilizationReader.cs" or "DarlingMcpPgCpuUtilizationTools.cs")
                {
                    /* Lane 32: the collector's host read is a reader of the six columns (asserted positively above);
                       the advice names them in prose — sentences, not a read. The third between-waves batch (#3809):
                       the CPU reader's served read is the second reader (asserted positively above) and the CPU tool
                       names them as that projection's payload keys. No other file is on this list. */
                    continue;
                }

                foreach (var column in MemoryColumns)
                {
                    Assert.False(text.Contains(column, StringComparison.Ordinal), $"{file} names {column}: a second reader of the memory columns landed — the only reader is PgTargetFactCollector.Memory.cs; name it here deliberately, or read the fact through get_analysis_facts instead");
                }
            }
        }
    }

    private static string WithoutComments(string source)
    {
        var noBlocks = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(noBlocks, @"^\s*//.*$", string.Empty, RegexOptions.Multiline);
    }
}

/// <summary>
/// The rung against a real PostgreSQL + TimescaleDB store (<c>DARLING_TEST_PG</c>): the table and the six columns
/// present after <c>MigrateAsync</c>, the six nullable, default-less and LAST; a simulated climb from 135 through
/// this rung applying EXACTLY one rung and then zero; then rows through each collector's real <c>WritePayload</c>
/// over a real binary COPY — <c>DarlingCollectorRunner</c>'s loop in shape — read back verbatim: a sized database,
/// a denied one (NULL size, NULL total on both), a CPU minute with every memory column and a CPU minute with none.
/// Serialized against every other live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class PgDatabaseSizeStatsAndHostMemoryLivePostgresTests
{
    private const int ServerId = -136136;
    private const string ServerName = "pg-v136-series-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheStoreClimbsToTheRung_AndBothCollectorsWriteTheirRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V136 round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* A store that stopped one rung short: the new table gone, the six columns gone, the stamp gone.
               MigrateAsync must apply this rung and put everything back — and since V137 (#3796 / #3712 / #3783,
               six nullable columns on three tables the store still has, every statement IF NOT EXISTS or
               CREATE OR REPLACE or DO-guarded) landed above it, the climb is every rung from this one to the
               ladder's top, counted against the ladder rather than as a literal (the V134 test's idiom); the
               exact single-rung climb belongs to the top rung's own test. The CPU table is a hypertable here
               (the fixture store converts them), so this is the ADD COLUMN the fleet will run. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP TABLE IF EXISTS collect.pg_database_size_stats");
            foreach (var column in new[] { "memory_total_bytes", "memory_free_bytes", "memory_cached_bytes", "memory_buffers_bytes", "memory_active_bytes", "configured_memory_bytes" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, $"ALTER TABLE collect.pg_cpu_utilization DROP COLUMN IF EXISTS {column}");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 136");
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 136), await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            using (var columns = new NpgsqlCommand(
                "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'pg_database_size_stats' ORDER BY ordinal_position", connection))
            {
                using var reader = await columns.ExecuteReaderAsync(ct);
                var rows = new List<(string, string, string, bool)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3)));
                }

                Assert.Equal(new (string, string, string, bool)[]
                {
                    ("collection_id", "bigint", "NO", true),
                    ("collection_time", "timestamp without time zone", "NO", true),
                    ("server_id", "integer", "NO", true),
                    ("server_name", "text", "NO", true),
                    ("database_name", "text", "YES", true),
                    ("size_bytes", "bigint", "YES", true),
                    ("total_bytes", "bigint", "YES", true),
                    ("is_template", "boolean", "YES", true),
                    ("allows_connections", "boolean", "YES", true),
                }, rows);
            }

            using (var columns = new NpgsqlCommand(
                "SELECT column_name, data_type, is_nullable, column_default IS NULL FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'pg_cpu_utilization' ORDER BY ordinal_position OFFSET 9", connection))
            {
                using var reader = await columns.ExecuteReaderAsync(ct);
                var rows = new List<(string, string, string, bool)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetBoolean(3)));
                }

                /* The six, LAST, after the nine V106 columns, each nullable with no default. */
                Assert.Equal(
                    new[] { "memory_total_bytes", "memory_free_bytes", "memory_cached_bytes", "memory_buffers_bytes", "memory_active_bytes", "configured_memory_bytes" },
                    rows.Select(r => r.Item1).ToArray());
                Assert.All(rows, r => { Assert.Equal("bigint", r.Item2); Assert.Equal("YES", r.Item3); Assert.True(r.Item4, $"{r.Item1} has a DEFAULT"); });
            }

            var t1 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);

            /* pg_database_size_stats through the REAL writer, the collector's own total rule applied first: a
               sized database and a denied one, so the total is NULL on both. */
            var sizeRows = PgDatabaseSizeStatsCollector.WithInstanceTotal(new List<PgDatabaseSizeStatsCollector.Row>
            {
                new("appdb", 9_663_676_416L, null, false, true),
                new("rdsadmin", null, null, false, true),
            });
            Assert.All(sizeRows, r => Assert.Null(r.TotalBytes));
            await WriteThroughTheCollectorAsync(connection, PgDatabaseSizeStatsCollector.Instance, t1, sizeRows, ct);

            /* And a complete instance, so the denormalized total is exercised too. */
            var t2 = t1.AddHours(1);
            await WriteThroughTheCollectorAsync(connection, PgDatabaseSizeStatsCollector.Instance, t2,
                PgDatabaseSizeStatsCollector.WithInstanceTotal(new List<PgDatabaseSizeStatsCollector.Row>
                {
                    new("appdb", 9_663_676_416L, null, false, true),
                    new("template0", 7_500_000L, null, true, false),
                }), ct);

            /* pg_cpu_utilization through the REAL writer: the ingestor's own arithmetic building one minute with
               every memory column (Serverless, 4 ACU) and one with none (PI had no memory sample). */
            var minute = DateTime.SpecifyKind(t1, DateTimeKind.Utc);
            var cpuRows = RdsCpuIngestor.BuildSamples(
                new[]
                {
                    Series("os.cpuUtilization.total.avg", (minute, 41.5), (minute.AddMinutes(1), 39.0)),
                    Series("os.general.serverlessDatabaseCapacity.avg", (minute, 4.0)),
                    Series("os.memory.total.avg", (minute, 16_000_000.0)),
                    Series("os.memory.free.avg", (minute, 1_000_000.0)),
                    Series("os.memory.cached.avg", (minute, 9_000_000.0)),
                    Series("os.memory.buffers.avg", (minute, 200_000.0)),
                    Series("os.memory.active.avg", (minute, 5_000_000.0)),
                },
                watermark: null);
            Assert.Equal(2, cpuRows.Count);
            await WriteThroughTheCollectorAsync(connection, PgCpuUtilizationCollector.Instance, t1, cpuRows, ct);

            using (var stored = new NpgsqlCommand(
                "SELECT collection_time, database_name, size_bytes, total_bytes, is_template, allows_connections FROM pg_database_size_stats WHERE server_id = $1 ORDER BY collection_time, database_name", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(DateTime, string, long?, long?, bool, bool)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetDateTime(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetInt64(2), reader.IsDBNull(3) ? null : reader.GetInt64(3), reader.GetBoolean(4), reader.GetBoolean(5)));
                }

                Assert.Equal(new (DateTime, string, long?, long?, bool, bool)[]
                {
                    (DarlingMcpTestData.Naive(t1), "appdb", 9_663_676_416L, null, false, true),
                    (DarlingMcpTestData.Naive(t1), "rdsadmin", null, null, false, true),
                    (DarlingMcpTestData.Naive(t2), "appdb", 9_663_676_416L, 9_663_676_416L + 7_500_000L, false, true),
                    (DarlingMcpTestData.Naive(t2), "template0", 7_500_000L, 9_663_676_416L + 7_500_000L, true, false),
                }, rows);
            }

            using (var stored = new NpgsqlCommand(
                "SELECT sample_time, cpu_percent, serverless_capacity_acu, memory_total_bytes, memory_free_bytes, memory_cached_bytes, memory_buffers_bytes, memory_active_bytes, configured_memory_bytes "
                + "FROM pg_cpu_utilization WHERE server_id = $1 ORDER BY sample_time", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<object?[]>();
                while (await reader.ReadAsync(ct))
                {
                    var values = new object?[reader.FieldCount];
                    for (var i = 0; i < values.Length; i++)
                    {
                        values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }

                    rows.Add(values);
                }

                Assert.Equal(2, rows.Count);
                Assert.Equal(new object?[] { DarlingMcpTestData.Naive(t1), 41.5, 4.0, 16_000_000L * 1024, 1_000_000L * 1024, 9_000_000L * 1024, 200_000L * 1024, 5_000_000L * 1024, 4L * PgCpuUtilizationCollector.BytesPerAcu }, rows[0]);
                Assert.Equal(new object?[] { DarlingMcpTestData.Naive(t1.AddMinutes(1)), 39.0, null, null, null, null, null, null, null }, rows[1]);
            }

            /* The existing CPU reads still work over rows carrying the six, and publish none of them. */
            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var latest = await DarlingPgCpuUtilizationReader.GetLatestAsync(postgres, ServerId, DateTime.UtcNow, ct);
                Assert.NotNull(latest);
                Assert.Equal(39.0, latest!.CpuPercent);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static Amazon.PI.Model.MetricKeyDataPoints Series(string metric, params (DateTime At, double Value)[] points) => new()
    {
        Key = new Amazon.PI.Model.ResponseResourceMetricKey { Metric = metric },
        DataPoints = points.Select(p => new Amazon.PI.Model.DataPoint { Timestamp = p.At, Value = p.Value }).ToList(),
    };

    /// <summary>DarlingCollectorRunner's COPY loop, verbatim in shape: prefix columns through the writer, then
    /// BeginPayload / WritePayload / EndPayload per row, so each collector's positional contract is exercised
    /// against the real, migrated table.</summary>
    private static async Task WriteThroughTheCollectorAsync<TRow>(NpgsqlConnection connection, ICollectorDefinition<TRow> definition, DateTime collectionTime,
        IReadOnlyList<TRow> rows, System.Threading.CancellationToken ct)
    {
        /* Neither collector differences at write time (both store levels), so no calculator is ever consulted;
           null! is the standing idiom for that. */
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = collectionTime, Deltas = null! };
        var writer = new PgCollectorRowWriter();

        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(DarlingMcpTestData.Naive(collectionTime)).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }

        await importer.CompleteAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "pg_database_size_stats", "pg_cpu_utilization" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            cleanup.Parameters.AddWithValue(ServerId);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
