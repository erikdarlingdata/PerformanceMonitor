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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V134 / #3653 item 13 (rulings Q7 + Q8) — the "time honesty" rung: <c>sample_time_utc</c> on
/// <c>collect.cpu_utilization_stats</c>, the same instant <c>sample_time</c> records in UTC, and
/// <c>time_zone_id</c> on <c>collect.server_properties</c>, the engine's own zone beside the offset V16 added.
/// Two lies retired: a server-LOCAL sample stamp that every UTC-window reader had to de-skew by a derived
/// offset (an hour wrong across a DST transition, silently), and an OFFSET that cannot say which side of a
/// transition an instant fell on where a ZONE can. The rung, the one passthrough refresh, the viewer probe's top
/// arm, the two windowed CPU readers that now prefer the stored UTC instant, the latest-row reads (three that
/// order on the local stamp as a within-batch tiebreak and never name the twin; the CPU alert gate's, which since
/// #3744 projects the twin as the gate's identity beside the local stamp it still orders on), and the
/// <c>get_server_properties</c> payload that publishes the clock pair.
///
/// <para>The "I am the top rung" claims this file carried when it landed (moved here off
/// <c>PgNumbackendsAndSampledMsRungTests</c>, V133) moved on again to <c>LongRunningQueryExclusionKnobRungTests</c>
/// when V135 (#3653 A5, Q5 — the Long-Running Query opt-out knob's store home) landed on top of it. What stays
/// is everything true of this rung wherever it sits: its name, its DDL, its probe sentinel at its own ordinal,
/// and that a store which stopped here maps to exactly 134.</para>
///
/// <para>The collectors' write shapes (the UTC twin projected LAST on every arm off <c>SYSUTCDATETIME()</c> with
/// <c>sample_time</c> unchanged; the zone read in its own version/edition-gated <c>sp_executesql</c> batch inside
/// TRY/CATCH, NULL pre-2022) are pinned value-by-value in <c>Lite.Tests/CpuUtilizationCollectorDefinitionTests</c>
/// and <c>Lite.Tests/ServerPropertiesCollectorDefinitionTests</c>, and the Lite ladder / reads in
/// <c>Lite.Tests/TimeHonestyRungTests</c> — all of which run off Windows. What is here is the PostgreSQL side.</para>
///
/// <para><b>#3778 moved the collector's WATERMARK onto the twin where the store has it.</b> The runner reads
/// <c>MAX(sample_time_utc)</c> and <c>MAX(sample_time)</c> in one statement for the one definition that declares a
/// <c>UtcWatermarkColumn</c>, hands the collector the twin's value with the frame stated where any row carries
/// one and the local maximum otherwise, and the ring-buffer dedup compares in that frame — so the autumn
/// fall-back's repeated local hour lands instead of being dropped as already collected. The SQL shapes and the
/// runner's branch are pinned in <see cref="TheRunner_ReadsTheWatermarkPair_OnlyForADefinitionWithAUtcTwin_AndBothSqlShapesArePinned"/>;
/// the read itself runs inside the live round trip below at the three moments that matter (pre-rung rows only,
/// after the writer has stored a twin, and after the post-rung rows are gone again); the dedup is pinned in
/// <c>Lite.Tests/CpuUtilizationCollectorDefinitionTests</c>.</para>
/// </summary>
public sealed class TimeHonestyRungTests
{
    private const int RungVersion = 134;
    private const int PreviousVersion = 133;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V135 appended its
    /// own — so this is a position within the signature rather than its end.</summary>
    private const int ProbeOrdinal = 109;

    private const string CpuTable = "cpu_utilization_stats";
    private const string CpuColumn = "sample_time_utc";
    private const string PropertiesTable = "server_properties";
    private const string PropertiesColumn = "time_zone_id";

    /// <summary>The projection both windowed CPU readers spell, identical modulo whitespace: the stored UTC
    /// instant first, the #1262 per-batch de-skew of the local stamp as the fallback, under the pre-rung alias
    /// so the reader's ordinal did not move. Compared against each SQL string with whitespace runs collapsed
    /// (see <see cref="BothWindowedCpuReads_PreferTheStoredUtcInstant_AndKeepTheDeSkewAsTheFallback"/>) since
    /// #4234 nested the viewer's copy one indent level deeper than the MCP twin's flat SELECT.</summary>
    private const string PreferredUtcProjection =
        "COALESCE( sample_time_utc, sample_time - INTERVAL '15 minutes' * ROUND(EXTRACT(EPOCH FROM ( " +
        "MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time " +
        ")) / 900.0)::double precision) AS sample_time,";

    private static PgMigrations.Migration V134 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("time-honesty", V134.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped being
           true when V135 landed. The invariant that outlives the handoff is that the LADDER's top and the
           declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// Two nullable, default-less <c>ADD COLUMN IF NOT EXISTS</c>, one per table, schema-qualified, in the type
    /// each collector declares (rendered by the generator's own mapping so this pin cannot disagree with
    /// <c>PgSchemaGeneratorTests</c> about what "the type" is); ONE view refresh, because the CPU table has a
    /// <c>v_</c> passthrough (V4) and <c>server_properties</c> does not (V16); no backfill, no index, no table, no
    /// data movement; and the rung doc carrying the argument.
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableColumnToEachTable_RefreshesTheOneView_AndNothingElse()
    {
        var sql = V134.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var declaredTwin = CpuUtilizationCollector.Instance.PayloadColumns[^1];
        Assert.Equal(CpuColumn, declaredTwin.Name);
        var renderedTwin = PgSchemaGenerator.TypeFor(declaredTwin);
        Assert.Equal("timestamp", renderedTwin);

        var declaredZone = ServerPropertiesCollector.Instance.PayloadColumns[^1];
        Assert.Equal(PropertiesColumn, declaredZone.Name);
        var renderedZone = PgSchemaGenerator.TypeFor(declaredZone);
        Assert.Equal("text", renderedZone);

        Assert.Contains($"ALTER TABLE collect.{CpuTable}\n    ADD COLUMN IF NOT EXISTS {CpuColumn} {renderedTwin};", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER TABLE collect.{PropertiesTable}\n    ADD COLUMN IF NOT EXISTS {PropertiesColumn} {renderedZone};", sql, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(sql, "ALTER TABLE").Count);
        Assert.Equal(2, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);

        /* The CPU passthrough is refreshed (the rung's SQL comment quotes the idiom's name, so the count is on
           the statement form); the properties table has none, and a CREATE here would invent one. */
        Assert.Contains($"CREATE OR REPLACE VIEW collect.v_{CpuTable} AS SELECT * FROM collect.{CpuTable};", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE OR REPLACE VIEW collect\\."));
        Assert.Contains($"v_{CpuTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{CpuTable}", PgSchemaGenerator.PayloadResolvingViews);
        Assert.DoesNotContain($"v_{PropertiesTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{PropertiesTable}", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP ", sql, StringComparison.Ordinal);

        /* A fresh store gets each column from the generated CREATE TABLE, LAST — the positional COPY writer and an
           upgraded store's ALTER agree on where it sits. */
        AssertGeneratedLast(CpuUtilizationCollector.Instance, CpuColumn, renderedTwin, "other_process_cpu_utilization ");
        AssertGeneratedLast(ServerPropertiesCollector.Instance, PropertiesColumn, renderedZone, "utc_offset_minutes ");

        /* The rung doc carries the argument in the words the next reader will look for. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V134 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V134Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the V134 rung doc is missing or sits after its constant");
        var doc = source[start..end];
        foreach (var phrase in new[]
        {
            "#3653 item 13", "Q7 and Q8", "time honesty", "LOCAL wall clock", "#1262", "DST transition",
            "utc_offset_minutes", "#3231", "AT TIME ZONE", "readers prefer the new column when present",
            "SYSUTCDATETIME()", "drs.end_time", "watermark", "COALESCE(sample_time_utc,", "observation identity",
            "east of UTC", "#3282", "CURRENT_TIMEZONE_ID()", "server_properties", "sp_executesql",
            "pre-2022 engine: only the offset is known", "Nullable, no DEFAULT, no backfill", "ONLY honest one",
            "several zones", "compressed hypertables", "One view refreshed, one not", "V14 lesson",
            "does NOT do", "TimeHonestyRungTests", "DuckDbInitializer",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        /* No table, no collector: the censuses did not move; each payload grew by exactly one. */
        /* 72 since V136 (#3691) added pg_database_size_stats; this rung itself added none. Restated as the current
           census figure rather than as "unchanged from before", which is the claim the line makes. */
        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(4, CpuUtilizationCollector.Instance.PayloadColumns.Count);
        Assert.Equal(23, ServerPropertiesCollector.Instance.PayloadColumns.Count);
    }

    private static void AssertGeneratedLast(ICollectorSchemaInfo definition, string column, string rendered, string expectedBefore)
    {
        var generated = PgSchemaGenerator.CreateTable(definition);
        var lines = generated.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, generated);
        Assert.Equal($"{column} {rendered}", lines[closing - 1]);
        Assert.StartsWith(expectedBefore, lines[closing - 2], StringComparison.Ordinal);
        Assert.EndsWith($", {column}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(definition), StringComparison.Ordinal);
    }

    /// <summary>The Lite twin is schema v63, and its ladder block carries both columns — a cross-SKU pin read off
    /// the Lite source, because this assembly cannot reference the WPF-hosted Lite project. "Is v63" became "is
    /// AT LEAST v63" when v64 (#3796 / Darling V137, the Query Store capture modes) landed on top of it: the
    /// invariant that outlives the handoff is that the v63 block exists and carries its two columns, and the
    /// current-version literal is the newest Lite rung's to pin (<c>QsCaptureModeRouteKnobToastRungTests</c>).</summary>
    [Fact]
    public void TheLiteTwinIsSchemaV63_AndCarriesBothColumns()
    {
        Assert.Equal(CollectorTargetEngine.SqlServer, CpuUtilizationCollector.Instance.TargetEngine);
        Assert.Equal(CollectorTargetEngine.SqlServer, ServerPropertiesCollector.Instance.TargetEngine);

        var lite = RepoFile.ReadRepoFile("Lite", "Database", "DuckDbInitializer.cs");
        var declaration = System.Text.RegularExpressions.Regex.Match(lite, @"internal const int CurrentSchemaVersion = (\d+);");
        Assert.True(declaration.Success, "DuckDbInitializer no longer declares CurrentSchemaVersion in the pinned shape");
        Assert.True(int.Parse(declaration.Groups[1].Value, CultureInfo.InvariantCulture) >= 63, "Lite's schema version fell below the v63 twin");
        var block = lite[lite.IndexOf("if (fromVersion < 63)", StringComparison.Ordinal)..];
        Assert.Contains("(\"cpu_utilization_stats\", \"sample_time_utc\", \"TIMESTAMP\")", block, StringComparison.Ordinal);
        Assert.Contains("(\"server_properties\", \"time_zone_id\", \"VARCHAR\")", block, StringComparison.Ordinal);
        Assert.Contains("twinning Darling's V134", block, StringComparison.Ordinal);
    }

    /* ---- the probe (three sites) --------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and a store that stopped here maps to it. The
    /// probe asks the question, the caller reads the answer, the map has the parameter — a sentinel present at
    /// only some of them shifts every LATER ordinal onto the wrong column. The top-arm claims (last argument,
    /// returns the build's version) moved to <c>LongRunningQueryExclusionKnobRungTests</c> with V135.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains(
            $"table_name = '{CpuTable}'\n                                                     AND   column_name = '{CpuColumn}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasTimeHonesty", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V135 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this rung's own literal — not the
           build's version: the "returns StorageVersion.SchemaVersion" half of the top-arm claim moved to V135's
           test with the top. */
        var thisArm = viewer.IndexOf("if (hasTimeHonesty)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf(PreviousArmSource, StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V134 sentinel arm — a store that stopped here would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V134 arm sits below the previous rung's, so a V134 store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the tables are named in the probe line and nowhere in the arm's prose. */
        Assert.DoesNotContain(CpuTable, viewer[thisArm..previousArm], StringComparison.Ordinal);
        Assert.DoesNotContain(PropertiesTable, viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>The previous rung's arm as the viewer spells it — V133's sentinel.</summary>
    private const string PreviousArmSource = "if (hasPgNumbackendsAndSampledMs)";

    /* ---- the readers ----------------------------------------------------------------------------------- */

    /// <summary>
    /// The two windowed per-sample CPU reads — the viewer's and the MCP <c>get_cpu_utilization</c>'s — prefer the
    /// stored UTC instant and fall back to the #1262 de-skew, structurally identical to each other (whitespace
    /// aside) under the pre-rung alias; both still window on <c>collection_time</c>. The de-skew text the
    /// consumed-frame census cites as this read's evidence is still there, inside the fallback.
    /// <para>#4234 wrapped the viewer's copy in a <c>raw</c> CTE (an outer query buckets it), nesting the
    /// projection one indent level deeper than the MCP twin's still-flat SELECT — so the comparison collapses
    /// whitespace runs instead of pinning exact column positions; a real wording drift still fails it.</para>
    /// </summary>
    [Fact]
    public void BothWindowedCpuReads_PreferTheStoredUtcInstant_AndKeepTheDeSkewAsTheFallback()
    {
        var viewer = ViewerDataService.CpuUtilizationSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var mcp = DarlingDataReader.CpuUtilizationSql.Replace("\r\n", "\n", StringComparison.Ordinal);

        foreach (var sql in new[] { viewer, mcp })
        {
            Assert.Contains(PreferredUtcProjection, Regex.Replace(sql, @"\s+", " "), StringComparison.Ordinal);
            Assert.Contains("MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time", sql, StringComparison.Ordinal);
            Assert.Contains("FROM cpu_utilization_stats", sql, StringComparison.Ordinal);
            Assert.Contains("AND   collection_time >= $2", sql, StringComparison.Ordinal);
            Assert.Single(Regex.Matches(sql, "sample_time_utc"));
            Assert.DoesNotContain("sample_time_utc >=", sql, StringComparison.Ordinal);   /* the window stays on collection_time */
        }

        /* The MCP twin is still the flat, unbucketed per-sample read (#3960's separate CPU-BUCKETED sibling
           wraps it for its own bucketed tool), so it still orders the raw rows by sample_time directly. */
        Assert.Contains("ORDER BY sample_time", mcp, StringComparison.Ordinal);

        /* #4234: the viewer's copy now buckets, so the outer query orders the bucket rows by the bucket_start
           ordinal instead of the (no longer projected at top level) sample_time column. */
        Assert.Contains("ORDER BY 1", viewer, StringComparison.Ordinal);

        /* The viewer's read still has no upper TIME bound (since $2 to the caller's endUtc, start-only
           server-side); the MCP's has both $2 and $3 as time bounds. #4234 gave the viewer a $3 too, but for
           a different reason — the bucket width in minutes, not a time bound — so it's asserted present here
           rather than absent. */
        Assert.DoesNotContain("collection_time <=", viewer, StringComparison.Ordinal);
        Assert.Contains("CAST($3 AS integer) * INTERVAL '1 minute'", viewer, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time <= $3", mcp, StringComparison.Ordinal);
    }

    /// <summary>
    /// The latest-row reads all keep the LOCAL <c>sample_time</c> as their within-batch tiebreak, and the three
    /// display/fleet reads never name the twin at all. The CPU alert gate's read is the exception since #3744: it
    /// PROJECTS <c>sample_time_utc</c> beside the local stamp (the twin is the gate's observation identity where
    /// the row has one, the local stamp where it does not) but still does not ORDER on it, because a
    /// cross-batch order on the twin, or on a <c>COALESCE</c> of the two, would compare a pre-rung local stamp
    /// against a post-rung UTC one and sort a stale row as newest east of UTC. #3730 pinned this read to the
    /// local stamp because the gate compared identities by <c>&gt;</c> and a one-time frame change would have
    /// frozen it for one offset's worth of hours; #3744 moved the gate to equality and this pin flipped with it.
    /// The worker's doc says all of that in the words a reader will look for.
    /// </summary>
    [Fact]
    public void TheLatestRowReads_KeepTheLocalTiebreak_TheGatesReadProjectsTheTwin_AndTheWorkerSaysWhy()
    {
        foreach (var sql in new[]
        {
            DarlingHealthReader.ServerSummaryCpuSql, DarlingFleetReader.FleetCpuSql, ViewerDataService.ServerSummaryCpuSql,
        })
        {
            Assert.DoesNotContain("sample_time_utc", sql, StringComparison.Ordinal);
            Assert.Contains("ORDER BY", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time DESC, sample_time DESC", sql, StringComparison.Ordinal);
        }

        /* The gate's read: both stamps projected, local first (ordinal 2) and the twin last (ordinal 3), so the
           pre-#3744 ordinals still read what they always read; the twin named exactly once, in the SELECT list
           and nowhere else — not in the ORDER BY, not in a predicate. */
        var gate = DarlingWorker.LatestCpuSql;
        Assert.Contains("SELECT sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time, sample_time_utc", gate, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC, sample_time DESC", gate, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(gate, "sample_time_utc"));
        Assert.DoesNotContain("COALESCE", gate, StringComparison.Ordinal);

        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var doc = worker[..worker.IndexOf("internal const string LatestCpuSql", StringComparison.Ordinal)];
        doc = doc[doc.LastIndexOf("/// <summary>", StringComparison.Ordinal)..];
        foreach (var phrase in new[]
        {
            "the UTC twin is the identity where the row has one (#3744", "OBSERVATION IDENTITY", "#3282", "EAST of UTC",
            "frozen", "fall-back", "EQUALITY", "The ORDER BY stays on the local stamp", "TimeHonestyRungTests",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("Not <c>sample_time_utc</c>, deliberately", doc, StringComparison.Ordinal);

        /* And the reader folds the pair the documented way — twin first, local as the fallback — rather than
           reading only one of them. Source pin because the read is private and needs a live store. #3854 split
           it into an expression-bodied ReadLatestCpuAsync forwarder over the retry seam and a
           ReadLatestCpuCoreAsync sibling holding the read byte-identical — the columns and the fold this pin
           cares about live in the Core body, so that is the declaration it slices. */
        var reader = CSharpSourceWalker.StripCommentsAndStrings(worker);
        var head = reader.IndexOf("ReadLatestCpuCoreAsync(int serverId", StringComparison.Ordinal);
        Assert.True(head >= 0, "DarlingWorker has no ReadLatestCpuCoreAsync");
        var body = reader.Substring(head, Math.Min(2500, reader.Length - head));
        Assert.Contains("reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3)", body, StringComparison.Ordinal);
        Assert.Contains("reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2)", body, StringComparison.Ordinal);
        Assert.Contains("sampleTime = sampleTimeUtc ?? sampleTimeLocal;", body, StringComparison.Ordinal);
    }

    /// <summary>The properties read projects the clock pair after the pre-rung columns, the row type carries them
    /// nullable, the reader reads them null-or-value, and <c>get_server_properties</c> publishes both with the note.</summary>
    [Fact]
    public void TheServerPropertiesRead_CarriesTheClockPair_AndThePayloadPublishesIt()
    {
        var sql = DarlingDataReader.LatestServerPropertiesSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("service_objective,\n    utc_offset_minutes,\n    time_zone_id\nFROM server_properties", Dedent(sql), StringComparison.Ordinal);

        Assert.Equal(typeof(int?), typeof(DarlingDataReader.ServerPropertiesReadRow).GetProperty("UtcOffsetMinutes")!.PropertyType);
        Assert.Equal(typeof(string), typeof(DarlingDataReader.ServerPropertiesReadRow).GetProperty("TimeZoneId")!.PropertyType);

        var reader = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs"));
        var head = reader.IndexOf("GetLatestServerPropertiesAsync(", StringComparison.Ordinal);
        Assert.True(head >= 0);
        var body = CSharpSourceWalker.BraceBalanced(reader, reader.IndexOf('{', head));
        Assert.Contains("reader.IsDBNull(15) ? null : reader.GetInt32(15)", body, StringComparison.Ordinal);
        Assert.Contains("reader.IsDBNull(16) ? null : reader.GetString(16)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("reader.IsDBNull(15) ? 0", body, StringComparison.Ordinal);

        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        foreach (var phrase in new[]
        {
            "utc_offset_minutes = row.UtcOffsetMinutes,",
            "time_zone_id = string.IsNullOrEmpty(row.TimeZoneId) ? null : row.TimeZoneId,",
            "time_zone_id is null: a pre-2022 engine (CURRENT_TIMEZONE_ID() is SQL Server 2022+ / Azure SQL only), so only the offset in force at captured_at is known.",
            "a null time_zone_id means a pre-2022 engine, where only the offset is known",
        })
        {
            Assert.Contains(phrase, tool, StringComparison.Ordinal);
        }
    }

    /* ---- the watermark (#3778) ------------------------------------------------------------------------- */

    /// <summary>
    /// The runner's watermark branch routes a definition with NO <c>UtcWatermarkColumn</c> through the unchanged
    /// <c>GetLastCollectedTimeAsync</c>, whose SQL is the byte-identical string it was in both its bounded and
    /// unbounded forms — so every collector but <c>cpu_utilization</c> reads exactly what it read — and only a
    /// definition WITH one through <c>GetLastCollectedTimeWithFrameAsync</c>, whose SQL puts the twin's maximum
    /// first and the declared column's second in ONE statement over ONE scan, with the same <c>server_id</c>
    /// predicate and the same optional partitioning-column bound. The frame rides onto the context as
    /// <c>WatermarkFromUtcColumn</c>. Both SQL builders are asserted as the SHIPPED strings (the #2796 discipline);
    /// the branch is a source pin because it sits inside <c>RunAsync</c>, which needs a live monitored server.
    /// The read runs against a real store in <see cref="TimeHonestyRungLivePostgresTests"/>.
    /// </summary>
    [Fact]
    public void TheRunner_ReadsTheWatermarkPair_OnlyForADefinitionWithAUtcTwin_AndBothSqlShapesArePinned()
    {
        Assert.Equal("sample_time", CpuUtilizationCollector.Instance.WatermarkColumn);
        Assert.Equal(CpuColumn, CpuUtilizationCollector.Instance.UtcWatermarkColumn);

        /* The plain read, unchanged: what every other watermarked definition still runs. */
        Assert.Equal(
            $"SELECT MAX(sample_time) FROM {CpuTable} WHERE server_id = $1",
            DarlingCollectorRunner.BuildServerWatermarkSql(CpuTable, "sample_time", bounded: false));
        Assert.Equal(
            $"SELECT MAX(sample_time) FROM {CpuTable} WHERE server_id = $1 AND collection_time > $2",
            DarlingCollectorRunner.BuildServerWatermarkSql(CpuTable, "sample_time", bounded: true));

        /* The pair read: twin first, declared column second, one statement. */
        Assert.Equal(
            $"SELECT MAX({CpuColumn}), MAX(sample_time) FROM {CpuTable} WHERE server_id = $1",
            DarlingCollectorRunner.BuildServerWatermarkPairSql(CpuTable, "sample_time", CpuColumn, bounded: false));
        Assert.Equal(
            $"SELECT MAX({CpuColumn}), MAX(sample_time) FROM {CpuTable} WHERE server_id = $1 AND collection_time > $2",
            DarlingCollectorRunner.BuildServerWatermarkPairSql(CpuTable, "sample_time", CpuColumn, bounded: true));

        var runner = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        /* #4197 part b moved this block one level deeper, into ResolveServerWatermarkAsync's own try, so
           the indentation grew by one level (4 spaces) versus its old home inline in RunCoreAsync. */
        Assert.Contains(
            "                else if (definition.UtcWatermarkColumn is null)\n" +
            "                {\n" +
            "                    watermark = await GetLastCollectedTimeAsync(server.ServerId, definition.TargetTable, definition.WatermarkColumn, cancellationToken, serverReadFloor);\n" +
            "                }",
            runner, StringComparison.Ordinal);
        Assert.Contains(
            "                    (watermark, watermarkFromUtcColumn) = await GetLastCollectedTimeWithFrameAsync(\n" +
            "                        server.ServerId, definition.TargetTable, definition.WatermarkColumn, definition.UtcWatermarkColumn,\n" +
            "                        cancellationToken, serverReadFloor);",
            runner, StringComparison.Ordinal);
        Assert.Contains("            WatermarkFromUtcColumn = watermarkFromUtcColumn,", runner, StringComparison.Ordinal);

        /* One call site for the pair read (the server-scoped read, behind the #2797 gate —
           DarlingCollectorRunnerTests' IL pin holds the gate half), and the pair read is its own method rather
           than a parameter on the plain one, so the plain one's body is untouched. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(runner);
        Assert.Single(Regex.Matches(code, @"await GetLastCollectedTimeWithFrameAsync\("));
        var plain = typeof(DarlingCollectorRunner).GetMethod(nameof(DarlingCollectorRunner.GetLastCollectedTimeAsync))!;
        Assert.Equal(typeof(Task<DateTime?>), plain.ReturnType);
        Assert.Equal(new[] { "serverId", "tableName", "columnName", "cancellationToken", "collectedSince" }, plain.GetParameters().Select(p => p.Name).ToArray());
        var pair = typeof(DarlingCollectorRunner).GetMethod(nameof(DarlingCollectorRunner.GetLastCollectedTimeWithFrameAsync))!;
        Assert.Equal(typeof(Task<(DateTime? Value, bool FromUtcColumn)>), pair.ReturnType);
        Assert.Equal(new[] { "serverId", "tableName", "columnName", "utcColumnName", "cancellationToken", "collectedSince" }, pair.GetParameters().Select(p => p.Name).ToArray());
    }

    /// <summary>Raw-string SQL constants dedent to their closing quotes' indentation; the pins above are spelled at
    /// the dedented text, so a four-space shift in either file's literal is normalised away here.</summary>
    private static string Dedent(string sql)
    {
        var lines = sql.Split('\n');
        var indent = lines.Where(l => l.Trim().Length > 0).Min(l => l.Length - l.TrimStart().Length);
        return string.Join("\n", lines.Select(l => l.Length >= indent ? l[indent..] : l.TrimStart()));
    }
}

/// <summary>
/// The rung against a real PostgreSQL + TimescaleDB store (<c>DARLING_TEST_PG</c>): both columns present, nullable,
/// default-less and LAST-of-their-tables after <c>MigrateAsync</c>, the refreshed view serving the twin; a simulated
/// 133 → top climb applying this rung and the one above it (V135) in order; then rows through each collector's real <c>WritePayload</c> over a real binary COPY
/// — <c>DarlingCollectorRunner</c>'s loop in shape — read back by the viewer's CPU read, the MCP CPU read, the
/// worker's latest-row read and <c>get_server_properties</c>. The planted rows are the DST shape the rung exists
/// for: a batch whose local stamp lies on the far side of a transition from what the de-skew recovers. Serialized
/// against every other live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class TimeHonestyRungLivePostgresTests
{
    private const int ServerId = -134134;
    private const string ServerName = "time-honesty-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheStoreClimbsToTheRung_TheCollectorsWriteBothColumns_AndTheReadersPreferThem_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V134 round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* A store that stopped one rung short OF THIS ONE: both columns gone (the view has to go first — Postgres
               will not drop a column a view selects — and the rung must put it back), the stamp gone. The applier
               ascends with `version <= MAX(version) ? skip`, so everything ABOVE this rung has to go too or this
               rung is skipped as already-passed — the exact hazard MigrationLadderPins.TheLadder_IsDenseAboveTheHistoricalGap
               guards at authoring time. Since V135 (#3653 A5, Q5 — the Long-Running Query opt-out knob's two
               config_alert_settings columns) landed on top, that is V135's two columns and its stamp as well, and
               MigrateAsync must apply EXACTLY the two rungs, this one first, and put everything back — and since
               V136 (#3691, two new PostgreSQL collector tables, CREATE TABLE IF NOT EXISTS over tables the store
               still has) landed above THAT, the climb is every rung from this one to the ladder's top, counted
               against the ladder rather than as a literal (the V133 test's idiom); the exact single-rung climb
               belongs to the top rung's own test. Both tables are hypertables here (the fixture store converts
               them), so this is the ADD COLUMN the fleet will run. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP VIEW IF EXISTS collect.v_cpu_utilization_stats");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.cpu_utilization_stats DROP COLUMN IF EXISTS sample_time_utc");
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE VIEW collect.v_cpu_utilization_stats AS SELECT * FROM collect.cpu_utilization_stats");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.server_properties DROP COLUMN IF EXISTS time_zone_id");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE config.config_alert_settings DROP COLUMN IF EXISTS long_running_query_excluded_program_name_prefixes");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE config.config_alert_settings DROP COLUMN IF EXISTS long_running_query_excluded_logins");
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 134");
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 134), await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            using (var stamped = new NpgsqlCommand("SELECT COUNT(*) FROM darling_schema_version WHERE version IN (134, 135)", connection))
            {
                Assert.Equal(2L, await stamped.ExecuteScalarAsync(ct));
            }

            await AssertColumnAsync(connection, ct, "cpu_utilization_stats", "sample_time_utc", "timestamp without time zone");
            await AssertColumnAsync(connection, ct, "server_properties", "time_zone_id", "text");

            /* The refreshed passthrough serves the twin — the V14 lesson, checked rather than assumed. */
            using (var view = new NpgsqlCommand("SELECT sample_time_utc FROM collect.v_cpu_utilization_stats LIMIT 0", connection))
            using (var reader = await view.ExecuteReaderAsync(ct))
            {
                Assert.Equal("sample_time_utc", reader.GetName(0));
            }

            /* The server sits in America/New_York. Batch 1 (pre-rung shape, planted directly, twin NULL) was
               polled under EDT (UTC-4). Batch 2 goes through the REAL writer with the twin and is the DST
               shape: a poll that straddles the fall-back, one sample on the EDT side (local = utc - 4 h) and
               the newer one on the EST side (local = utc - 5 h). Because the clock fell back, the OLDER sample
               carries the LATER local stamp, so the de-skew's MAX(sample_time) recovers -4 h for the whole batch
               and places the newer, EST-side sample an hour EARLY. Only the twin knows; the readers take it. */
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var batch1 = now.AddMinutes(-20);
            var batch2 = now.AddMinutes(-5);

            /* Batch 1: two pre-rung rows, local = utc - 4 h, twin NULL; the de-skew recovers exactly -4 h. */
            var pre1Utc = batch1.AddSeconds(-90);
            var pre2Utc = batch1.AddSeconds(-30);
            await PlantPreRungAsync(connection, ct, batch1, pre1Utc.AddHours(-4), 21);
            await PlantPreRungAsync(connection, ct, batch1, pre2Utc.AddHours(-4), 22);

            /* #3778, moment one — the morning of the upgrade: no row carries a twin, so the runner's pair read hands
               back the LOCAL maximum and says so, which is exactly what the plain read hands back; the first
               post-upgrade run therefore dedups local-to-local, as it always did. */
            await using var runnerSource = NpgsqlDataSource.Create(cs!);
            var runner = new DarlingCollectorRunner(runnerSource, new CollectorDeltaCalculator());
            Assert.Equal(((DateTime?)pre2Utc.AddHours(-4), false), await runner.GetLastCollectedTimeWithFrameAsync(ServerId, "cpu_utilization_stats", "sample_time", "sample_time_utc", ct));
            Assert.Equal(pre2Utc.AddHours(-4), await runner.GetLastCollectedTimeAsync(ServerId, "cpu_utilization_stats", "sample_time", ct));

            /* Batch 2 through CpuUtilizationCollector.WritePayload over the binary COPY. */
            var newestUtc = batch2.AddSeconds(-30);          /* on the EST side: local = utc - 5 h */
            var straddleUtc = batch2.AddSeconds(-90);        /* on the EDT side: local = utc - 4 h */
            await WriteCpuThroughTheCollectorAsync(connection, batch2, new[]
            {
                new CpuUtilizationCollector.Row(straddleUtc.AddHours(-4), 31, 3, straddleUtc),
                new CpuUtilizationCollector.Row(newestUtc.AddHours(-5), 32, 4, newestUtc),
            }, ct);

            /* What the writer stored: the twin beside the untouched local stamp, nullable where absent. */
            using (var stored = new NpgsqlCommand(
                "SELECT sqlserver_cpu_utilization, sample_time, sample_time_utc FROM cpu_utilization_stats WHERE server_id = $1 ORDER BY sqlserver_cpu_utilization", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(int Cpu, DateTime Local, DateTime? Utc)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetInt32(0), reader.GetDateTime(1), reader.IsDBNull(2) ? null : reader.GetDateTime(2)));
                }

                Assert.Equal(new (int, DateTime, DateTime?)[]
                {
                    (21, pre1Utc.AddHours(-4), null),
                    (22, pre2Utc.AddHours(-4), null),
                    (31, straddleUtc.AddHours(-4), straddleUtc),
                    (32, newestUtc.AddHours(-5), newestUtc),
                }, rows);
            }

            /* #3778, moment two — after the first post-upgrade run has stored rows with the twin: the pair read hands
               back the newest INSTANT (cpu 32's twin) in the UTC frame. This batch is the fall-back shape, so the
               plain read's answer is a DIFFERENT row: by local stamp the EDT-side sample (cpu 31) is fifty-nine
               minutes "newer" than the EST-side one, and under the old rule that local maximum was the watermark
               every EST-side sample for the next hour sat below. The frame is what tells the two apart. */
            Assert.Equal(((DateTime?)newestUtc, true), await runner.GetLastCollectedTimeWithFrameAsync(ServerId, "cpu_utilization_stats", "sample_time", "sample_time_utc", ct));
            Assert.Equal(straddleUtc.AddHours(-4), await runner.GetLastCollectedTimeAsync(ServerId, "cpu_utilization_stats", "sample_time", ct));
            Assert.True(straddleUtc.AddHours(-4) > newestUtc.AddHours(-5), "the fixture must put the older sample's LOCAL stamp above the newer one's, or the two reads agree by accident");

            /* The MCP read: pre-rung rows de-skewed (exactly, the batch was one offset), post-rung rows by their
               twin — including the EST-side sample, which the de-skew alone would have placed an hour early. */
            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var samples = await DarlingDataReader.GetCpuUtilizationAsync(postgres, ServerId, now.AddHours(-1), now, ct);
                Assert.Equal(new[] { (pre1Utc, 21), (pre2Utc, 22), (straddleUtc, 31), (newestUtc, 32) },
                    samples.Select(s => (s.SampleTime, s.SqlServerCpu)).ToArray());

                /* Control: what the fallback alone says about the EST-side sample — an hour early — so the
                   preference above is shown to matter rather than to coincide. The window function needs the
                   whole batch, so the filter sits outside it. */
                using (var deskewOnly = new NpgsqlCommand(@"
SELECT deskewed FROM (
    SELECT sqlserver_cpu_utilization,
           sample_time - INTERVAL '15 minutes' * ROUND(EXTRACT(EPOCH FROM (MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time)) / 900.0)::double precision AS deskewed
    FROM cpu_utilization_stats WHERE server_id = $1
) AS d WHERE sqlserver_cpu_utilization = 32", connection))
                {
                    deskewOnly.Parameters.AddWithValue(ServerId);
                    Assert.Equal(newestUtc.AddHours(-1), (DateTime)(await deskewOnly.ExecuteScalarAsync(ct))!);
                }

                /* The worker's latest-row read projects BOTH stamps (#3744): the local stamp at ordinal 2, where
                   it always was, and the UTC twin at ordinal 3, which is what the gate now takes as this row's
                   identity. It also still shows the one-batch fall-back quirk the tiebreak leaves: ordering on the
                   local stamp, inside the single batch that straddles the fall-back the EDT-side sample (cpu 31,
                   sixty seconds OLDER in UTC) sorts as the newest — a reading stale by one sample, once a year.
                   Before #3744 that identity was also one the next batch's EST stamps would not EXCEED for an
                   hour, and the gate compared by >, so it froze; the gate now compares for equality, and the next
                   batch's twins simply differ from this one. The ORDER BY cannot move to the twin without
                   comparing a pre-rung local stamp against a post-rung UTC one across batches — the worker's doc
                   says why — so the one-sample staleness inside the straddling batch is the accepted residue. */
                using (var latest = new NpgsqlCommand(DarlingWorker.LatestCpuSql, connection))
                {
                    latest.Parameters.AddWithValue(ServerId);
                    using var reader = await latest.ExecuteReaderAsync(ct);
                    Assert.True(await reader.ReadAsync(ct));
                    Assert.Equal(4, reader.FieldCount);
                    Assert.Equal(31, reader.GetInt32(0));
                    Assert.Equal(straddleUtc.AddHours(-4), reader.GetDateTime(2));
                    Assert.Equal("sample_time_utc", reader.GetName(3));
                    Assert.Equal(straddleUtc, reader.GetDateTime(3));
                }


                /* server_properties through ServerPropertiesCollector.WritePayload: a 2022+ row with the zone and
                   an older-engine row with NULL, the offset on both. */
                var older = now.AddMinutes(-10);
                await WritePropertiesThroughTheCollectorAsync(connection, older, PropertiesRow(-240, null), ct);
                await WritePropertiesThroughTheCollectorAsync(connection, now.AddMinutes(-1), PropertiesRow(-300, "Eastern Standard Time"), ct);

                var props = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, ServerId, ct);
                Assert.NotNull(props);
                Assert.Equal(-300, props!.UtcOffsetMinutes);
                Assert.Equal("Eastern Standard Time", props.TimeZoneId);

                using (var payload = JsonDocument.Parse(await DarlingMcpDataTools.GetServerProperties(postgres, ServerName)))
                {
                    Assert.Equal(-300, payload.RootElement.GetProperty("utc_offset_minutes").GetInt32());
                    Assert.Equal("Eastern Standard Time", payload.RootElement.GetProperty("time_zone_id").GetString());
                    Assert.Contains("engine's own zone", payload.RootElement.GetProperty("time_zone_note").GetString(), StringComparison.Ordinal);
                }

                /* The older-engine snapshot, read as the latest once the newer one is gone: NULL zone, offset kept,
                   and the note says what NULL means. */
                await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM server_properties WHERE server_id = $1 AND time_zone_id IS NOT NULL", ServerId);
                using (var payload = JsonDocument.Parse(await DarlingMcpDataTools.GetServerProperties(postgres, ServerName)))
                {
                    Assert.Equal(-240, payload.RootElement.GetProperty("utc_offset_minutes").GetInt32());
                    Assert.Equal(JsonValueKind.Null, payload.RootElement.GetProperty("time_zone_id").ValueKind);
                    Assert.Contains("pre-2022 engine", payload.RootElement.GetProperty("time_zone_note").GetString(), StringComparison.Ordinal);
                }
            }

            /* The viewer's CPU read, same rows, same preference. */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var samples = await viewer.GetCpuUtilizationAsync(ServerId, now.AddHours(-1), now, ct);
                Assert.Equal(new (DateTime, double)[] { (pre1Utc, 21), (pre2Utc, 22), (straddleUtc, 31), (newestUtc, 32) },
                    samples.Select(s => (s.SampleTime, s.SqlServerCpu)).ToArray());
            }

            /* Last, because it removes the post-rung batch: a store whose NEWEST CPU row predates the rung —
               fixture 5 of #3744, today's behaviour. The gate's read returns the pre-rung row with its local
               stamp at ordinal 2 and a NULL twin at ordinal 3, which is the reader's cue to fall back to the
               local stamp as the identity. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM cpu_utilization_stats WHERE server_id = $1 AND sample_time_utc IS NOT NULL", ServerId);
            using (var latest = new NpgsqlCommand(DarlingWorker.LatestCpuSql, connection))
            {
                latest.Parameters.AddWithValue(ServerId);
                using var reader = await latest.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(22, reader.GetInt32(0));
                Assert.Equal(pre2Utc.AddHours(-4), reader.GetDateTime(2));
                Assert.True(reader.IsDBNull(3), "a pre-rung row must read a NULL twin, or the fallback arm is never exercised");
            }

            /* #3778, moment three — the twins gone again (retention on a store that upgraded and then aged every
               post-rung row out would look like this only if it also kept older pre-rung rows, which retention
               does not do; the point is the read's contract, not a fleet scenario): back to the local maximum,
               frame LOCAL, never a UTC value invented from a local stamp. */
            Assert.Equal(((DateTime?)pre2Utc.AddHours(-4), false), await runner.GetLastCollectedTimeWithFrameAsync(ServerId, "cpu_utilization_stats", "sample_time", "sample_time_utc", ct));

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

    private static ServerPropertiesCollector.Row PropertiesRow(int offset, string? zone) => new(
        Edition: "Developer Edition (64-bit)", ProductVersion: "16.0.4150.1", ProductLevel: "RTM", ProductUpdateLevel: "CU15",
        EngineEdition: 3, CpuCount: 8, HyperthreadRatio: 1, PhysicalMemoryMb: 16_384, SocketCount: 1, CoresPerSocket: 8,
        IsHadrEnabled: false, IsClustered: false, ServiceObjective: null, VcoreCount: null,
        LockPagesInMemory: null, InstantFileInitializationEnabled: null, MemoryDumpCount: null,
        SqlServerStartTime: null, HostOsVersion: "Windows Server 2022", AgReplicaRole: "Standalone",
        UtcOffsetMinutes: offset, TimeZoneId: zone);

    private static async Task AssertColumnAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string table, string column, string type)
    {
        using var describe = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default, ordinal_position = (SELECT MAX(ordinal_position) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1) " +
            "FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 AND column_name = $2", connection);
        describe.Parameters.AddWithValue(table);
        describe.Parameters.AddWithValue(column);
        using var reader = await describe.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{table} has no {column} column after MigrateAsync");
        Assert.Equal(type, reader.GetString(0));
        Assert.Equal("YES", reader.GetString(1));
        Assert.True(reader.IsDBNull(2), $"{table}.{column} has a DEFAULT, which a compressed hypertable would have to rewrite to honour");
        Assert.True(reader.GetBoolean(3), $"{table}.{column} is not the LAST column, so the positional COPY writer would land it in the wrong slot");
    }

    /// <summary>DarlingCollectorRunner's COPY loop, verbatim in shape: prefix columns through the writer, then
    /// BeginPayload / WritePayload / EndPayload per row, so the collector's positional contract is exercised
    /// against the real, migrated table.</summary>
    private static async Task WriteCpuThroughTheCollectorAsync(NpgsqlConnection connection, DateTime collectionTime,
        IReadOnlyList<CpuUtilizationCollector.Row> rows, System.Threading.CancellationToken ct)
    {
        var definition = CpuUtilizationCollector.Instance;
        /* CPU samples are gauges; the collector never consults the calculator, so null! is the standing idiom. */
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

    private static async Task WritePropertiesThroughTheCollectorAsync(NpgsqlConnection connection, DateTime collectionTime,
        ServerPropertiesCollector.Row row, System.Threading.CancellationToken ct)
    {
        var definition = ServerPropertiesCollector.Instance;
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = collectionTime, Deltas = null! };
        var writer = new PgCollectorRowWriter();

        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        await importer.StartRowAsync(ct);
        writer.Value(CollectionIdGenerator.Next());
        writer.Value(DarlingMcpTestData.Naive(collectionTime)).Value(ServerId).Value(ServerName);
        writer.BeginPayload();
        definition.WritePayload(row, writer, context);
        writer.EndPayload(definition.PayloadColumns.Count);
        await importer.CompleteAsync(ct);
    }

    /// <summary>A row in the pre-rung shape: the local stamp, no twin — what every existing row looks like the
    /// morning of the upgrade.</summary>
    private static Task PlantPreRungAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime collectionTime, DateTime sampleTimeLocal, int cpu) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, 0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName, DarlingMcpTestData.Naive(sampleTimeLocal), cpu);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "cpu_utilization_stats", "server_properties" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            cleanup.Parameters.AddWithValue(ServerId);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
