/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgTargetFactCollector"/> (#3542): the PostgreSQL-target fact collector's SELF-pin census and
/// dialect pins, and the gated end-to-end that is the plumbing lane's exit criterion.
///
/// <para><b>Why a self-pin and not a parity pin (D1).</b> <c>PgFactCollectorTests</c> censuses
/// <see cref="PgFactCollector"/> against Lite's <c>DuckDbFactCollector</c> by name because the two are a
/// method-for-method port. This collector has no Lite twin — Lite monitors no PostgreSQL target — so the
/// census here anchors on nothing but itself, and its value shifts accordingly: not "matches Lite" but
/// "changes are deliberate". A content lane that adds a family must add its method here, in the same PR,
/// and say why.</para>
///
/// <para><b>The exit criterion (gated on <c>DARLING_TEST_PG</c>).</b> A server stamped <c>engine_kind =
/// postgres</c> with a day of <c>pg_database_stats</c> and nothing else: <c>analyze_server</c> answers
/// <c>empty</c> with a coverage block at ~1.0 — not <c>insufficient_data</c> (the span gate measured the
/// wrong table) and not <c>unavailable</c> (no coverage witness stamped the window) — and a sibling with two
/// hours of history is held by the span gate. That is the whole claim of this lane: the pipeline runs for a
/// PostgreSQL target on the right series, with nothing yet to say.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetFactCollectorTests
{
    private const string ServerName = "darling-pg-target-plumbing-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string YoungServerName = "darling-pg-target-plumbing-young";
    private static readonly int YoungServerId = ServerIdHelper.GetDeterministicHashCode(YoungServerName);

    /// <summary>
    /// The collect surface, ordinal-sorted: the two plumbing methods (coverage witness, registry metadata),
    /// the ten v1 family partials, (#3691 v2 plumbing) the three v2 family stubs — I/O (lane 11),
    /// replication (lane 12), bloat (lane 13) — (#3691 between waves) the wave-3 blocking stub (lane 17), and
    /// (#3691 v3 plumbing) the three v3 family stubs — plans (lane 27, lane 30 shares it), kernel (lane 28), memory
    /// (lane 32) — which the content lanes fill in place. Adding a family is a deliberate edit here.
    /// </summary>
    private static readonly string[] CollectSurface =
    {
        "CollectBloatFactsAsync",
        "CollectBlockingFactsAsync",
        "CollectBufferFactsAsync",
        "CollectConfigFactsAsync",
        "CollectCpuFactsAsync",
        "CollectDatabaseFactsAsync",
        "CollectIoFactsAsync",
        "CollectKernelFactsAsync",
        "CollectMemoryFactsAsync",
        "CollectObservedCoverageAsync",
        "CollectPlanFactsAsync",
        "CollectPostureFactsAsync",
        "CollectQueryFactsAsync",
        "CollectReplicationFactsAsync",
        "CollectServerMetadataFactsAsync",
        "CollectSessionFactsAsync",
        "CollectVacuumFactsAsync",
        "CollectWaitFactsAsync",
        "CollectWriteFactsAsync",
    };

    /* ---------------- ungated: census, SQL inventory, dialect ---------------- */

    [Fact]
    public void ImplementsTheSharedSeam_WithExactlyTheDeclaredCollectSurface()
    {
        Assert.True(typeof(IFactCollector).IsAssignableFrom(typeof(PgTargetFactCollector)));

        var declared = typeof(PgTargetFactCollector)
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Where(m => m.Name.StartsWith("Collect", StringComparison.Ordinal) && m.Name.EndsWith("Async", StringComparison.Ordinal))
            .Select(m => m.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(CollectSurface, declared);

        /* One partial file per family, named for it, and every stub names the lane that fills it. */
        var files = Directory.GetFiles(AnalysisDirectory(), "PgTargetFactCollector.*.cs").Select(Path.GetFileName).ToHashSet(StringComparer.Ordinal);
        foreach (var method in CollectSurface)
        {
            var family = method["Collect".Length..^"FactsAsync".Length];
            var file = method switch
            {
                "CollectObservedCoverageAsync" => "PgTargetFactCollector.Coverage.cs",
                "CollectServerMetadataFactsAsync" => "PgTargetFactCollector.Metadata.cs",
                "CollectSessionFactsAsync" => "PgTargetFactCollector.Sessions.cs",
                "CollectWaitFactsAsync" => "PgTargetFactCollector.Waits.cs",
                "CollectQueryFactsAsync" => "PgTargetFactCollector.Queries.cs",
                "CollectPlanFactsAsync" => "PgTargetFactCollector.Plans.cs",   /* v3: the family file is plural, the method singular (the Queries shape) */
                _ => $"PgTargetFactCollector.{family}.cs",
            };
            Assert.Contains(file, files);
        }

        var stubs = files.Where(f => f is not ("PgTargetFactCollector.Coverage.cs" or "PgTargetFactCollector.Metadata.cs")).ToList();
        /* Ten v1 families plus the three v2 families (#3691) plus the wave-3 blocking family (#3691 between waves) plus
           the three v3 families (#3691 v3 plumbing: plans, kernel, memory). Every family file names its lane — two
           digits now that v2's lanes are 11–13, which \d alone would have matched on their first digit and told no one. */
        Assert.Equal(17, stubs.Count);
        foreach (var stub in stubs)
        {
            var text = File.ReadAllText(Path.Combine(AnalysisDirectory(), stub!));
            Assert.Matches(new Regex(@"filled by lane \d+\b"), text);
        }

        /* The v2 files carry the exact marker the content briefs quote (a filled family keeps it, as v1's did). */
        foreach (var (file, lane) in new[] { ("PgTargetFactCollector.Io.cs", 11), ("PgTargetFactCollector.Replication.cs", 12), ("PgTargetFactCollector.Bloat.cs", 13), ("PgTargetFactCollector.Blocking.cs", 17), ("PgTargetFactCollector.Plans.cs", 27), ("PgTargetFactCollector.Kernel.cs", 28), ("PgTargetFactCollector.Memory.cs", 32) })
        {
            var text = File.ReadAllText(Path.Combine(AnalysisDirectory(), file));
            Assert.Contains($"/* filled by lane {lane}", text, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The SQL inventory is derived by reflection (every <c>public const string …Sql</c>), so the pin on it
    /// is not a count but a CLOSURE: every command any partial constructs names such a const, and every such
    /// const is in the inventory. A lane that runs a query from a private literal fails here before the
    /// dialect pins below could miss it.
    /// </summary>
    [Fact]
    public void AllSql_IsEveryPublicSqlConst_AndEveryExecutedQueryIsOne()
    {
        var consts = typeof(PgTargetFactCollector)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Sql", StringComparison.Ordinal))
            .ToDictionary(f => f.Name, f => (string)f.GetRawConstantValue()!, StringComparer.Ordinal);

        Assert.NotEmpty(consts);
        Assert.Equal(consts.Values.Order(StringComparer.Ordinal), PgTargetFactCollector.AllSql.Order(StringComparer.Ordinal));
        Assert.Contains(PgTargetFactCollector.PgTargetCoverageSql, PgTargetFactCollector.AllSql);
        Assert.Contains(PgTargetFactCollector.PgTargetServerMetadataSql, PgTargetFactCollector.AllSql);

        /* Every construction site, in code (comments and literals stripped), names a const. */
        var ctor = new Regex(@"new\s+(?:Npgsql\.)?NpgsqlCommand\s*\(\s*([A-Za-z_][A-Za-z0-9_.]*)\s*,");
        var sites = 0;
        foreach (var file in Directory.GetFiles(AnalysisDirectory(), "PgTargetFactCollector.*.cs"))
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            foreach (Match m in ctor.Matches(code))
            {
                sites++;
                var name = m.Groups[1].Value;
                Assert.True(consts.ContainsKey(name), $"{Path.GetFileName(file)} constructs a command from '{name}', which is not a public const string *Sql on PgTargetFactCollector");
            }

            /* And no construction from anything else — a string literal or a local. */
            Assert.DoesNotMatch(new Regex(@"new\s+(?:Npgsql\.)?NpgsqlCommand\s*\(\s*(?:\$?@?""|sql\b|query\b)"), code);
        }

        /* A floor, not an equality: the two sites are the plumbing's (coverage witness, registry metadata), and
           the count exists so a scan that read the wrong directory cannot report clean on nothing. Every
           content lane adds a site, and a pin that broke on each one would be a tax on writing a family — the
           per-site assertion above is the closure; this is the proof the scan saw the files. */
        Assert.True(sites >= 2, $"expected at least the plumbing's two command construction sites; found {sites}");
    }

    [Fact]
    public void AllSql_PgDialect_NoDuckDbOnlyConstructs_NoBareNow_PositionalParams()
    {
        foreach (var sql in PgTargetFactCollector.AllSql)
        {
            var upper = sql.ToUpperInvariant();
            Assert.DoesNotContain("QUALIFY", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("NOW(", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("READ_PARQUET", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            /* Every read is server-scoped. */
            Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The V4 view half of the SQL Server pin drops (PostgreSQL collector tables are view-less); the collector
    /// catalog half stays, plus <c>servers</c> — the one registry table an analysis read may name.
    /// </summary>
    [Fact]
    public void AllSql_EveryFromJoinTarget_ResolvesToACollectorTable_TheRegistry_OrACte()
    {
        var tables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);
        tables.Add("servers");
        /* One admission beyond the collector tables and the registry (#3542, between waves): the xmin-horizon arm
           of the vacuum family (#3537 / #3642's honest denominator) counts SUCCESS captures from collection_log,
           because "how many minutes of the window did the collector actually observe" is a question only the log
           answers. Not a v_ view, not a SQL Server table by another name; nothing else is admitted. */
        tables.Add("collection_log");

        /* #3691 v2 plumbing: the tables the v2 lanes read are collector tables already — the catalog admits them
           with no edit here — but a lane building on that must not discover otherwise at CI, so each is pinned
           by name with the lane that reads it. A table leaving CollectorCatalog fails here, not in the lane. */
        foreach (var (table, lane) in new[]
        {
            ("pg_io_stats", "lane 11 — I/O latency"),
            ("pg_replication_stats", "lane 12 — replication lag"),
            ("pg_replication_slot_stats", "lane 12 — slot retention / slot xmin"),
            ("pg_table_bloat_stats", "lane 13 — table bloat trend (hourly)"),
            ("pg_index_bloat", "lane 13 — index bloat trend (daily)"),
            ("pg_deadlocks", "lane 16 — deadlock drill-down"),
            ("pg_blocking_edges", "lane 16 — deadlock / blocking drill-down"),
            ("pg_lock_stats", "lane 14 — idle-in-transaction lock holders"),
            ("pg_wait_sampling", "lane 15 / lane 5 residue — the stock sampled wait estimate"),
            /* wave 3 (#3691 between waves): the blocking family's three tables, pinned for lane 17. */
            ("pg_log_events", "lane 17 — the lock_wait event family"),
            ("pg_session_states", "lane 17 — long-running active statements (lane 14 reads it too)"),
            /* v3 (#3691 plumbing): the plan, kernel and buffer-composition families' tables, pinned for lanes 27 / 28 /
               29 / 30. The memory family's host columns ride pg_cpu_utilization's row (V136, lane R5 — no pg_host_memory
               table), pinned here for lane 32. NOT pinned: pg_database_size — it arrives with lane R5's V136 catalog
               row; the first lane to read it adds the pin the day the row exists. */
            ("pg_plan_capture", "lane 27 — plan_hash flips and captured plan_json (lane 30 reads the Seq Scan nodes)"),
            ("pg_statement_stats", "lane 27 — the per-statement mean-ms step (lane 7's bad actors read it too)"),
            ("pg_column_stats", "lane 27 — top_value_frequency skew behind parameter sensitivity"),
            ("pg_predicate_stats", "lane 30 — predicate evaluation count and selectivity beside the Seq Scan"),
            ("pg_kernel_stats", "lane 28 — pg_stat_kcache user/system CPU time, the self-hosted CPU proxy"),
            ("pg_extension_availability", "lane 28 — whether pg_stat_kcache is installed, READ before the kernel rows are (the first analysis read of the daily availability capture)"),
            ("pg_buffer_usage", "lane 29 — the buffer-composition drill-down on PG_BUFFER_CACHE_PRESSURE"),
            ("pg_cpu_utilization", "lane 32 — the V136 host-memory columns beside the capacity percent (lane 9 reads it too)"),
        })
        {
            Assert.True(tables.Contains(table), $"{table} ({lane}) is not a CollectorCatalog target table; the v2 lane that reads it would fail the FROM/JOIN census");
        }

        foreach (var sql in PgTargetFactCollector.AllSql)
        {
            var scanSql = Regex.Replace(sql, @"--[^\n]*", " ");
            scanSql = Regex.Replace(scanSql, @"\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\b", " ", RegexOptions.IgnoreCase);
            var ctes = Regex.Matches(scanSql, @"(?:WITH|,)\s*(\w+)\s+AS\s*\(", RegexOptions.Singleline)
                .Select(m => m.Groups[1].Value)
                .ToHashSet(StringComparer.Ordinal);

            foreach (Match m in Regex.Matches(scanSql, @"\b(?:FROM|JOIN)\s+(\w+)", RegexOptions.IgnoreCase))
            {
                var target = m.Groups[1].Value;
                Assert.True(
                    tables.Contains(target) || ctes.Contains(target),
                    $"FROM/JOIN target '{target}' resolves to no collector table, the registry, or a CTE in:\n{sql}");
            }

            /* No v_ view: the SQL Server analysis reads go through Lite's passthrough views; PostgreSQL tables
               have none, and a v_ reference here would be a SQL Server table by another name. */
            Assert.DoesNotMatch(new Regex(@"\bFROM\s+v_"), scanSql);
        }
    }

    /* ---------------- the coverage witness ---------------- */

    /// <summary>
    /// The witness is the SQL Server one with the table substituted — the same three rules (one-policy
    /// look-back, clip to the window, discard past the policy) with the same bound parameters, finished through
    /// the same C#. Pinned as a TRANSFORMATION of the sibling's text rather than as a copy of its pins, so a
    /// rule added to one witness and not the other fails here.
    /// </summary>
    [Fact]
    public void PgTargetCoverageSql_IsTheSqlServerWitnessOverPgDatabaseStats_AndFinishesThroughTheSharedBuildCoverage()
    {
        var sql = PgTargetFactCollector.PgTargetCoverageSql;
        Assert.Contains("FROM pg_database_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_stats", sql, StringComparison.Ordinal);
        /* LF-normalised on both sides: the two consts live in different files and a checkout may give them
           different line endings; the text is the pin, not the newline flavour. */
        Assert.Equal(
            Lf(PgFactCollector.CoverageSql.Replace("FROM v_wait_stats", "FROM pg_database_stats", StringComparison.Ordinal)),
            Lf(sql));

        /* The rules, named, because the equality above is what keeps them in step and this is what they are. */
        Assert.Contains("SELECT DISTINCT collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("> $5 THEN 0", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(previous_time, $2)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("3600", sql, StringComparison.Ordinal);

        /* The C# edge-finishing is SHARED, not copied: the PostgreSQL witness calls PgFactCollector.BuildCoverage
           and declares no BuildCoverage of its own; the sibling made it internal for exactly this. */
        var coverage = File.ReadAllText(Path.Combine(AnalysisDirectory(), "PgTargetFactCollector.Coverage.cs"));
        Assert.Contains("PgFactCollector.BuildCoverage(", coverage, StringComparison.Ordinal);
        Assert.Null(typeof(PgTargetFactCollector).GetMethod("BuildCoverage", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public));
        var buildCoverage = typeof(PgFactCollector).GetMethod("BuildCoverage", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(buildCoverage);
        Assert.True(buildCoverage!.IsAssembly, "PgFactCollector.BuildCoverage must be internal so the PostgreSQL witness can share it");

        /* The witness runs FIRST, before any family, and the metadata fact second. */
        var main = File.ReadAllText(Path.Combine(AnalysisDirectory(), "PgTargetFactCollector.cs"));
        var coverageCall = main.IndexOf("await CollectObservedCoverageAsync(context, facts);", StringComparison.Ordinal);
        var metadataCall = main.IndexOf("await CollectServerMetadataFactsAsync(context, facts);", StringComparison.Ordinal);
        var firstFamilyCall = main.IndexOf("await CollectConfigFactsAsync(context, facts);", StringComparison.Ordinal);
        Assert.True(coverageCall > 0 && coverageCall < metadataCall && metadataCall < firstFamilyCall);

        /* And it carries no catch — the canary series fails the pass loudly, as its sibling does. */
        var witnessBody = coverage[coverage.IndexOf("CollectObservedCoverageAsync(AnalysisContext context", StringComparison.Ordinal)..];
        Assert.DoesNotContain("catch", CSharpSourceWalker.StripCommentsAndStrings(witnessBody), StringComparison.Ordinal);
    }

    [Fact]
    public void TheDeadline_IsTheSqlServerCollectorsByReference_AndTheDegradePostureIsTheSame()
    {
        Assert.Equal(PgFactCollector.FactCommandTimeoutSeconds, PgTargetFactCollector.FactCommandTimeoutSeconds);
        var main = File.ReadAllText(Path.Combine(AnalysisDirectory(), "PgTargetFactCollector.cs"));
        Assert.Contains("internal const int FactCommandTimeoutSeconds = PgFactCollector.FactCommandTimeoutSeconds;", main, StringComparison.Ordinal);

        /* The three-outcome classifier: timeout through the ONE structural predicate, both pre-migration
           SQLSTATEs quiet, everything else ERROR. */
        Assert.Contains("if (PgBaselineProvider.IsCommandTimeout(ex))", main, StringComparison.Ordinal);
        Assert.Contains("ex is PostgresException { SqlState: \"42P01\" or \"42703\" } pgEx", main, StringComparison.Ordinal);
        Assert.Contains("[CallerMemberName] string collectMethod", main, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMetadataFact_IsTheServerMajorVersionTwin_ReadFromTheRegistry()
    {
        var sql = PgTargetFactCollector.PgTargetServerMetadataSql;
        Assert.Contains("postgres_major_version, engine_kind", sql, StringComparison.Ordinal);
        Assert.Contains("FROM servers", sql, StringComparison.Ordinal);

        var metadata = File.ReadAllText(Path.Combine(AnalysisDirectory(), "PgTargetFactCollector.Metadata.cs"));
        Assert.Contains("Key = PgTargetFactKeys.ServerMajorVersion", metadata, StringComparison.Ordinal);
        Assert.Contains("Source = PgTargetSources.ConfigSource", metadata, StringComparison.Ordinal);
        Assert.Contains("MonitoredEngineKind.IsAurora(kind)", metadata, StringComparison.Ordinal);
        /* A NULL major is a row that makes no claim: no fact, never a fabricated zero. */
        Assert.Contains("if (reader.IsDBNull(0)) return;", metadata, StringComparison.Ordinal);
    }

    /* ---------------- gated: the exit criterion ---------------- */

    [Fact]
    public async Task APostgresTargetWithADayOfHistory_ReturnsEmptyWithFullCoverage_AndAYoungOneIsHeldByTheSpanGate()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the PostgreSQL-target plumbing e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);
            await RegisterServerAsync(connection, YoungServerId, YoungServerName, "aurora-postgres", 17, ct);

            /* Whole-minute bounds so the PG microsecond comparisons are exact; the window ends a minute
               ago so "now" inside the tool is safely past every planted row. */
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* 25 hours of history: one old row carries the span, one row a minute covers the window. */
            await PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* Two hours for the young sibling: under the gate on the RIGHT series. */
            for (var minute = 0; minute <= 120; minute++)
                await PlantDatabaseStatsAsync(connection, YoungServerId, YoungServerName, windowEnd.AddMinutes(-minute), ct);

            /* ── the collector alone: full coverage from the pg_database_stats series, one metadata fact. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);

            var coverage = context.Coverage!;
            Assert.False(coverage.IsPartial);
            Assert.Equal(1.0, coverage.Fraction, precision: 6);
            Assert.Equal(14_400_000, coverage.ObservedMs, precision: 3);
            Assert.Equal(241, coverage.SampleCount);

            var major = Assert.Single(facts);
            Assert.Equal(PgTargetFactKeys.ServerMajorVersion, major.Key);
            Assert.Equal(PgTargetSources.ConfigSource, major.Source);
            Assert.Equal(18, major.Value);
            Assert.Equal(0, major.Metadata["is_aurora"]);

            /* ── the service routes to the PostgreSQL set off the registry, for both PostgreSQL tokens. */
            var service = new DarlingAnalysisService(postgres);
            var (engine, kind) = await service.ResolveEngineAsync(ServerId, ct);
            Assert.Equal("postgres", kind);
            Assert.IsType<PgTargetFactCollector>(engine.Collector);
            Assert.Equal(DarlingAnalysisService.PgTargetDataSpanSql, engine.DataSpanSql);
            var (youngEngine, youngKind) = await service.ResolveEngineAsync(YoungServerId, ct);
            Assert.Equal("aurora-postgres", youngKind);
            Assert.Same(engine, youngEngine);

            /* ── THE EXIT CRITERION: the real analyze_server tool. */
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("empty", root.GetProperty("status").GetString());
                /* The all-clear envelope carries its structured payload under hints (McpHelpers.Status). */
                var hints = root.GetProperty("hints");
                var cov = hints.GetProperty("coverage");
                Assert.False(cov.GetProperty("partial").GetBoolean());
                Assert.InRange(cov.GetProperty("observed_fraction").GetDouble(), 0.98, 1.0);
                Assert.True(hints.GetProperty("persisted").GetBoolean());
            }

            /* The facts read shows the one fact, under its pg_ source, and the source filter accepts it. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.ConfigSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(1, root.GetProperty("total_facts").GetInt32());
                Assert.False(root.GetProperty("coverage").GetProperty("partial").GetBoolean());
                var fact = Assert.Single(root.GetProperty("facts").EnumerateArray());
                Assert.Equal(PgTargetFactKeys.ServerMajorVersion, fact.GetProperty("key").GetString());
                Assert.Equal(PgTargetSources.ConfigSource, fact.GetProperty("source").GetString());
            }

            /* ── the span gate, on the PostgreSQL series: two hours is not a day. */
            var young = await DarlingMcpTools.AnalyzeServer(service, postgres, YoungServerName, 4);
            using (var doc = JsonDocument.Parse(young))
            {
                var root = doc.RootElement;
                Assert.Equal("insufficient_data", root.GetProperty("status").GetString());
                var message = root.GetProperty("message").GetString()!;
                Assert.Contains("have 2.0 hours", message, StringComparison.Ordinal);
                /* Stamped: the NULL-kind caveat is not owed. */
                Assert.DoesNotContain("engine stamp", message, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static string Lf(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    internal static async Task RegisterServerAsync(
        NpgsqlConnection connection, int serverId, string serverName, string? engineKind, int? postgresMajor, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, postgres_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $4, $5, $5)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, engine_kind = EXCLUDED.engine_kind, postgres_major_version = EXCLUDED.postgres_major_version;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)engineKind ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = postgresMajor.HasValue ? postgresMajor.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One <c>pg_database_stats</c> row as the collector writes it: one database, flat counters — the
    /// plumbing pass reads only the collection SERIES (span and coverage), never the values.</summary>
    internal static async Task PlantDatabaseStatsAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', 1000, 10, 100, 9000, 0, 0, 0, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({ServerId}, {YoungServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({ServerId}, {YoungServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({ServerId}, {YoungServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({ServerId}, {YoungServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static string AnalysisDirectory() => RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Analysis");
}
