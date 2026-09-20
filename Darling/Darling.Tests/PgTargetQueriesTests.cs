/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
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
/// The <c>pg_queries</c> family (#3542 v1 step 7, lane 7): <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> from
/// <c>pg_statement_stats</c> — the query-shaped leaf every PostgreSQL story needs from day one.
///
/// <para><b>What is pinned, and why each.</b> The scorer grades ONE decision variable, the statement's share of
/// the WINDOW's total execution time, between two share bars, gated on an idle-server floor, and stamps
/// <c>threshold_lineage = 1</c> (the floor and the bars are fleet-measured since the #3691 calibration of
/// 2026-09-19 — the shares conditionally on the floor; the rule <c>PgTargetThresholdLineageTests</c> enforces the
/// comment shape on the source, this pins the behaviour). The collector's read reads the STORED deltas (never re-differences the cumulative
/// columns), differences the one unstored counter it needs over the full series identity, takes the share's
/// denominator over the window with the <c>SUM(…) OVER ()</c> idiom (#3541 A7), and computes the call rate's
/// span with the three-state interval idiom (#3540, V128) — each a source pin, because each is a way the read
/// could silently become wrong while still returning rows. The advice states the numbers it read and the
/// <c>queryid</c> re-key caveat. The drill-down joins the text and its hash and never de-skews.</para>
///
/// <para><b>The exit criterion (gated on <c>DARLING_TEST_PG</c>).</b> A planted window where one statement
/// holds 60% of <c>total_exec_time</c> yields a <c>PG_BAD_ACTOR</c> fact with the share, calls/sec, mean ms,
/// its <c>queryid</c> in the key, and — through the REAL <c>analyze_server</c> — a card at severity 1.0 whose
/// drill-down carries the statement text and its hash.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetQueriesTests
{
    private const string ServerName = "darling-pg-target-bad-actor-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>A signed 64-bit id spread over the int8 range, the way real <c>queryid</c>s are — exact in the
    /// key, NOT exact in a double, which is why the fact carries it in the key alone.</summary>
    private const long HeavyQueryId = -1234567890123456789L;
    private const long MediumQueryId = 42L;
    private const long LightQueryId = 7L;
    private const string HeavyText = "SELECT o.id, o.total FROM orders AS o WHERE o.customer_id = $1 AND o.placed_at >= $2 ORDER BY o.placed_at DESC LIMIT $3";

    private static Fact BadActor(long queryId, double share, double busy = 0.2, double tempBlocks = 0) => new()
    {
        Source = PgTargetSources.QueriesSource,
        Key = PgTargetFactKeys.BadActorKey(queryId),
        Value = share,
        ServerId = 1,
        Metadata =
        {
            ["share_of_window_time"] = share,
            ["window_total_exec_ms"] = 1_000_000,
            ["window_busy_fraction"] = busy,
            ["calls"] = 2640,
            ["total_exec_ms"] = share * 1_000_000,
            ["mean_exec_ms"] = 227.36,
            ["max_exec_ms"] = 900.5,
            ["calls_per_sec"] = 0.1833,
            ["temp_blks_written"] = tempBlocks,
            ["database_count"] = 2,
        },
    };

    /* ── the scorer ── */

    [Fact]
    public void ScoreQueriesFact_GradesTheShareBetweenTheTwoBars_AndStampsTheMeasuredLineage()
    {
        var critical = BadActor(1, 0.60);
        var concerning = BadActor(2, 0.25);
        var under = BadActor(3, 0.10);
        var between = BadActor(4, 0.30);

        Assert.Equal(1.0, PgTargetScorer.ScoreBase(critical), precision: 9);
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(concerning), precision: 9);
        /* Below the concerning bar the shared formula ramps linearly to 0.5 AT the bar: 0.5 × 0.10 / 0.25. */
        Assert.Equal(0.2, PgTargetScorer.ScoreBase(under), precision: 9);
        /* Between the bars: 0.5 + 0.5 × (0.30 − 0.25) / (0.60 − 0.25). */
        Assert.Equal(0.5 + 0.5 * (0.05 / 0.35), PgTargetScorer.ScoreBase(between), precision: 9);

        /* Every graded fact says its bars are fleet-measured (#3691, 2026-09-19). */
        foreach (var fact in new[] { critical, concerning, under, between })
            Assert.Equal(1, fact.Metadata["threshold_lineage"]);

        Assert.Equal(0.25, PgTargetScorer.BadActorShareConcerning);
        Assert.Equal(0.60, PgTargetScorer.BadActorShareCritical);
    }

    [Fact]
    public void ScoreQueriesFact_IsZeroOnAnIdleWindow_WithoutAShare_AndForAnyOtherKeyUnderTheSource()
    {
        /* 60% of nothing: the busy floor gates the share, and the gate is itself a (fleet-measured) bar, so the
           lineage stamp lands even on the fact it zeroes. */
        var idle = BadActor(1, 0.60, busy: 0.04);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(idle));
        Assert.Equal(1, idle.Metadata["threshold_lineage"]);
        Assert.Equal(0.05, PgTargetScorer.BadActorBusyFloor);

        /* At the floor exactly, the gate opens. */
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(BadActor(1, 0.60, busy: 0.05)), precision: 9);

        /* No share, no grade — and no stamp, because nothing was graded. */
        var noShare = new Fact { Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(9), Value = 0.6 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(noShare));
        Assert.False(noShare.Metadata.ContainsKey("threshold_lineage"));

        /* The routing probe the shared-switch test uses: a non-bad-actor key under pg_queries scores 0. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.QueriesSource, Key = "PG_PROBE", Value = 99 }));
    }

    [Fact]
    public void QueriesAmplifiers_FireOnlyWhenTheSiblingFired_AndTheStatementItselfCorroborates()
    {
        var key = PgTargetFactKeys.BadActorKey(HeavyQueryId);
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definitions = ((System.Collections.IEnumerable)amplifiers.Invoke(null, [key])!).Cast<object>().ToList();
        Assert.Equal(2, definitions.Count);

        static (double Boost, Func<Dictionary<string, Fact>, bool> Predicate) Read(object definition)
        {
            var type = definition.GetType();
            return (
                (double)type.GetProperty("Boost")!.GetValue(definition)!,
                (Func<Dictionary<string, Fact>, bool>)type.GetProperty("Predicate")!.GetValue(definition)!);
        }

        var spill = Read(definitions[0]);
        var cpu = Read(definitions[1]);
        Assert.Equal(PgTargetScorer.BadActorCoFireBoost, spill.Boost);
        Assert.Equal(PgTargetScorer.BadActorCoFireBoost, cpu.Boost);

        var spiller = BadActor(HeavyQueryId, 0.6, tempBlocks: 1912);
        var nonSpiller = BadActor(HeavyQueryId, 0.6, tempBlocks: 0);
        var firedSpill = new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, BaseSeverity = 0.7 };
        var quietSpill = new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, BaseSeverity = 0.0 };
        var firedCpu = new Fact { Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, BaseSeverity = 0.9 };

        Assert.True(spill.Predicate(Lookup(spiller, firedSpill)));
        /* The server spilled but THIS statement wrote no temp blocks: not corroborated. */
        Assert.False(spill.Predicate(Lookup(nonSpiller, firedSpill)));
        /* The statement spilled but the server-level fact did not fire (its own bar decided). */
        Assert.False(spill.Predicate(Lookup(spiller, quietSpill)));
        Assert.False(spill.Predicate(Lookup(spiller)));

        Assert.True(cpu.Predicate(Lookup(spiller, firedCpu)));
        Assert.False(cpu.Predicate(Lookup(spiller)));

        /* Through the real scorer today: the sibling families are stubs (base 0), so a bad actor scores its
           base and no boost — the amplifier arms are wired and inert until lanes 6 / 9 land their bars. */
        var facts = new List<Fact> { BadActor(HeavyQueryId, 0.6, tempBlocks: 1912), new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, Value = 5 } };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(1.0, facts[0].BaseSeverity, precision: 9);
        Assert.Equal(1.0, facts[0].Severity, precision: 9);
        Assert.Equal(2, facts[0].AmplifierResults.Count);
        Assert.All(facts[0].AmplifierResults, r => Assert.False(r.Matched));
    }

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();

    /* ── the advice ── */

    [Fact]
    public void ComposeQueries_StatesTheNumbersItRead_BothLevers_TheirCounterObjectives_AndTheQueryidCaveat()
    {
        var key = PgTargetFactKeys.BadActorKey(HeavyQueryId);
        var fact = BadActor(HeavyQueryId, 0.6002, tempBlocks: 1912);
        fact.Metadata["total_exec_ms"] = 600_240;
        var block = PgTargetAdvice.Compose(key, Lookup(fact));

        Assert.NotNull(block);
        Assert.Equal("One statement shape held 60% of the window's execution time", block!.Headline);

        /* Value-stated: the share, the totals at a readable scale, the calls, the rate, the mean, the max. */
        Assert.Contains("queryid -1234567890123456789", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("60% of the window's total statement execution time (10 min of 16.7 min)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("2,640 calls (0.18/s", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("averaging 227.4 ms per call", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("worst single execution of 900.5 ms", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("ran against 2 databases", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("wrote 1,912 temp blocks", block.Investigation, StringComparison.Ordinal);
        /* Honest about the bar and the identity. */
        Assert.Contains("the busy floor that admitted it and the share bars it crossed are fleet-measured (threshold_lineage = 1)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("re-keyed by a major upgrade", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("occurrence history restarts at one", block.Investigation, StringComparison.Ordinal);

        /* Both levers with the arithmetic, each with its counter-objective; no DDL (D8). */
        Assert.Contains("total = calls × mean: 2,640 × 227.4 ms", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("trades result freshness", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("trades write amplification", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_query_duration_trend", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_plans", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CREATE INDEX", block.Investigation, StringComparison.OrdinalIgnoreCase);
        Assert.Null(block.RemediationTsql);
    }

    [Fact]
    public void ComposeQueries_OmitsWhatTheFactDoesNotCarry_AndTheStaticBlockClaimsNoFigure()
    {
        var key = PgTargetFactKeys.BadActorKey(MediumQueryId);
        var fact = BadActor(MediumQueryId, 0.3);
        fact.Metadata.Remove("mean_exec_ms");
        fact.Metadata.Remove("calls_per_sec");
        fact.Metadata.Remove("max_exec_ms");
        fact.Metadata["database_count"] = 1;
        var block = PgTargetAdvice.Compose(key, Lookup(fact))!;

        Assert.DoesNotContain("averaging", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("/s over", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("worst single execution", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("databases", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("temp blocks", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the mean is unknown for this window", block.Remediation, StringComparison.Ordinal);

        /* The static block: the family's conclusion with no number in it, the same object from both entry points. */
        var statik = PgTargetAdvice.Static(key);
        Assert.NotNull(statik);
        Assert.Same(statik, PgTargetAdvice.Static(PgTargetFactKeys.BadActorKey(LightQueryId)));
        Assert.Equal(statik, FactAdvice.GetForFactKey(key));
        Assert.DoesNotContain("%", statik!.Headline, StringComparison.Ordinal);
        Assert.Contains("queryid", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("RE-KEYED by a major", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("the share bars are read given it (threshold_lineage = 1)", statik.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", statik.Remediation, StringComparison.OrdinalIgnoreCase);
    }

    /* ── the reads, as source ── */

    [Fact]
    public void TheCollectorRead_ReadsStoredDeltas_TakesTheWindowDenominator_AndUsesTheThreeStateInterval()
    {
        var sql = PgTargetFactCollector.PgTargetTopStatementsSql;
        Assert.Contains(sql, PgTargetFactCollector.AllSql);

        /* Stored deltas, never a re-derivation of the cumulative columns. */
        Assert.Contains("SUM(delta_calls)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_total_exec_time_ms)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(calls)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(total_exec_time_ms)", sql, StringComparison.Ordinal);
        /* The one unstored counter, differenced over the FULL identity and clamped — the reference reader's shape. */
        Assert.Contains("GREATEST(temp_blks_written - LAG(temp_blks_written) OVER identity, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY queryid, database_id, user_id, toplevel", sql, StringComparison.Ordinal);
        /* #3541 A7 / #3613: the denominator is the window's, evaluated over the grouped result before LIMIT. */
        Assert.Contains("SUM(SUM(p.total_exec_ms)) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(p.total_exec_ms) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        /* The three-state interval (V128): stored → NULLIF, NULL → LAG, and no fabricated zero. */
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (PARTITION BY queryid ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        /* The rate is calls over the KNOWN span only. */
        Assert.Contains("FILTER (WHERE p.interval_seconds IS NOT NULL)", sql, StringComparison.Ordinal);
        /* Units: the ≥ 13 column name is the collector's; no pre-13 branch exists to write. */
        Assert.DoesNotContain("total_time", sql.Replace("total_exec_time", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        /* No text join here — the census confines this collector to collector tables; the text is the drill-down's. */
        Assert.DoesNotContain("pg_statement_text", sql, StringComparison.Ordinal);

        Assert.Equal(5, PgTargetFactCollector.TopStatementCount);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Queries.cs");
        Assert.Contains("Key = PgTargetFactKeys.BadActorKey(queryId)", source, StringComparison.Ordinal);
        Assert.Contains("Source = PgTargetSources.QueriesSource", source, StringComparison.Ordinal);
        /* The idle gate divides by OBSERVED time (#3538 A2/A7), and nothing is emitted without it. */
        Assert.Contains("[\"window_busy_fraction\"] = windowTotalExecMs / observedMs", source, StringComparison.Ordinal);
        Assert.Contains("var observedMs = context.ObservedDurationMs;", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PeriodDurationMs", source, StringComparison.Ordinal);
        /* The queryid lives in the key alone — a double would round most real ids. */
        Assert.DoesNotContain("[\"queryid\"]", source, StringComparison.Ordinal);
        /* Absent, not zero: the three conditional metadata writes. */
        Assert.Contains("if (calls > 0) fact.Metadata[\"mean_exec_ms\"]", source, StringComparison.Ordinal);
        Assert.Contains("if (callsPerSec is { } rate) fact.Metadata[\"calls_per_sec\"]", source, StringComparison.Ordinal);
        Assert.Contains("if (maxExecMs is { } max) fact.Metadata[\"max_exec_ms\"]", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDrillDown_JoinsTheTextForDisplayAndHash_ParsesTheQueryidFromTheKey_AndNeverDeSkews()
    {
        var sql = PgTargetDrillDownCollector.PgTargetBadActorDetailSql;
        Assert.Contains("FROM pg_statement_stats", sql, StringComparison.Ordinal);
        Assert.Contains("AND   queryid = $4", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_statement_text AS t", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(MAX(t.query_text), $5)", sql, StringComparison.Ordinal);
        Assert.Contains("hashtext(MAX(t.query_text))", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(t.first_seen)", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY queryid, database_id, user_id, toplevel", sql, StringComparison.Ordinal);
        Assert.Contains("array_agg(pd.database_id ORDER BY pd.total_exec_ms DESC)", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.Queries.cs");
        /* In CODE: the doc comment names DeSkew to say why it is absent, and prose is not a call. */
        Assert.DoesNotContain("DeSkew", CSharpSourceWalker.StripCommentsAndStrings(source), StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds", source, StringComparison.Ordinal);
        Assert.Contains("long.TryParse(key.AsSpan(PgTargetFactKeys.BadActorKeyPrefix.Length)", source, StringComparison.Ordinal);
        Assert.Contains("finding.DrillDown![\"pg_bad_actor_statements\"]", source, StringComparison.Ordinal);
        /* queryid as a STRING in the JSON, as get_pg_top_queries returns it. */
        Assert.Contains("queryid = queryId.ToString(CultureInfo.InvariantCulture)", source, StringComparison.Ordinal);
        /* Lane 6's seam is marked, not silently absent. */
        Assert.Contains("filled by lane 6", source, StringComparison.Ordinal);
        Assert.Equal(2000, PgTargetDrillDownCollector.StatementTextCap);
    }

    [Fact]
    public void TheNextReads_ForABadActor_AreTheStatementAndPlanTools()
    {
        /* The duration trend leads since the between-waves pass: the advice's "first question" (mean stepped vs
           calls changed) is the read that answers it. */
        var recommendations = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.BadActorKey(HeavyQueryId));
        Assert.NotNull(recommendations);
        Assert.Equal(new[] { "get_pg_query_duration_trend", "get_pg_top_queries", "get_pg_plans" }, recommendations!.Select(r => r.Tool).ToArray());
    }

    /* ── gated: the exit criterion ── */

    [Fact]
    public async Task APlantedWindowWhereOneStatementHoldsSixtyPercent_YieldsTheBadActorFact_ItsCard_AndItsDrillDown()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the PG_BAD_ACTOR e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* The coverage witness and the span gate read pg_database_stats: 25 h of span, one row a minute. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* Statement snapshots every five minutes, 48 in the window, each carrying the interval it accrued over.
               Per snapshot: heavy 12,500 ms over 50 calls (+ a one-call, one-ms sibling series against a second
               database), medium 6,250 ms over 250 calls, light 2,083 ms over 5 calls with NO stored interval (the
               pre-V128 row shape, so the LAG fallback is the path it takes). Window total 1,000,032 ms → shares
               0.600 / 0.300 / 0.100; busy fraction 1,000,032 / 14,400,000 = 0.069, above the 0.05 floor. */
            long heavyCalls = 0, heavyMs = 0, heavyTemp = 0, mediumCalls = 0, mediumMs = 0, lightCalls = 0, lightMs = 0;
            for (var snapshot = 1; snapshot <= 48; snapshot++)
            {
                var at = windowStart.AddMinutes(snapshot * 5);
                heavyCalls += 50; heavyMs += 12_500; heavyTemp += 40;
                await PlantStatementAsync(connection, at, HeavyQueryId, 16384, heavyCalls, heavyMs, 50, 12_500, 300, heavyTemp, ct);
                await PlantStatementAsync(connection, at, HeavyQueryId, 16385, snapshot, snapshot, 1, 1, 300, 0, ct);
                mediumCalls += 250; mediumMs += 6_250;
                await PlantStatementAsync(connection, at, MediumQueryId, 16384, mediumCalls, mediumMs, 250, 6_250, 300, 0, ct);
                lightCalls += 5; lightMs += 2_083;
                await PlantStatementAsync(connection, at, LightQueryId, 16384, lightCalls, lightMs, 5, 2_083, null, 0, ct);
            }
            await PlantTextAsync(connection, HeavyQueryId, HeavyText, ct);

            /* ── the collector alone, on the exact planted window. */
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
            Assert.Equal(14_400_000, context.ObservedDurationMs, precision: 3);

            var badActors = facts.Where(f => f.Source == PgTargetSources.QueriesSource).OrderByDescending(f => f.Value).ToList();
            Assert.Equal(3, badActors.Count);
            Assert.All(badActors, f => Assert.Null(f.DatabaseName));

            var heavy = badActors[0];
            Assert.Equal(PgTargetFactKeys.BadActorKey(HeavyQueryId), heavy.Key);
            /* 600,048 of 1,000,032 (600,000 + 48 + 300,000 + 48 × 2,083): the one-ms sibling series is in both
               numerator and denominator, and the light statement's per-snapshot 2,083 ms is 99,984 over 48. */
            Assert.Equal(0.60, heavy.Value, precision: 3);
            Assert.Equal(heavy.Value, heavy.Metadata["share_of_window_time"]);
            Assert.Equal(1_000_032, heavy.Metadata["window_total_exec_ms"]);
            Assert.Equal(1_000_032 / 14_400_000.0, heavy.Metadata["window_busy_fraction"], precision: 9);
            Assert.Equal(2_448, heavy.Metadata["calls"]);
            Assert.Equal(600_048, heavy.Metadata["total_exec_ms"]);
            Assert.Equal(600_048 / 2_448.0, heavy.Metadata["mean_exec_ms"], precision: 9);
            Assert.Equal(900.5, heavy.Metadata["max_exec_ms"]);
            /* Stored interval, MAX per snapshot: 48 × 300 s of known span. */
            Assert.Equal(2_448 / 14_400.0, heavy.Metadata["calls_per_sec"], precision: 9);
            Assert.Equal(1_880, heavy.Metadata["temp_blks_written"]);
            Assert.Equal(2, heavy.Metadata["database_count"]);
            Assert.False(heavy.Metadata.ContainsKey("queryid"));

            var medium = badActors[1];
            Assert.Equal(PgTargetFactKeys.BadActorKey(MediumQueryId), medium.Key);
            Assert.Equal(0.30, medium.Value, precision: 3);
            Assert.Equal(1, medium.Metadata["database_count"]);

            /* The pre-V128 shape: no stored interval, so the span is the LAG over its 48 snapshots — 47 known
               five-minute gaps, and the first snapshot's calls are excluded from the rate with it. */
            var light = badActors[2];
            Assert.Equal(PgTargetFactKeys.BadActorKey(LightQueryId), light.Key);
            Assert.Equal(0.10, light.Value, precision: 3);
            Assert.Equal(240, light.Metadata["calls"]);
            Assert.Equal(235 / (47 * 300.0), light.Metadata["calls_per_sec"], precision: 9);

            /* ── scored through the real scorer: 60% is critical, 30% is between the bars, 10% is under. */
            new FactScorer().ScoreAll(facts);
            Assert.Equal(1.0, heavy.Severity, precision: 9);
            Assert.Equal(0.5 + 0.5 * ((medium.Value - 0.25) / 0.35), medium.Severity, precision: 6);
            Assert.Equal(0.5 * (light.Value / 0.25), light.Severity, precision: 6);
            Assert.All(badActors, f => Assert.Equal(1, f.Metadata["threshold_lineage"]));

            /* ── THE EXIT CRITERION: the real analyze_server tool. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.BadActorKey(HeavyQueryId));

                Assert.Equal(1.0, card.GetProperty("severity").GetDouble());
                Assert.Equal(PgTargetSources.QueriesSource, card.GetProperty("category").GetString());
                /* The tool's window is now − 4 h → now, a minute off the planted bounds; the ratio holds. */
                Assert.InRange(card.GetProperty("root_fact").GetProperty("value").GetDouble(), 0.58, 0.62);

                var advice = card.GetProperty("advice");
                Assert.Equal("One statement shape held 60% of the window's execution time", advice.GetProperty("headline").GetString());
                Assert.Contains("queryid -1234567890123456789", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);

                var detail = card.GetProperty("drill_down").GetProperty("pg_bad_actor_statements").GetProperty(PgTargetFactKeys.BadActorKey(HeavyQueryId));
                Assert.Equal("-1234567890123456789", detail.GetProperty("queryid").GetString());
                Assert.Equal(HeavyText, detail.GetProperty("query_text").GetString());
                Assert.Equal(JsonValueKind.Number, detail.GetProperty("text_hash").ValueKind);
                Assert.Equal(2, detail.GetProperty("databases").GetArrayLength());
                Assert.Equal(16384, detail.GetProperty("databases")[0].GetProperty("database_id").GetInt64());
                Assert.True(detail.GetProperty("temp_blks_written").GetInt64() > 0);

                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()).ToList();
                Assert.Contains("get_pg_top_queries", tools);
                Assert.Contains("get_pg_plans", tools);

                /* The medium statement's card carries no text — null, never "". */
                var mediumCard = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.BadActorKey(MediumQueryId));
                var mediumDetail = mediumCard.GetProperty("drill_down").GetProperty("pg_bad_actor_statements").GetProperty(PgTargetFactKeys.BadActorKey(MediumQueryId));
                Assert.Equal(JsonValueKind.Null, mediumDetail.GetProperty("query_text").ValueKind);
                Assert.Equal(JsonValueKind.Null, mediumDetail.GetProperty("text_hash").ValueKind);
            }

            /* ── get_analysis_facts under the family's source shows the lineage stamp. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.QueriesSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                /* total_facts is the pass's unfiltered count (three bad actors + the registry major + the memory family's
                   stock honesty arm, PG_HOST_MEMORY_PRESSURE unavailable, lane 32); shown is the filtered page. */
                Assert.Equal(5, root.GetProperty("total_facts").GetInt32());
                Assert.Equal(3, root.GetProperty("shown").GetInt32());
                Assert.Equal(3, root.GetProperty("facts").GetArrayLength());
                foreach (var fact in root.GetProperty("facts").EnumerateArray())
                {
                    Assert.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, fact.GetProperty("key").GetString(), StringComparison.Ordinal);
                    Assert.Equal(1, fact.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_statement_stats</c> row as the collector writes it: cumulative counters beside the
    /// deltas it computed at the write, and the interval those deltas accrued over (NULL = a pre-V128 row).</summary>
    private static async Task PlantStatementAsync(
        NpgsqlConnection connection, DateTime at, long queryId, long databaseId, long calls, long totalMs,
        long deltaCalls, long deltaMs, int? intervalSeconds, long tempBlocksWritten, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, 10, TRUE, $7, $8, 900.5, 100, 10, 5, 0, $9, 0, $10, $11, 10, $12)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(databaseId);
        command.Parameters.AddWithValue(calls);
        command.Parameters.AddWithValue((double)totalMs);
        command.Parameters.AddWithValue(tempBlocksWritten);
        command.Parameters.AddWithValue(deltaCalls);
        command.Parameters.AddWithValue(deltaMs);
        command.Parameters.Add(new NpgsqlParameter { Value = intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantTextAsync(NpgsqlConnection connection, long queryId, string text, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_statement_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_statement_text WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
