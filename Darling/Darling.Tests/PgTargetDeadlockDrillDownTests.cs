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
/// Lane 16 of #3691 — the deadlock exemplar drill-down beside <c>PG_DEADLOCK_RATE</c> /
/// <c>ANOMALY_PG_DEADLOCK_RATE</c>: the SQL by its own text, the shape key (participants, modes, resources — NOT
/// <c>deadlock_hash</c>, which identifies one report), the bounds and their flags, the three-arm exemplar
/// sentence, the advice fold and its idempotence, and — gated on <c>DARLING_TEST_PG</c> — the exit criterion
/// through the REAL <c>analyze_server</c>: a planted 6-deadlock counter delta beside four captured rows (two of
/// one shape under different hashes, one of another shape stored twice by the tail re-read) produces a card whose
/// drill-down says <c>engine_counted = 6</c>, <c>log_captured = 4</c>, <c>reports_captured = 3</c>,
/// <c>distinct_shapes = 2</c>, top exemplar recurrence 2, and whose frozen advice carries the same numbers.
///
/// <para>Every pure value asserted here was executed on this machine through a net10.0 harness over the built
/// assemblies before the first CI run; the SQL and the e2e were executed against a throwaway PostgreSQL 18 store.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetDeadlockDrillDownTests
{
    private const string ServerName = "darling-pg-target-deadlock-exemplars-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* ───────────────────────── the read ───────────────────────── */

    [Fact]
    public void TheExemplarSql_GroupsByShapeNotByHash_WindowsOnCollectionTime_AndBoundsEverythingInTheRead()
    {
        var sql = PgTargetDrillDownCollector.PgTargetDeadlockExemplarsSql;
        Assert.Contains("FROM pg_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time <= $3", sql, StringComparison.Ordinal);
        /* The shape, and the recurrence as distinct REPORTS within it. */
        Assert.Contains("GROUP BY participant_count, lock_modes, resources", sql, StringComparison.Ordinal);
        Assert.Contains("count(DISTINCT deadlock_hash)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GROUP BY deadlock_hash", sql, StringComparison.Ordinal);
        /* Ranked by recurrence, capped, the totals on every row. */
        Assert.Contains("ORDER BY reports DESC, last_seen DESC NULLS LAST", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.Contains("count(*) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(reports) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(rows_captured) OVER ()", sql, StringComparison.Ordinal);
        /* Bounded in the read, with the untruncated lengths for the flags. */
        Assert.Contains("LEFT(latest_victim_statement, $5)", sql, StringComparison.Ordinal);
        Assert.Contains("length(latest_victim_statement)", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(latest_graph_text, $6)", sql, StringComparison.Ordinal);
        Assert.Contains("length(latest_graph_text)", sql, StringComparison.Ordinal);
        /* Store clock discipline and dialect. */
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("occurred_at >=", sql, StringComparison.Ordinal);

        Assert.Equal(3, PgTargetDrillDownCollector.DeadlockExemplarCap);
        Assert.Equal(24, PgTargetDrillDownCollector.GraphTextLineCap);
        Assert.Equal(2 * PgTargetDrillDownCollector.StatementTextCap, PgTargetDrillDownCollector.GraphTextCharCap);
        Assert.Equal(120, PgTargetDrillDownCollector.VictimFingerprintCap);
    }

    [Fact]
    public void TheDrillDown_AttachesOnEitherDeadlockRoot_ReusesTheRootFactCounter_AndRefreezesTheAdvice()
    {
        var root = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.cs");
        Assert.Contains("pathKeys.Contains(PgTargetFactKeys.DeadlockRate) || pathKeys.Contains(PgTargetFactKeys.AnomalyDeadlockRate)", root, StringComparison.Ordinal);
        Assert.Contains("await CollectDeadlockExemplarsAsync(finding, context);", root, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.Deadlocks.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Contains("CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds", source, StringComparison.Ordinal);
        Assert.Contains("new NpgsqlCommand(PgTargetDeadlockExemplarsSql, connection)", code, StringComparison.Ordinal);
        Assert.Contains("context.CancellationToken", code, StringComparison.Ordinal);
        /* The counter is the root fact's, both spellings; never a second read of pg_database_stats. */
        Assert.Contains("finding.RootFactMetadata", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.DeadlockCounterCountKey", code, StringComparison.Ordinal);
        Assert.Contains("AnomalyCurrentCountKey", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.DeadlockExemplarCountKey", code, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_database_stats", source, StringComparison.Ordinal);
        /* One object under one section; the advice re-frozen through the composer, not hand-written here. */
        Assert.Contains("finding.DrillDown![DeadlockExemplarsSection]", code, StringComparison.Ordinal);
        Assert.Equal("pg_deadlock_exemplars", PgTargetDrillDownCollector.DeadlockExemplarsSection);
        Assert.Contains("FactAdvice.TryReadStoryText(finding.StoryText)", code, StringComparison.Ordinal);
        Assert.Contains("FactAdvice.SerializeForStoryText(PgTargetAdvice.WithDeadlockExemplars(frozen, summary))", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetAdvice.DeadlockExemplarSentence(summary)", code, StringComparison.Ordinal);
        /* No DeSkew in CODE (pg_deadlocks stamps collection_time host-UTC). */
        Assert.DoesNotContain("DeSkew", code, StringComparison.Ordinal);
    }

    /* ───────────────────────── bounds ───────────────────────── */

    [Fact]
    public void BoundGraphText_CutsAtTheLineCap_ReportsTheTotal_AndFlagsEitherBound()
    {
        Assert.Equal((null, 0, false), PgTargetDrillDownCollector.BoundGraphText(null, readCut: false));
        Assert.Equal(("", 0, false), PgTargetDrillDownCollector.BoundGraphText("", readCut: false));

        var four = "Process 11 waits for ShareLock on transaction 900; blocked by process 12.\r\nProcess 12 waits for ShareLock on transaction 901; blocked by process 11.\r\nProcess 11: UPDATE a SET x = 1\r\nProcess 12: UPDATE b SET y = 2";
        var (text, lines, truncated) = PgTargetDrillDownCollector.BoundGraphText(four, readCut: false);
        Assert.Equal(4, lines);
        Assert.False(truncated);
        Assert.Equal(four.Replace("\r\n", "\n", StringComparison.Ordinal), text);

        /* Thirty lines: cut to 24, total 30, flagged. */
        var thirty = string.Join('\n', Enumerable.Range(1, 30).Select(i => $"line {i}"));
        var (cut, total, cutFlag) = PgTargetDrillDownCollector.BoundGraphText(thirty, readCut: false);
        Assert.Equal(30, total);
        Assert.True(cutFlag);
        Assert.Equal(24, cut!.Split('\n').Length);
        Assert.EndsWith("line 24", cut, StringComparison.Ordinal);

        /* The READ cut it (untruncated length longer than what arrived, which the caller measures before it
           normalizes the graph, #4005): flagged even under the line cap. */
        var (_, shortLines, readCut) = PgTargetDrillDownCollector.BoundGraphText("one\ntwo", readCut: true);
        Assert.Equal(2, shortLines);
        Assert.True(readCut);
    }

    [Fact]
    public void Fingerprint_CollapsesWhitespace_CapsWithAnEllipsis_AndIsNullForNoStatement()
    {
        Assert.Null(PgTargetDrillDownCollector.Fingerprint(null));
        Assert.Null(PgTargetDrillDownCollector.Fingerprint("   \n\t "));
        Assert.Equal("UPDATE orders SET status = $1 WHERE id = $2",
            PgTargetDrillDownCollector.Fingerprint("  UPDATE orders\n   SET status = $1\n\tWHERE id = $2  "));

        var longStatement = "SELECT " + string.Join(", ", Enumerable.Range(1, 60).Select(i => $"col{i:00}")) + " FROM wide";
        var fingerprint = PgTargetDrillDownCollector.Fingerprint(longStatement)!;
        Assert.True(fingerprint.Length <= PgTargetDrillDownCollector.VictimFingerprintCap + 1, fingerprint);
        Assert.EndsWith("…", fingerprint, StringComparison.Ordinal);
        Assert.StartsWith("SELECT col01, col02", fingerprint, StringComparison.Ordinal);
        /* Cut at the cap, trailing space trimmed before the ellipsis — never "col1 …". */
        Assert.DoesNotContain(" …", fingerprint, StringComparison.Ordinal);
    }

    /* ───────────────────────── the sentence and the fold ───────────────────────── */

    [Fact]
    public void TheSentence_WithNoCapture_NamesTheSettingsAndTheCounter_AndStatesNoValue()
    {
        var summary = new PgTargetDeadlockExemplarSummary(EngineCounted: 6, LogCaptured: 0, LogCapturedSource: "drill-down read of pg_deadlocks", ReportsCaptured: 0, DistinctShapes: 0, Exemplars: []);
        var sentence = PgTargetAdvice.DeadlockExemplarSentence(summary);
        Assert.StartsWith("Exemplars: the engine counted 6 deadlocks in the window and the log capture holds none of them.", sentence, StringComparison.Ordinal);
        Assert.Contains("log_lock_waits", sentence, StringComparison.Ordinal);
        Assert.Contains("log_min_messages", sentence, StringComparison.Ordinal);
        Assert.Contains("deadlock_timeout", sentence, StringComparison.Ordinal);
        Assert.Contains("get_pg_server_config", sentence, StringComparison.Ordinal);
        /* Named, not valued: the pass did not read them. */
        Assert.DoesNotContain("log_lock_waits = ", sentence, StringComparison.Ordinal);
        Assert.DoesNotContain(" off", sentence, StringComparison.Ordinal);
        Assert.DoesNotContain("1s", sentence, StringComparison.Ordinal);

        /* One deadlock, singular; no metadata, no fabricated count. */
        var one = PgTargetAdvice.DeadlockExemplarSentence(summary with { EngineCounted = 1 });
        Assert.Contains("counted 1 deadlock in the window", one, StringComparison.Ordinal);
        var unknown = PgTargetAdvice.DeadlockExemplarSentence(summary with { EngineCounted = null });
        Assert.Contains("counted a non-zero number of deadlocks", unknown, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSentence_WithExemplars_StatesEveryNumber_AndTheMostFrequentShape()
    {
        var summary = TwoShapes();
        var sentence = PgTargetAdvice.DeadlockExemplarSentence(summary);
        Assert.Equal(
            "Exemplars: 4 reports captured (3 distinct once the overlapping log tail is de-duplicated) against 6 the engine counted, in 2 distinct shapes. " +
            "The most frequent 2-participant shape involves RowExclusiveLock, ShareLock on relation orders, tuple orders with the victim `UPDATE orders SET status = $1 WHERE id = $2`, seen 2 times, last at 2026-09-19 10:45:00 UTC.",
            sentence);

        /* One shape, recurring; every report its own shape; more shapes than carried. */
        var single = summary with { ReportsCaptured = 2, LogCaptured = 2, DistinctShapes = 1, Exemplars = [summary.Exemplars[0]] };
        Assert.Contains("2 reports captured against 6 the engine counted, in 1 shape — one access pattern, recurring.", PgTargetAdvice.DeadlockExemplarSentence(single), StringComparison.Ordinal);
        var allDistinct = summary with { ReportsCaptured = 4, DistinctShapes = 4, Exemplars = [summary.Exemplars[1] with { Reports = 1 }, summary.Exemplars[1], summary.Exemplars[1]] };
        var distinctSentence = PgTargetAdvice.DeadlockExemplarSentence(allDistinct);
        Assert.Contains("in 4 distinct shapes — every report its own pattern, none recurring yet.", distinctSentence, StringComparison.Ordinal);
        Assert.EndsWith("The drill-down carries the top 3; get_pg_deadlocks lists the rest.", distinctSentence, StringComparison.Ordinal);
        /* A shape the parser recovered nothing for still reads as a sentence. */
        var bare = summary with { Exemplars = [summary.Exemplars[0] with { ParticipantCount = null, LockModes = null, Resources = null, VictimStatementFingerprint = null }] };
        Assert.Contains("The most frequent shape (the victim's statement text was not recovered), seen 2 times", PgTargetAdvice.DeadlockExemplarSentence(bare), StringComparison.Ordinal);
    }

    [Fact]
    public void TheFold_AppendsTheSentenceAndTheShapeLever_OnceOnly_AndKeepsTheHeadline()
    {
        var rate = DeadlockRate(6, counter: 6, exemplars: 4, observedHours: 1);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.DeadlockRate, new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.DeadlockRate] = rate })!;
        var folded = PgTargetAdvice.WithDeadlockExemplars(block, TwoShapes());

        Assert.Equal(block.Headline, folded.Headline);
        Assert.StartsWith(block.Investigation, folded.Investigation, StringComparison.Ordinal);
        Assert.Contains("Exemplars: 4 reports captured", folded.Investigation, StringComparison.Ordinal);
        Assert.StartsWith(block.Remediation, folded.Remediation, StringComparison.Ordinal);
        Assert.Contains("Here: the shape to fix first is the one that recurred most — 2-participant shape involves RowExclusiveLock, ShareLock on relation orders, tuple orders", folded.Remediation, StringComparison.Ordinal);
        Assert.Contains("Two participants is the ordering deadlock", folded.Remediation, StringComparison.Ordinal);
        Assert.Contains("take them in ONE order", folded.Remediation, StringComparison.Ordinal);
        Assert.Contains("deadlock_timeout decides only how long a waiter sits", folded.Remediation, StringComparison.Ordinal);
        Assert.Contains("never whether it happens", folded.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("is a chain, not a pair", folded.Remediation, StringComparison.Ordinal);

        /* Idempotent: re-folding the same card does not stack the sentence. */
        Assert.Equal(folded, PgTargetAdvice.WithDeadlockExemplars(folded, TwoShapes()));
        Assert.Equal(1, CountOf(folded.Investigation, "Exemplars: "));

        /* Many participants read as a chain; the ordering fix is explicitly NOT the lever. */
        var chain = TwoShapes();
        var manyFirst = chain with { Exemplars = [chain.Exemplars[1] with { Reports = 5 }, chain.Exemplars[0]] };
        var foldedChain = PgTargetAdvice.WithDeadlockExemplars(block, manyFirst);
        Assert.Contains("3 participants is a chain, not a pair", foldedChain.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("Two participants is the ordering deadlock", foldedChain.Remediation, StringComparison.Ordinal);

        /* No capture: the sentence lands, the remediation is the base one (no shape to point at). */
        var none = new PgTargetDeadlockExemplarSummary(6, 0, "drill-down read of pg_deadlocks", 0, 0, []);
        var foldedNone = PgTargetAdvice.WithDeadlockExemplars(block, none);
        Assert.Contains("the log capture holds none of them", foldedNone.Investigation, StringComparison.Ordinal);
        Assert.Equal(block.Remediation, foldedNone.Remediation);

        /* Round-trips through the frozen StoryText the drill-down rewrites. */
        var thawed = FactAdvice.TryReadStoryText(FactAdvice.SerializeForStoryText(folded))!;
        Assert.Equal(folded.Investigation, thawed.Investigation);
        Assert.Equal(folded.Remediation, thawed.Remediation);

        /* No SQL Server nouns anywhere in the PostgreSQL prose. */
        foreach (var noun in new[] { "sys.", "T-SQL", "SQL Server", "XEvent", "xml_deadlock_report", "DBCC", "trace flag" })
        {
            Assert.DoesNotContain(noun, folded.Investigation + folded.Remediation, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(noun, foldedNone.Investigation + foldedNone.Remediation, StringComparison.OrdinalIgnoreCase);
        }
    }

    /* ───────────────────────── THE EXIT CRITERION (gated) ───────────────────────── */

    /// <summary>
    /// A 6-deadlock counter delta over one observed hour (6 per hour — over the 5-per-hour Warning tier, so the
    /// rate roots a card; with no baseline the first-occurrence anomaly roots beside it) and four captured rows:
    /// shape A twice under two hashes, shape B once, stored twice. Through the REAL <c>analyze_server</c>,
    /// anchored at the planted window's end.
    /// </summary>
    [Fact]
    public async Task APlantedCounterDeltaBesideFourCapturedRows_CarriesTheExemplarDrillDown_AndTheRefrozenAdvice()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the deadlock-exemplar e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-1);

            /* The span gate: one flat row 25 h back. */
            await PlantDatabaseStatsAsync(connection, windowEnd.AddHours(-25), 1000, 0, ct);

            /* Minute -1 … 60: the counter climbs one every ten minutes → 6 over the hour. */
            for (var m = -1; m <= 60; m++)
            {
                var mm = Math.Max(m, 0);
                await PlantDatabaseStatsAsync(connection, windowStart.AddMinutes(m), 1000 + 100L * mm, mm / 10, ct);
            }

            const string modesA = "RowExclusiveLock, ShareLock";
            const string resourcesA = "relation orders, tuple orders";
            /* The later report of shape A as a build before #4005 stored it: a literal in the graph, and a hash
               over that raw graph. The exemplar carries neither (#4005). */
            var graphA = "Process 11 waits for ShareLock on transaction 900; blocked by process 12.\nProcess 12 waits for ShareLock on transaction 901; blocked by process 11.\nProcess 11: UPDATE orders SET status = $1 WHERE id = $2\nProcess 12: UPDATE orders SET status = 'held-4721' WHERE id = 7";
            var rawHashA2 = PgDeadlockLogParser.HashOf(graphA);
            await PlantDeadlockAsync(connection, windowStart.AddMinutes(10), "hash-a1", 2, modesA, resourcesA, "UPDATE orders SET status = $1 WHERE id = $2", graphA, ct);
            await PlantDeadlockAsync(connection, windowStart.AddMinutes(45), rawHashA2, 2, modesA, resourcesA, "UPDATE   orders\n SET status = $1 WHERE id = $2", graphA, ct);

            /* Shape B: a three-way chain with a statement past the text cap and a graph past the line cap, stored
               twice under ONE hash (the overlapping tail re-read). */
            var longStatement = "SELECT " + string.Join(", ", Enumerable.Range(1, 400).Select(i => $"c{i}")) + " FROM wide_table WHERE id = $1";
            Assert.True(longStatement.Length > PgTargetDrillDownCollector.StatementTextCap);
            var thirtyLineGraph = string.Join('\n', Enumerable.Range(1, 30).Select(i => $"Process {i} waits for ShareLock on transaction {1000 + i}; blocked by process {i + 1}."));
            await PlantDeadlockAsync(connection, windowStart.AddMinutes(20), "hash-b1", 3, "ShareLock", "transaction", longStatement, thirtyLineGraph, ct);
            await PlantDeadlockAsync(connection, windowStart.AddMinutes(25), "hash-b1", 3, "ShareLock", "transaction", longStatement, thirtyLineGraph, ct);

            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, as_of: asOf);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();

                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.DeadlockRate);
                Assert.Contains("the engine counted 6; 4 were captured from the log", card.GetProperty("advice").GetProperty("headline").GetString(), StringComparison.Ordinal);

                var exemplars = card.GetProperty("drill_down").GetProperty(PgTargetDrillDownCollector.DeadlockExemplarsSection);
                Assert.Equal(6, exemplars.GetProperty("engine_counted").GetInt32());
                Assert.Equal(4, exemplars.GetProperty("log_captured").GetInt32());
                Assert.Equal($"{PgTargetFactKeys.DeadlockRate}.{PgTargetScorer.DeadlockExemplarCountKey}", exemplars.GetProperty("log_captured_source").GetString());
                Assert.Equal(3, exemplars.GetProperty("reports_captured").GetInt32());
                Assert.Equal(2, exemplars.GetProperty("distinct_shapes").GetInt32());
                Assert.Equal(2, exemplars.GetProperty("exemplars_shown").GetInt32());

                var shapes = exemplars.GetProperty("exemplars").EnumerateArray().ToList();
                Assert.Equal(2, shapes.Count);

                /* Shape A leads: two reports under two hashes; the exemplar is the LATER one. */
                var a = shapes[0];
                Assert.Equal(1, a.GetProperty("rank").GetInt32());
                Assert.Equal(2, a.GetProperty("participant_count").GetInt32());
                Assert.Equal(modesA, a.GetProperty("lock_modes").GetString());
                Assert.Equal(resourcesA, a.GetProperty("resources").GetString());
                Assert.Equal(2, a.GetProperty("reports").GetInt32());
                Assert.Equal(2, a.GetProperty("rows_captured").GetInt32());
                Assert.Equal(PgDeadlockLogParser.LegacyIdentity(windowStart.AddMinutes(45), 4242), a.GetProperty("deadlock_hash").GetString());
                Assert.Equal("UPDATE orders SET status = $1 WHERE id = $2", a.GetProperty("victim_statement_fingerprint").GetString());
                Assert.False(a.GetProperty("victim_statement_may_be_truncated").GetBoolean());
                Assert.Equal(4, a.GetProperty("graph_text_lines_total").GetInt32());
                Assert.False(a.GetProperty("graph_text_truncated").GetBoolean());
                Assert.Equal(graphA.Replace("'held-4721' WHERE id = 7", "'?' WHERE id = ?", StringComparison.Ordinal), a.GetProperty("graph_text").GetString());
                Assert.DoesNotContain(rawHashA2, analysis, StringComparison.Ordinal);
                Assert.DoesNotContain("4721", analysis, StringComparison.Ordinal);

                /* Shape B: one report seen twice; both bounds cut and both say so. */
                var b = shapes[1];
                Assert.Equal(3, b.GetProperty("participant_count").GetInt32());
                Assert.Equal(1, b.GetProperty("reports").GetInt32());
                Assert.Equal(2, b.GetProperty("rows_captured").GetInt32());
                Assert.Equal(PgTargetDrillDownCollector.StatementTextCap, b.GetProperty("victim_statement").GetString()!.Length);
                Assert.True(b.GetProperty("victim_statement_may_be_truncated").GetBoolean());
                Assert.Equal(30, b.GetProperty("graph_text_lines_total").GetInt32());
                Assert.True(b.GetProperty("graph_text_truncated").GetBoolean());
                Assert.Equal(PgTargetDrillDownCollector.GraphTextLineCap, b.GetProperty("graph_text").GetString()!.Split('\n').Length);

                /* The card's frozen prose carries the same numbers — the advice was re-frozen, not left as composed. */
                var investigation = card.GetProperty("advice").GetProperty("investigation").GetString()!;
                Assert.Contains("Exemplars: 4 reports captured (3 distinct once the overlapping log tail is de-duplicated) against 6 the engine counted, in 2 distinct shapes.", investigation, StringComparison.Ordinal);
                Assert.Contains("The most frequent 2-participant shape involves RowExclusiveLock, ShareLock on relation orders, tuple orders with the victim `UPDATE orders SET status = $1 WHERE id = $2`, seen 2 times", investigation, StringComparison.Ordinal);
                Assert.Equal(1, CountOf(investigation, "Exemplars: "));
                Assert.Contains("Two participants is the ordering deadlock", card.GetProperty("advice").GetProperty("remediation").GetString(), StringComparison.Ordinal);
                Assert.Equal(exemplars.GetProperty("note").GetString(), investigation[investigation.IndexOf("Exemplars: ", StringComparison.Ordinal)..]);

                /* The first-occurrence anomaly (no baseline, 6/h over the fallback bar) roots beside it and carries the
                   same drill-down from the OTHER counter spelling and this read's own row total. */
                var anomaly = findings.SingleOrDefault(f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.AnomalyDeadlockRate);
                if (anomaly.ValueKind == JsonValueKind.Object)
                {
                    var anomalyExemplars = anomaly.GetProperty("drill_down").GetProperty(PgTargetDrillDownCollector.DeadlockExemplarsSection);
                    Assert.Equal(6, anomalyExemplars.GetProperty("engine_counted").GetInt32());
                    Assert.Equal(4, anomalyExemplars.GetProperty("log_captured").GetInt32());
                    Assert.Equal("drill-down read of pg_deadlocks", anomalyExemplars.GetProperty("log_captured_source").GetString());
                    Assert.Contains("Exemplars: 4 reports captured", anomaly.GetProperty("advice").GetProperty("investigation").GetString(), StringComparison.Ordinal);
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

    /* ───────────────────────── helpers ───────────────────────── */

    private static PgTargetDeadlockExemplarSummary TwoShapes()
    {
        var a = new PgTargetDeadlockExemplar(
            Rank: 1, ParticipantCount: 2, LockModes: "RowExclusiveLock, ShareLock", Resources: "relation orders, tuple orders",
            Reports: 2, RowsCaptured: 2,
            FirstSeen: new DateTime(2026, 9, 19, 10, 10, 0, DateTimeKind.Unspecified), LastSeen: new DateTime(2026, 9, 19, 10, 45, 0, DateTimeKind.Unspecified),
            DeadlockHash: "hash-a2", VictimPid: 12,
            VictimStatement: "UPDATE orders SET status = $1 WHERE id = $2", VictimStatementMayBeTruncated: false,
            VictimStatementFingerprint: "UPDATE orders SET status = $1 WHERE id = $2",
            GraphText: "Process 11 waits …\nProcess 12 waits …", GraphTextLinesTotal: 4, GraphTextTruncated: false);
        var b = new PgTargetDeadlockExemplar(
            Rank: 2, ParticipantCount: 3, LockModes: "ShareLock", Resources: "transaction",
            Reports: 1, RowsCaptured: 2,
            FirstSeen: new DateTime(2026, 9, 19, 10, 20, 0, DateTimeKind.Unspecified), LastSeen: new DateTime(2026, 9, 19, 10, 20, 0, DateTimeKind.Unspecified),
            DeadlockHash: "hash-b1", VictimPid: 31,
            VictimStatement: "SELECT c1, c2 FROM wide_table WHERE id = $1", VictimStatementMayBeTruncated: true,
            VictimStatementFingerprint: "SELECT c1, c2 FROM wide_table WHERE id = $1",
            GraphText: "Process 1 waits …", GraphTextLinesTotal: 30, GraphTextTruncated: true);
        return new PgTargetDeadlockExemplarSummary(
            EngineCounted: 6, LogCaptured: 4, LogCapturedSource: $"{PgTargetFactKeys.DeadlockRate}.{PgTargetScorer.DeadlockExemplarCountKey}",
            ReportsCaptured: 3, DistinctShapes: 2, Exemplars: [a, b]);
    }

    private static Fact DeadlockRate(double perHour, double counter, double? exemplars, double observedHours)
    {
        var fact = new Fact { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.DeadlockRate, Value = perHour, ServerId = 1, DatabaseName = "appdb" };
        fact.Metadata[PgTargetScorer.DeadlockCounterCountKey] = counter;
        fact.Metadata[PgTargetScorer.DeadlocksPerHourKey] = perHour;
        fact.Metadata[PgTargetScorer.DeadlockObservedHoursKey] = observedHours;
        fact.Metadata[PgTargetScorer.DeadlockTopDatabaseCountKey] = counter;
        fact.Metadata[PgTargetScorer.CounterIntervalsKey] = 60;
        fact.Metadata[PgTargetScorer.CounterStatsResetCountKey] = 0;
        fact.Metadata[PgTargetScorer.CounterRewindCountKey] = 0;
        if (exemplars is { } e) fact.Metadata[PgTargetScorer.DeadlockExemplarCountKey] = e;
        return fact;
    }

    private static int CountOf(string text, string needle) => (text.Length - text.Replace(needle, string.Empty, StringComparison.Ordinal).Length) / needle.Length;

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_database_stats</c> row for one database: the commit counter climbing so the series is
    /// live, the deadlock counter as given, everything else flat.</summary>
    private static async Task PlantDatabaseStatsAsync(NpgsqlConnection connection, DateTime at, long xactCommit, long deadlocks, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', $5, 10, 100, 9000, 0, 0, $6, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(xactCommit);
        command.Parameters.AddWithValue(deadlocks);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantDeadlockAsync(
        NpgsqlConnection connection, DateTime at, string hash, int participants, string lockModes, string resources, string victimStatement, string graphText, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $2, 4242, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(participants);
        command.Parameters.AddWithValue(hash);
        command.Parameters.AddWithValue(lockModes);
        command.Parameters.AddWithValue(resources);
        command.Parameters.AddWithValue(victimStatement);
        command.Parameters.AddWithValue(graphText);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_deadlocks WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
