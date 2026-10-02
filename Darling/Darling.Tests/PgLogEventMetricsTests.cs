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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3602 / #3603: the two parser families that lift NUMBERS out of the log-event pipeline's prose — a
/// spill's bytes, an autovacuum run's cost — into the V130 columns. The fixtures are the lines PostgreSQL
/// ACTUALLY writes, per version, read from <c>vacuumlazy.c</c>, <c>analyze.c</c>, <c>fd.c</c> and
/// <c>pg_rusage.c</c> at REL_16 / REL_17 / REL_18 rather than remembered: the pages clause gained a term in
/// 18, the buffer clause renamed one, the WAL clause gained one and reached the analyze line for the first
/// time. A parser that pins only the version the fleet runs today reads the next major's log as null.
/// </summary>
public sealed class PgLogEventMetricsParserTests
{
    private const string P = "2026-09-18 03:07:12.345 UTC ";

    /* ---- fixtures: the real line shapes, per version ------------------------------------------------- */

    /// <summary>PostgreSQL 16 / 17 <c>automatic vacuum</c> — <c>vacuumlazy.c</c> REL_16_STABLE and REL_17_STABLE emit the identical text.</summary>
    private const string Vacuum16 =
        P + "[3001] LOG:  automatic vacuum of table \"app_db.public.orders\": index scans: 1\n"
        + "\tpages: 12 removed, 48213 remain, 48225 scanned (100.00% of total)\n"
        + "\ttuples: 187654 removed, 9012345 remain, 321 are dead but not yet removable\n"
        + "\tremovable cutoff: 812345678, which was 42 XIDs old when operation ended\n"
        + "\tnew relfrozenxid: 812300000, which is 1234567 XIDs ahead of previous value\n"
        + "\tfrozen: 1024 pages from table (2.12% of total) had 65536 tuples frozen\n"
        + "\tindex scan needed: 9876 pages from table (20.48% of total) had 187654 dead item identifiers removed\n"
        + "\tindex \"orders_pkey\": pages: 24567 in total, 0 newly deleted, 0 currently deleted, 0 reusable\n"
        + "\tindex \"orders_customer_idx\": pages: 12345 in total, 3 newly deleted, 3 currently deleted, 0 reusable\n"
        + "\tI/O timings: read: 1234.567 ms, write: 89.012 ms\n"
        + "\tavg read rate: 12.345 MB/s, avg write rate: 6.789 MB/s\n"
        + "\tbuffer usage: 98765 hits, 43210 misses, 23456 dirtied\n"
        + "\tWAL usage: 54321 records, 1234 full page images, 123456789 bytes\n"
        + "\tsystem usage: CPU: user: 3.21 s, system: 0.87 s, elapsed: 27.35 s\n";

    /// <summary>PostgreSQL 18 <c>automatic vacuum</c> — <c>, N eagerly scanned</c> on pages, <c>reads</c> for <c>misses</c>, <c>, N buffers full</c> on WAL, the new <c>visibility map</c> line.</summary>
    private const string Vacuum18 =
        P + "[3001] LOG:  automatic vacuum of table \"app_db.public.orders\": index scans: 1\n"
        + "\tpages: 12 removed, 48213 remain, 48225 scanned (100.00% of total), 0 eagerly scanned\n"
        + "\ttuples: 187654 removed, 9012345 remain, 321 are dead but not yet removable\n"
        + "\tremovable cutoff: 812345678, which was 42 XIDs old when operation ended\n"
        + "\tfrozen: 1024 pages from table (2.12% of total) had 65536 tuples frozen\n"
        + "\tvisibility map: 47000 pages set all-visible, 1024 pages set all-frozen (0 were all-visible)\n"
        + "\tindex scan needed: 9876 pages from table (20.48% of total) had 187654 dead item identifiers removed\n"
        + "\tindex \"orders_pkey\": pages: 24567 in total, 0 newly deleted, 0 currently deleted, 0 reusable\n"
        + "\tavg read rate: 12.345 MB/s, avg write rate: 6.789 MB/s\n"
        + "\tbuffer usage: 98765 hits, 43210 reads, 23456 dirtied\n"
        + "\tWAL usage: 54321 records, 1234 full page images, 123456789 bytes, 7 buffers full\n"
        + "\tsystem usage: CPU: user: 3.21 s, system: 0.87 s, elapsed: 27.35 s\n";

    /// <summary>PostgreSQL 16 / 17 <c>automatic analyze</c> — no pages, no tuples, no WAL clause (<c>analyze.c</c>).</summary>
    private const string Analyze16 =
        P + "[3002] LOG:  automatic analyze of table \"app_db.public.orders\"\n"
        + "\tavg read rate: 0.000 MB/s, avg write rate: 0.000 MB/s\n"
        + "\tbuffer usage: 1200 hits, 34 misses, 12 dirtied\n"
        + "\tsystem usage: CPU: user: 0.10 s, system: 0.02 s, elapsed: 0.98 s\n";

    /// <summary>PostgreSQL 18 <c>automatic analyze</c> — the first major to write a WAL clause on an analyze line.</summary>
    private const string Analyze18 =
        P + "[3002] LOG:  automatic analyze of table \"app_db.public.orders\"\n"
        + "\tI/O timings: read: 12.345 ms, write: 0.000 ms\n"
        + "\tavg read rate: 0.271 MB/s, avg write rate: 0.096 MB/s\n"
        + "\tbuffer usage: 1200 hits, 34 reads, 12 dirtied\n"
        + "\tWAL usage: 15 records, 0 full page images, 1780 bytes, 0 buffers full\n"
        + "\tsystem usage: CPU: user: 0.10 s, system: 0.02 s, elapsed: 0.98 s\n";

    /// <summary>The wraparound variant, and a relation whose schema name carries a dot — the split is on the FIRST dot.</summary>
    private const string WraparoundVacuum =
        P + "[3003] LOG:  automatic aggressive vacuum to prevent wraparound of table \"app_db.sch.v2.events\": index scans: 0\n"
        + "\tpages: 0 removed, 7 remain, 7 scanned (100.00% of total)\n"
        + "\ttuples: 0 removed, 900 remain, 0 are dead but not yet removable\n"
        + "\tbuffer usage: 30 hits, 0 misses, 0 dirtied\n"
        + "\tWAL usage: 0 records, 0 full page images, 0 bytes\n"
        + "\tsystem usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s\n";

    /// <summary>PostgreSQL 15 and earlier wrote the autoanalyze report as ONE line; the relation and the duration still come out.</summary>
    private const string Analyze15 =
        P + "[3004] LOG:  automatic analyze of table \"app_db.public.orders\" system usage: CPU: user: 0.10 s, system: 0.02 s, elapsed: 1.23 s\n";

    /// <summary>Two spills from one statement (a multi-batch sort), the statement spanning two lines with literals.</summary>
    private const string Spills =
        P + "[4102] LOG:  temporary file: path \"base/pgsql_tmp/pgsql_tmp4102.0\", size 4294967296\n"
        + P + "[4102] STATEMENT:  SELECT o.id, o.note FROM orders AS o\n"
        + "\tWHERE o.customer_id = 1007 AND o.note <> 'gift for O''Brien' ORDER BY o.created_at\n"
        + P + "[4102] LOG:  temporary file: path \"base/pgsql_tmp/pgsql_tmp4102.1\", size 1073741824\n"
        + P + "[4102] STATEMENT:  SELECT o.id, o.note FROM orders AS o\n"
        + "\tWHERE o.customer_id = 1007 AND o.note <> 'gift for O''Brien' ORDER BY o.created_at\n";

    private static List<PgLogEvent> Classify(string text) => new PgLogEventClassifier(TestLogHashKeys.Fixed).Classify(text);

    /* ---- autovacuum ---------------------------------------------------------------------------------- */

    [Fact]
    public void AVacuumLine_On16And17_LiftsEveryFigure_AndTheRelationSplitOnce()
    {
        var run = Classify(Vacuum16).Single();

        Assert.Equal(PgLogFamilies.Autovacuum, run.Family);
        Assert.Equal("app_db", run.DatabaseName);
        Assert.Equal("public.orders", run.Metrics.RelationName);
        Assert.False(run.Metrics.IsAnalyze);
        Assert.Equal(27_350L, run.Metrics.DurationMs);
        Assert.Equal(12L, run.Metrics.PagesRemoved);
        Assert.Equal(48_213L, run.Metrics.PagesRemaining);
        Assert.Equal(187_654L, run.Metrics.TuplesRemoved);
        Assert.Equal(9_012_345L, run.Metrics.TuplesRemaining);
        Assert.Equal(98_765L, run.Metrics.BufferHits);
        Assert.Equal(43_210L, run.Metrics.BufferMisses);
        Assert.Equal(23_456L, run.Metrics.BufferDirtied);
        Assert.Equal(54_321L, run.Metrics.WalRecords);
        Assert.Equal(123_456_789L, run.Metrics.WalBytes);
        /* A spill's member on a vacuum row is null: the families are disjoint. */
        Assert.Null(run.Metrics.Bytes);

        /* The prose is still there, as PostgreSQL wrote it (#3944). */
        Assert.StartsWith("automatic vacuum of table \"app_db.public.orders\": index scans: 1", run.Message, StringComparison.Ordinal);
        Assert.Contains("index \"orders_pkey\": pages: 24567 in total", run.Message, StringComparison.Ordinal);
        Assert.Null(run.StatementFingerprint);
    }

    [Fact]
    public void AVacuumLine_On18_ReadsTheRenamedAndExtendedClauses_ToTheSameColumns()
    {
        var sixteen = Classify(Vacuum16).Single().Metrics;
        var eighteen = Classify(Vacuum18).Single().Metrics;

        /* Same numbers, three changed clause shapes: the columns do not know which version wrote them. */
        Assert.Equal(sixteen, eighteen);
        Assert.Equal(43_210L, eighteen.BufferMisses);
        Assert.Equal(123_456_789L, eighteen.WalBytes);
        Assert.Equal(48_213L, eighteen.PagesRemaining);
    }

    [Fact]
    public void AnAnalyzeLine_LiftsWhatItCarries_AndLeavesTheClausesItDoesNotHaveNull()
    {
        var sixteen = Classify(Analyze16).Single();
        Assert.Equal(PgLogFamilies.Autovacuum, sixteen.Family);
        Assert.True(sixteen.Metrics.IsAnalyze);
        Assert.Equal("public.orders", sixteen.Metrics.RelationName);
        Assert.Equal("app_db", sixteen.DatabaseName);
        Assert.Equal(980L, sixteen.Metrics.DurationMs);
        Assert.Equal(1200L, sixteen.Metrics.BufferHits);
        Assert.Equal(34L, sixteen.Metrics.BufferMisses);
        Assert.Equal(12L, sixteen.Metrics.BufferDirtied);
        /* No pages, tuples or WAL clause on a 16/17 analyze: null, not zero. */
        Assert.Null(sixteen.Metrics.PagesRemoved);
        Assert.Null(sixteen.Metrics.PagesRemaining);
        Assert.Null(sixteen.Metrics.TuplesRemoved);
        Assert.Null(sixteen.Metrics.TuplesRemaining);
        Assert.Null(sixteen.Metrics.WalRecords);
        Assert.Null(sixteen.Metrics.WalBytes);

        /* 18 writes WAL on an analyze for the first time, and it is read. */
        var eighteen = Classify(Analyze18).Single();
        Assert.True(eighteen.Metrics.IsAnalyze);
        Assert.Equal(15L, eighteen.Metrics.WalRecords);
        Assert.Equal(1780L, eighteen.Metrics.WalBytes);
        Assert.Equal(34L, eighteen.Metrics.BufferMisses);
        Assert.Null(eighteen.Metrics.PagesRemoved);

        /* PostgreSQL 15's one-line shape: relation and duration, nothing else, no failure. */
        var fifteen = Classify(Analyze15).Single();
        Assert.Equal(PgLogFamilies.Autovacuum, fifteen.Family);
        Assert.True(fifteen.Metrics.IsAnalyze);
        Assert.Equal("public.orders", fifteen.Metrics.RelationName);
        Assert.Equal(1230L, fifteen.Metrics.DurationMs);
        Assert.Null(fifteen.Metrics.BufferHits);
    }

    [Fact]
    public void TheWraparoundVariant_IsAVacuum_AndTheRelationSplitsOnTheFirstDotOnly()
    {
        var run = Classify(WraparoundVacuum).Single();
        Assert.Equal(PgLogFamilies.Autovacuum, run.Family);
        Assert.False(run.Metrics.IsAnalyze);
        Assert.Equal("app_db", run.DatabaseName);
        Assert.Equal("sch.v2.events", run.Metrics.RelationName);
        Assert.Equal(0L, run.Metrics.DurationMs);
        Assert.Equal(0L, run.Metrics.TuplesRemoved);
        Assert.Equal(900L, run.Metrics.TuplesRemaining);
        Assert.Equal(0L, run.Metrics.WalBytes);

        Assert.Equal((null, "orders"), PgAutovacuumEventParser.SplitRelation("orders"));
        Assert.Equal(("db", "s.t"), PgAutovacuumEventParser.SplitRelation("db.s.t"));
        Assert.Equal((null, "db."), PgAutovacuumEventParser.SplitRelation("db."));
    }

    /// <summary>
    /// PostgreSQL prints through its own <c>snprintf</c> — <c>.</c> always — and the parser reads invariantly,
    /// so a monitoring host in a comma-decimal culture does not turn 27.35 s into 27,350 s. Pinned by
    /// running the parse under de-DE on this thread.
    /// </summary>
    [Fact]
    public void TheNumbers_ParseInvariantly_UnderACommaDecimalCulture()
    {
        using var _ = new CultureScope("de-DE");
        Assert.Equal(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

        var run = Classify(Vacuum16).Single().Metrics;
        Assert.Equal(27_350L, run.DurationMs);
        Assert.Equal(123_456_789L, run.WalBytes);
        Assert.Equal(4_294_967_296L, Classify(Spills).First().Metrics.Bytes);

        Assert.Equal(27_350L, PgAutovacuumEventParser.ElapsedMilliseconds("27.35"));
        Assert.Equal(0L, PgAutovacuumEventParser.ElapsedMilliseconds("0.00"));
        Assert.Equal(5L, PgAutovacuumEventParser.ElapsedMilliseconds("0.005"));
        Assert.Equal(3_600_000L, PgAutovacuumEventParser.ElapsedMilliseconds("3600"));
        Assert.Null(PgAutovacuumEventParser.ElapsedMilliseconds("27,35"));
    }

    /// <summary>Sets the thread's culture for the scope and restores it on dispose — a scope rather than a
    /// try block because <c>LiveCleanupConversionRatchetTests</c> sweeps every <c>finally</c> in a file that
    /// carries the live collection attribute, and this file does.</summary>
    private sealed class CultureScope : IDisposable
    {
        private readonly CultureInfo _before = CultureInfo.CurrentCulture;
        public CultureScope(string name) => CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
        public void Dispose() => CultureInfo.CurrentCulture = _before;
    }

    /* ---- temp_file ----------------------------------------------------------------------------------- */

    [Fact]
    public void ASpill_LiftsItsBytes_FingerprintsItsStatement_AndStoresNoLiteral()
    {
        var spills = Classify(Spills);
        Assert.Equal(2, spills.Count);
        Assert.All(spills, s => Assert.Equal(PgLogFamilies.TempFile, s.Family));

        Assert.Equal(4_294_967_296L, spills[0].Metrics.Bytes);
        Assert.Equal(1_073_741_824L, spills[1].Metrics.Bytes);
        /* Two files from one statement: two events, one fingerprint — the per-execution grain the issue
           asked for, with the statement's shape as the join back to pg_stat_statements' counters. */
        Assert.NotNull(spills[0].StatementFingerprint);
        Assert.Equal(spills[0].StatementFingerprint, spills[1].StatementFingerprint);
        Assert.NotEqual(spills[0].RawLineHash, spills[1].RawLineHash);

        /* The path is not a relation and is not lifted; the message keeps it, as written like any message. */
        Assert.Null(spills[0].Metrics.RelationName);
        Assert.Null(spills[0].Metrics.IsAnalyze);
        Assert.Null(spills[0].Metrics.DurationMs);
        Assert.Equal("temporary file: path \"base/pgsql_tmp/pgsql_tmp4102.0\", size 4294967296", spills[0].Message);

        /* The statement's literals reach nothing stored. */
        foreach (var s in spills)
        {
            var stored = s.Message + s.Detail + s.Context + s.StatementFingerprint + s.Metrics.RelationName;
            Assert.DoesNotContain("O'Brien", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("gift", stored, StringComparison.Ordinal);
            Assert.DoesNotContain("1007", stored, StringComparison.Ordinal);
        }
    }

    /// <summary>A spill line without a size clause (not a shape PostgreSQL writes) is still the family's event, bytes null.</summary>
    [Fact]
    public void ASpillWithoutASizeClause_IsStillAnEvent_WithNullBytes()
    {
        var odd = Classify(P + "[1] LOG:  temporary file: path \"base/pgsql_tmp/pgsql_tmp1.0\"\n").Single();
        Assert.Equal(PgLogFamilies.TempFile, odd.Family);
        Assert.Null(odd.Metrics.Bytes);
    }

    /* ---- the classifier's order, and what did not change --------------------------------------------- */

    [Fact]
    public void TheTwoParsers_TakeTheirLines_AheadOfTheRecognisedArm_AndAcceptEveryShapeItDid()
    {
        var parsers = PgLogEventClassifier.DefaultParsers;
        var tempFile = parsers.ToList().FindIndex(p => p is PgTempFileEventParser);
        var autovacuum = parsers.ToList().FindIndex(p => p is PgAutovacuumEventParser);
        var recognised = parsers.ToList().FindIndex(p => p is PgRecognisedFamilyParser);
        Assert.True(tempFile >= 0 && autovacuum >= 0 && recognised >= 0);
        Assert.True(tempFile < recognised);
        Assert.True(autovacuum < recognised);

        /* The recognised arm no longer claims either family — if it did, a bug in the structured parser
           would be masked by the generic one silently storing the line without its numbers. */
        foreach (var text in new[] { Vacuum16, Vacuum18, Analyze16, Analyze18, WraparoundVacuum, Analyze15, Spills })
        {
            foreach (var entry in PgLogEntryAssembler.Assemble(text))
            {
                Assert.Null(PgRecognisedFamilyParser.FamilyOf(entry));
                Assert.False(new PgRecognisedFamilyParser().TryParse(entry, out _));
            }
        }

        /* And every header variant the #3601 recogniser admitted is admitted by the structured parser. */
        foreach (var header in new[]
        {
            "automatic vacuum of table \"d.s.t\": index scans: 0",
            "automatic aggressive vacuum of table \"d.s.t\": index scans: 0",
            "automatic vacuum to prevent wraparound of table \"d.s.t\": index scans: 0",
            "automatic aggressive vacuum to prevent wraparound of table \"d.s.t\": index scans: 0",
            "automatic analyze of table \"d.s.t\"",
        })
        {
            var entry = PgLogEntryAssembler.Assemble(P + "[1] LOG:  " + header + "\n").Single();
            Assert.True(new PgAutovacuumEventParser().TryParse(entry, out var e), header);
            Assert.Equal("s.t", e.Metrics.RelationName);
            Assert.Equal(header.Contains("analyze", StringComparison.Ordinal), e.Metrics.IsAnalyze);
        }

        /* Checkpoint is untouched: recognised, stored, nothing lifted. A manual VACUUM VERBOSE's INFO lines
           are not autovacuum and are not claimed (severity is INFO, not LOG). */
        var checkpoint = Classify(P + "[2999] LOG:  checkpoint complete: wrote 42 buffers (0.3%); 0 WAL file(s) added, 0 removed, 1 recycled\n").Single();
        Assert.Equal(PgLogFamilies.Checkpoint, checkpoint.Family);
        Assert.Equal(PgLogEventMetrics.None, checkpoint.Metrics);
        Assert.Empty(Classify(P + "[5] INFO:  vacuuming \"app_db.public.orders\"\n"));
        Assert.Empty(Classify(P + "[5] LOG:  finished analyzing table \"app_db.public.orders\"\n"));
    }

    /// <summary>The structured parsers are the seam's implementers, not a change to the seam: the constructor path is still the one, and the row still has no statement column.</summary>
    [Fact]
    public void TheSeam_DidNotChange_ForTheTwoFamilies()
    {
        var from = typeof(PgLogEvent).GetMethods(BindingFlags.Public | BindingFlags.Static).Where(m => m.Name == "From").ToList();
        Assert.Single(from);
        Assert.Contains(from[0].GetParameters(), p => p.Name == "metrics" && p.ParameterType == typeof(PgLogEventMetrics) && p.IsOptional);
        Assert.DoesNotContain(typeof(PgLogEvent).GetProperties(), p => p.Name.Contains("Statement", StringComparison.Ordinal) && p.PropertyType == typeof(string) && p.Name != "StatementFingerprint");
        Assert.DoesNotContain(typeof(PgLogEventMetrics).GetProperties(), p => p.PropertyType == typeof(string) && p.Name != "RelationName");

        /* Every metrics member maps to a V130 column, name for name in snake_case, and in the collector's
           order — read off the positional constructor, which is the one place the order is declared. */
        var members = typeof(PgLogEventMetrics).GetConstructors().OrderByDescending(c => c.GetParameters().Length).First()
            .GetParameters().Select(p => Regex.Replace(p.Name!, "(?<=[a-z])([A-Z])", "_$1").ToLowerInvariant()).ToList();
        Assert.Equal(members, PgLogEventsCollector.Instance.PayloadColumns.Skip(13).Select(c => c.Name));
    }

    /* ---- the reads' shapes --------------------------------------------------------------------------- */

    [Fact]
    public void TheRunShape_IsOneSpelling_OnBothSurfaces_AndKeepsNullsInside()
    {
        var metrics = new DarlingPgLogEventReader.PgLogEventMetricsRow(
            "public.orders", null, 27_350, 12, 48_213, 187_654, 9_012_345, 98_765, 43_210, 23_456, 54_321, 123_456_789, false);
        var json = JsonSerializer.Serialize(DarlingMcpPgLogEventTools.AutovacuumRunShape(metrics), McpHelpers.JsonOptions);
        JsonAssert.Contains("\"kind\": \"vacuum\"", json);
        JsonAssert.Contains("\"duration_ms\": 27350", json);
        JsonAssert.Contains("\"read_mb\": 337.58", json);
        JsonAssert.Contains("\"written_mb\": 183.25", json);
        JsonAssert.Contains("\"wal_bytes\": 123456789", json);
        Assert.DoesNotContain("bytes\":null", json.Replace(" ", string.Empty), StringComparison.Ordinal);

        var analyze = metrics with { IsAnalyze = true, PagesRemoved = null, PagesRemaining = null, TuplesRemoved = null, TuplesRemaining = null, WalRecords = null, WalBytes = null };
        var analyzeJson = JsonSerializer.Serialize(DarlingMcpPgLogEventTools.AutovacuumRunShape(analyze), McpHelpers.JsonOptions);
        JsonAssert.Contains("\"kind\": \"analyze\"", analyzeJson);
        /* Inside the run object a null is kept and means "the line had no such clause". */
        JsonAssert.Contains("\"pages_removed\": null", analyzeJson);
        JsonAssert.Contains("\"wal_bytes\": null", analyzeJson);

        /* recent_runs: aggregates over what was read, the newest five shown, sums skipping nulls and saying so. */
        var at = new DateTime(2026, 9, 18, 3, 0, 0, DateTimeKind.Utc);
        var runs = Enumerable.Range(0, 7).Select(i => new DarlingPgLogEventReader.PgAutovacuumRunRow(
            at.AddMinutes(-i * 10), "app_db", "public.orders", IsAnalyze: i % 3 == 2,
            DurationMs: 1000 * (i + 1), PagesRemoved: 0, PagesRemaining: 100,
            TuplesRemoved: i % 3 == 2 ? null : 50, TuplesRemaining: 1000,
            BufferHits: 10, BufferMisses: 128, BufferDirtied: 64, WalRecords: 5, WalBytes: 1024)).ToList();
        var block = JsonSerializer.Serialize(DarlingMcpPgAutovacuumTools.RecentRuns(runs), McpHelpers.JsonOptions);
        JsonAssert.Contains("\"runs_counted\": 7", block);
        JsonAssert.Contains("\"truncated\": false", block);
        JsonAssert.Contains("\"vacuum_runs\": 5", block);
        JsonAssert.Contains("\"analyze_runs\": 2", block);
        JsonAssert.Contains("\"total_duration_ms\": 28000", block);
        JsonAssert.Contains("\"max_duration_ms\": 7000", block);
        JsonAssert.Contains("\"avg_duration_ms\": 4000", block);
        JsonAssert.Contains("\"total_tuples_removed\": 250", block);
        JsonAssert.Contains("\"runs_with_tuples\": 5", block);
        JsonAssert.Contains("\"total_read_mb\": 7", block);
        JsonAssert.Contains("\"total_wal_bytes\": 7168", block);
        Assert.Equal(DarlingMcpPgAutovacuumTools.RunsShownPerTable, Regex.Matches(block, "\"occurred_at\"").Count);
        Assert.Null(DarlingMcpPgAutovacuumTools.RecentRuns(null));
        Assert.Null(DarlingMcpPgAutovacuumTools.RecentRuns(Array.Empty<DarlingPgLogEventReader.PgAutovacuumRunRow>()));

        /* #3653: the cut is OBSERVED, not inferred. Exactly RunsReadPerTable runs is a COMPLETE history (the old
           `>= RunsReadPerTable` read it as capped); one more is the signal, and the aggregates then cover the
           bound page of RunsReadPerTable, never the 26th row. */
        var exactlyTheCap = Enumerable.Range(0, DarlingMcpPgAutovacuumTools.RunsReadPerTable).Select(i => runs[0] with { OccurredAtUtc = at.AddMinutes(-i) }).ToList();
        var complete = JsonSerializer.Serialize(DarlingMcpPgAutovacuumTools.RecentRuns(exactlyTheCap), McpHelpers.JsonOptions);
        JsonAssert.Contains("\"truncated\": false", complete);
        JsonAssert.Contains($"\"runs_counted\": {DarlingMcpPgAutovacuumTools.RunsReadPerTable}", complete);
        var onePast = Enumerable.Range(0, DarlingMcpPgAutovacuumTools.RunsReadPerTable + 1).Select(i => runs[0] with { OccurredAtUtc = at.AddMinutes(-i) }).ToList();
        var capped = JsonSerializer.Serialize(DarlingMcpPgAutovacuumTools.RecentRuns(onePast), McpHelpers.JsonOptions);
        JsonAssert.Contains("\"truncated\": true", capped);
        JsonAssert.Contains($"\"runs_counted\": {DarlingMcpPgAutovacuumTools.RunsReadPerTable}", capped);
        /* The oldest counted run is the 25th newest, not the 26th: the page is the newest RunsReadPerTable. */
        JsonAssert.Contains("\"oldest_counted_run_at\": \"" + at.AddMinutes(-(DarlingMcpPgAutovacuumTools.RunsReadPerTable - 1)).ToString("yyyy-MM-dd'T'HH:mm:ss"), capped);
    }

    [Fact]
    public void TheReaders_NameTheColumns_AndTheDescriptionsSayWhatANullMeans()
    {
        foreach (var column in PgLogEventsCollector.Instance.PayloadColumns.Skip(13).Select(c => c.Name))
        {
            Assert.Contains("e." + column, DarlingPgLogEventReader.EventsSql, StringComparison.Ordinal);
            Assert.Contains("d." + column, DarlingPgLogEventReader.EventsSql, StringComparison.Ordinal);
        }

        /* The run history: the family pinned in the SQL, the relation list a typed array, the per-relation cap
           a parameter, the same dedupe and chunk bound as the events read. */
        Assert.Contains("e.family = 'autovacuum'", DarlingPgLogEventReader.AutovacuumRunsSql, StringComparison.Ordinal);
        Assert.Contains("e.relation_name = ANY($4::text[])", DarlingPgLogEventReader.AutovacuumRunsSql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn <= $5", DarlingPgLogEventReader.AutovacuumRunsSql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (e.raw_line_hash)", DarlingPgLogEventReader.AutovacuumRunsSql, StringComparison.Ordinal);
        Assert.Contains("e.collection_time >= $2", DarlingPgLogEventReader.AutovacuumRunsSql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"LIMIT\s+\d", DarlingPgLogEventReader.AutovacuumRunsSql);

        var events = typeof(DarlingMcpPgLogEventTools).GetMethods().Single(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name == "get_pg_log_events");
        var eventsDescription = events.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("bytes", eventsDescription, StringComparison.Ordinal);
        Assert.Contains("relation_name", eventsDescription, StringComparison.Ordinal);
        Assert.Contains("only when the line carried them", eventsDescription, StringComparison.Ordinal);
        Assert.Contains("spill-storm", eventsDescription, StringComparison.Ordinal);
        Assert.DoesNotContain("gain structured tables in later work", eventsDescription, StringComparison.Ordinal);

        var health = typeof(DarlingMcpPgAutovacuumTools).GetMethods().Single(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name == "get_pg_autovacuum_health");
        var healthDescription = health.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("recent_runs", healthDescription, StringComparison.Ordinal);
        Assert.Contains("log_autovacuum_min_duration", healthDescription, StringComparison.Ordinal);
        Assert.Contains("never zero", healthDescription, StringComparison.Ordinal);
        /* No new parameter: the run history rides the tool's existing window and page. */
        Assert.Equal(new[] { "postgres", "server_name", "hours_back", "limit", "as_of" }, health.GetParameters().Where(p => p.ParameterType != typeof(CancellationToken)).Select(p => p.Name));

        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgAutovacuumTools.cs");
        Assert.Contains("GetAutovacuumRunsAsync(", tool, StringComparison.Ordinal);
        Assert.Contains("recent_runs = RecentRuns(tableRuns)", tool, StringComparison.Ordinal);
        Assert.Contains("run_history_note", tool, StringComparison.Ordinal);
    }
}

/// <summary>
/// V130 (#3602, #3603): the family-specific columns on <c>collect.pg_log_events</c>. The "I am the top rung"
/// claims this class carried moved to <c>NotificationRoutesRungTests</c> (V131) when that rung landed, the
/// same handoff this class received from <see cref="PgLogEventsRungTests"/> (V129). What stays here is the
/// one-rung-behind half: a store carrying this and not V131 maps to 130, which is the honest answer for it
/// and what makes the upgrade banner correct in both directions.
/// </summary>
public sealed class PgLogEventMetricsRungTests
{
    private const int RungVersion = 130;
    private const int PreviousVersion = 129;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V131 appended
    /// its own — so the invariant that outlives the handoff is that the ordinal is FIXED: a later rung
    /// appends after it and never shifts it.</summary>
    private const int ProbeOrdinal = 105;

    private static readonly string[] Columns =
    {
        "relation_name", "bytes", "duration_ms", "pages_removed", "pages_remaining", "tuples_removed", "tuples_remaining",
        "buffer_hits", "buffer_misses", "buffer_dirtied", "wal_records", "wal_bytes", "is_analyze",
    };

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-log-event-metrics", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* One below the top since V131 landed; the "RungVersion == StorageVersion.SchemaVersion" half of
           the top-arm claim moved to NotificationRoutesRungTests with the top. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion, "V130 is expected to sit below the ladder's top now that V131 has landed");
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>Thirteen nullable, default-less ADD COLUMN IF NOT EXISTS on the one table, schema-qualified; no index, no backfill, no view; and the V101 pairing with V129's CREATE, column for column and type for type.</summary>
    [Fact]
    public void TheRung_AddsTheThirteenColumns_NullableWithoutDefault_AndNothingElse()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;
        Assert.Contains("ALTER TABLE collect.pg_log_events", sql, StringComparison.Ordinal);
        Assert.Equal(Columns.Length, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);
        Assert.Single(Regex.Matches(sql, "ALTER TABLE"));
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE OR REPLACE VIEW", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);

        /* Each column, with the type the collector declares — rendered by the generator's own type mapping so
           this pin cannot disagree with PgSchemaGeneratorTests about what "the type" is. */
        var declared = PgLogEventsCollector.Instance.PayloadColumns.Skip(13).ToList();
        Assert.Equal(Columns, declared.Select(c => c.Name));
        var generated = PgSchemaGenerator.CreateTable(PgLogEventsCollector.Instance);
        var v129 = PgMigrations.Scripts.Single(s => s.Version == PreviousVersion).Sql;
        foreach (var column in declared)
        {
            var rendered = Regex.Match(generated, $@"\b{column.Name} (\w+)").Groups[1].Value;
            Assert.False(string.IsNullOrEmpty(rendered), column.Name);
            Assert.Contains($"ADD COLUMN IF NOT EXISTS {column.Name} {rendered}", sql, StringComparison.Ordinal);
            /* The same column, in V129's CREATE, for the fresh population. */
            Assert.Contains($"    {column.Name} {rendered}", v129, StringComparison.Ordinal);
        }

        /* In the collector's order, on both texts. */
        var alterOrder = Regex.Matches(sql, @"ADD COLUMN IF NOT EXISTS (\w+)").Select(m => m.Groups[1].Value);
        Assert.Equal(Columns, alterOrder);
        var createTail = v129[(v129.IndexOf("raw_line_hash text,", StringComparison.Ordinal) + "raw_line_hash text,".Length)..];
        var createOrder = Regex.Matches(createTail, @"^\s+(\w+) (?:text|bigint|boolean)", RegexOptions.Multiline).Select(m => m.Groups[1].Value);
        Assert.Equal(Columns, createOrder);

        /* The rung doc carries the argument: columns not tables, the V101 rule, nullable/no backfill. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V130 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V130Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start);
        var doc = source[start..end];
        Assert.Contains("not sibling tables", doc, StringComparison.Ordinal);
        Assert.Contains("V101 rule", doc, StringComparison.Ordinal);
        Assert.Contains("Nullable, no DEFAULT, no backfill", doc, StringComparison.Ordinal);
        Assert.Contains("#3602", doc, StringComparison.Ordinal);
        Assert.Contains("#3603", doc, StringComparison.Ordinal);

        /* No table, no collector, no tool: THIS rung moved no census. Restated as the current figures (72 / 29
           since V136's pg_database_size_stats, #3691) rather than as "unchanged", which is the claim made. */
        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(29, CollectorCatalog.All.Count(c => c.TargetEngine == CollectorTargetEngine.PostgreSql));
        Assert.Equal(30, CollectorScheduleDefaults.All["pg_log_events"].RetentionDays);
        Assert.Contains("thirty-five are the PostgreSQL reads", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpInstructions.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains(
            "table_name = 'pg_log_events'\n                                                     AND   column_name = 'wal_bytes'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgLogEventMetrics", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* This rung's sentinel sits strictly BELOW the last argument now that V131 has appended its own; the
           "is the last argument" claim moved to NotificationRoutesRungTests with the top. */
        Assert.True(ProbeOrdinal < arity - 1, "V130's sentinel is expected to sit below the top rung's now that V131 has landed");

        /* Every sentinel true = a fully-migrated store, which must map to exactly the ladder's top. Stated
           against StorageVersion rather than this rung's number, so it survives every later rung. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasPgLogEventMetrics)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasPgLogEvents)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V130 sentinel arm — a store that stopped here would map to 129");
        Assert.True(previousArm >= 0);
        Assert.True(thisArm < previousArm, "the V130 arm sits below the previous rung's, so a store that stopped here maps one rung low");
        Assert.Contains("return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    [Fact]
    public void TheRunbookAndTheReadme_NameBothFamilies()
    {
        var runbook = RepoFile.ReadRepoFile("docs", "postgres-first-target-runbook.md");
        /* #4004: no tool or viewer returns a statement fingerprint, so the runbook no longer promises one beside the bytes. */
        Assert.Contains("A `temp_file` event carries the spill's exact bytes once `log_temp_files` is on (#3602)", runbook, StringComparison.Ordinal);
        Assert.DoesNotContain("fingerprint of the statement that spilled", runbook, StringComparison.Ordinal);
        Assert.Contains("`get_pg_autovacuum_health` shows them per table as `recent_runs` (#3603)", runbook, StringComparison.Ordinal);
        Assert.Contains("| `get_pg_autovacuum_health` | tables ranked by how far past their **own** trigger threshold, each with `recent_runs`", runbook, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        Assert.Contains("**V130** (#3602, #3603) widened the row rather than adding sibling tables", readme, StringComparison.Ordinal);
        Assert.Contains("(#3603, V130 — `get_pg_autovacuum_health` shows them per table as `recent_runs`)", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("for the structured tables #3602 / #3603 add", readme, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the V130 columns: the parsers' rows through the collector's
/// COPY into the migrated store, then both reads — <c>get_pg_log_events</c> with the lifted figures on the
/// families that carry them, and <c>get_pg_autovacuum_health</c> with <c>recent_runs</c> attached to the
/// table whose catalog row it also holds. Serialized against every other live class because it writes the
/// shared store.
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogEventMetricsLivePostgresTests
{
    private const string ServerName = "darling-pg-log-event-metrics-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheLiftedFigures_SurviveCopy_AndReachBothReads()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live log-event metrics test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* The V130 columns exist on the migrated store with the collector's types — the ALTER ran, or the
               CREATE carried them; either way information_schema agrees with PayloadColumns. */
            await using (var probe = new NpgsqlCommand(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'pg_log_events' AND ordinal_position > 17 ORDER BY ordinal_position", connection))
            await using (var reader = await probe.ExecuteReaderAsync(ct))
            {
                var columns = new List<(string, string)>();
                while (await reader.ReadAsync(ct)) columns.Add((reader.GetString(0), reader.GetString(1)));
                Assert.Equal(
                    PgLogEventsCollector.Instance.PayloadColumns.Skip(13).Select(c => c.Name),
                    columns.Select(c => c.Item1));
                Assert.Equal("text", columns[0].Item2);
                Assert.Equal("boolean", columns[^1].Item2);
                Assert.All(columns.Skip(1).Take(11), c => Assert.Equal("bigint", c.Item2));
            }

            var stamp = DateTime.UtcNow.AddMinutes(-3).ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
            var fixture = string.Concat(
                Fixture("Vacuum18"), Fixture("Analyze18"), Fixture("Vacuum16"), Fixture("Spills"), Fixture("WraparoundVacuum"))
                .Replace("2026-09-18 03:07:12.345", stamp, StringComparison.Ordinal);
            var events = new PgLogEventClassifier(TestLogHashKeys.Fixed).Classify(fixture);
            Assert.Equal(6, events.Count);
            await WriteAsync(postgres, events, ct);

            /* Distinct raw text per entry — the two 18/16 vacuums differ in their clause shapes, so they are
               two events; the two spills differ in path and size. */
            var vacuums = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "autovacuum", null, 100);
            JsonAssert.Contains("\"events_returned\": 4", vacuums);
            JsonAssert.Contains("\"relation_name\": \"public.orders\"", vacuums);
            JsonAssert.Contains("\"relation_name\": \"sch.v2.events\"", vacuums);
            JsonAssert.Contains("\"duration_ms\": 27350", vacuums);
            JsonAssert.Contains("\"tuples_removed\": 187654", vacuums);
            JsonAssert.Contains("\"wal_bytes\": 123456789", vacuums);
            JsonAssert.Contains("\"kind\": \"analyze\"", vacuums);
            JsonAssert.Contains("\"wal_bytes\": 1780", vacuums);
            JsonAssert.Contains("\"read_mb\": 337.58", vacuums);

            var spills = await DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, 1, "temp_file", null, 100);
            JsonAssert.Contains("\"events_returned\": 2", spills);
            JsonAssert.Contains("\"bytes\": 4294967296", spills);
            JsonAssert.Contains("\"bytes\": 1073741824", spills);
            JsonAssert.Contains("\"mb\": 1024", spills);
            Assert.DoesNotContain("O'Brien", spills, StringComparison.Ordinal);

            /* The run history reader, directly: per-relation cap and the newest-first order. */
            var runs = await DarlingPgLogEventReader.GetAutovacuumRunsAsync(
                postgres, ServerId, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow.AddMinutes(5), new[] { "public.orders", "sch.v2.events", "nope.nothing" }, 2, ct);
            Assert.Equal(3, runs.Count);
            Assert.Equal(2, runs.Count(r => r.RelationName == "public.orders"));
            Assert.Single(runs, r => r.RelationName == "sch.v2.events");

            /* And beside the catalog half: a pg_autovacuum_stats row for the same table gets recent_runs; one
               for a table with no run event gets null and the note says why it cannot tell. */
            await SeedAutovacuumStatsAsync(connection, ct, "orders", deadTuples: 5000);
            await SeedAutovacuumStatsAsync(connection, ct, "quiet", deadTuples: 4000);
            var health = await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(postgres, ServerName, 1, 20);
            JsonAssert.Contains("\"status\": \"tables_with_pending_maintenance\"", health);
            JsonAssert.Contains("\"tables_with_run_history\": 1", health);
            JsonAssert.Contains("\"runs_counted\": 3", health);
            JsonAssert.Contains("\"vacuum_runs\": 2", health);
            JsonAssert.Contains("\"analyze_runs\": 1", health);
            JsonAssert.Contains("\"max_duration_ms\": 27350", health);
            JsonAssert.Contains("\"total_wal_bytes\": 246915358", health);
            JsonAssert.Contains("\"recent_runs\": null", health);
            Assert.Contains("recent_runs on 1 of 2 tables", health, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_autovacuum_stats WHERE server_id = $1", ServerId);
    }

    private static string Fixture(string name)
        => (string)typeof(PgLogEventMetricsParserTests).GetField(name, BindingFlags.NonPublic | BindingFlags.Static)!.GetRawConstantValue()!;

    private static async Task SeedAutovacuumStatsAsync(NpgsqlConnection connection, CancellationToken ct, string tableName, long deadTuples) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_autovacuum_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name,
     live_tuples, dead_tuples, vacuum_threshold, mods_since_analyze, analyze_threshold,
     inserts_since_vacuum, insert_vacuum_threshold, autovacuum_disabled, total_bytes, autovacuum_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2)),
            ServerId, ServerName, "app_db", "public", tableName,
            10_000L, deadTuples, 1000L, 0L, 500L,
            0L, 1000L, false, 1_000_000L, 1L);

    private static async Task WriteAsync(NpgsqlDataSource postgres, IReadOnlyList<PgLogEvent> rows, CancellationToken ct)
    {
        var definition = PgLogEventsCollector.Instance;
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        var writer = new PgCollectorRowWriter();
        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        var context = new CollectorContext
        {
            LogHashKey = TestLogHashKeys.Fixed,
            ServerId = ServerId, ServerName = ServerName, CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(), Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(collectionTime).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }
        await importer.CompleteAsync(ct);
    }
}
