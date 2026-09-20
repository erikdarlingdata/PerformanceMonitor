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
/// Lane 29 of #3691 — the buffer-composition drill-down beside <c>PG_BUFFER_CACHE_PRESSURE</c>: the three reads
/// by their own text (latest capture taken whole, the totals as window aggregates, the two "why nothing" reads),
/// the dispatch on the pressure key alone, the kind and cold partitions, the four-arm sentence with its numbers,
/// the advice fold and its idempotence, and — gated on <c>DARLING_TEST_PG</c> — the exit criterion through the
/// REAL <c>analyze_server</c>: a planted hit-ratio shortage beside a 20-relation capture yields the memory-chain
/// story carrying <c>pool_fill</c>, ten <c>top_relations</c>, <c>cold_share</c>, the kind shares summing to 1, the
/// root fact's pressure figures reused, and the re-frozen advice with the same sentence; a second server with the
/// same shortage, no capture and <c>pg_buffercache</c> merely <c>available</c> carries the extension arm instead.
///
/// <para>Every pure value asserted here was executed on this machine through a net10.0 harness over the built
/// assemblies before the first CI run; the SQL and the e2e were executed against a throwaway PostgreSQL 18 store.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetBufferDrillDownTests
{
    private const string ServerName = "darling-pg-target-buffer-composition-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string NoCaptureServerName = "darling-pg-target-buffer-composition-nocapture-e2e";
    private static readonly int NoCaptureServerId = ServerIdHelper.GetDeterministicHashCode(NoCaptureServerName);

    /* ───────────────────────── the reads ───────────────────────── */

    [Fact]
    public void TheCompositionSql_TakesTheLatestCaptureWhole_TotalsByWindowFunction_AndBoundsInTheRead()
    {
        var sql = PgTargetDrillDownCollector.PgTargetBufferCompositionSql;
        Assert.Contains("FROM pg_buffer_usage", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT max(collection_time) AS at", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("JOIN newest AS n ON b.collection_time = n.at", sql, StringComparison.Ordinal);
        /* The totals over the capture, on every row; never a second read. */
        Assert.Contains("CROSS JOIN totals AS t", sql, StringComparison.Ordinal);
        Assert.Contains("max(pool_buffers_total)", sql, StringComparison.Ordinal);
        Assert.Contains("max(pool_buffers_used)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(buffers) FILTER (WHERE relation_kind IN ('r', 'm'))", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(buffers) FILTER (WHERE relation_kind = 'i')", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(buffers) FILTER (WHERE relation_kind = 't')", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(buffers) FILTER (WHERE avg_usage_count <= $6)", sql, StringComparison.Ordinal);
        /* Bounded in the read: the cap and the name bound with its length for the flag. */
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(c.relation_name, $5)", sql, StringComparison.Ordinal);
        Assert.Contains("length(c.relation_name)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY c.buffers DESC, c.relation_name NULLS LAST", sql, StringComparison.Ordinal);

        var last = PgTargetDrillDownCollector.PgTargetBufferLastCaptureSql;
        Assert.Contains("SELECT max(collection_time) AS last_capture", last, StringComparison.Ordinal);
        Assert.Contains("FROM pg_buffer_usage", last, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", last, StringComparison.Ordinal);

        var extension = PgTargetDrillDownCollector.PgTargetBufferCacheExtensionSql;
        Assert.Contains("FROM pg_extension_availability", extension, StringComparison.Ordinal);
        Assert.Contains("extension_name = 'pg_buffercache'", extension, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $2", extension, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT e.state", extension, StringComparison.Ordinal);

        /* Store clock discipline and dialect, on all three; only collector tables named. */
        var tables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);
        foreach (var text in new[] { sql, last, extension })
        {
            Assert.DoesNotContain("now(", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("@", text, StringComparison.Ordinal);
            Assert.DoesNotContain("FROM v_", text, StringComparison.Ordinal);
            var ctes = System.Text.RegularExpressions.Regex.Matches(text, @"(?:WITH|,)\s*(\w+)\s+AS\s*\(").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(text, @"\b(?:FROM|JOIN)\s+(\w+)", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                Assert.True(tables.Contains(m.Groups[1].Value) || ctes.Contains(m.Groups[1].Value), $"'{m.Groups[1].Value}' is neither a collector table nor a CTE");
        }

        Assert.Equal(10, PgTargetDrillDownCollector.BufferTopRelationCap);
        Assert.Equal(63, PgTargetDrillDownCollector.RelationNameCap);
        Assert.Equal(1.0, PgTargetDrillDownCollector.ColdUsageCountCeiling);
        Assert.Equal("pg_buffer_composition", PgTargetDrillDownCollector.BufferCompositionSection);
    }

    [Fact]
    public void TheDrillDown_AttachesOnThePressureKeyOnly_ReusesTheRootFactFigures_AndRefreezesTheAdvice()
    {
        var root = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.cs");
        Assert.Contains("if (pathKeys.Contains(PgTargetFactKeys.BufferCachePressure))", root, StringComparison.Ordinal);
        Assert.Contains("await CollectBufferCompositionAsync(finding, context);", root, StringComparison.Ordinal);
        /* NOT on the knob: the shared_buffers advisory rooting alone (0.4) is not enriched. */
        Assert.DoesNotContain("pathKeys.Contains(PgTargetFactKeys.ConfigSharedBuffers)", root, StringComparison.Ordinal);
        Assert.Contains("private partial Task CollectBufferCompositionAsync(AnalysisFinding finding, AnalysisContext context);", root, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.Buffer.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(code, @"CommandTimeout = DarlingAnalysisService\.AnalysisCommandTimeoutSeconds").Count);
        Assert.Contains("new NpgsqlCommand(PgTargetBufferCompositionSql, connection)", code, StringComparison.Ordinal);
        Assert.Contains("new NpgsqlCommand(PgTargetBufferLastCaptureSql, connection)", code, StringComparison.Ordinal);
        Assert.Contains("new NpgsqlCommand(PgTargetBufferCacheExtensionSql, connection)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ExecuteReaderAsync()", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ReadAsync()", code, StringComparison.Ordinal);
        /* The pressure figures are the root fact's; never a second read of the counter tables. */
        Assert.Contains("finding.RootFactMetadata", code, StringComparison.Ordinal);
        Assert.Contains("miss_share", source, StringComparison.Ordinal);
        Assert.Contains("hit_ratio_suppressed", source, StringComparison.Ordinal);
        Assert.Contains("evictions_per_sec", source, StringComparison.Ordinal);
        Assert.Contains("cache_turnovers_per_hour", source, StringComparison.Ordinal);
        Assert.Contains("buffers_alloc_per_sec", source, StringComparison.Ordinal);
        /* In CODE and in the SQL consts — the prose may name the tables to say they are NOT read. */
        var sqlTexts = PgTargetDrillDownCollector.PgTargetBufferCompositionSql + PgTargetDrillDownCollector.PgTargetBufferLastCaptureSql + PgTargetDrillDownCollector.PgTargetBufferCacheExtensionSql;
        foreach (var counterTable in new[] { "pg_database_stats", "pg_io_stats", "pg_write_stats" })
        {
            Assert.DoesNotContain(counterTable, code, StringComparison.Ordinal);
            Assert.DoesNotContain(counterTable, sqlTexts, StringComparison.Ordinal);
        }
        /* One object under one section; the advice re-frozen through the composer, not hand-written here. */
        Assert.Contains("finding.DrillDown![BufferCompositionSection]", code, StringComparison.Ordinal);
        Assert.Contains("FactAdvice.TryReadStoryText(finding.StoryText)", code, StringComparison.Ordinal);
        Assert.Contains("FactAdvice.SerializeForStoryText(PgTargetAdvice.WithBufferComposition(frozen, summary))", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetAdvice.BufferCompositionSentence(summary)", code, StringComparison.Ordinal);
        /* No DeSkew in CODE (pg_buffer_usage stamps collection_time host-UTC); block size by name, not 8192 in code. */
        Assert.DoesNotContain("DeSkew", code, StringComparison.Ordinal);
        Assert.Contains("PgSettingValue.DefaultBlockBytes", code, StringComparison.Ordinal);
        Assert.DoesNotContain("8192", code, StringComparison.Ordinal);
    }

    /* ───────────────────────── the partitions ───────────────────────── */

    [Fact]
    public void KindLabel_PartitionsRelkindTheWayTheSqlDoes_AndTheNullKindIsOther()
    {
        Assert.Equal("heap", PgTargetDrillDownCollector.KindLabel("r"));
        Assert.Equal("heap", PgTargetDrillDownCollector.KindLabel("m"));
        Assert.Equal("index", PgTargetDrillDownCollector.KindLabel("i"));
        Assert.Equal("toast", PgTargetDrillDownCollector.KindLabel("t"));
        Assert.Equal("other", PgTargetDrillDownCollector.KindLabel("S"));
        Assert.Equal("other", PgTargetDrillDownCollector.KindLabel(null));
        Assert.Equal("other", PgTargetDrillDownCollector.KindLabel(string.Empty));
        /* Partitioned parents have no storage and never appear; if one did, it is not a heap. */
        Assert.Equal("other", PgTargetDrillDownCollector.KindLabel("p"));
        Assert.Equal("other", PgTargetDrillDownCollector.KindLabel("I"));
    }

    [Fact]
    public void ClassifyExtensionStates_InstalledAnywhereIsCadence_ThenAvailable_ThenAbsent_AndNoRowIsCadence()
    {
        Assert.Equal(PgTargetBufferCompositionStatus.NoCapture, PgTargetDrillDownCollector.ClassifyExtensionStates([]));
        Assert.Equal(PgTargetBufferCompositionStatus.NoCapture, PgTargetDrillDownCollector.ClassifyExtensionStates(["installed"]));
        Assert.Equal(PgTargetBufferCompositionStatus.NoCapture, PgTargetDrillDownCollector.ClassifyExtensionStates(["outdated"]));
        /* Installed in one database, available in another: the collector can run — cadence. */
        Assert.Equal(PgTargetBufferCompositionStatus.NoCapture, PgTargetDrillDownCollector.ClassifyExtensionStates(["available", "installed"]));
        Assert.Equal(PgTargetBufferCompositionStatus.ExtensionAvailable, PgTargetDrillDownCollector.ClassifyExtensionStates(["available"]));
        Assert.Equal(PgTargetBufferCompositionStatus.ExtensionAvailable, PgTargetDrillDownCollector.ClassifyExtensionStates(["absent", "available"]));
        Assert.Equal(PgTargetBufferCompositionStatus.ExtensionAbsent, PgTargetDrillDownCollector.ClassifyExtensionStates(["absent"]));
        /* An unknown spelling claims nothing. */
        Assert.Equal(PgTargetBufferCompositionStatus.NoCapture, PgTargetDrillDownCollector.ClassifyExtensionStates(["something-else"]));
    }

    [Fact]
    public void Share_IsNullOverAnEmptyDenominator_AndIsTruncated_FlagsOnlyARealCut()
    {
        Assert.Null(PgTargetDrillDownCollector.Share(5, 0));
        Assert.Equal(0.25, PgTargetDrillDownCollector.Share(1, 4)!.Value, precision: 12);
        Assert.Equal(0.0, PgTargetDrillDownCollector.Share(0, 4)!.Value, precision: 12);

        Assert.False(PgTargetDrillDownCollector.IsTruncated(null, 100));
        Assert.False(PgTargetDrillDownCollector.IsTruncated(string.Empty, 100));
        Assert.False(PgTargetDrillDownCollector.IsTruncated("orders", 6));
        Assert.True(PgTargetDrillDownCollector.IsTruncated("orders", 7));
    }

    [Fact]
    public void TheKindSharesAndTheColdShare_AreOverTheListedBuffers_AndSumToOne()
    {
        var summary = TwentyRelations();
        Assert.Equal(1.0, summary.HeapShare!.Value + summary.IndexShare!.Value + summary.ToastShare!.Value + summary.OtherShare!.Value, precision: 12);
        Assert.Equal(6500 / 13300.0, summary.HeapShare.Value, precision: 12);
        Assert.Equal(5000 / 13300.0, summary.IndexShare.Value, precision: 12);
        Assert.Equal(1000 / 13300.0, summary.ToastShare.Value, precision: 12);
        Assert.Equal(800 / 13300.0, summary.OtherShare.Value, precision: 12);
        Assert.Equal(4500 / 13300.0, summary.ColdShare!.Value, precision: 12);
        Assert.Equal(15400 / 16384.0, summary.PoolFill!.Value, precision: 12);
        Assert.Equal(1650 / 16384.0, summary.DirtyShareOfPool!.Value, precision: 12);
        Assert.Equal(128L * 1024 * 1024, summary.PoolBytes);
        Assert.Equal(10, summary.TopRelations.Count);
        Assert.Equal(20, summary.RelationsInCapture);
    }

    /* ───────────────────────── the sentence and the fold ───────────────────────── */

    /// <summary>The captured arm, as one string — the same string the e2e below expects from the real pass.</summary>
    internal const string ExpectedCapturedSentence =
        "Composition: the pool is 94 % full (15,400 of 16,384 buffers, 128 MB); `orders_idx` (index, appdb) alone holds 30.5 %; " +
        "of the resident buffers, indexes hold 37.6 %, heaps 48.9 %, TOAST 7.5 %, other databases and shared catalogs 6 %; " +
        "10.1 % of the pool is dirty; 33.8 % of resident buffers sit in relations whose average usage count is at or below 1 — the clock sweep evicts those first. " +
        "Captured at 2026-09-19 10:30:00 UTC — a level, not the window's counters; pg_buffer_usage collects hourly. " +
        "The drill-down carries the top 10 of 20 resident relations; get_pg_buffer_usage lists the rest.";

    [Fact]
    public void TheSentence_WithACapture_StatesEveryNumber_FromTheSummaryAlone()
    {
        Assert.Equal(ExpectedCapturedSentence, PgTargetAdvice.BufferCompositionSentence(TwentyRelations()));

        /* No "other" share → the clause is dropped; an unnameable top relation is described, never "" or "null". */
        var noOther = TwentyRelations() with { OtherShare = 0.0 };
        Assert.DoesNotContain("other databases and shared catalogs", PgTargetAdvice.BufferCompositionSentence(noOther), StringComparison.Ordinal);
        var foreignTop = TwentyRelations() with { TopRelations = [new PgTargetBufferResident(1, "otherdb", null, false, null, "other", 800, 800 / 16384.0, 0, 0, 2.0)] };
        Assert.Contains("a relation of database otherdb (not nameable from the collector's database) alone holds 4.9 %", PgTargetAdvice.BufferCompositionSentence(foreignTop), StringComparison.Ordinal);
        var sharedTop = TwentyRelations() with { TopRelations = [new PgTargetBufferResident(1, null, null, false, null, "other", 800, 800 / 16384.0, 0, 0, 2.0)] };
        Assert.Contains("a shared catalog (no relation name from here) alone holds", PgTargetAdvice.BufferCompositionSentence(sharedTop), StringComparison.Ordinal);
        /* A cut name carries the ellipsis into the prose. */
        var cut = TwentyRelations() with { TopRelations = [TwentyRelations().TopRelations[0] with { RelationNameTruncated = true }] };
        Assert.Contains("`orders_idx…` (index, appdb)", PgTargetAdvice.BufferCompositionSentence(cut), StringComparison.Ordinal);
        /* Fewer relations than the cap: no "top N of M" clause. */
        var few = TwentyRelations() with { RelationsInCapture = 10 };
        Assert.DoesNotContain("The drill-down carries the top", PgTargetAdvice.BufferCompositionSentence(few), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSentence_WithNothingToShow_SaysWhich_AndStatesNoNumberItDidNotRead()
    {
        var windowStart = new DateTime(2026, 9, 19, 10, 0, 0, DateTimeKind.Unspecified);
        var none = Empty(PgTargetBufferCompositionStatus.NoCapture) with { LastCaptureAt = windowStart.AddHours(-5.5), LastCaptureHoursBeforeWindow = 5.5 };
        Assert.Equal("Composition: no buffer-cache capture in the window (pg_buffer_usage collects hourly; the last capture was 5.5 h before the window).", PgTargetAdvice.BufferCompositionSentence(none));
        Assert.Equal("Composition: no buffer-cache capture in the window (pg_buffer_usage collects hourly; the last capture was 30 minutes before the window).",
            PgTargetAdvice.BufferCompositionSentence(none with { LastCaptureHoursBeforeWindow = 0.5 }));
        Assert.Equal("Composition: no buffer-cache capture in the window (pg_buffer_usage collects hourly; the last capture was 3 days before the window).",
            PgTargetAdvice.BufferCompositionSentence(none with { LastCaptureHoursBeforeWindow = 72 }));
        Assert.Equal("Composition: no buffer-cache capture in the window (pg_buffer_usage collects hourly; this server has no capture at all).",
            PgTargetAdvice.BufferCompositionSentence(Empty(PgTargetBufferCompositionStatus.NoCapture)));
        Assert.Equal("Composition: no buffer-cache capture in the window (pg_buffer_usage collects hourly; the last capture is after the window).",
            PgTargetAdvice.BufferCompositionSentence(none with { LastCaptureHoursBeforeWindow = -2 }));

        var available = PgTargetAdvice.BufferCompositionSentence(Empty(PgTargetBufferCompositionStatus.ExtensionAvailable));
        Assert.StartsWith("Composition: what the cache holds cannot be shown yet — pg_buffercache is offered by this server but not installed", available, StringComparison.Ordinal);
        Assert.Contains("one CREATE EXTENSION pg_buffercache away", available, StringComparison.Ordinal);
        Assert.Contains("pg_extension_availability", available, StringComparison.Ordinal);

        var absent = PgTargetAdvice.BufferCompositionSentence(Empty(PgTargetBufferCompositionStatus.ExtensionAbsent));
        Assert.StartsWith("Composition: what the cache holds cannot be shown — pg_buffercache is not offered by this server", absent, StringComparison.Ordinal);
        Assert.Contains("the pressure counters above stand on their own", absent, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE EXTENSION pg_buffercache away", absent, StringComparison.Ordinal);

        /* None of the empty arms states a pool number. */
        foreach (var sentence in new[] { available, absent, PgTargetAdvice.BufferCompositionSentence(none) })
        {
            Assert.DoesNotContain("% full", sentence, StringComparison.Ordinal);
            Assert.DoesNotContain("alone holds", sentence, StringComparison.Ordinal);
            Assert.DoesNotContain("CREATE INDEX", sentence, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void TheFold_AppendsTheSentence_ExtendsTheSizingSentenceWithoutRepeatingIt_OnceOnly_AndKeepsTheHeadline()
    {
        var pressure = PressureFact(missShare: 0.5, aurora: false);
        var knob = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigSharedBuffers, Value = 128, ServerId = 1 };
        knob.Metadata["bytes"] = 128.0 * 1024 * 1024;
        var facts = new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.BufferCachePressure] = pressure, [PgTargetFactKeys.ConfigSharedBuffers] = knob };
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.BufferCachePressure, facts)!;
        var folded = PgTargetAdvice.WithBufferComposition(block, TwentyRelations());

        Assert.Equal(block.Headline, folded.Headline);
        Assert.StartsWith(block.Investigation, folded.Investigation, StringComparison.Ordinal);
        Assert.EndsWith(ExpectedCapturedSentence, folded.Investigation, StringComparison.Ordinal);
        Assert.StartsWith(block.Remediation, folded.Remediation, StringComparison.Ordinal);
        Assert.Contains("What the capture says about sizing:", folded.Remediation, StringComparison.Ordinal);
        /* 33.8 % cold, below the majority: the cache is being re-read. No dominant relation (30.5 % < 50 %), pool full, not Aurora. */
        Assert.Contains("Only 33.8 % of resident buffers are cold", folded.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("holds 30.5 % of the pool on its own", folded.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("The pool is only", folded.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("On Aurora", folded.Remediation, StringComparison.Ordinal);
        /* The D1 counter-objective is stated ONCE — lane 2's sentence, not repeated by the fold. */
        Assert.Equal(1, CountOf(folded.Remediation, "Counter-objectives:"));
        Assert.Equal(1, CountOf(folded.Remediation, "operating system's page cache"));

        /* Idempotent: re-folding the same card does not stack the sentence. */
        Assert.Equal(folded, PgTargetAdvice.WithBufferComposition(folded, TwentyRelations()));
        Assert.Equal(1, CountOf(folded.Investigation, "Composition: "));

        /* A dominant relation, a cold majority, a pool with room, on Aurora: every arm of the sizing paragraph. */
        var dominant = TwentyRelations() with
        {
            TopRelations = [TwentyRelations().TopRelations[0] with { Buffers = 9000, ShareOfPool = 9000 / 16384.0 }],
            ColdShare = 0.62,
            PoolFill = 0.75,
            HitRatioSuppressed = true,
        };
        var foldedDominant = PgTargetAdvice.WithBufferComposition(block, dominant);
        Assert.Contains("`orders_idx` (index, appdb) holds 54.9 % of the pool on its own — before the knob, look at what reads it (get_pg_top_queries", foldedDominant.Remediation, StringComparison.Ordinal);
        Assert.Contains("62 % of resident buffers sit in relations whose average usage count is at or below 1", foldedDominant.Remediation, StringComparison.Ordinal);
        Assert.Contains("raising it buys little here and still pays the counter-objective named above", foldedDominant.Remediation, StringComparison.Ordinal);
        Assert.Contains("The pool is only 75 % full: shared_buffers is not the constraint while it has room", foldedDominant.Remediation, StringComparison.Ordinal);
        Assert.Contains("On Aurora the hit ratio is stated but not graded", foldedDominant.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("Only 62 %", foldedDominant.Remediation, StringComparison.Ordinal);

        /* Nothing to show: the sentence lands, the remediation is the base one (no capture to size against). */
        foreach (var status in new[] { PgTargetBufferCompositionStatus.NoCapture, PgTargetBufferCompositionStatus.ExtensionAvailable, PgTargetBufferCompositionStatus.ExtensionAbsent })
        {
            var foldedEmpty = PgTargetAdvice.WithBufferComposition(block, Empty(status));
            Assert.Contains("Composition: ", foldedEmpty.Investigation, StringComparison.Ordinal);
            Assert.Equal(block.Remediation, foldedEmpty.Remediation);
        }

        /* Round-trips through the frozen StoryText the drill-down rewrites. */
        var thawed = FactAdvice.TryReadStoryText(FactAdvice.SerializeForStoryText(folded))!;
        Assert.Equal(folded.Investigation, thawed.Investigation);
        Assert.Equal(folded.Remediation, thawed.Remediation);

        /* No SQL Server nouns, no DDL (D8), no effective_cache_size as a memory figure (§6 D), no posture knob. */
        foreach (var noun in new[] { "sys.", "T-SQL", "SQL Server", "buffer pool extension", "DBCC", "max server memory", "CREATE INDEX", "effective_cache_size", "fsync", "synchronous_commit", "full_page_writes" })
        {
            Assert.DoesNotContain(noun, folded.Investigation + folded.Remediation, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(noun, foldedDominant.Investigation + foldedDominant.Remediation, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void ThePressureStorysNextTools_NameTheCompositionRead_AndTheStatementsBehindIt()
    {
        var tools = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.BufferCachePressure)!.Select(r => r.Tool).ToList();
        Assert.Contains("get_pg_buffer_usage", tools);
        Assert.Contains("get_pg_top_queries", tools);
        Assert.Equal(tools.Count, tools.Distinct(StringComparer.Ordinal).Count());
    }

    /* ───────────────────────── THE EXIT CRITERION (gated) ───────────────────────── */

    /// <summary>
    /// A hit-ratio shortage — 3,000 hits and 3,000 reads a minute across one hour (miss share 0.5, 100 blocks/s: at
    /// the critical line, so the composite grades 1.0 and roots the memory chain over the default
    /// <c>shared_buffers</c>) — beside a 20-relation capture 30 minutes before the window's end and an OLDER capture
    /// three hours earlier that must NOT be the one read. Through the REAL <c>analyze_server</c>, anchored at the
    /// planted window's end. A second server with the same shortage, no capture and <c>pg_buffercache</c>
    /// <c>available</c> in <c>pg_extension_availability</c> carries the extension arm.
    /// </summary>
    [Fact]
    public async Task APlantedShortageBesideATwentyRelationCapture_CarriesTheCompositionDrillDown_AndTheRefrozenAdvice()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the buffer-composition e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-1);

            foreach (var (id, name) in new[] { (ServerId, ServerName), (NoCaptureServerId, NoCaptureServerName) })
            {
                await PgTargetFactCollectorTests.RegisterServerAsync(connection, id, name, MonitoredEngineKind.Postgres, 18, ct);
                /* The span gate: one row 25 h back. Then minute -1 … 60: 3,000 reads and 3,000 hits a minute. */
                await PlantDatabaseStatsAsync(connection, id, name, windowEnd.AddHours(-25), blksRead: 1000, blksHit: 1000, ct);
                for (var m = -1; m <= 60; m++)
                {
                    var mm = Math.Max(m, 0);
                    await PlantDatabaseStatsAsync(connection, id, name, windowStart.AddMinutes(m), blksRead: 1_000_000 + 3000L * mm, blksHit: 1_000_000 + 3000L * mm, ct);
                }
                await PgTargetKnobsTests.PlantConfigSnapshotAsync(connection, id, name, windowEnd.AddMinutes(-30), ct);
            }

            /* The capture the drill-down must read (30 min before the window's end) and the older one it must not. */
            var capturedAt = windowEnd.AddMinutes(-30);
            await PlantCaptureAsync(connection, capturedAt, poolUsed: 15400, ct);
            await PlantCaptureAsync(connection, windowEnd.AddHours(-3), poolUsed: 8000, ct);

            /* The no-capture server: pg_buffercache offered, not installed. */
            await PlantExtensionAvailabilityAsync(connection, NoCaptureServerId, NoCaptureServerName, windowEnd.AddMinutes(-20), "available", ct);

            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, as_of: asOf);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();

                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.BufferCachePressure);
                Assert.Equal($"{PgTargetFactKeys.BufferCachePressure} → {PgTargetFactKeys.ConfigSharedBuffers}", card.GetProperty("story_path").GetString());
                Assert.True(card.GetProperty("severity").GetDouble() >= 0.5);

                var composition = card.GetProperty("drill_down").GetProperty(PgTargetDrillDownCollector.BufferCompositionSection);
                Assert.Equal("captured", composition.GetProperty("status").GetString());
                Assert.Equal(capturedAt.ToString("o", System.Globalization.CultureInfo.InvariantCulture), composition.GetProperty("captured_at").GetString());
                Assert.Equal(16384, composition.GetProperty("pool_buffers_total").GetInt64());
                Assert.Equal(15400, composition.GetProperty("pool_buffers_used").GetInt64());
                Assert.Equal(128L * 1024 * 1024, composition.GetProperty("pool_bytes").GetInt64());
                Assert.Equal(Math.Round(15400 / 16384.0, 4), composition.GetProperty("pool_fill").GetDouble(), precision: 6);
                Assert.Equal(20, composition.GetProperty("relations_in_capture").GetInt32());
                Assert.Equal(13300, composition.GetProperty("buffers_listed").GetInt64());
                Assert.Equal(1650, composition.GetProperty("dirty_buffers_listed").GetInt64());
                Assert.Equal(Math.Round(1650 / 16384.0, 4), composition.GetProperty("dirty_share_of_pool").GetDouble(), precision: 6);
                Assert.Equal(4500, composition.GetProperty("cold_buffers").GetInt64());
                Assert.Equal(Math.Round(4500 / 13300.0, 4), composition.GetProperty("cold_share").GetDouble(), precision: 6);

                var kinds = composition.GetProperty("kind_shares");
                var heap = kinds.GetProperty("heap").GetDouble();
                var index = kinds.GetProperty("index").GetDouble();
                var toast = kinds.GetProperty("toast").GetDouble();
                var other = kinds.GetProperty("other").GetDouble();
                Assert.Equal(Math.Round(6500 / 13300.0, 4), heap, precision: 6);
                Assert.Equal(Math.Round(5000 / 13300.0, 4), index, precision: 6);
                Assert.Equal(Math.Round(1000 / 13300.0, 4), toast, precision: 6);
                Assert.Equal(Math.Round(800 / 13300.0, 4), other, precision: 6);
                Assert.Equal(1.0, heap + index + toast + other, precision: 3);

                var top = composition.GetProperty("top_relations").EnumerateArray().ToList();
                Assert.Equal(10, top.Count);
                Assert.Equal(1, top[0].GetProperty("rank").GetInt32());
                Assert.Equal("orders_idx", top[0].GetProperty("relation_name").GetString());
                Assert.Equal("appdb", top[0].GetProperty("database_name").GetString());
                Assert.Equal("i", top[0].GetProperty("relation_kind").GetString());
                Assert.Equal("index", top[0].GetProperty("kind").GetString());
                Assert.Equal(5000, top[0].GetProperty("buffers").GetInt64());
                Assert.Equal(5000L * 8192, top[0].GetProperty("bytes").GetInt64());
                Assert.Equal(Math.Round(5000 / 16384.0, 4), top[0].GetProperty("share_of_pool").GetDouble(), precision: 6);
                Assert.Equal(0.1, top[0].GetProperty("dirty_share").GetDouble(), precision: 6);
                Assert.Equal(4.5, top[0].GetProperty("avg_usage_count").GetDouble(), precision: 6);
                Assert.False(top[0].GetProperty("relation_name_truncated").GetBoolean());
                Assert.Equal("orders", top[1].GetProperty("relation_name").GetString());
                Assert.Equal("heap", top[1].GetProperty("kind").GetString());
                Assert.Equal("events", top[2].GetProperty("relation_name").GetString());
                Assert.Equal("pg_toast_16401", top[3].GetProperty("relation_name").GetString());
                Assert.Equal("toast", top[3].GetProperty("kind").GetString());
                /* The other database's buffers: kept, unnamed, "other". */
                Assert.Equal(JsonValueKind.Null, top[4].GetProperty("relation_name").ValueKind);
                Assert.Equal("otherdb", top[4].GetProperty("database_name").GetString());
                Assert.Equal("other", top[4].GetProperty("kind").GetString());
                Assert.Equal(800, top[4].GetProperty("buffers").GetInt64());
                /* Ranks 6–10 are the first five of the fifteen 100-buffer tables, by name. */
                Assert.Equal(new[] { "small_01", "small_02", "small_03", "small_04", "small_05" }, top.Skip(5).Select(r => r.GetProperty("relation_name").GetString()).ToArray());
                Assert.All(top, r => Assert.True(r.GetProperty("buffers").GetInt64() >= 100));

                /* The root fact's figures, reused. */
                var pressure = composition.GetProperty("pressure");
                Assert.Equal(0.5, pressure.GetProperty("miss_share").GetDouble(), precision: 6);
                Assert.False(pressure.GetProperty("hit_ratio_suppressed").GetBoolean());
                Assert.Equal(0.0, pressure.GetProperty("evictions_per_sec").GetDouble(), precision: 6);
                Assert.Equal(0.0, pressure.GetProperty("buffers_alloc_per_sec").GetDouble(), precision: 6);

                /* The sentence is the pinned one with the real capture stamp, once, and equal to the payload note. */
                var expected = ExpectedCapturedSentence.Replace("2026-09-19 10:30:00", capturedAt.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal);
                Assert.Equal(expected, composition.GetProperty("note").GetString());
                var investigation = card.GetProperty("advice").GetProperty("investigation").GetString()!;
                Assert.EndsWith(expected, investigation, StringComparison.Ordinal);
                Assert.Equal(1, CountOf(investigation, "Composition: "));
                var remediation = card.GetProperty("advice").GetProperty("remediation").GetString()!;
                Assert.Contains("What the capture says about sizing: Only 33.8 % of resident buffers are cold", remediation, StringComparison.Ordinal);
                Assert.Equal(1, CountOf(remediation, "Counter-objectives:"));

                /* next_tools carries both reads. */
                var nextTools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()).ToList();
                Assert.Contains("get_pg_buffer_usage", nextTools);
                Assert.Contains("get_pg_top_queries", nextTools);
            }

            var noCapture = await DarlingMcpTools.AnalyzeServer(service, postgres, NoCaptureServerName, 1, as_of: asOf);
            using (var doc = JsonDocument.Parse(noCapture))
            {
                var findings = doc.RootElement.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.BufferCachePressure);
                var composition = card.GetProperty("drill_down").GetProperty(PgTargetDrillDownCollector.BufferCompositionSection);
                Assert.Equal("pg_buffercache_available", composition.GetProperty("status").GetString());
                Assert.Equal(JsonValueKind.Null, composition.GetProperty("captured_at").ValueKind);
                Assert.Equal(JsonValueKind.Null, composition.GetProperty("pool_fill").ValueKind);
                Assert.Empty(composition.GetProperty("top_relations").EnumerateArray());
                Assert.Equal(0, composition.GetProperty("relations_in_capture").GetInt32());
                var note = composition.GetProperty("note").GetString()!;
                Assert.StartsWith("Composition: what the cache holds cannot be shown yet", note, StringComparison.Ordinal);
                Assert.EndsWith(note, card.GetProperty("advice").GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("What the capture says about sizing", card.GetProperty("advice").GetProperty("remediation").GetString(), StringComparison.Ordinal);
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

    /// <summary>
    /// The twenty-relation capture the e2e plants, as the summary the read produces from it: pool 16,384 / 15,400
    /// used; <c>orders_idx</c> (index) 5,000 of which 500 dirty; <c>orders</c> (heap) 3,000 / 900 dirty; <c>events</c>
    /// (heap, cold) 2,000; a TOAST table (cold) 1,000 / 100 dirty; 800 unnameable buffers of another database; and
    /// fifteen 100-buffer tables (cold, 10 dirty each). Listed 13,300; heap 6,500; index 5,000; TOAST 1,000; other
    /// 800; cold 4,500; dirty 1,650.
    /// </summary>
    private static PgTargetBufferCompositionSummary TwentyRelations()
    {
        const long total = 16384;
        var top = new List<PgTargetBufferResident>
        {
            new(1, "appdb", "orders_idx", false, "i", "index", 5000, 5000 / (double)total, 500, 0.1, 4.5),
            new(2, "appdb", "orders", false, "r", "heap", 3000, 3000 / (double)total, 900, 0.3, 3.0),
            new(3, "appdb", "events", false, "r", "heap", 2000, 2000 / (double)total, 0, 0, 0.8),
            new(4, "appdb", "pg_toast_16401", false, "t", "toast", 1000, 1000 / (double)total, 100, 0.1, 1.0),
            new(5, "otherdb", null, false, null, "other", 800, 800 / (double)total, 0, 0, 2.0),
        };
        for (var i = 1; i <= 5; i++)
            top.Add(new PgTargetBufferResident(5 + i, "appdb", $"small_{i:00}", false, "r", "heap", 100, 100 / (double)total, 10, 0.1, 0.5));

        return new PgTargetBufferCompositionSummary(
            Status: PgTargetBufferCompositionStatus.Captured,
            CapturedAt: new DateTime(2026, 9, 19, 10, 30, 0, DateTimeKind.Unspecified),
            LastCaptureAt: null,
            LastCaptureHoursBeforeWindow: null,
            PoolBuffersTotal: total,
            PoolBuffersUsed: 15400,
            PoolBytes: total * 8192,
            PoolFill: 15400 / (double)total,
            BuffersListed: 13300,
            DirtyBuffersListed: 1650,
            DirtyShareOfPool: 1650 / (double)total,
            HeapShare: 6500 / 13300.0,
            IndexShare: 5000 / 13300.0,
            ToastShare: 1000 / 13300.0,
            OtherShare: 800 / 13300.0,
            ColdBuffers: 4500,
            ColdShare: 4500 / 13300.0,
            RelationsInCapture: 20,
            TopRelations: top,
            MissShare: 0.5,
            HitRatioSuppressed: false,
            EvictionsPerSec: 0,
            CacheTurnoversPerHour: null,
            BuffersAllocPerSec: 0);
    }

    private static PgTargetBufferCompositionSummary Empty(PgTargetBufferCompositionStatus status) =>
        new(status, null, null, null, 0, 0, 0, null, 0, 0, null, null, null, null, null, 0, null, 0, [], 0.5, false, 0, null, 0);

    private static Fact PressureFact(double missShare, bool aurora)
    {
        var fact = new Fact { Source = PgTargetSources.BufferSource, Key = PgTargetFactKeys.BufferCachePressure, Value = missShare, ServerId = 1 };
        fact.Metadata["blks_hit"] = 180_000;
        fact.Metadata["blks_read"] = 180_000;
        fact.Metadata["blocks_total"] = 360_000;
        fact.Metadata["block_requests_per_sec"] = 100;
        fact.Metadata["miss_share"] = missShare;
        fact.Metadata["hit_ratio"] = 1 - missShare;
        fact.Metadata["hit_ratio_suppressed"] = aurora ? 1 : 0;
        fact.Metadata["evictions_tracked"] = 0;
        fact.Metadata["bgwriter_tracked"] = 0;
        fact.Metadata["arms_graded"] = 1;
        fact.BaseSeverity = 1.0;
        return fact;
    }

    private static int CountOf(string text, string needle) => (text.Length - text.Replace(needle, string.Empty, StringComparison.Ordinal).Length) / needle.Length;

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_database_stats</c> row for one database: the block counters as given, the commit counter climbing so the series is live, everything else flat.</summary>
    private static async Task PlantDatabaseStatsAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, long blksRead, long blksHit, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', $5, 10, $6, $7, 0, 0, 0, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(blksRead + blksHit);
        command.Parameters.AddWithValue(blksRead);
        command.Parameters.AddWithValue(blksHit);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One <c>pg_buffer_usage</c> capture of twenty relations for <see cref="ServerId"/> — the fixture <see cref="TwentyRelations"/> describes — with the pool totals repeated on every row as the collector writes them.</summary>
    private static async Task PlantCaptureAsync(NpgsqlConnection connection, DateTime at, long poolUsed, CancellationToken ct)
    {
        var rows = new List<(string? Db, string? Rel, string? Kind, long Buffers, long Dirty, double? Avg)>
        {
            ("appdb", "orders_idx", "i", 5000, 500, 4.5),
            ("appdb", "orders", "r", 3000, 900, 3.0),
            ("appdb", "events", "r", 2000, 0, 0.8),
            ("appdb", "pg_toast_16401", "t", 1000, 100, 1.0),
            ("otherdb", null, null, 800, 0, 2.0),
        };
        for (var i = 1; i <= 15; i++)
            rows.Add(("appdb", $"small_{i:00}", "r", 100, 10, 0.5));

        var collectionId = CollectionIdGenerator.Next();
        foreach (var row in rows)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_buffer_usage
    (collection_id, collection_time, server_id, server_name, database_name, relation_name, relation_kind, buffers, dirty_buffers, avg_usage_count, pool_buffers_total, pool_buffers_used)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 16384, $11)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Db ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Rel ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Kind ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddWithValue(row.Buffers);
            command.Parameters.AddWithValue(row.Dirty);
            command.Parameters.Add(new NpgsqlParameter { Value = row.Avg.HasValue ? row.Avg.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double });
            command.Parameters.AddWithValue(poolUsed);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task PlantExtensionAvailabilityAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, string state, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_extension_availability
    (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, installed_version, default_version, is_monitoring_relevant, comment)
VALUES ($1, $2, $3, $4, 'appdb', 'pg_buffercache', $5, NULL, '1.5', true, 'What is resident in shared buffers, by relation.')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(state);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({ServerId}, {NoCaptureServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({ServerId}, {NoCaptureServerId}); " +
            $"DELETE FROM pg_buffer_usage WHERE server_id IN ({ServerId}, {NoCaptureServerId}); " +
            $"DELETE FROM pg_extension_availability WHERE server_id IN ({ServerId}, {NoCaptureServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({ServerId}, {NoCaptureServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({ServerId}, {NoCaptureServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({ServerId}, {NoCaptureServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
