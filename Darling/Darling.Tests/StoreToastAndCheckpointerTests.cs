/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins #3783's code half — the consumers of V137's five <c>collect.store_metrics</c> columns, which the rung
/// (<see cref="QsCaptureModeRouteKnobToastRungTests"/>) created and left unread on purpose:
/// <list type="bullet">
/// <item>the WRITER: the checkpointer row (<c>StoreSelfMetrics.CheckpointerInsertSql</c> on 17+, the
/// <c>pg_stat_bgwriter</c> shape before, chosen by the connection's major) stores the server's CUMULATIVE
/// counters, and the dimension rows' <c>toast_live_bytes</c> is filled by a SEPARATE statement that runs only
/// where <c>pg_freespacemap</c> is installed — so on the shipped store it stays the NULL the rung wrote;</item>
/// <item>the READER: <c>toast_bytes</c> / <c>toast_live_bytes</c> ride the latest and daily reads on dimension
/// rows only; <c>ToastFacts</c> turns them into a percentage and a sentence; <c>CheckpointerReading</c>
/// differences the two newest checkpointer rows into the interval the tool and the alert both use;</item>
/// <item>the TOOL: <c>get_store_metrics</c> publishes the four trailing TOAST fields on dimension objects (null
/// on every other kind) and a <c>checkpointer</c> block, and its description says what utilisation is, what
/// the reclaim costs, and what the tool does NOT do;</item>
/// <item>the two INFORMATIONAL self-alerts: Store TOAST Slack (under 50 % on a file over 10 GiB; daily
/// re-fire; DORMANT wherever live bytes are NULL) and Store Checkpointer Pressure (sync over 10 s in an
/// interval or any requested checkpoint; shared cooldown), each with a resolution row, each fired with no
/// severity override so the declared INFO arm styles it, each registered in the family census.</item>
/// </list>
/// The live half — the sweep writing the checkpointer row and the pair read differencing a forced CHECKPOINT
/// against a real store — is <c>StoreSelfMetricsTests.Sweep_EndToEnd_…</c>; the Lite-side lockstep for the two
/// INFO arms is <c>Lite.Tests.AlertSeverityTests</c>.
/// </summary>
public sealed class StoreToastAndCheckpointerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private const long TenGib = 10L << 30;

    /* ---- the writer ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheCheckpointerWriter_StoresTheRawCountersUnderOneKindAndName_OnBothMajors()
    {
        Assert.Equal("checkpointer", StoreSelfMetrics.CheckpointerObjectKind);
        Assert.Equal("pg_stat_checkpointer", StoreSelfMetrics.CheckpointerObjectName);
        Assert.Equal(17, StoreSelfMetrics.CheckpointerViewMajorVersion);

        var modern = StoreSelfMetrics.CheckpointerInsertSql;
        Assert.Contains("(metric_time, object_name, object_kind, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested, postmaster_start_time, checkpoints_timed)", modern, StringComparison.Ordinal);
        Assert.Contains("FROM pg_stat_checkpointer AS c", modern, StringComparison.Ordinal);
        Assert.Contains("round(c.write_time)::bigint", modern, StringComparison.Ordinal);
        Assert.Contains("round(c.sync_time)::bigint", modern, StringComparison.Ordinal);
        Assert.Contains("c.num_requested", modern, StringComparison.Ordinal);
        Assert.Contains("c.num_timed", modern, StringComparison.Ordinal);

        var legacy = StoreSelfMetrics.CheckpointerBgwriterInsertSql;
        Assert.Contains("(metric_time, object_name, object_kind, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested, postmaster_start_time, checkpoints_timed)", legacy, StringComparison.Ordinal);
        Assert.Contains("FROM pg_stat_bgwriter AS b", legacy, StringComparison.Ordinal);
        Assert.Contains("round(b.checkpoint_write_time)::bigint", legacy, StringComparison.Ordinal);
        Assert.Contains("round(b.checkpoint_sync_time)::bigint", legacy, StringComparison.Ordinal);
        Assert.Contains("b.checkpoints_req", legacy, StringComparison.Ordinal);
        Assert.Contains("b.checkpoints_timed", legacy, StringComparison.Ordinal);

        foreach (var sql in new[] { modern, legacy })
        {
            /* One series across a major upgrade: same name, same kind, on both statements. */
            Assert.Contains($"'{StoreSelfMetrics.CheckpointerObjectName}'", sql, StringComparison.Ordinal);
            Assert.Contains($"'{StoreSelfMetrics.CheckpointerObjectKind}'", sql, StringComparison.Ordinal);
            /* #3955: which postmaster produced the counters, as naive UTC — never a bare cast, which renders in the
               session's zone — on both majors, because checkpoints_req counts the shutdown checkpoint too. */
            Assert.Contains("pg_postmaster_start_time() AT TIME ZONE 'UTC',", sql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.DoesNotContain("pg_postmaster_start_time()::", sql, StringComparison.Ordinal);
            /* No subtraction anywhere: the row is the counter, the READ is the difference. */
            Assert.DoesNotContain(" - ", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("LAG(", sql, StringComparison.OrdinalIgnoreCase);
            /* And no other kind's column is touched. */
            foreach (var foreign in new[] { "total_bytes", "row_count", "toast_bytes", "toast_live_bytes", "last_run_duration_ms", "schedule_interval_ms", "total_runs", "total_failures", "chunk_count" })
            {
                Assert.DoesNotContain(foreign, sql, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void TheSweep_PicksTheCheckpointerStatementByMajor_AndFencesTheLiveBytesUpdateBehindTheProbe()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains("connection.PostgreSqlVersion.Major >= CheckpointerViewMajorVersion", source, StringComparison.Ordinal);
        Assert.Contains("? CheckpointerInsertSql", source, StringComparison.Ordinal);
        Assert.Contains(": CheckpointerBgwriterInsertSql", source, StringComparison.Ordinal);

        /* The probe runs unconditionally; the UPDATE only inside its verdict; the INSERT that leaves the column
           NULL runs before both. Order in the source is order on the wire here — one connection, sequential. */
        var insert = source.IndexOf("new NpgsqlCommand(DimensionInsertSql", StringComparison.Ordinal);
        var probe = source.IndexOf("new NpgsqlCommand(ExtensionInstalledSql", StringComparison.Ordinal);
        var gate = source.IndexOf("if (freespacemapInstalled)", StringComparison.Ordinal);
        var update = source.IndexOf("new NpgsqlCommand(ToastLiveBytesUpdateSql", StringComparison.Ordinal);
        var checkpointer = source.IndexOf("new NpgsqlCommand(checkpointerSql", StringComparison.Ordinal);
        var store = source.IndexOf("new NpgsqlCommand(StoreInsertSql", StringComparison.Ordinal);
        Assert.True(insert >= 0 && probe > insert && gate > probe && update > gate && checkpointer > update && store > checkpointer,
            $"the sweep's statement order moved: insert {insert}, probe {probe}, gate {gate}, update {update}, checkpointer {checkpointer}, store {store}");
        Assert.Contains("probe.Parameters.AddWithValue(FreespacemapExtensionName)", source, StringComparison.Ordinal);
        /* The store row stays LAST: LatestStoreSizeSql's backward scan leans on it. */
        Assert.True(source.IndexOf("new NpgsqlCommand(RetentionDeleteSql", StringComparison.Ordinal) > store);
    }

    [Fact]
    public void TheLiveBytesUpdate_ReadsTheFreeSpaceMap_SubtractsFromTheRowsOwnSize_AndTouchesOnlyThisRunsDimensionRows()
    {
        Assert.Equal("pg_freespacemap", StoreSelfMetrics.FreespacemapExtensionName);

        var probe = StoreSelfMetrics.ExtensionInstalledSql;
        Assert.Contains("FROM pg_extension", probe, StringComparison.Ordinal);
        Assert.Contains("WHERE extname = $1", probe, StringComparison.Ordinal);
        /* pg_extension, never pg_available_extensions: SHIPPED is not INSTALLED, and only the install creates the function. */
        Assert.DoesNotContain("pg_available_extensions", probe, StringComparison.Ordinal);

        var update = StoreSelfMetrics.ToastLiveBytesUpdateSql;
        Assert.Contains("UPDATE collect.store_metrics AS m", update, StringComparison.Ordinal);
        Assert.Contains("SET    toast_live_bytes = greatest(m.toast_bytes - f.free_bytes, 0)", update, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN LATERAL pg_freespace(c.reltoastrelid) AS fs", update, StringComparison.Ordinal);
        Assert.Contains("coalesce(sum(fs.avail), 0)::bigint", update, StringComparison.Ordinal);
        Assert.Contains("AND   c.reltoastrelid <> 0", update, StringComparison.Ordinal);
        Assert.Contains($"('{PayloadDimensions.QueryTextDimTable}'), ('{PayloadDimensions.QueryPlanDimTable}')", update, StringComparison.Ordinal);
        Assert.Contains("WHERE m.metric_time = $1", update, StringComparison.Ordinal);
        Assert.Contains($"AND   m.object_kind = '{StoreSelfMetrics.DimensionObjectKind}'", update, StringComparison.Ordinal);
        Assert.Contains("AND   m.object_name = f.dim_name", update, StringComparison.Ordinal);
        Assert.Contains("AND   m.toast_bytes IS NOT NULL", update, StringComparison.Ordinal);
        /* It never re-sizes the file: the quotient must be over the size the row already holds. */
        Assert.DoesNotContain("pg_relation_size", update, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_total_relation_size", update, StringComparison.Ordinal);
        /* And the proxies the rung measured as lies stay out. */
        Assert.DoesNotContain("n_live_tup", update, StringComparison.Ordinal);
        Assert.DoesNotContain("pgstattuple", update, StringComparison.Ordinal);

        /* The INSERT is untouched: it names no extension, so the rung's own pin on it still holds. */
        Assert.DoesNotContain("pg_freespace", StoreSelfMetrics.DimensionInsertSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSweepDoc_StatesThePerKindMeaningOfTheCheckpointerColumns()
    {
        /* Single-line anchors only (the RepoFileAdoptionTests rule), each the load-bearing clause of one
           paragraph: the per-kind mapping, the raw-vs-delta decision, the reset idiom, the extension fence. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs");
        foreach (var phrase in new[]
        {
            "hold the server's CUMULATIVE counters as the",
            "NOT the interval's delta",
            "Why the row stores the raw counters and the READ computes the interval",
            "BACKWARDS means <c>pg_stat_reset_shared('checkpointer')</c>",
            "the #3705 discontinuity idiom",
            "just found <c>pg_freespacemap</c> installed (#3783's code half)",
            "reads 48.5 %",
            "~20 million rows",
        })
        {
            Assert.Contains(phrase, source, StringComparison.Ordinal);
        }
    }

    /* ---- the reader ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheLatestAndDailyReads_ProjectTheToastPairTrailing_AndNeverTheCheckpointerCounters()
    {
        /* #3934 restructured StoreMetricsLatestSql into a recursive-CTE skip-scan, so its trailing three
           columns sit in a nested LATERAL's SELECT list (indented under its own parens) rather than at the
           flat top level StoreMetricsDailySql still has; the trailing-pair-before-FROM shape is checked
           against each read's own indentation instead of one shared literal. */
        var dailyNormalised = DarlingStoreMetricsReader.StoreMetricsDailySql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("    total_failures,\n    toast_bytes,\n    toast_live_bytes\nFROM collect.store_metrics", dailyNormalised, StringComparison.Ordinal);

        var latestNormalised = DarlingStoreMetricsReader.StoreMetricsLatestSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("        total_failures,\n        toast_bytes,\n        toast_live_bytes\n    FROM collect.store_metrics", latestNormalised, StringComparison.Ordinal);

        foreach (var sql in new[] { DarlingStoreMetricsReader.StoreMetricsLatestSql, DarlingStoreMetricsReader.StoreMetricsDailySql })
        {
            foreach (var counter in new[] { "checkpoint_write_ms", "checkpoint_sync_ms", "checkpoints_requested" })
            {
                Assert.DoesNotContain(counter, sql, StringComparison.Ordinal);
            }
        }

        /* Both readers read the two new ordinals — the review catch the sweep test names: written but never
           read back would leave get_store_metrics with null TOAST fields on every row. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingStoreMetricsReader.cs"));
        Assert.Equal(2, Regex.Matches(source, @"reader\.IsDBNull\(13\) \? null : reader\.GetInt64\(13\)").Count);
        Assert.Equal(2, Regex.Matches(source, @"reader\.IsDBNull\(14\) \? null : reader\.GetInt64\(14\)").Count);
        Assert.DoesNotContain("reader.IsDBNull(15)", source, StringComparison.Ordinal);

        var pair = DarlingStoreMetricsReader.CheckpointerPairSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        /* #3955: the postmaster start time rides the pair, so the differencer can tell a restart-spanning interval.
           #4037: the timed-checkpoint count rides beside it, so the differencer can judge the per-checkpoint average. */
        Assert.Contains("    metric_time,\n    checkpoint_write_ms,\n    checkpoint_sync_ms,\n    checkpoints_requested,\n    postmaster_start_time,\n    checkpoints_timed\nFROM collect.store_metrics", pair, StringComparison.Ordinal);
        Assert.Contains($"WHERE object_kind = '{StoreSelfMetrics.CheckpointerObjectKind}'", pair, StringComparison.Ordinal);
        Assert.Contains("AND   checkpoint_write_ms IS NOT NULL", pair, StringComparison.Ordinal);
        /* The cap is BOUND, the LargestUnenumeratedSql shape: a literal terminal LIMIT is what the page census
           inventories, and two rows for a difference is not a page. */
        Assert.Contains("ORDER BY metric_time DESC\nLIMIT $1", pair, StringComparison.Ordinal);
        Assert.Equal(2, DarlingStoreMetricsReader.CheckpointerPairRows);
        var reader = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingStoreMetricsReader.cs"));
        Assert.Contains("command.Parameters.AddWithValue(CheckpointerPairRows);", reader, StringComparison.Ordinal);

        /* The POSITIVE ownership pin the rung's retired "read by nothing" census handed to the consumer lanes: this
           reader names all five V137 store_metrics columns, in CODE (the SQL constants are code; comments are
           stripped), and knows the checkpointer kind by its constant. A refactor that drops one reds here with the
           rung's history attached. */
        var readerWithSql = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingStoreMetricsReader.cs");
        var readerCode = Regex.Replace(Regex.Replace(readerWithSql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"^\s*//.*$", string.Empty, RegexOptions.Multiline);
        foreach (var column in new[] { "toast_bytes", "toast_live_bytes", "checkpoint_write_ms", "checkpoint_sync_ms", "checkpoints_requested", "postmaster_start_time", "checkpoints_timed" })
        {
            Assert.Contains(column, readerCode, StringComparison.Ordinal);
        }

        Assert.Contains("StoreSelfMetrics.CheckpointerObjectKind", readerCode, StringComparison.Ordinal);
        /* And the fifth ordinal is read, nullable: a row written before V139 has no postmaster start. */
        Assert.Contains("reader.IsDBNull(4) ? null : reader.GetDateTime(4)", readerCode, StringComparison.Ordinal);
        /* And the sixth ordinal is read, nullable: a row written before V140 has no timed count (#4037). */
        Assert.Contains("reader.IsDBNull(5) ? null : reader.GetInt64(5)", readerCode, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1000L, 499L, 49.9)]
    [InlineData(1000L, 500L, 50.0)]
    [InlineData(3L, 1L, 33.3)]
    [InlineData(10L, 10L, 100.0)]
    [InlineData(1000L, 0L, 0.0)]
    public void TheUtilisation_IsLiveOverFile_OneDecimal(long file, long live, double expected) =>
        Assert.Equal(expected, DarlingStoreMetricsReader.ToastFacts.UtilisationPercentOf(file, live));

    [Theory]
    [InlineData(null, 5L)]
    [InlineData(1000L, null)]
    [InlineData(null, null)]
    [InlineData(0L, 0L)]
    [InlineData(0L, 10L)]
    public void TheUtilisation_IsNullWhenEitherSideIsNullOrTheFileIsEmpty(long? file, long? live) =>
        Assert.Null(DarlingStoreMetricsReader.ToastFacts.UtilisationPercentOf(file, live));

    [Theory]
    [InlineData(StoreSelfMetrics.HypertableObjectKind)]
    [InlineData(StoreSelfMetrics.ContinuousAggregateObjectKind)]
    [InlineData(StoreSelfMetrics.BackgroundJobObjectKind)]
    [InlineData(StoreSelfMetrics.TableObjectKind)]
    [InlineData(StoreSelfMetrics.OtherObjectKind)]
    [InlineData(StoreSelfMetrics.SystemObjectKind)]
    [InlineData(StoreSelfMetrics.JobHistoryObjectKind)]
    [InlineData(StoreSelfMetrics.CheckpointerObjectKind)]
    [InlineData(StoreSelfMetrics.StoreObjectKind)]
    public void ToastFacts_AreNullOnEveryKindButDimension_SoThoseObjectsCarryTheFourFieldsAsNull(string kind)
    {
        /* Even with byte values planted on the row: the KIND decides, because no other kind has a TOAST column
           the sweep fills, and a sentence about an unmeasured file would be prose about nothing. */
        Assert.Null(DarlingStoreMetricsReader.ToastFacts.For(Row(kind, "x", toast: 1000, live: 500)));
        Assert.Null(DarlingStoreMetricsReader.ToastFacts.For(Point(kind, "x", toast: 1000, live: 500)));
    }

    [Fact]
    public void ToastFacts_OnADimensionRow_SayWhyTheyAreNullOrWhatTheyMean()
    {
        /* Pre-V137 row: no file size recorded. */
        var predates = DarlingStoreMetricsReader.ToastFacts.For(Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, toast: null, live: null))!;
        Assert.Null(predates.UtilisationPercent);
        Assert.Contains("predates V137", predates.Note, StringComparison.Ordinal);

        /* The shipped store: file real, live NULL — the DORMANT arm, in words, naming the one option. */
        var unmeasured = DarlingStoreMetricsReader.ToastFacts.For(Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, toast: 154L << 30, live: null))!;
        Assert.Equal(154L << 30, unmeasured.ToastBytes);
        Assert.Null(unmeasured.ToastLiveBytes);
        Assert.Null(unmeasured.UtilisationPercent);
        Assert.StartsWith("toast_utilisation_pct is not measured", unmeasured.Note, StringComparison.Ordinal);
        Assert.Contains("154.0 GiB TOAST file size is real", unmeasured.Note, StringComparison.Ordinal);
        Assert.Contains("CREATE EXTENSION IF NOT EXISTS pg_freespacemap", unmeasured.Note, StringComparison.Ordinal);
        Assert.Contains("maintainer's call", unmeasured.Note, StringComparison.Ordinal);
        Assert.Contains("a tuple share reads 100 %", unmeasured.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("--recompress-plan-dim", unmeasured.Note, StringComparison.Ordinal);

        /* The finding: the issue's numbers — 154 GB holding ~61 GB — reads under the bar over the floor, and the
           note carries the reclaim verb, who runs it, the lock and the disk, and what the tool does not do. */
        var slack = DarlingStoreMetricsReader.ToastFacts.For(Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, toast: 154L << 30, live: 61L << 30))!;
        Assert.Equal(39.6, slack.UtilisationPercent);
        Assert.StartsWith($"39.6 % of the 154.0 GiB TOAST file behind {PayloadDimensions.QueryPlanDimTable} holds live data (61.0 GiB live, 93.0 GiB free inside the file).", slack.Note, StringComparison.Ordinal);
        Assert.Contains("--recompress-plan-dim --vacuum-full compacts the file, at the maintainer's word in a maintenance window", slack.Note, StringComparison.Ordinal);
        Assert.Contains("ACCESS EXCLUSIVE lock", slack.Note, StringComparison.Ordinal);
        Assert.Contains("needs free disk for a full copy", slack.Note, StringComparison.Ordinal);
        Assert.Contains("This tool never reclaims anything by itself.", slack.Note, StringComparison.Ordinal);
        Assert.Contains("under the 50 % bar on a file over 10.0 GiB", slack.Note, StringComparison.Ordinal);

        /* Under the bar but under the floor: real, small, and said so — no reclaim sentence. */
        var small = DarlingStoreMetricsReader.ToastFacts.For(Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryTextDimTable, toast: 2L << 30, live: 600L << 20))!;
        Assert.Equal(29.3, small.UtilisationPercent);
        Assert.Contains("no finding", small.Note, StringComparison.Ordinal);
        Assert.DoesNotContain("--recompress-plan-dim", small.Note, StringComparison.Ordinal);

        /* Healthy: the head sentence and nothing else. */
        var healthy = DarlingStoreMetricsReader.ToastFacts.For(Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, toast: 64L << 30, live: 41L << 30))!;
        Assert.Equal(64.1, healthy.UtilisationPercent);
        Assert.EndsWith("free inside the file).", healthy.Note, StringComparison.Ordinal);

        /* Empty file: no percentage of nothing. */
        var empty = DarlingStoreMetricsReader.ToastFacts.For(Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryTextDimTable, toast: 0, live: 0))!;
        Assert.Null(empty.UtilisationPercent);
        Assert.Contains("empty", empty.Note, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(TenGib + (100L << 20), 49.9, true)]   /* 10.1 GiB at 49.9 % — the brief's firing corner */
    [InlineData(TenGib + (100L << 20), 50.0, false)]  /* 50 % is not under the bar */
    [InlineData(9L << 30, 49.0, false)]                /* 9 GiB is under the floor */
    [InlineData(TenGib, 49.0, false)]                  /* exactly the floor is not OVER it */
    [InlineData(TenGib + 1, 0.0, true)]
    [InlineData(TenGib + (100L << 20), null, false)]   /* NULL utilisation never — the dormant arm */
    [InlineData(null, 49.0, false)]
    public void IsSlack_IsUnderTheBarOverTheFloor_AndNeverOnANull(long? file, double? pct, bool expected) =>
        Assert.Equal(expected, DarlingStoreMetricsReader.ToastFacts.IsSlack(file, pct));

    [Fact]
    public void TheCheckpointerReading_DifferencesThePair_AndNamesEveryOtherShape()
    {
        var t0 = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);
        var t1 = t0.AddHours(1);

        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Absent, DarlingStoreMetricsReader.CheckpointerReading.From(null, null).Status);

        var only = DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 5_000, 30_000, 7), null);
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.NoPrevious, only.Status);
        Assert.Equal(DateTime.SpecifyKind(t1, DateTimeKind.Utc), only.ObservedAt);
        Assert.Null(only.PreviousAt);
        Assert.Null(only.SyncMs);
        Assert.Equal(30_000, only.CumulativeSyncMs);
        Assert.False(only.IsPressure);

        var observed = DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 5_000, 55_200, 8), new(t0, 4_000, 30_000, 7));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed, observed.Status);
        Assert.Equal(3_600.0, observed.IntervalSeconds);
        Assert.Equal(1_000, observed.WriteMs);
        Assert.Equal(25_200, observed.SyncMs);
        Assert.Equal(1, observed.Requested);
        Assert.Equal(55_200, observed.CumulativeSyncMs);
        Assert.Equal(DateTime.SpecifyKind(t0, DateTimeKind.Utc), observed.PreviousAt);
        Assert.True(observed.IsPressure);

        /* A skipped tick: the span is MEASURED, two hours, not assumed. */
        var skipped = DarlingStoreMetricsReader.CheckpointerReading.From(new(t0.AddHours(2), 5_000, 30_000, 7), new(t0, 5_000, 30_000, 7));
        Assert.Equal(7_200.0, skipped.IntervalSeconds);
        Assert.Equal(0, skipped.SyncMs);
        Assert.False(skipped.IsPressure);

        /* Any one counter lower is a reset: no deltas, the reason, the cumulative still carried. */
        foreach (var newest in new DarlingStoreMetricsReader.CheckpointerSample[]
        {
            new(t1, 3_999, 30_000, 7), new(t1, 4_000, 29_999, 7), new(t1, 4_000, 30_000, 6),
        })
        {
            var reset = DarlingStoreMetricsReader.CheckpointerReading.From(newest, new(t0, 4_000, 30_000, 7));
            Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Reset, reset.Status);
            Assert.Null(reset.SyncMs);
            Assert.Null(reset.WriteMs);
            Assert.Null(reset.Requested);
            Assert.Null(reset.IntervalSeconds);
            Assert.Equal(newest.SyncMs, reset.CumulativeSyncMs);
            Assert.False(reset.IsPressure);
        }

        /* Two rows at one instant cannot be an interval either. */
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Reset,
            DarlingStoreMetricsReader.CheckpointerReading.From(new(t0, 5_000, 30_000, 7), new(t0, 4_000, 30_000, 7)).Status);
    }

    /* #4037: the bar is judged against the PER-CHECKPOINT average (SyncMs / (timed + requested)), not the summed
       sync milliseconds — timed and requested vary per case so the average lands where the old sum-based cases
       intended. */
    [Theory]
    [InlineData(10_001L, 0L, 1L, true)]   // 1 checkpoint, average 10,001ms: over the bar
    [InlineData(10_000L, 0L, 1L, false)]  // 1 checkpoint, average exactly 10,000ms: not OVER the bar
    [InlineData(0L, 1L, 0L, true)]        // any requested checkpoint fires regardless of sync
    [InlineData(9_999L, 0L, 1L, false)]   // 1 checkpoint, average 9,999ms: under the bar
    [InlineData(25_200L, 1L, 0L, true)]   // requested fires; the average would too (25,200ms / 1)
    public void CheckpointerPressure_IsSyncOverTheBarOrAnyRequestedCheckpoint(long syncMs, long requested, long timed, bool expected)
    {
        var t0 = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);
        var reading = DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t0.AddHours(1), 1_000, 100_000 + syncMs, 3 + requested, Timed: 5 + timed), new(t0, 0, 100_000, 3, Timed: 5));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed, reading.Status);
        Assert.Equal(expected, reading.IsPressure);
        Assert.Equal(10_000, DarlingSelfAlertEvaluator.CheckpointSyncBarMs);
    }

    /* ---- #3955: an interval that spans a postmaster restart -------------------------------------------- */

    /// <summary>
    /// The rule every checkpointer reader shares (<see cref="PostmasterRestart.Spans"/>), as a truth table. Both
    /// samples stamped: a restart exactly when the two starts differ, whatever the sample times say (so a test's
    /// synthetic stamps, forty years before the cluster started, are not a restart). Only the newer stamped: a
    /// restart when the older sample predates the newer one's postmaster, the upgrade interval. The newer not
    /// stamped: no evidence, judged as before.
    /// </summary>
    [Theory]
    [InlineData("2026-09-22 20:00", "2026-09-01 08:00", "2026-09-01 08:00", false)]  // one postmaster, both stamped
    [InlineData("1986-09-22 20:00", "2026-09-01 08:00", "2026-09-01 08:00", false)]  // synthetic sample time, same postmaster
    [InlineData("2026-09-22 20:00", "2026-09-01 08:00", "2026-09-22 20:30", true)]   // two postmasters
    [InlineData("2026-09-22 20:00", "2026-09-22 20:30", "2026-09-01 08:00", true)]   // two postmasters, whatever their order
    [InlineData("2026-09-22 20:00", null, "2026-09-22 20:30", true)]                 // the upgrade interval: older row predates V139
    [InlineData("2026-09-22 20:00", null, "2026-09-01 08:00", false)]                // older unstamped, but that postmaster was already up
    [InlineData("2026-09-22 20:00", "2026-09-01 08:00", null, false)]                // newer unstamped: no evidence
    [InlineData("2026-09-22 20:00", null, null, false)]                              // neither stamped: judged as before V139
    public void PostmasterRestart_Spans_IsTheOneRule(string previousSampleTime, string? previousStart, string? newestStart, bool expected)
    {
        static DateTime At(string text) => DateTime.ParseExact(text, "yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture);

        Assert.Equal(expected, PostmasterRestart.Spans(
            At(previousSampleTime),
            previousStart is null ? null : At(previousStart),
            newestStart is null ? null : At(newestStart)));
    }

    /// <summary>
    /// The issue's own interval (DARLING01, 2026-09-22): two shutdown checkpoints (the service stop and the #3908
    /// update's private-port stop) and nothing WAL forced. Across the restart the reading is Restarted: no delta at
    /// all, because the shutdown checkpoint is in all three counters, so it is not pressure however slow its sync
    /// was. The raw counters, the span's two stamps and the postmaster start still ride along. The same counters on
    /// one postmaster ARE pressure (the control).
    /// </summary>
    [Fact]
    public void TheCheckpointerReading_AcrossARestart_StatesNoDelta_AndIsNotPressure_EvenOnASlowSync()
    {
        var t0 = new DateTime(2026, 9, 22, 20, 14, 0, DateTimeKind.Unspecified);
        var t1 = t0.AddMinutes(7.1);
        var before = new DateTime(2026, 9, 1, 8, 0, 0, DateTimeKind.Unspecified);
        var after = t0.AddMinutes(6);

        var restarted = DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t1, 12_700, 3_450, 42, after), new(t0, 12_600, 3_400, 40, before));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted, restarted.Status);
        Assert.True(restarted.PostmasterRestarted);
        Assert.Null(restarted.Requested);
        Assert.Null(restarted.WriteMs);
        Assert.Null(restarted.SyncMs);
        Assert.Null(restarted.IntervalSeconds);
        Assert.Equal(DateTime.SpecifyKind(t0, DateTimeKind.Utc), restarted.PreviousAt);
        Assert.Equal(DateTime.SpecifyKind(t1, DateTimeKind.Utc), restarted.ObservedAt);
        Assert.Equal(42, restarted.CumulativeRequested);
        Assert.Equal(3_450, restarted.CumulativeSyncMs);
        Assert.Equal(DateTime.SpecifyKind(after, DateTimeKind.Utc), restarted.PostmasterStartTime);
        Assert.Equal(DateTimeKind.Utc, restarted.PostmasterStartTime!.Value.Kind);
        Assert.False(restarted.IsPressure, "two shutdown checkpoints are not WAL pressure");

        /* The control: the same counters on ONE postmaster are two requested checkpoints, and that IS pressure. */
        var sameProcess = DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t1, 12_700, 3_450, 42, before), new(t0, 12_600, 3_400, 40, before));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed, sameProcess.Status);
        Assert.False(sameProcess.PostmasterRestarted);
        Assert.Equal(2, sameProcess.Requested);
        Assert.Equal(50, sameProcess.SyncMs);
        Assert.True(sameProcess.IsPressure);

        /* A shutdown checkpoint's fsync past the bar is still not judged: nothing separates it from the live
           checkpoints' sync, and a fast shutdown flushes with every client already gone. */
        var slowSync = DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t1, 12_700, 3_400 + 25_200, 41, after), new(t0, 12_600, 3_400, 40, before));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted, slowSync.Status);
        Assert.Null(slowSync.SyncMs);
        Assert.False(slowSync.IsPressure);

        /* The upgrade interval: the older row predates V139 and was taken before the newer row's postmaster started. */
        var upgrade = DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t1, 12_700, 3_450, 42, after), new(t0, 12_600, 3_400, 40));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted, upgrade.Status);
        Assert.False(upgrade.IsPressure);

        /* Rows written before V139 on both sides: no evidence, judged exactly as before. */
        var legacy = DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 12_700, 3_450, 42), new(t0, 12_600, 3_400, 40));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed, legacy.Status);
        Assert.False(legacy.PostmasterRestarted);
        Assert.Null(legacy.PostmasterStartTime);
        Assert.Equal(2, legacy.Requested);
        Assert.True(legacy.IsPressure);

        /* A crash restart discards the statistics, so the counters fall: Reset, as before, and it says the
           postmaster did restart. NoPrevious carries the start time and no restart. */
        var crash = DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 3, 4, 1, after), new(t0, 115, 78, 2, before));
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Reset, crash.Status);
        Assert.True(crash.PostmasterRestarted);
        Assert.Null(crash.Requested);

        var first = DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 1, 1, 1, after), null);
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.NoPrevious, first.Status);
        Assert.False(first.PostmasterRestarted);
        Assert.Equal(DateTime.SpecifyKind(after, DateTimeKind.Utc), first.PostmasterStartTime);
        Assert.False(DarlingStoreMetricsReader.CheckpointerReading.Absent.PostmasterRestarted);
        Assert.Null(DarlingStoreMetricsReader.CheckpointerReading.Absent.PostmasterStartTime);
    }

    [Fact]
    public void TheCheckpointerNote_AcrossARestart_StatesNoDelta_AndSaysWhy()
    {
        var t0 = new DateTime(2026, 9, 22, 20, 14, 0, DateTimeKind.Unspecified);
        var t1 = t0.AddHours(1);
        var before = new DateTime(2026, 9, 1, 8, 0, 0, DateTimeKind.Unspecified);
        var after = t0.AddMinutes(30);

        foreach (var syncMs in new[] { 3_000L, 26_200L })
        {
            var note = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(
                new(t1, 2_000, syncMs, 7, after), new(t0, 1_000, 1_000, 5, before)));
            Assert.Contains("the store's postmaster RESTARTED (it started at 2026-09-22T20:44:00.0000000Z)", note, StringComparison.Ordinal);
            Assert.Contains("counts the shutdown checkpoint as REQUESTED and keeps the count across the restart", note, StringComparison.Ordinal);
            Assert.Contains("checkpoint's own write and sync phases land in the same counters", note, StringComparison.Ordinal);
            Assert.Contains("no delta is stated for this interval (write_ms, sync_ms and requested are null)", note, StringComparison.Ordinal);
            Assert.Contains("judges neither arm: a standing alert neither fires nor recovers on it", note, StringComparison.Ordinal);
            Assert.Contains("That costs one skipped hourly interval; the next sweep differences cleanly against the post-restart row and is judged normally.", note, StringComparison.Ordinal);
            /* Never a figure, never the pressure prose, whatever the sync did. */
            Assert.DoesNotContain("checkpoint(s) were REQUESTED", note, StringComparison.Ordinal);
            Assert.DoesNotContain("in its sync (fsync) phase", note, StringComparison.Ordinal);
            Assert.DoesNotContain("CHECKPOINTER PRESSURE", note, StringComparison.Ordinal);
        }

        /* Five statuses, five different notes: the Restarted one says something none of the others do. */
        var notes = new[]
        {
            DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.Absent),
            DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 1, 1, 1, after), null)),
            DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 0, 0, 0), new(t0, 5, 5, 5))),
            DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 3_000, 7, after), new(t0, 1_000, 1_000, 5, before))),
            DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 3_000, 7), new(t0, 1_000, 1_000, 5))),
        };
        Assert.Equal(5, Enum.GetValues<DarlingStoreMetricsReader.CheckpointerDeltaStatus>().Length);
        Assert.Equal(notes.Length, notes.Distinct(StringComparer.Ordinal).Count());

        /* A crash restart is a Reset, and the note names the restart as the cause it was. */
        var crash = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t1, 3, 4, 1, after), new(t0, 115, 78, 2, before)));
        Assert.Contains("read LOWER than before", crash, StringComparison.Ordinal);
        Assert.Contains("The store's postmaster did restart inside this interval (#3955)", crash, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCheckpointerNote_SaysOneThingPerStatus_AndNamesTheLeversOnPressure()
    {
        var t0 = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);
        var t1 = t0.AddHours(1);

        Assert.Contains("no checkpointer row yet", DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.Absent), StringComparison.Ordinal);
        Assert.Contains("One row so far", DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 1, 1, 1), null)), StringComparison.Ordinal);

        var reset = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 0, 0, 0), new(t0, 5, 5, 5)));
        Assert.Contains("read LOWER than before", reset, StringComparison.Ordinal);
        Assert.Contains("pg_stat_reset_shared('checkpointer')", reset, StringComparison.Ordinal);
        Assert.Contains("no delta is stated", reset, StringComparison.Ordinal);

        /* Healthy per-checkpoint: 12 checkpoints, 84s summed sync -> 7s average, under the 10s bar; the old sum
           rule would have judged 84s against the same bar and misfired (#4037). */
        var quiet = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 85_000, 5, Timed: 12), new(t0, 1_000, 1_000, 5, Timed: 0)));
        Assert.Contains("12 checkpoint(s), spending", quiet, StringComparison.Ordinal);
        Assert.Contains("an average of 7.0s of sync per checkpoint", quiet, StringComparison.Ordinal);
        Assert.Contains("0 checkpoint(s) were REQUESTED", quiet, StringComparison.Ordinal);
        Assert.Contains("Both under the lines the self-alert judges (average sync over 10s PER CHECKPOINT, or any requested checkpoint).", quiet, StringComparison.Ordinal);
        Assert.DoesNotContain("#3802", quiet, StringComparison.Ordinal);

        /* Real pressure: 2 checkpoints, 30s summed -> 15s average, over the bar. */
        var storm = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 31_000, 5, Timed: 2), new(t0, 1_000, 1_000, 5, Timed: 0)));
        Assert.Contains("an average of 15.0s of sync per checkpoint", storm, StringComparison.Ordinal);
        Assert.Contains("CHECKPOINTER PRESSURE", storm, StringComparison.Ordinal);
        Assert.Contains("a per-checkpoint sync average past 10s", storm, StringComparison.Ordinal);
        Assert.Contains("25.2 s and 14.0 s sync phases", storm, StringComparison.Ordinal);

        /* Requested arm: low sync average, but one requested checkpoint still fires. */
        var forced = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 3_000, 6, Timed: 5), new(t0, 1_000, 1_000, 5, Timed: 0)));
        Assert.Contains("a requested checkpoint means the store wrote more WAL between checkpoints than max_wal_size allows", forced, StringComparison.Ordinal);

        /* Zero checkpoints in the interval: no average to state, unmeasured, not judged either way. */
        var idle = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 1_000, 1_000, 5, Timed: 0), new(t0, 1_000, 1_000, 5, Timed: 0)));
        Assert.DoesNotContain("average of", idle, StringComparison.Ordinal);
        Assert.False(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 1_000, 1_000, 5, Timed: 0), new(t0, 1_000, 1_000, 5, Timed: 0)).IsPressure);

        /* Pre-rung pair: null timed on both sides -> unmeasured average arm, no pressure from the sum. */
        var preRung = DarlingMcpStoreMetricsTools.CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 85_000, 5), new(t0, 1_000, 1_000, 5)));
        Assert.Contains("an unmeasured number of checkpoints (a row before V140 has no timed count)", preRung, StringComparison.Ordinal);
        Assert.DoesNotContain("average of", preRung, StringComparison.Ordinal);
        Assert.DoesNotContain("CHECKPOINTER PRESSURE", preRung, StringComparison.Ordinal);
        Assert.False(DarlingStoreMetricsReader.CheckpointerReading.From(new(t1, 2_000, 85_000, 5), new(t0, 1_000, 1_000, 5)).IsPressure);
    }

    /* ---- the tool -------------------------------------------------------------------------------------- */

    [Fact]
    public void TheTool_PublishesTheFourToastFieldsTrailingOnObjects_ThreeOnDaily_AndACheckpointerBlock_AndExcludesTheCheckpointerRow()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpStoreMetricsTools.cs");
        /* The stripper blanks a comment to spaces and keeps its newlines, so the adjacent-line anchors below
           drop whitespace-only lines first: what remains is the CODE in source order. */
        var source = string.Join("\n",
            CSharpSourceWalker.StripCommentsAndStrings(raw)
                .Replace("\r\n", "\n", StringComparison.Ordinal)
                .Split('\n')
                .Where(l => l.Trim().Length > 0));

        /* Object rows: the four, trailing the byte-bearing row shape (#3903 split the job fields into a shape of
           their own, so the aggregate's retention policy is what precedes them now), through the
           null-conditional so every non-dimension kind reads null. */
        Assert.Contains(
            "            retention_policy_job_id = AggregateState(r, aggregateStateByView)?.RetentionJobId,\n" +
            "            toast_bytes = ToastFacts(r)?.ToastBytes,\n" +
            "            toast_live_bytes = ToastFacts(r)?.ToastLiveBytes,\n" +
            "            toast_utilisation_pct = ToastFacts(r)?.UtilisationPercent,\n" +
            "            toast_utilisation_note = ToastFacts(r)?.Note,\n" +
            "        };",
            source, StringComparison.Ordinal);

        /* Series points (the object view's daily series since #3903): bytes, live bytes, the quotient; no note
           per point. */
        Assert.Contains(
            "            row_count = p.RowCount,\n" +
            "            toast_bytes = DarlingStoreMetricsReader.ToastFacts.For(p)?.ToastBytes,\n" +
            "            toast_live_bytes = DarlingStoreMetricsReader.ToastFacts.For(p)?.ToastLiveBytes,\n" +
            "            toast_utilisation_pct = DarlingStoreMetricsReader.ToastFacts.For(p)?.UtilisationPercent,\n" +
            "        };",
            source, StringComparison.Ordinal);

        /* The checkpointer block, and its keys. */
        Assert.Contains("var checkpointer = await DarlingStoreMetricsReader.GetCheckpointerAsync(postgres, cancellationToken);", source, StringComparison.Ordinal);
        foreach (var key in new[] { "status = checkpointer.Status.ToString()", "observed_at = ", "previous_at = ", "interval_seconds = checkpointer.IntervalSeconds", "write_ms = checkpointer.WriteMs", "sync_ms = checkpointer.SyncMs", "requested = checkpointer.Requested", "cumulative_write_ms = ", "cumulative_sync_ms = ", "cumulative_requested = ", "pressure = checkpointer.IsPressure", "sync_bar_ms = DarlingSelfAlertEvaluator.CheckpointSyncBarMs", "note = CheckpointerNote(checkpointer)" })
        {
            Assert.Contains(key, source, StringComparison.Ordinal);
        }

        /* The checkpointer row is never a list row, the job_history precedent. Until #3903 two inequalities kept it
           out of objects[] and daily[]; now every list is built from DarlingStoreMetricsReader.SelectObjects, whose
           candidates are ListedKinds, so the pin is that list and that the tool builds from nothing else. */
        Assert.DoesNotContain(StoreSelfMetrics.CheckpointerObjectKind, DarlingStoreMetricsReader.ListedKinds);
        Assert.DoesNotContain(StoreSelfMetrics.JobHistoryObjectKind, DarlingStoreMetricsReader.ListedKinds);
        Assert.Contains("var selection = DarlingStoreMetricsReader.SelectObjects(latest, kind, name);", source, StringComparison.Ordinal);
        Assert.Empty(DarlingStoreMetricsReader.SelectObjects(
            new[] { new DarlingStoreMetricsReader.StoreMetricRow(StoreSelfMetrics.CheckpointerObjectKind, StoreSelfMetrics.CheckpointerObjectName, DateTime.UtcNow, null, null, null, null, null, null) },
            null, null).Matched);

        /* The description carries the load-bearing sentences a caller reads before trusting a number. Read off
           the DESCRIPTION itself since #3903 rather than the whole source file, so a comment repeating a
           phrase can no longer stand in for the sentence a caller actually reads. */
        var description = typeof(DarlingMcpStoreMetricsTools)
            .GetMethod(nameof(DarlingMcpStoreMetricsTools.GetStoreMetrics))!
            .GetCustomAttribute<DescriptionAttribute>()!.Description;
        foreach (var phrase in new[]
        {
            /* #3898: TOAST UTILISATION and CHECKPOINTER used to cite their own issue number, (V137, #3783); D4
               takes the issue tag off the wire (the schema-version fact, V137, stays load-bearing and is kept).
               The two motivating anecdotes below the block headers - the 40%/154GB TOAST measurement and the
               25.2s/14.0s checkpoint sync-phase incident - are D4 "story of why" removals too: each one's RULE
               (the VACUUM/free-space behavior; the sync_ms/requested self-alert gate) stays in the description,
               only the specific past incident's numbers are gone. See the #3898 PR body's D4 section. */
            "TOAST UTILISATION (V137)",
            "they live in its TOAST file, and pg_total_relation_size says how big that file is and nothing about how FULL it is",
            "ordinary VACUUM returns to the table and never to the operating system",
            "toast_utilisation_pct is toast_live_bytes / toast_bytes (one decimal)",
            "pg_freespacemap extension, which the bundled store image ships and does not install",
            "NOTHING is computed from total_bytes or a tuple share",
            "--recompress-plan-dim --vacuum-full, at the MAINTAINER's word in a maintenance window",
            "ACCESS EXCLUSIVE lock",
            "This tool never reclaims anything by itself.",
            "Hypertable, aggregate, table and catch-all objects carry the four TOAST fields as null",
            "CHECKPOINTER (V137)",
            "interval_seconds (MEASURED between the two sweeps that bound it, not the assumed cadence)",
            "Reset (a counter went backwards",
            "WAL sizing and refresh slicing",
            "The checkpointer row is never an object row",
        })
        {
            Assert.Contains(phrase, description, StringComparison.Ordinal);
        }

        /* Web and Viewer store-metrics surfaces: neither enumerates dimension columns — the Viewer only PROBES
           the table's existence for its schema gate, and the web dashboard has no store-metrics panel — so
           there was nothing to add there; pinned so a surface that starts enumerating has to revisit this. */
        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.DoesNotContain("FROM collect.store_metrics", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("toast_utilisation", viewer, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3955: the checkpointer block publishes WHY an interval states no delta across a restart — the Restarted
    /// status, the restart flag and the newest row's postmaster start — and the description a caller reads before
    /// trusting the number names the fifth status and says what it means. Read off the DescriptionAttribute itself,
    /// not the source file, so a comment repeating the sentence cannot stand in for it.
    /// </summary>
    [Fact]
    public void TheTool_PublishesTheRestartFields_AndTheDescriptionSaysWhatRestartedMeans()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpStoreMetricsTools.cs"));
        Assert.Contains("postmaster_restarted = checkpointer.PostmasterRestarted,", code, StringComparison.Ordinal);
        Assert.Contains("postmaster_start_time = checkpointer.PostmasterStartTime?.ToString(", code, StringComparison.Ordinal);

        var method = typeof(DarlingMcpStoreMetricsTools).GetMethod(nameof(DarlingMcpStoreMetricsTools.GetStoreMetrics))!;
        var description = ((System.ComponentModel.DescriptionAttribute)Attribute.GetCustomAttribute(
            method, typeof(System.ComponentModel.DescriptionAttribute))!).Description;
        Assert.Contains("Restarted (the store's postmaster restarted between the two sweeps, postmaster_restarted true", description, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL counts the shutdown checkpoint as requested and keeps the count across the restart", description, StringComparison.Ordinal);
        Assert.Contains("that checkpoint's own write and sync phases land in the same counters, so no delta is stated and the self-alert judges neither arm", description, StringComparison.Ordinal);
        Assert.Contains("the next sweep's interval is judged normally", description, StringComparison.Ordinal);
        /* The Observed-with-a-null-requested shape this replaced is gone from what callers read. */
        Assert.DoesNotContain("requested null", description, StringComparison.Ordinal);
    }

    /* ---- the two informational self-alerts ------------------------------------------------------------- */

    [Fact]
    public void BothMetrics_AreDeclaredInfo_ClassifiedInformational_AndInTheSelfMonitorFamily()
    {
        foreach (var metric in new[] { DarlingSelfAlertEvaluator.ToastSlackMetric, DarlingSelfAlertEvaluator.CheckpointerPressureMetric })
        {
            var (hex, badge, _) = AlertSeverity.ForMetric(metric);
            Assert.Equal("INFO", badge);
            Assert.Equal("#2eaef1", hex);
            Assert.True(AlertMetricClassifier.IsInformational(metric), $"{metric} would render amber in the history grids");
            Assert.False(AlertMetricClassifier.IsWarning(metric));
            Assert.Equal(AlertFamily.SelfMonitor, AlertFamily.Of(metric));
            Assert.True(AlertFamily.IsClassified(metric));
        }

        Assert.Equal("Store TOAST Slack", DarlingSelfAlertEvaluator.ToastSlackMetric);
        Assert.Equal("Store Checkpointer Pressure", DarlingSelfAlertEvaluator.CheckpointerPressureMetric);
        Assert.True(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.ToastSlackClearedMetric));
        Assert.True(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.CheckpointerPressureRecoveredMetric));

        /* The thresholds, where they live, and that they are constants (the doc says why they are not knobs). */
        Assert.Equal(50.0, DarlingSelfAlertEvaluator.ToastSlackUtilisationBarPercent);
        Assert.Equal(TenGib, DarlingSelfAlertEvaluator.ToastSlackFileFloorBytes);
        Assert.Equal(TimeSpan.FromDays(1), DarlingSelfAlertEvaluator.ToastSlackRefire);
        var evaluator = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");
        Assert.Contains("Why a constant and not a <c>config_alert_settings</c> knob", evaluator, StringComparison.Ordinal);
        Assert.Contains("A knob needs a migration rung", evaluator, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ToastSlack_FiresInfoAtTheBriefsCorner_AndNotAtTheThreeNonCorners()
    {
        var h = new Harness();
        var e = h.Build();

        /* 49.9 % at 10.1 GiB fires. */
        await e.ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, TenGib + (100L << 20), 49.9) }, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.ToastSlackMetric, fired.MetricName);
        Assert.Null(fired.Severity);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Equal("toastslack:" + PayloadDimensions.QueryPlanDimTable, fired.ServerKey);
        Assert.Equal(49.9, fired.NumericCurrentValue);
        Assert.Equal(50.0, fired.NumericThresholdValue);
        Assert.Contains("--recompress-plan-dim --vacuum-full reclaims the slack; maintenance window", fired.ShortMessage!, StringComparison.Ordinal);
        Assert.Contains("ACCESS EXCLUSIVE lock", fired.DetailText!, StringComparison.Ordinal);
        Assert.Contains("maintainer's word", fired.DetailText!, StringComparison.Ordinal);
        Assert.Contains("never runs it by itself", fired.DetailText!, StringComparison.Ordinal);

        /* 50 % does not; 49 % at 9 GiB does not; 49 % at exactly 10 GiB does not. Fresh evaluators, so the
           standing state from the fire above cannot turn these into resolutions. */
        foreach (var (file, pct) in new[] { (TenGib + (100L << 20), 50.0), (9L << 30, 49.0), (TenGib, 49.0) })
        {
            var quiet = new Harness();
            await quiet.Build().ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, file, pct) }, Ct);
            Assert.Empty(quiet.Deliverer.Outcomes);
            Assert.Empty(quiet.History.Records);
        }
    }

    [Fact]
    public async Task ToastSlack_NullLiveBytes_NeverFires_AndNeverResolvesAStandingAlert()
    {
        var h = new Harness();
        var e = h.Build();

        /* The shipped store: 154 GiB file, live NULL. Dormant. */
        await e.ApplyToastSlackAsync(new[] { DimRaw(PayloadDimensions.QueryPlanDimTable, 154L << 30, null) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* A standing alert followed by an unmeasured row: no resolution row, the state is left as found —
           and a later measured breach does not fire again inside the daily interval, which proves the state survived. */
        await e.ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, 154L << 30, 39.6) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);
        await e.ApplyToastSlackAsync(new[] { DimRaw(PayloadDimensions.QueryPlanDimTable, 154L << 30, null) }, Ct);
        Assert.Empty(h.History.Records);
        h.Clock = h.Clock.AddHours(1);
        await e.ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, 154L << 30, 39.6) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ToastSlack_RefiresDaily_NotOnTheSharedCooldown_AndClearsWhenTheReclaimLands()
    {
        var h = new Harness();
        h.Settings.CooldownMinutes = 5;
        var e = h.Build();
        var slack = Dim(PayloadDimensions.QueryPlanDimTable, 154L << 30, 39.6);

        await e.ApplyToastSlackAsync(new[] { slack }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Six minutes on — past the shared cooldown — still one. Twenty-three hours on — still one. */
        h.Clock = h.Clock.AddMinutes(6);
        await e.ApplyToastSlackAsync(new[] { slack }, Ct);
        h.Clock = h.Clock.AddHours(23);
        await e.ApplyToastSlackAsync(new[] { slack }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* A day on: the daily restatement. */
        h.Clock = h.Clock.AddHours(1).AddSeconds(1);
        await e.ApplyToastSlackAsync(new[] { slack }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* The rebuild lands: 61 GiB live in a 62 GiB file. One Cleared row naming the bar it came back over. */
        await e.ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, 62L << 30, 98.4) }, Ct);
        var cleared = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.ToastSlackClearedMetric, cleared.MetricName);
        Assert.Contains("back at 98.4% live, over the 50% bar", cleared.DetailText!, StringComparison.Ordinal);
        Assert.StartsWith(DarlingSelfAlertEvaluator.StoreServerLabel + ": ", cleared.DetailText!, StringComparison.Ordinal);

        /* And a quiet store writes nothing at all — a Cleared row needs a standing alert to clear. */
        await e.ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, 62L << 30, 98.4) }, Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task ToastSlack_TracksTheTwoDimensionsIndependently_AndClearsUnderTheFloorToo()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyToastSlackAsync(new[]
        {
            Dim(PayloadDimensions.QueryPlanDimTable, 154L << 30, 39.6),
            Dim(PayloadDimensions.QueryTextDimTable, 12L << 30, 45.0),
            Row(StoreSelfMetrics.HypertableObjectKind, "query_stats", toast: null, live: null),
        }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(
            new[] { "toastslack:" + PayloadDimensions.QueryPlanDimTable, "toastslack:" + PayloadDimensions.QueryTextDimTable },
            h.Deliverer.Outcomes.Select(o => o.ServerKey).ToArray());

        /* The text dim's file shrinks under the floor (the plan dim stays slack): one Cleared row, for the text dim, by floor. */
        await e.ApplyToastSlackAsync(new[]
        {
            Dim(PayloadDimensions.QueryPlanDimTable, 154L << 30, 39.6),
            Dim(PayloadDimensions.QueryTextDimTable, 8L << 30, 45.0),
        }, Ct);
        var cleared = Assert.Single(h.History.Records);
        Assert.Equal("toastslack:" + PayloadDimensions.QueryTextDimTable, cleared.ServerId);
        Assert.Contains("under the 10 GB floor", cleared.DetailText!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ToastSlack_MasterOff_ReadsNothingAndFiresNothing()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        await h.Build().ApplyToastSlackAsync(new[] { Dim(PayloadDimensions.QueryPlanDimTable, 154L << 30, 39.6) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task CheckpointerPressure_FiresInfoOnEitherArm_AndNotUnderBothLines()
    {
        var h = new Harness();
        var e = h.Build();

        /* 1 checkpoint, 25.2 s average sync, no forced checkpoint. */
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 25_200, requested: 0, timed: 1), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.CheckpointerPressureMetric, fired.MetricName);
        Assert.Null(fired.Severity);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Equal("checkpointer", fired.ServerKey);
        Assert.Equal(25_200, fired.NumericCurrentValue);
        Assert.Equal(10_000, fired.NumericThresholdValue);
        Assert.Equal("average sync 25.2s per checkpoint over 1 checkpoint(s) in 60.0 min", fired.CurrentValue);
        Assert.Equal("average sync > 10s per checkpoint, or any requested checkpoint", fired.ThresholdValue);
        Assert.Contains("25.2 s and 14.0 s sync phases", fired.DetailText!, StringComparison.Ordinal);

        /* The other arm alone: one WAL-forced checkpoint with a short average sync. */
        var forced = new Harness();
        await forced.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 800, requested: 1, timed: 1), Ct);
        var forcedFire = Assert.Single(forced.Deliverer.Outcomes);
        Assert.Contains("1 WAL-forced checkpoint(s)", forcedFire.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("more WAL between checkpoints than max_wal_size allows", forcedFire.DetailText!, StringComparison.Ordinal);

        /* Exactly the bar, no forced checkpoint: quiet. */
        var quiet = new Harness();
        await quiet.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 10_000, requested: 0, timed: 1), Ct);
        Assert.Empty(quiet.Deliverer.Outcomes);
        Assert.Empty(quiet.History.Records);

        /* Healthy store: 12 checkpoints, 84s summed sync -> 7s average, no pressure (the #4037 headline case). */
        var healthy = new Harness();
        await healthy.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 84_000 - 2_500, requested: 0, timed: 12), Ct);
        Assert.Empty(healthy.Deliverer.Outcomes);
        Assert.Empty(healthy.History.Records);

        /* Real pressure: 2 checkpoints, 15s average. */
        var pressure = new Harness();
        await pressure.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 30_000 - 2_500, requested: 0, timed: 2), Ct);
        Assert.Single(pressure.Deliverer.Outcomes);
    }

    [Fact]
    public async Task CheckpointerPressure_NonMeasurements_NeitherFireNorResolve()
    {
        var t0 = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);
        var h = new Harness();
        var e = h.Build();

        foreach (var reading in new[]
        {
            DarlingStoreMetricsReader.CheckpointerReading.Absent,
            DarlingStoreMetricsReader.CheckpointerReading.From(new(t0, 1, 99_000, 9), null),
            DarlingStoreMetricsReader.CheckpointerReading.From(new(t0.AddHours(1), 0, 0, 0), new(t0, 1, 99_000, 9)),
        })
        {
            await e.ApplyCheckpointerPressureAsync(reading, Ct);
        }

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* Standing, then a reset pair: no recovery row — a reset is not a clean hour. */
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 25_200, requested: 0), Ct);
        Assert.Single(h.Deliverer.Outcomes);
        await e.ApplyCheckpointerPressureAsync(
            DarlingStoreMetricsReader.CheckpointerReading.From(new(t0.AddHours(3), 0, 0, 0), new(t0.AddHours(2), 1, 99_000, 9)), Ct);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task CheckpointerPressure_RefiresOnTheSharedCooldown_AndRecoversOnACleanInterval()
    {
        var h = new Harness();
        h.Settings.CooldownMinutes = 60;
        var e = h.Build();

        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 14_000, requested: 0), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Inside the cooldown: held. Past it: the new breaching hour fires. */
        h.Clock = h.Clock.AddMinutes(30);
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 14_000, requested: 0), Ct);
        Assert.Single(h.Deliverer.Outcomes);
        h.Clock = h.Clock.AddMinutes(31);
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 14_000, requested: 0), Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* A clean interval: one Recovered row with the interval's figures. */
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 1_200, requested: 0), Ct);
        var recovered = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.CheckpointerPressureRecoveredMetric, recovered.MetricName);
        Assert.Contains("average sync per checkpoint and 0 requested checkpoint(s)", recovered.DetailText!, StringComparison.Ordinal);
        Assert.StartsWith(DarlingSelfAlertEvaluator.StoreServerLabel + ": ", recovered.DetailText!, StringComparison.Ordinal);

        /* Quiet stays quiet. */
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 1_200, requested: 0), Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task CheckpointerPressure_MasterOff_FiresNothing()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        await h.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 25_200, requested: 1), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    /// <summary>
    /// #3955, the issue's headline: every service restart that stops the store puts a shutdown checkpoint in
    /// <c>num_requested</c>, and the alert fired "2 WAL-forced checkpoint(s) in the last 7.1 min" on DARLING01 for an
    /// interval with no WAL-triggered checkpoint in it. Across a restart neither arm is judged, so the same counters
    /// no longer fire; the SAME counters on one postmaster still do, exactly as before.
    /// </summary>
    [Fact]
    public async Task CheckpointerPressure_AnIntervalSpanningARestart_DoesNotFireOnTheShutdownCheckpoints()
    {
        var h = new Harness();
        await h.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 800, requested: 2, restarted: true), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task CheckpointerPressure_TheSameCountersWithoutARestart_StillFireAsBefore()
    {
        var h = new Harness();
        await h.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 800, requested: 2, restarted: false, timed: 1), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.CheckpointerPressureMetric, fired.MetricName);
        Assert.Contains("2 WAL-forced checkpoint(s)", fired.CurrentValue, StringComparison.Ordinal);
        Assert.DoesNotContain("restart", fired.ShortMessage!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("RESTARTED", fired.DetailText!, StringComparison.Ordinal);
    }

    /// <summary>
    /// Across a restart the SYNC arm is not judged either: the shutdown checkpoint's own fsync lands in the same
    /// counter as the live checkpoints' and nothing separates them, and a fast shutdown flushes every dirty buffer
    /// with every client already gone. So a sync phase far past the bar, the 25.2 s the production read kills sat
    /// inside, fires nothing on an interval that spans a restart. The control is the next test's.
    /// </summary>
    [Fact]
    public async Task CheckpointerPressure_ARestartSpanningInterval_DoesNotFireEvenOnASlowSync()
    {
        var h = new Harness();
        await h.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 25_200, requested: 1, restarted: true), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task CheckpointerPressure_TheSameSlowSyncWithoutARestart_StillFires()
    {
        var h = new Harness();
        await h.Build().ApplyCheckpointerPressureAsync(Interval(syncMs: 25_200, requested: 1, restarted: false, timed: 1), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("average sync", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("1 WAL-forced checkpoint(s)", fired.CurrentValue, StringComparison.Ordinal);
        Assert.DoesNotContain("restart", fired.ShortMessage!, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A restart interval is a non-measurement on BOTH arms, so a standing alert neither re-fires on it (even past
    /// the cooldown, even with a slow sync) nor recovers on it (even with a clean one). The cost is that one hourly
    /// interval: the next interval on one postmaster is judged normally, and a clean one writes the Recovered row.
    /// </summary>
    [Fact]
    public async Task CheckpointerPressure_ARestartSpanningInterval_NeitherFiresNorResolvesAStandingAlert()
    {
        var h = new Harness();
        h.Settings.CooldownMinutes = 5;
        var e = h.Build();

        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 0, requested: 3), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Clock = h.Clock.AddHours(1);
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 25_200, requested: 1, restarted: true), Ct);
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 1_200, requested: 0, restarted: true), Ct);
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        h.Clock = h.Clock.AddHours(1);
        await e.ApplyCheckpointerPressureAsync(Interval(syncMs: 1_200, requested: 0), Ct);
        var recovered = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.CheckpointerPressureRecoveredMetric, recovered.MetricName);
    }

    [Fact]
    public void TheWorker_EvaluatesBothOnTheStoreSelfMetricsTick_AfterTheSweep()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var tick = worker.IndexOf("if (DateTime.UtcNow >= _nextStoreMetricsUtc)", StringComparison.Ordinal);
        var sweep = worker.IndexOf("await SweepStoreSelfMetricsAsync(stoppingToken);", tick, StringComparison.Ordinal);
        var toast = worker.IndexOf("await _selfAlerts.EvaluateToastSlackAsync(_postgres!, stoppingToken);", sweep, StringComparison.Ordinal);
        var checkpointer = worker.IndexOf("await _selfAlerts.EvaluateCheckpointerPressureAsync(_postgres!, stoppingToken);", toast, StringComparison.Ordinal);
        var nextTick = worker.IndexOf("await Task.Delay(s_sweepInterval, stoppingToken);", tick, StringComparison.Ordinal);

        Assert.True(tick >= 0 && sweep > tick && toast > sweep && checkpointer > toast && checkpointer < nextTick,
            "the two #3783 conditions must be evaluated on the store self-metrics tick, after the sweep that writes their evidence");

        /* Exactly one call site each — the census in DarlingSelfAlertTests asserts they exist; this asserts where. */
        Assert.Single(Regex.Matches(worker, @"EvaluateToastSlackAsync\("));
        Assert.Single(Regex.Matches(worker, @"EvaluateCheckpointerPressureAsync\("));
    }

    /* ---- fixtures -------------------------------------------------------------------------------------- */

    private static DarlingStoreMetricsReader.StoreMetricRow Row(string kind, string name, long? toast, long? live) =>
        new(kind, name, new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified),
            TotalBytes: 1, CompressedBeforeBytes: null, CompressedAfterBytes: null, ChunkCount: null, RowCount: null, EnabledServerCount: null,
            ToastBytes: toast, ToastLiveBytes: live);

    private static DarlingStoreMetricsReader.StoreMetricDailyPoint Point(string kind, string name, long? toast, long? live) =>
        new(kind, name, new DateTime(2026, 9, 20, 0, 0, 0, DateTimeKind.Unspecified),
            TotalBytes: 1, CompressedBeforeBytes: null, CompressedAfterBytes: null, ChunkCount: null, RowCount: null, EnabledServerCount: null,
            ToastBytes: toast, ToastLiveBytes: live);

    /// <summary>A dimension row whose live bytes are chosen so the utilisation reads EXACTLY <paramref name="pct"/>
    /// to one decimal — the boundary cases must sit on the number, not near it.</summary>
    private static DarlingStoreMetricsReader.StoreMetricRow Dim(string name, long file, double pct)
    {
        var live = (long)Math.Round(file * pct / 100.0);
        var row = DimRaw(name, file, live);
        var actual = DarlingStoreMetricsReader.ToastFacts.UtilisationPercentOf(file, live);
        Assert.Equal(pct, actual);
        return row;
    }

    private static DarlingStoreMetricsReader.StoreMetricRow DimRaw(string name, long? file, long? live) =>
        Row(StoreSelfMetrics.DimensionObjectKind, name, file, live);

    /// <summary>
    /// A one-hour interval with the given counter movements, built through the real differencer. <paramref name="restarted"/>
    /// (#3955): null leaves both rows unstamped, the pre-V139 shape every earlier test was written against; false
    /// stamps both with one postmaster; both of those are Observed. True stamps them with two, the newer started
    /// half an hour into the interval, which is Restarted.
    /// </summary>
    private static DarlingStoreMetricsReader.CheckpointerReading Interval(long syncMs, long requested, bool? restarted = null, long timed = 1)
    {
        var t0 = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);
        var longAgo = t0.AddDays(-20);
        DateTime? previousStart = restarted is null ? null : longAgo;
        DateTime? newestStart = restarted switch
        {
            null => null,
            true => t0.AddMinutes(30),
            false => longAgo,
        };

        var reading = DarlingStoreMetricsReader.CheckpointerReading.From(
            new(t0.AddHours(1), 10_000 + 2_500, 500_000 + syncMs, 40 + requested, newestStart, Timed: 100 + timed), new(t0, 10_000, 500_000, 40, previousStart, Timed: 100));
        Assert.Equal(
            restarted is true ? DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted : DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed,
            reading.Status);
        Assert.Equal(restarted is true, reading.PostmasterRestarted);
        return reading;
    }

    /* ---- the evaluator harness (the DarlingSelfAlertTests fakes, narrowed to what these two conditions read) ---- */

    private sealed class Harness
    {
        public FakeSettings Settings { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public FakeHistoryStore History { get; } = new();
        public DateTime Clock { get; set; } = new(2026, 9, 20, 13, 0, 0, DateTimeKind.Utc);

        public DarlingSelfAlertEvaluator Build() =>
            new(Settings, Deliverer, History, _ => false, logger: null, utcNow: () => Clock);
    }

    private sealed class FakeSettings : IAlertEngineSettings
    {
        public bool AlertsEnabled { get; set; } = true;
        public bool CpuEnabled { get; set; }
        public bool BlockingEnabled { get; set; } = true;
        public bool DeadlockEnabled { get; set; } = true;
        public bool PoisonWaitEnabled { get; set; }
        public bool LongRunningQueryEnabled { get; set; }
        public bool TempDbSpaceEnabled { get; set; }
        public bool LowDiskEnabled { get; set; }
        public bool LongRunningJobEnabled { get; set; }
        public bool FailedJobEnabled { get; set; }
        public bool PvsEnabled { get; set; }
        public bool DatabaseStateEnabled { get; set; }
        public bool ForcePlanFailureEnabled { get; set; } = true;
        public int CpuThresholdPercent { get; set; } = 80;
        public int BlockingCountThreshold { get; set; } = 1;
        public int BlockingWaitSecondsThreshold { get; set; }
        public int DeadlockCountThreshold { get; set; } = 1;
        public DeadlockRateThresholds DeadlockRateThresholds => DeadlockRateThresholds.Default;
        public int PoisonWaitThresholdMs { get; set; } = 500;
        public int LongRunningQueryThresholdMinutes { get; set; } = 30;
        public int LongRunningQueryMaxResults { get; set; } = 5;
        public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
        public bool LongRunningQueryExcludeWaitFor { get; set; } = true;
        public bool LongRunningQueryExcludeBackups { get; set; } = true;
        public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;
        public bool LongRunningQueryExcludeCdc { get; set; } = true;
        public IReadOnlyList<string> LongRunningQueryExcludedProgramNamePrefixes { get; set; } = Array.Empty<string>();
        public IReadOnlyList<string> LongRunningQueryExcludedLogins { get; set; } = Array.Empty<string>();
        public int TempDbSpaceThresholdPercent { get; set; } = 80;
        public int LowDiskThresholdPercent { get; set; } = 10;
        public int LowDiskThresholdGb { get; set; } = 5;
        public int DiskCriticalFreePercent { get; set; } = 3;
        public int DiskCriticalFreeGb { get; set; } = 2;
        public int SelfDiskFreeWarnPercent { get; set; } = 10;
        public int SelfDiskFreeWarnGb { get; set; } = 50;
        public int CollectionStaleMinutes { get; set; } = 30;
        public int CollectionFailureThreshold { get; set; } = 10;
        public int PvsThresholdPercent { get; set; } = 40;
        public int PvsFloorGb { get; set; } = 1;
        public bool FileGrowthEnabled { get; set; }
        public int FileGrowthRiseMb { get; set; } = 10240;
        public int FileGrowthVolumePercent { get; set; } = 60;
        public int FileGrowthLookbackMinutes { get; set; } = 60;
        public int LongRunningJobMultiplier { get; set; } = 3;
        public int FailedJobLookbackMinutes { get; set; } = 60;
        public int CooldownMinutes { get; set; } = 5;
        public List<string> ExcludedDatabasesList { get; } = new();
        public IReadOnlyList<string> ExcludedDatabases => ExcludedDatabasesList;
        public CpuAlertMode CpuAlertMode { get; set; } = CpuAlertMode.TotalServer;
    }

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    private sealed class FakeHistoryStore : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }
}

/// <summary>
/// The fenced live-bytes path against a REAL TOASTed dimension (#3783) — the pin the unit tests cannot be: the
/// <c>pg_freespacemap</c> UPDATE runs against the migrated <c>collect.query_plan_dim</c> on a store that HAS the
/// extension, and the number it stores is judged against the two facts the rung's measurement established.
/// 2,000 rows of 9.6 KB values in the plan dimension's shape (<c>STORAGE EXTERNAL</c>, as gzip bytes behave),
/// half deleted, ordinary <c>VACUUM</c>: the file keeps EVERY page it had (the slack mechanism itself), the
/// free-space-map figure reads about half live (48.4 % on the rig that wrote this; the band asserted is 40–60),
/// and the tuple-share proxy the rung rejected reads 100 % on the very same relation at the very same instant —
/// asserted here so the lie is executed, not described. Before the delete the same read is over 90 % live, so
/// the instrument is proven to MOVE with the data and not merely to return a plausible constant.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database through <c>ScratchPostgres</c> (it installs an
/// extension and alters a product column's storage, neither of which may touch the shared store) and so does
/// not serialize against the live collection. Plain-PostgreSQL sweep (<c>timescaleAvailable: false</c>): the
/// dimension, checkpointer, table, catch-all and store arms are every store shape and are all this proves.</para>
/// </summary>
public sealed class StoreToastAndCheckpointerLivePostgresTests
{
    [Fact]
    public async Task WithTheExtensionInstalled_TheSweepFillsLiveBytesFromTheFreeSpaceMap_AndTheTupleShareProxyReadsTheLie_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live TOAST utilisation test (it mints its own scratch database and installs pg_freespacemap there).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The maintainer's one statement, on the scratch store only. The shipped image ships the module; a store
           without it simply skips this test's whole premise, so the availability is asserted rather than assumed. */
        await using (var available = new NpgsqlCommand(
            $"SELECT count(*) FROM pg_available_extensions WHERE name = '{StoreSelfMetrics.FreespacemapExtensionName}'", connection) { CommandTimeout = 30 })
        {
            Assert.Equal(1L, Convert.ToInt64(await available.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture));
        }

        await Exec(connection, $"CREATE EXTENSION IF NOT EXISTS {StoreSelfMetrics.FreespacemapExtensionName}", ct);

        /* The plan dimension's real shape: gzip bytes neither compress nor fit a page, so every value is TOASTed
           out of line. EXTERNAL makes the synthetic payload behave the same way without needing real gzip. */
        var dim = $"collect.{PayloadDimensions.QueryPlanDimTable}";
        await Exec(connection, $"ALTER TABLE {dim} ALTER COLUMN {PayloadDimensions.CompressedContentColumn} SET STORAGE EXTERNAL", ct);
        await Exec(connection,
            $"INSERT INTO {dim} ({PayloadDimensions.DigestColumn}, {PayloadDimensions.CompressedContentColumn}, {PayloadDimensions.LastSeenColumn}) " +
            "SELECT sha256(g::text::bytea), repeat(md5(g::text), 300)::bytea, '2026-01-01' FROM generate_series(1, 2000) AS g", ct);

        var t0 = DateTime.SpecifyKind(DateTime.UtcNow.AddYears(-40), DateTimeKind.Unspecified);
        await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: false, t0, null, ct);

        var full = await ReadDimAsync(connection, t0, ct);
        Assert.True(full.Toast > 15_000_000, $"2,000 x 9.6 KB values must TOAST into a file of ~20 MB, not {full.Toast} bytes");
        Assert.NotNull(full.Live);
        var fullPct = DarlingStoreMetricsReader.ToastFacts.UtilisationPercentOf(full.Toast, full.Live)!.Value;
        Assert.True(fullPct > 90, $"a freshly loaded file must read almost entirely live, not {fullPct} %");

        /* The #3783 shape: half the rows go, VACUUM runs, the file keeps its pages. */
        await Exec(connection, $"DELETE FROM {dim} WHERE get_byte({PayloadDimensions.DigestColumn}, 0) % 2 = 0", ct);
        /* The proxy read below compares n_live_tup to n_dead_tup on the TOAST relation, and those two counters
           reach pg_stat_all_tables by different roads: VACUUM reports its reset straight into shared stats,
           while the DELETE's dead-tuple increment sits in this backend's PENDING stats until the next report,
           which PostgreSQL throttles to once per second (PGSTAT_MIN_INTERVAL). A fast DELETE → VACUUM therefore
           lets the reset land FIRST and the increment SECOND, and the proxy reads ~64 % live on a freshly vacuumed
           file — which is not the lie the rung rejected but a stats race, and it failed this assertion on two
           consecutive CI runs of one PR (#3846) at the same 64.1. pg_stat_force_next_flush() (PostgreSQL 15+,
           the store's floor) makes the DELETE's pending counters report at the next opportunity, ahead of the
           VACUUM; the second call after VACUUM flushes anything the vacuum itself queued. */
        /* Retried, bounded (#3977): full-suite load once left this exact free-space map reading at 96.2 %
           live right after one VACUUM. Direct repro ruled out the #3945 mechanism — a transaction held open
           in a DIFFERENT database never blocks this database's own VACUUM (verified live: 10,000 dead tuples
           still fully removed with one held open throughout) — and this test mints its OWN scratch database
           (#1776 own-store), so no other test shares it to hold a snapshot on in the first place. What
           full-suite load can still do to an own-store test is starve its CPU/IO, so poll the FSM directly
           (the same live-bytes formula ToastLiveBytesUpdateSql uses, without writing a row) after each VACUUM
           instead of trusting the first one. The real, once-only sweep below still runs exactly once, and
           only after the poll sees the expected shape or the bound is spent — which then fails loud on the
           real reading, unchanged from before. */
        var vacuumAttempt = 0;
        const int maxVacuumAttempts = 10;
        while (true)
        {
            await Exec(connection, "SELECT pg_stat_force_next_flush()", ct);
            await Exec(connection, $"VACUUM {dim}", ct);
            await Exec(connection, "SELECT pg_stat_force_next_flush()", ct);

            double? polledPct;
            await using (var probe = new NpgsqlCommand(
                "SELECT greatest(pg_relation_size(c.reltoastrelid) - coalesce((SELECT sum(fs.avail) FROM pg_freespace(c.reltoastrelid) AS fs), 0), 0)::float8" +
                "  / nullif(pg_relation_size(c.reltoastrelid), 0) * 100 " +
                "FROM pg_class AS c WHERE c.oid = $1::regclass", connection) { CommandTimeout = 30 })
            {
                probe.Parameters.AddWithValue(dim);
                var scalar = await probe.ExecuteScalarAsync(ct);
                polledPct = scalar is null or DBNull ? null : Convert.ToDouble(scalar, System.Globalization.CultureInfo.InvariantCulture);
            }

            vacuumAttempt++;
            if (polledPct is > 35 and < 65 || vacuumAttempt >= maxVacuumAttempts)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }

        var t1 = t0.AddHours(1);
        await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: false, t1, null, ct);

        var half = await ReadDimAsync(connection, t1, ct);
        Assert.Equal(full.Toast, half.Toast);
        Assert.NotNull(half.Live);
        var halfPct = DarlingStoreMetricsReader.ToastFacts.UtilisationPercentOf(half.Toast, half.Live)!.Value;
        var horizon = halfPct is > 35 and < 65 ? string.Empty : await VacuumHorizonHoldersAsync(connection, ct);
        Assert.True(halfPct is > 35 and < 65,
            $"after deleting half and vacuuming, the free-space map must read about half live (the rig read 48.4 %), not {halfPct} % — {half.Live} of {half.Toast} bytes{horizon}");

        /* #3977's cause is unconfirmed. This database is the test's own, so another database's ordinary transaction
           cannot hold its VACUUM back; only cluster-wide holders can: a replication slot's xmin, a prepared
           transaction, a backend whose snapshot reaches every database. A failure names whichever it saw. */
        static async Task<string> VacuumHorizonHoldersAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var probe = new NpgsqlCommand(@"
SELECT concat_ws('; ',
    (SELECT 'replication slots holding xmin: ' || string_agg(slot_name || ' xmin ' || xmin::text, ', ')
     FROM pg_catalog.pg_replication_slots WHERE xmin IS NOT NULL),
    (SELECT 'prepared transactions: ' || count(*)::text FROM pg_catalog.pg_prepared_xacts HAVING count(*) > 0),
    (SELECT 'oldest backend_xmin: ' || string_agg(format('%s pid %s age %s (%s)', coalesce(a.datname, '-'), a.pid, age(a.backend_xmin), coalesce(a.state, '-')), ', ')
     FROM (SELECT datname, pid, backend_xmin, state FROM pg_catalog.pg_stat_activity
           WHERE backend_xmin IS NOT NULL ORDER BY age(backend_xmin) DESC LIMIT 3) AS a))", connection);
            var text = await probe.ExecuteScalarAsync(ct) as string;
            return string.IsNullOrEmpty(text)
                ? " (no cluster-wide VACUUM horizon holder was visible at failure)"
                : " — cluster-wide VACUUM horizon holders at failure: " + text;
        }
        Assert.True(half.Live < full.Live, "live bytes must FALL when half the values are deleted");

        /* The proxy the rung measured as a lie, executed on the same relation at the same instant: after VACUUM the
           dead tuples are gone and the file keeps the pages, so the tuple share says the half-empty file is full. */
        await using (var proxy = new NpgsqlCommand(
            "SELECT round(100.0 * s.n_live_tup / nullif(s.n_live_tup + s.n_dead_tup, 0), 1) " +
            "FROM pg_class c JOIN pg_stat_all_tables s ON s.relid = c.reltoastrelid WHERE c.oid = $1::regclass", connection) { CommandTimeout = 30 })
        {
            proxy.Parameters.AddWithValue(dim);
            var tupleShare = Convert.ToDouble(await proxy.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(tupleShare >= 99.0, $"the tuple-share proxy was expected to read ~100 % on the vacuumed file (the lie the rung rejected); it read {tupleShare}");
        }

        /* Through the real reader: the facts compute the percentage, and the file is far under the 10 GiB floor,
           so the note says the slack is real and small and the alert arm stays quiet on a REAL measured row. */
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var latest = await DarlingStoreMetricsReader.GetLatestAsync(dataSource, ct);
        var planRow = Assert.Single(latest, r => r.ObjectKind == StoreSelfMetrics.DimensionObjectKind && r.ObjectName == PayloadDimensions.QueryPlanDimTable);
        var facts = DarlingStoreMetricsReader.ToastFacts.For(planRow)!;
        Assert.Equal(halfPct, facts.UtilisationPercent);
        Assert.Contains("no finding", facts.Note, StringComparison.Ordinal);
        Assert.False(DarlingStoreMetricsReader.ToastFacts.IsSlack(planRow.ToastBytes, facts.UtilisationPercent));

        /* And the checkpointer row rode both sweeps: the pair differences to an Observed hour. Both sweeps ran on
           one postmaster, so both rows carry one start time and the forty-years-ago stamps are NOT a restart. */
        var checkpointer = await DarlingStoreMetricsReader.GetCheckpointerAsync(dataSource, ct);
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed, checkpointer.Status);
        Assert.Equal(3_600.0, checkpointer.IntervalSeconds);
        Assert.False(checkpointer.PostmasterRestarted);
        Assert.NotNull(checkpointer.Requested);
    }

    /// <summary>
    /// #3955 against a real server: the sweep's checkpointer row carries the server's OWN
    /// <c>pg_postmaster_start_time()</c> as naive UTC (and no other kind's row carries one), and the real pair read
    /// takes a planted pre-restart sample against it (one requested checkpoint fewer, the shape a fast stop leaves)
    /// as a Restarted interval that states NO delta at all, no requested count and no write or sync time, and is not
    /// pressure. The same planted sample stamped with the SAME postmaster is one requested checkpoint and IS pressure
    /// (the control), and the upgrade shape (an older row written before V139, taken before the postmaster started)
    /// is a restart too.
    /// The restart itself is not performed here: the shared cluster cannot be bounced under a running suite, and
    /// the counter's behaviour across a fast stop is the measurement the rule rests on (PostmasterRestart's doc).
    /// Own store through <c>ScratchPostgres</c>, like the test above.
    /// </summary>
    [Fact]
    public async Task TheCheckpointerRow_CarriesThePostmasterStart_AndAPairAcrossARestart_StatesNoDelta_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3955 checkpointer restart test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* One REQUESTED checkpoint on the counter, so the planted earlier sample can sit one below it the way a
           shutdown checkpoint leaves the pair. The fixture role is superuser (StoreSelfMetricsTests does the same). */
        await Exec(connection, "CHECKPOINT", ct);

        var sweptAt = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow));
        await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: false, sweptAt, null, ct);

        DateTime serverStart;
        await using (var live = new NpgsqlCommand("SELECT pg_postmaster_start_time() AT TIME ZONE 'UTC'", connection) { CommandTimeout = 30 })
        {
            serverStart = (DateTime)(await live.ExecuteScalarAsync(ct))!;
        }

        long write, sync, requested;
        await using (var read = new NpgsqlCommand(
            $"SELECT postmaster_start_time, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested FROM collect.store_metrics WHERE object_kind = '{StoreSelfMetrics.CheckpointerObjectKind}' AND metric_time = $1",
            connection) { CommandTimeout = 30 })
        {
            read.Parameters.AddWithValue(sweptAt);
            await using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "the sweep wrote no checkpointer row");
            Assert.False(reader.IsDBNull(0), "the checkpointer row carries no postmaster start");
            Assert.Equal(serverStart, reader.GetDateTime(0));
            write = reader.GetInt64(1);
            sync = reader.GetInt64(2);
            requested = reader.GetInt64(3);
        }

        Assert.True(requested >= 1, $"the forced CHECKPOINT must be on the counter, not {requested}");

        await using (var others = new NpgsqlCommand(
            $"SELECT count(*) FROM collect.store_metrics WHERE object_kind <> '{StoreSelfMetrics.CheckpointerObjectKind}' AND postmaster_start_time IS NOT NULL", connection) { CommandTimeout = 30 })
        {
            Assert.Equal(0L, Convert.ToInt64(await others.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture));
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The pre-restart sample: an hour earlier, a PREVIOUS postmaster (started a day before that), one requested
           checkpoint fewer — the fast stop's shutdown checkpoint — and the same write and sync time. */
        var previousAt = sweptAt.AddHours(-1);
        await PlantCheckpointerRowAsync(connection, previousAt, write, sync, requested - 1, serverStart.AddDays(-1), ct);

        var restarted = await DarlingStoreMetricsReader.GetCheckpointerAsync(dataSource, ct);
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted, restarted.Status);
        Assert.True(restarted.PostmasterRestarted);
        Assert.Null(restarted.Requested);
        Assert.Null(restarted.WriteMs);
        Assert.Null(restarted.SyncMs);
        Assert.Equal(DateTime.SpecifyKind(previousAt, DateTimeKind.Utc), restarted.PreviousAt);
        Assert.Equal(DateTime.SpecifyKind(sweptAt, DateTimeKind.Utc), restarted.ObservedAt);
        Assert.Equal(requested, restarted.CumulativeRequested);
        Assert.Equal(sync, restarted.CumulativeSyncMs);
        Assert.Equal(DateTime.SpecifyKind(serverStart, DateTimeKind.Utc), restarted.PostmasterStartTime);
        Assert.False(restarted.IsPressure, "a shutdown checkpoint across a restart is not WAL pressure");

        /* The control: the same sample on the SAME postmaster is one requested checkpoint, and that is pressure. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"UPDATE collect.store_metrics SET postmaster_start_time = $1 WHERE object_kind = '{StoreSelfMetrics.CheckpointerObjectKind}' AND metric_time = $2",
            serverStart, previousAt);
        var sameProcess = await DarlingStoreMetricsReader.GetCheckpointerAsync(dataSource, ct);
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed, sameProcess.Status);
        Assert.False(sameProcess.PostmasterRestarted);
        Assert.Equal(1, sameProcess.Requested);
        Assert.Equal(0, sameProcess.WriteMs);
        Assert.Equal(0, sameProcess.SyncMs);
        Assert.True(sameProcess.IsPressure);

        /* The upgrade shape: the earlier row was written before V139 (no start) and taken before this postmaster
           started, so the interval spans the restart the upgrade itself made. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM collect.store_metrics WHERE object_kind = '{StoreSelfMetrics.CheckpointerObjectKind}' AND metric_time = $1",
            previousAt);
        await PlantCheckpointerRowAsync(connection, serverStart.AddMinutes(-1), write, sync, requested - 1, null, ct);
        var upgrade = await DarlingStoreMetricsReader.GetCheckpointerAsync(dataSource, ct);
        Assert.Equal(DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted, upgrade.Status);
        Assert.True(upgrade.PostmasterRestarted);
        Assert.Null(upgrade.Requested);
        Assert.Null(upgrade.WriteMs);
        Assert.Null(upgrade.SyncMs);
        Assert.False(upgrade.IsPressure);
    }

    private static async Task PlantCheckpointerRowAsync(NpgsqlConnection connection, DateTime metricTime, long write, long sync, long requested, DateTime? postmasterStart, CancellationToken ct)
    {
        await using var plant = new NpgsqlCommand(
            $"INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested, postmaster_start_time) " +
            $"VALUES ($1, '{StoreSelfMetrics.CheckpointerObjectName}', '{StoreSelfMetrics.CheckpointerObjectKind}', $2, $3, $4, $5)", connection) { CommandTimeout = 30 };
        plant.Parameters.AddWithValue(DarlingMcpTestData.Naive(metricTime));
        plant.Parameters.AddWithValue(write);
        plant.Parameters.AddWithValue(sync);
        plant.Parameters.AddWithValue(requested);
        plant.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp,
            Value = postmasterStart is DateTime start ? DarlingMcpTestData.Naive(start) : DBNull.Value,
        });
        await plant.ExecuteNonQueryAsync(ct);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(long Toast, long? Live)> ReadDimAsync(NpgsqlConnection connection, DateTime metricTime, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT toast_bytes, toast_live_bytes FROM collect.store_metrics WHERE metric_time = $1 AND object_kind = '{StoreSelfMetrics.DimensionObjectKind}' AND object_name = '{PayloadDimensions.QueryPlanDimTable}'",
            connection) { CommandTimeout = 30 };
        read.Parameters.AddWithValue(metricTime);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the sweep wrote no plan-dimension row");
        return (reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetInt64(1));
    }
}
