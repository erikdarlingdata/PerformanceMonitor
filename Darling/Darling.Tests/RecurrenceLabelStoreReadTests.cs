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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 item 3 (ruling Q3): the store side of the recurrence label. <c>RecurrenceLabeler</c> is pure over
/// one read of the prior weeks — <see cref="PgFindingStore.GetPriorOccurrencesAsync"/> on Darling, Lite's
/// <c>FindingStore.GetPriorOccurrencesAsync</c> — and the labeler's own pins live in Lite.Tests
/// (<c>RecurrenceLabelerTests</c>, shared assembly). This class pins what the labeler cannot: that the
/// statement has the shape the labeler was written against, that both SKUs carry ONE statement (the Lite
/// twin differs by the one token its offset view is spelled with), that both pass methods call the labeler at
/// the one point where a frozen-text edit reaches the persisted row, that both MCP twins publish the label —
/// and, against a real Postgres (gated on <c>DARLING_TEST_PG</c>), that the read returns the rows the labeler
/// expects and the labeler turns them into the sentence.
/// </summary>
[Collection("live-postgres")]
public sealed class RecurrenceLabelStoreReadTests
{
    private const string ServerName = "recurrence-label-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string UtcServerName = "recurrence-label-e2e-no-offset";
    private static readonly int UtcServerId = ServerIdHelper.GetDeterministicHashCode(UtcServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* Tuesday 14:37:12 UTC; on the UTC-4 server that is 10:37 local, so the slot is 10:00 Tuesday. */
    private static readonly DateTime ReferenceUtc = new(2026, 9, 15, 14, 37, 12, DateTimeKind.Utc);
    private const int OffsetMinutes = -240;
    private const string ChainA = "rl-chain-a";
    private const string ChainC = "rl-chain-c";
    private const string JobChain = "rl-job-chain";
    private const string JobName = "Nightly Index Maintenance";

    /* ---------------- ungated: shape, twin, wiring, MCP pins ---------------- */

    [Fact]
    public void PriorOccurrencesSql_HasTheShapeTheLabelerWasWrittenAgainst()
    {
        var sql = PgFindingStore.GetPriorOccurrencesSql;

        /* Three parameters and no fourth: server, lower bound, and the reference instant doing double duty as
           the exclusive upper bound AND the slot anchor — reused so the two cannot disagree. */
        Assert.Contains("WHERE f.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("AND   f.analysis_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("AND   f.analysis_time <  $3", sql, StringComparison.Ordinal);
        Assert.Contains("$3 + COALESCE(svr.offset_minutes, 0) * INTERVAL '1' MINUTE AS reference_local", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);

        /* The slot is on the target's clock: the latest non-null offset, applied to every row AND the
           reference before the hour and weekday are taken; the raw (nullable) offset is projected so the
           caller knows when it fell back to UTC. */
        Assert.Contains("SELECT sp.utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("AND   sp.utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sp.collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('hour', f.analysis_time + COALESCE(svr.offset_minutes, 0) * INTERVAL '1' MINUTE) AS local_bucket", sql, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(HOUR FROM local_bucket) = EXTRACT(HOUR FROM reference_local)", sql, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(DOW FROM local_bucket) = EXTRACT(DOW FROM reference_local)", sql, StringComparison.Ordinal);
        Assert.Contains("    offset_minutes\nFROM local_rows", sql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* Two families, one scan: the in-slot chains and every job card in any slot; the text read for the
           job rows only. Collapsed per (chain, root, hour). */
        Assert.Contains("OR    root_fact_key = 'RUNNING_JOBS'", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN root_fact_key = 'RUNNING_JOBS' THEN story_text END) AS job_story_text", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY story_path_hash, root_fact_key, local_bucket, offset_minutes", sql, StringComparison.Ordinal);

        /* No page cap on the outer read: the only LIMIT is the offset subquery's LIMIT 1. A capped read here
           would drop the oldest week silently and the label would under-count. */
        Assert.Equal(1, CountOf(sql, "LIMIT"));
        Assert.Equal("RUNNING_JOBS", RecurrenceLabeler.JobKey);
    }

    [Fact]
    public void BothFindingStores_CarryOneStatement_DifferingOnlyInTheOffsetView()
    {
        /* Lite reads the offset through v_server_properties (its live + archive view); Darling has the bare
           table. That is the ONE permitted difference — normalise it away and the two must be byte-equal, so
           a fix to one twin's slot arithmetic that misses the other fails here. */
        var lite = ExtractConst(RepoFile.ReadRepoFileLf("Lite", "Analysis", "FindingStore.cs"), "GetPriorOccurrencesSql");
        var darling = ExtractConst(RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Analysis", "PgFindingStore.cs"), "GetPriorOccurrencesSql");

        Assert.Contains("FROM v_server_properties AS sp", lite, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties AS sp", darling, StringComparison.Ordinal);
        Assert.Equal(darling, lite.Replace("FROM v_server_properties AS sp", "FROM server_properties AS sp", StringComparison.Ordinal));

        /* And the compiled Darling const is the source's, so the pin above is about the statement that runs. */
        Assert.Equal(darling, PgFindingStore.GetPriorOccurrencesSql.Replace("\r\n", "\n", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Analysis/DarlingAnalysisService.cs")]
    [InlineData("Lite/Analysis/AnalysisService.cs")]
    public void BothPassMethods_LabelAfterTheFoldAndBeforeTheMuteFilter(string file)
    {
        /* The labeler edits frozen StoryText. It must run after the last other writer of that text (the #3709
           fold) and before the store copies it onto the finding — otherwise the sentence is written to a
           story the row never sees. Both SKUs, same order, pinned by position in the pass body. */
        var source = RepoFile.ReadRepoFileLf(file.Split('/'));

        var fold = source.IndexOf("AnomalyIncidentReconciler.Reconcile(stories, facts);", StringComparison.Ordinal);
        var read = source.IndexOf("_findingStore.GetPriorOccurrencesAsync(context, context.TimeRangeEnd);", StringComparison.Ordinal);
        var label = source.IndexOf("RecurrenceLabeler.Label(stories, facts, context.TimeRangeEnd, priorOccurrences);", StringComparison.Ordinal);
        var mute = source.IndexOf("_findingStore.FilterMutedFindingsAsync(stories, context);", StringComparison.Ordinal);

        Assert.True(fold > 0, "the fold call site moved");
        Assert.True(read > fold, "the prior-weeks read must follow the fold");
        Assert.True(label > read, "the label must follow its read");
        Assert.True(mute > label, "the mute filter must follow the label — it copies StoryText onto the finding");

        /* Once, on the full pass only. The facts-only path (CollectAndScoreFactsAsync) builds no stories and
           is fenced besides. */
        Assert.Equal(1, CountOf(source, "RecurrenceLabeler.Label("));
        var factsOnly = source.IndexOf("CollectAndScoreFactsAsync(", StringComparison.Ordinal);
        Assert.True(factsOnly > label, "the facts-only method is expected after the pass method; the labeler must not be in it");
        Assert.DoesNotContain("RecurrenceLabeler", source[factsOnly..], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs")]
    [InlineData("Lite/Mcp/McpAnalysisTools.cs")]
    public void BothGetAnalysisFindingsTwins_PublishTheLabel_OffTheRepresentativesFrozenText(string file)
    {
        var source = RepoFile.ReadRepoFileLf(file.Split('/'));
        var tool = source.IndexOf("Name = \"get_analysis_findings\"", StringComparison.Ordinal);
        var next = source.IndexOf("Name = \"mute_analysis_finding\"", StringComparison.Ordinal);
        Assert.True(tool > 0 && next > tool);
        var body = source[tool..next];

        /* Read once per representative, off the frozen text — the row's only carrier — and published as three
           fields present on every entry (false / null is "not labelled"). */
        Assert.Contains("var recurrence = RecurrenceLabeler.TryReadLabel(f.StoryText);", body, StringComparison.Ordinal);
        Assert.Contains("recurring_at_this_hour = recurrence?.RecurringAtThisHour ?? false,", body, StringComparison.Ordinal);
        Assert.Contains("recurrence_weeks = recurrence?.RecurrenceWeeks,", body, StringComparison.Ordinal);
        Assert.Contains("maintenance_window_moved = recurrence?.MaintenanceWindowMoved ?? false,", body, StringComparison.Ordinal);

        /* The description says what the fields are and that they are labels at unchanged severity — one
           sentence, on both SKUs. */
        Assert.Contains("recurring_at_this_hour is true when the chain fired in the same hour×weekday slot", body, StringComparison.Ordinal);
        Assert.Contains("These are LABELS at unchanged severity", body, StringComparison.Ordinal);
    }

    [Fact]
    public void BothGetAnalysisFindingsDescriptions_AreOneText()
    {
        static string Description(string source)
        {
            var start = source.IndexOf("Name = \"get_analysis_findings\"), Description(\"", StringComparison.Ordinal);
            Assert.True(start > 0);
            start += "Name = \"get_analysis_findings\"), Description(\"".Length;
            var end = source.IndexOf("\")]", start, StringComparison.Ordinal);
            return source[start..end];
        }

        Assert.Equal(
            Description(RepoFile.ReadRepoFileLf("Lite", "Mcp", "McpAnalysisTools.cs")),
            Description(RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs")));
    }

    /* ---------------- gated: the read against a real store, through to the sentence ---------------- */

    [Fact]
    public async Task GetPriorOccurrences_ReturnsInSlotChainsPerHour_JobCardsInAnySlot_TheOffset_AndLabelsFromThem()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live recurrence-read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var store = new PgFindingStore(postgres);
            var context = new AnalysisContext { ServerId = ServerId, ServerName = ServerName, TimeRangeStart = ReferenceUtc.AddHours(-4), TimeRangeEnd = ReferenceUtc };

            /* The offset row: collected a day before the reference, UTC-4. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(ReferenceUtc.AddDays(-1)), ServerId, ServerName, OffsetMinutes);

            /* Chain A in the slot (10:00 Tuesday local = 14:xx UTC Tuesday): three passes last week collapse to
               ONE row; one pass two weeks ago; one pass three weeks ago at 14:10 UTC, which is INSIDE the lower
               bound (reference − 21 d − 1 h = 13:37 UTC) — the slack the labeler asks for. */
            var rows = new List<AnalysisFinding>
            {
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 5)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 20)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 50)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 1, 14, 30)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 8, 25, 14, 10)),
                /* Excluded: the hour before (09:xx local), a Wednesday, before the lower bound, and after the
                   reference instant. */
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 13, 30)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 9, 14, 30)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 8, 25, 13, 0)),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 15, 14, 40)),
                /* Returned: an earlier pass TODAY in the slot — the store's contract is the slot, the labeler
                   drops week 0. */
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 15, 14, 10)),
                /* The job card last Thursday 02:xx local (06:xx UTC), two passes -> one row carrying its frozen
                   text; and one more pass at 03:xx local -> a second bucket. Neither is in the slot; both come
                   back because they are RUNNING_JOBS-rooted. */
                Finding(JobChain, "RUNNING_JOBS", Utc(2026, 9, 10, 6, 15), JobCard(JobName)),
                Finding(JobChain, "RUNNING_JOBS", Utc(2026, 9, 10, 6, 45), JobCard(JobName)),
                Finding(JobChain, "RUNNING_JOBS", Utc(2026, 9, 10, 7, 10), JobCard(JobName)),
                /* Another chain in the job's slot but not job-rooted: excluded. */
                Finding(ChainC, "CPU_SQL_PERCENT", Utc(2026, 9, 10, 6, 20)),
            };
            await store.InsertFindingsAsync(rows, context);

            var read = await store.GetPriorOccurrencesAsync(context, ReferenceUtc);

            Assert.Equal(OffsetMinutes, read.UtcOffsetMinutes);
            Assert.All(read.Occurrences, o => Assert.Equal(DateTimeKind.Unspecified, o.LocalBucket.Kind));

            var chainA = read.Occurrences.Where(o => o.StoryPathHash == ChainA).OrderBy(o => o.LocalBucket).ToList();
            Assert.Equal(
                new[] { Local(2026, 8, 25, 10), Local(2026, 9, 1, 10), Local(2026, 9, 8, 10), Local(2026, 9, 15, 10) },
                chainA.Select(o => o.LocalBucket).ToArray());
            Assert.All(chainA, o => Assert.Null(o.StoryText)); // text is read for job rows only
            Assert.All(chainA, o => Assert.Equal("SOS_SCHEDULER_YIELD", o.RootFactKey));

            var job = read.Occurrences.Where(o => o.StoryPathHash == JobChain).OrderBy(o => o.LocalBucket).ToList();
            Assert.Equal(new[] { Local(2026, 9, 10, 2), Local(2026, 9, 10, 3) }, job.Select(o => o.LocalBucket).ToArray());
            Assert.All(job, o => Assert.Equal(JobName, RecurrenceLabeler.TryReadJobName(o.StoryText)));

            Assert.DoesNotContain(read.Occurrences, o => o.StoryPathHash == ChainC);
            Assert.Equal(6, read.Occurrences.Count);

            /* Through to the sentence: this pass's SOS story is on its FOURTH consecutive week in the slot, and
               this pass's fired job (by name) sat at 02:00 Thursday last week — moved. */
            var sos = Story("SOS_SCHEDULER_YIELD", ChainA);
            var jobStory = Story("RUNNING_JOBS", JobChain, severity: 0.5);
            var jobFact = new Fact { Key = "RUNNING_JOBS", Source = "jobs", BaseSeverity = 0.5, Severity = 0.5, ObjectName = JobName, Metadata = new() { ["running_long_count"] = 1 } };
            RecurrenceLabeler.Label(new[] { sos, jobStory }, new[] { jobFact }, ReferenceUtc, read);

            var sosLabel = RecurrenceLabeler.TryReadLabel(sos.StoryText);
            Assert.NotNull(sosLabel);
            Assert.Equal(4, sosLabel!.RecurrenceWeeks);
            Assert.Contains("10:00 Tuesday, server local time", FactAdvice.TryReadStoryText(sos.StoryText)!.Investigation, StringComparison.Ordinal);
            Assert.Equal(1.1, sos.Severity);

            var jobLabel = RecurrenceLabeler.TryReadLabel(jobStory.StoryText);
            Assert.NotNull(jobLabel);
            Assert.True(jobLabel!.MaintenanceWindowMoved);
            Assert.Contains($"Agent job `{JobName}` ran at 02:00 Thursday last week, 10:00 Tuesday this week (server local time", FactAdvice.TryReadStoryText(jobStory.StoryText)!.Investigation, StringComparison.Ordinal);

            /* The UTC fallback: a server with NO server_properties row reads a null offset and UTC buckets. */
            var utcContext = new AnalysisContext { ServerId = UtcServerId, ServerName = UtcServerName, TimeRangeStart = ReferenceUtc.AddHours(-4), TimeRangeEnd = ReferenceUtc };
            await store.InsertFindingsAsync(new List<AnalysisFinding>
            {
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 5), serverId: UtcServerId, serverName: UtcServerName),
                Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 1, 14, 5), serverId: UtcServerId, serverName: UtcServerName),
            }, utcContext);

            var utcRead = await store.GetPriorOccurrencesAsync(utcContext, ReferenceUtc);
            Assert.Null(utcRead.UtcOffsetMinutes);
            Assert.Equal(new[] { Local(2026, 9, 1, 14), Local(2026, 9, 8, 14) }, utcRead.Occurrences.Select(o => o.LocalBucket).OrderBy(x => x).ToArray());

            var utcSos = Story("SOS_SCHEDULER_YIELD", ChainA);
            RecurrenceLabeler.Label(new[] { utcSos }, null, ReferenceUtc, utcRead);
            var utcInvestigation = FactAdvice.TryReadStoryText(utcSos.StoryText)!.Investigation;
            Assert.Contains("14:00 Tuesday UTC", utcInvestigation, StringComparison.Ordinal);
            Assert.Contains("the store carries no UTC offset for this server", utcInvestigation, StringComparison.Ordinal);
            Assert.Equal(3, RecurrenceLabeler.TryReadLabel(utcSos.StoryText)!.RecurrenceWeeks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ---------------- helpers ---------------- */

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Utc);
    private static DateTime Local(int y, int mo, int d, int h) => new(y, mo, d, h, 0, 0);

    private static string JobCard(string name) =>
        FactAdvice.SerializeForStoryText(new AdviceBlock($"Agent job `{name}` running well past normal — likely stuck, not busy", "job investigation.", "job remediation."));

    private static AnalysisFinding Finding(string hash, string rootKey, DateTime analysisTimeUtc, string? storyText = null, int? serverId = null, string? serverName = null) => new()
    {
        FindingId = CollectionIdGenerator.Next(),
        AnalysisTime = analysisTimeUtc,
        ServerId = serverId ?? ServerId,
        ServerName = serverName ?? ServerName,
        TimeRangeStart = analysisTimeUtc.AddHours(-4),
        TimeRangeEnd = analysisTimeUtc,
        Severity = 1.0,
        Confidence = 0.4,
        Category = "test",
        StoryPath = rootKey,
        StoryPathHash = hash,
        StoryText = storyText ?? FactAdvice.SerializeForStoryText(new AdviceBlock($"{rootKey} headline", $"{rootKey} investigation.", $"{rootKey} remediation.")),
        RootFactKey = rootKey,
        FactCount = 1,
        IncidentId = "rl-incident",
    };

    private static AnalysisStory Story(string rootKey, string hash, double severity = 1.1) => new()
    {
        RootFactKey = rootKey,
        StoryPathHash = hash,
        StoryPath = rootKey,
        Path = new List<string> { rootKey },
        Severity = severity,
        Confidence = 0.4,
        Category = "test",
        IncidentId = "rl-incident",
        StoryText = FactAdvice.SerializeForStoryText(new AdviceBlock($"{rootKey} headline", $"{rootKey} investigation.", $"{rootKey} remediation.")),
    };

    private static int CountOf(string text, string token)
    {
        var count = 0;
        for (var i = text.IndexOf(token, StringComparison.Ordinal); i >= 0; i = text.IndexOf(token, i + token.Length, StringComparison.Ordinal))
            count++;
        return count;
    }

    /// <summary>The verbatim body of <c>public const string NAME = @"…";</c> in a source file, LF-normalised.</summary>
    private static string ExtractConst(string source, string name)
    {
        var marker = $"public const string {name} = @\"";
        var start = source.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(start > 0, $"{name} not found");
        start += marker.Length;
        var end = source.IndexOf("\";", start, StringComparison.Ordinal);
        Assert.True(end > start);
        return source[start..end];
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id IN ({ServerId}, {UtcServerId});"
            + $" DELETE FROM server_properties WHERE server_id IN ({ServerId}, {UtcServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
