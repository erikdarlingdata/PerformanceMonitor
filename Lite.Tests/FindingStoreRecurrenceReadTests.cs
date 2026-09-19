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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 item 3 (ruling Q3), Lite's store side: <see cref="FindingStore.GetPriorOccurrencesAsync"/> against
/// a real DuckDB — the read the <see cref="RecurrenceLabeler"/> labels from. The labeler's own pins are in
/// <c>RecurrenceLabelerTests</c>; this pins that the DuckDB statement (one token off Darling's, see the
/// twin pin in Darling.Tests) returns what the labeler was written against: every chain that fired in the
/// reference instant's hour×weekday slot ON THE SERVER'S CLOCK collapsed to one row per hour, every
/// <c>RUNNING_JOBS</c>-rooted card in any slot with its frozen text, the offset the slot was keyed on, and
/// the UTC fallback with a null offset when <c>v_server_properties</c> carries none — then that the
/// labeler turns the read into the sentence.
/// </summary>
public sealed class FindingStoreRecurrenceReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 36530003;
    private const string ServerName = "recurrence-read-lite";
    private const int UtcServerId = 36530004;
    private const string UtcServerName = "recurrence-read-lite-no-offset";

    /* Tuesday 14:37:12 UTC; on the UTC-4 server that is 10:37 local, so the slot is 10:00 Tuesday. */
    private static readonly DateTime ReferenceUtc = new(2026, 9, 15, 14, 37, 12, DateTimeKind.Utc);
    private const int OffsetMinutes = -240;
    private const string ChainA = "rl-lite-chain-a";
    private const string ChainC = "rl-lite-chain-c";
    private const string JobChain = "rl-lite-job-chain";
    private const string JobName = "Nightly Index Maintenance";

    private readonly DuckDbInitializer _duckDb;

    public FindingStoreRecurrenceReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task GetPriorOccurrences_ReturnsInSlotChainsPerHour_JobCardsInAnySlot_TheOffset_AndLabelsFromThem()
    {
        var store = new FindingStore(_duckDb);
        var context = new AnalysisContext { ServerId = ServerId, ServerName = ServerName, TimeRangeStart = ReferenceUtc.AddHours(-4), TimeRangeEnd = ReferenceUtc };

        await SeedServerPropertiesAsync(ServerId, ServerName, OffsetMinutes);

        /* Chain A in the slot (10:00 Tuesday local = 14:xx UTC Tuesday): three passes last week collapse to ONE
           row; one pass two weeks ago; one pass three weeks ago at 14:10 UTC, INSIDE the lower bound
           (reference − 21 d − 1 h = 13:37 UTC) — the slack the labeler asks for. Then the exclusions: the hour
           before (09:xx local), a Wednesday, before the lower bound, after the reference instant. An earlier
           pass TODAY in the slot IS returned — the store's contract is the slot; the labeler drops week 0. The
           job card last Thursday 02:xx local (06:xx UTC) twice -> one row carrying its frozen text, plus one
           pass at 03:xx local -> a second bucket; neither is in the slot, both return because they are
           RUNNING_JOBS-rooted. Another chain in the job's slot, not job-rooted: excluded. */
        await store.InsertFindingsAsync(new List<AnalysisFinding>
        {
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 5)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 20)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 50)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 1, 14, 30)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 8, 25, 14, 10)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 13, 30)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 9, 14, 30)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 8, 25, 13, 0)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 15, 14, 40)),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 15, 14, 10)),
            Finding(JobChain, "RUNNING_JOBS", Utc(2026, 9, 10, 6, 15), JobCard(JobName)),
            Finding(JobChain, "RUNNING_JOBS", Utc(2026, 9, 10, 6, 45), JobCard(JobName)),
            Finding(JobChain, "RUNNING_JOBS", Utc(2026, 9, 10, 7, 10), JobCard(JobName)),
            Finding(ChainC, "CPU_SQL_PERCENT", Utc(2026, 9, 10, 6, 20)),
        }, context);

        var read = await store.GetPriorOccurrencesAsync(context, ReferenceUtc);

        Assert.Equal(OffsetMinutes, read.UtcOffsetMinutes);

        var chainA = read.Occurrences.Where(o => o.StoryPathHash == ChainA).OrderBy(o => o.LocalBucket).ToList();
        Assert.Equal(
            new[] { Local(2026, 8, 25, 10), Local(2026, 9, 1, 10), Local(2026, 9, 8, 10), Local(2026, 9, 15, 10) },
            chainA.Select(o => o.LocalBucket).ToArray());
        Assert.All(chainA, o => Assert.Null(o.StoryText));
        Assert.All(chainA, o => Assert.Equal("SOS_SCHEDULER_YIELD", o.RootFactKey));

        var job = read.Occurrences.Where(o => o.StoryPathHash == JobChain).OrderBy(o => o.LocalBucket).ToList();
        Assert.Equal(new[] { Local(2026, 9, 10, 2), Local(2026, 9, 10, 3) }, job.Select(o => o.LocalBucket).ToArray());
        Assert.All(job, o => Assert.Equal(JobName, RecurrenceLabeler.TryReadJobName(o.StoryText)));

        Assert.DoesNotContain(read.Occurrences, o => o.StoryPathHash == ChainC);
        Assert.Equal(6, read.Occurrences.Count);

        /* Through to the sentence: fourth consecutive week for the SOS chain; the fired job (by name) sat at
           02:00 Thursday last week — moved. */
        var sos = Story("SOS_SCHEDULER_YIELD", ChainA);
        var jobStory = Story("RUNNING_JOBS", JobChain, severity: 0.5);
        var jobFact = new Fact { Key = "RUNNING_JOBS", Source = "jobs", BaseSeverity = 0.5, Severity = 0.5, ObjectName = JobName, Metadata = new() { ["running_long_count"] = 1 } };
        RecurrenceLabeler.Label(new[] { sos, jobStory }, new[] { jobFact }, ReferenceUtc, read);

        Assert.Equal(4, RecurrenceLabeler.TryReadLabel(sos.StoryText)!.RecurrenceWeeks);
        Assert.Contains("10:00 Tuesday, server local time", FactAdvice.TryReadStoryText(sos.StoryText)!.Investigation, StringComparison.Ordinal);
        Assert.Equal(1.1, sos.Severity);
        Assert.True(RecurrenceLabeler.TryReadLabel(jobStory.StoryText)!.MaintenanceWindowMoved);
        Assert.Contains($"Agent job `{JobName}` ran at 02:00 Thursday last week, 10:00 Tuesday this week (server local time", FactAdvice.TryReadStoryText(jobStory.StoryText)!.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoServerPropertiesRow_ReadsANullOffset_UtcBuckets_AndTheSentenceSaysSo()
    {
        var store = new FindingStore(_duckDb);
        var context = new AnalysisContext { ServerId = UtcServerId, ServerName = UtcServerName, TimeRangeStart = ReferenceUtc.AddHours(-4), TimeRangeEnd = ReferenceUtc };

        await store.InsertFindingsAsync(new List<AnalysisFinding>
        {
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 8, 14, 5), serverId: UtcServerId, serverName: UtcServerName),
            Finding(ChainA, "SOS_SCHEDULER_YIELD", Utc(2026, 9, 1, 14, 5), serverId: UtcServerId, serverName: UtcServerName),
        }, context);

        var read = await store.GetPriorOccurrencesAsync(context, ReferenceUtc);

        Assert.Null(read.UtcOffsetMinutes);
        Assert.Equal(new[] { Local(2026, 9, 1, 14), Local(2026, 9, 8, 14) }, read.Occurrences.Select(o => o.LocalBucket).OrderBy(x => x).ToArray());

        var sos = Story("SOS_SCHEDULER_YIELD", ChainA);
        RecurrenceLabeler.Label(new[] { sos }, null, ReferenceUtc, read);
        var investigation = FactAdvice.TryReadStoryText(sos.StoryText)!.Investigation;
        Assert.Contains("14:00 Tuesday UTC", investigation, StringComparison.Ordinal);
        Assert.Contains("the store carries no UTC offset for this server", investigation, StringComparison.Ordinal);
        Assert.Equal(3, RecurrenceLabeler.TryReadLabel(sos.StoryText)!.RecurrenceWeeks);
    }

    [Fact]
    public async Task AnEmptyStore_ReadsEmpty_AndLabelsNothing()
    {
        var store = new FindingStore(_duckDb);
        var context = new AnalysisContext { ServerId = ServerId, ServerName = ServerName, TimeRangeStart = ReferenceUtc.AddHours(-4), TimeRangeEnd = ReferenceUtc };

        var read = await store.GetPriorOccurrencesAsync(context, ReferenceUtc);

        Assert.Null(read.UtcOffsetMinutes);
        Assert.Empty(read.Occurrences);

        var sos = Story("SOS_SCHEDULER_YIELD", ChainA);
        var before = sos.StoryText;
        RecurrenceLabeler.Label(new[] { sos }, null, ReferenceUtc, read);
        Assert.Equal(before, sos.StoryText);
    }

    /* ---------------- helpers ---------------- */

    private async Task SeedServerPropertiesAsync(int serverId, string serverName, int offsetMinutes)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = CollectionIdGenerator.Next() });
        cmd.Parameters.Add(new DuckDBParameter { Value = ReferenceUtc.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = offsetMinutes });
        await cmd.ExecuteNonQueryAsync();
    }

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
}
