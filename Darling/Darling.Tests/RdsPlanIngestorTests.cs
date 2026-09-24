/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The RDS PLAN ingestor's resume-marker discipline (#3008) — the same invariant
/// <see cref="RdsDeadlockIngestorTests"/> pins for the deadlock route, on the other caller of
/// <c>RdsLogSource.ReadNewestAsync</c>.
///
/// <para><b>Why this is a second suite and not a line in the first.</b> The two ingestors hold separate
/// <see cref="RdsLogSource"/> instances and separate copies of the read-store-commit sequence, so the
/// ordering can regress in one without touching the other. The obvious future edit — folding the two
/// <c>StoreAsync</c> copies into a shared helper — is exactly the one that could put the commit back on the
/// wrong side of the write here while the deadlock suite stayed green. <c>RdsPlanIngestionFromRealLogTests</c>
/// covers <c>PgPlanLogParser</c> and never reaches <c>IngestAsync</c>, so before this file the plan half of
/// the fix had no regression test at all.</para>
///
/// <para>Deliberately a subset rather than a transcription: the parse-refusal case has no plan-route
/// equivalent (the non-UTC refusal is the deadlock parser's), and the reasoning behind the fake and the
/// closed-port store is recorded once, on the deadlock suite.</para>
/// </summary>
public sealed class RdsPlanIngestorTests
{
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    /* The REAL auto_explain capture the plan parser's own suite is built on, read from the same place it
       reads it. A hand-written plan block would agree with whatever I believed auto_explain's shape was. */
    private static readonly string PlanText =
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "auto_explain_real_block.txt"));

    /* Log traffic with no plan in it — a server whose auto_explain threshold nothing crossed. */
    private const string QuietText =
        "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n"
        + "2026-08-26 22:30:12.000 UTC [1600] LOG:  checkpoint complete: wrote 42 buffers\n";

    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"),
            Amazon.RegionEndpoint.USEast1) { }

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();

        public string FirstBody { get; init; } = PlanText;

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
                {
                    new() { LogFileName = "error/postgresql.log.2026-08-26-15", LastWritten = 9999 },
                },
            });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);

            var resumed = request.Marker == "MARKER-1";

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = resumed ? QuietText : FirstBody,
                Marker = resumed ? "MARKER-2" : "MARKER-1",
                AdditionalDataPending = false,
            });
        }
    }

    private static (RdsPlanIngestor Ingestor, FakeRds Client, RdsLogSource Logs) Build(
        NpgsqlDataSource store, FakeRds? client = null)
    {
        var fake = client ?? new FakeRds();
        var logs = new RdsLogSource(_ => fake);
        return (new RdsPlanIngestor(store, logs), fake, logs);
    }

    /// <summary>
    /// The precondition the failure tests rest on: the fixture really does parse to a plan, so a cycle over
    /// it really does reach the COPY. A fixture that stopped parsing would make the ingestor store nothing —
    /// a legitimate commit — and the tests below would fail for an unrelated reason.
    /// </summary>
    [Fact]
    public void TheFixtureReallyParsesToAPlan()
    {
        Assert.Single(PgPlanLogParser.Extract(PlanText));
        Assert.Empty(PgPlanLogParser.Extract(QuietText));
    }

    /// <summary>
    /// The plan route's copy of the assertion this fix exists for: the store write fails, so the marker
    /// must not move, and the next cycle asks RDS for the same window rather than resuming past it.
    ///
    /// <para>Moving the commit back inside the fetch fails <c>Assert.Null(client.Downloads[1].Marker)</c>
    /// with <c>MARKER-1</c>.</para>
    /// </summary>
    [Fact]
    public async Task AFailedStoreWriteDoesNotAdvanceTheMarker()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, logs) = Build(store);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.Single(client.Downloads);

        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));

        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[1].Marker);

        /* And the bytes are genuinely still there, which is what "nothing was lost" means. */
        var third = await logs.ReadNewestAsync(Host);
        Assert.Equal(PlanText, third!.Value.Text);
        Assert.Single(PgPlanLogParser.Extract(third.Value.Text));
    }

    /// <summary>
    /// Cancellation is the same category as a failed write — shutdown mid-COPY consumed the window just as
    /// permanently, and nothing about it looked like an error worth investigating.
    /// </summary>
    [Fact]
    public async Task ACancelledCycleDoesNotAdvanceTheMarker()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ingestor.IngestAsync(1, "target-a", Host, cancellationToken: cancelled.Token));

        Assert.Single(client.Downloads);

        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));

        Assert.Null(client.Downloads[1].Marker);
    }

    /// <summary>
    /// The control that stops the two tests above passing over a commit that never fires: a cycle with
    /// nothing to store loses nothing, so the marker must advance — otherwise a server below its
    /// auto_explain threshold would re-request the same bounded tail every cycle forever.
    /// </summary>
    [Fact]
    public async Task AQuietCycleAdvancesTheMarker()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store, new FakeRds { FirstBody = QuietText });

        Assert.Equal(RdsIngestOutcome.Read(0), await ingestor.IngestAsync(1, "target-a", Host));
        Assert.Equal(RdsIngestOutcome.Read(0), await ingestor.IngestAsync(1, "target-a", Host));

        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[0].Marker);
        Assert.Equal("MARKER-1", client.Downloads[1].Marker);
        Assert.Equal(0, client.Downloads[1].NumberOfLines);
    }

    /// <summary>
    /// A non-RDS host reaches nothing, and the outcome says so rather than reporting an empty log (#3017).
    /// That target reads its log through <c>pg_read_file</c> instead, so no log file was listed and none was
    /// downloaded — "no new auto_explain plans in the RDS log window" would be a claim about a log this
    /// cycle never opened.
    ///
    /// <para>Sits directly beside <see cref="AQuietCycleAdvancesTheMarker"/> on purpose: that one reaches
    /// the log and finds nothing worth storing, this one never reaches it, and the pair is what makes the
    /// over-exclusion direction visible too — a reached-but-empty cycle must NOT be reported as
    /// unreached.</para>
    /// </summary>
    [Fact]
    public async Task ANonRdsHost_ReportsTheSourceUnreached_WithNoAwsCall()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store);

        var outcome = await ingestor.IngestAsync(1, "target-a", "db.internal.example.com");

        Assert.Equal(RdsIngestOutcome.NotReached, outcome);
        Assert.False(outcome.SourceReached);
        Assert.NotEqual(RdsIngestOutcome.Read(0), outcome);
        Assert.Empty(client.Downloads);
    }

    /* --- #4053 part c3: this ingestor's own csvlog route, mirroring RdsDeadlockIngestorTests' own -------- */

    private const string StderrFile = "error/postgresql.log.2026-08-26-15";
    private const string CsvFile = StderrFile + ".csv";

    /* One real auto_explain csvlog record, the same shape PgPlanCaptureCsvUnitTests' own RealPlanRecord is
       built on — verbatim what a live pg18 rig wrote, only the Relation Name and query id are this test's
       own choice. */
    private const string CsvPlanRecord =
        "2026-09-24 04:29:03.817 UTC,\"postgres\",\"postgres\",352,\"[local]\",6ab4a70f.160,1,\"SELECT\","
        + "2026-09-24 04:29:03 UTC,6/2,781,LOG,00000,\"duration: 0.020 ms  plan:\n"
        + "{\n"
        + "  \"\"Plan\"\": {\n"
        + "    \"\"Node Type\"\": \"\"Seq Scan\"\",\n"
        + "    \"\"Relation Name\"\": \"\"plan_capture_c3\"\",\n"
        + "    \"\"Filter\"\": \"\"(id > 5)\"\"\n"
        + "  }\n"
        + "}\",,,,,,,,,\"psql\",\"client backend\",,-3560200806914842915\n";

    /* A plan-shaped look-alike inside another record's quoted field — PgPlanCaptureCsvUnitTests' own
       RecordWithForgedPlanInUserName shape: the forged newline plus fake plan line stay inside the ONE
       quoted user_name field of a FATAL-severity record, never becoming a second plan record of their
       own. */
    private static readonly string CsvRecordWithForgedPlanLookAlike =
        "2026-09-24 04:29:03.008 UTC,\"nosuchuser\n"
        + "2026-09-24 04:29:03.000 UTC,,,1,,1.1,1,,2026-09-24 04:29:03 UTC,,0,LOG,00000,"
        + "\"\"duration: 1.0 ms  plan:\n{\"\"Plan\"\": {\"\"Relation Name\"\": \"\"FORGED\"\"}}\"\"\","
        + "\"postgres\",83,\"[local]\",6ab482e3.53,1,\"startup\",2026-09-24 04:29:03 UTC,3/3,0,FATAL,28000,"
        + "\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\",\"client backend\",,0\n";

    /* The same record at NOTICE severity — auto_explain writes at LOG only, and the csv route requires the
       LOG label exactly as the stderr route's regex does. */
    private static readonly string CsvPlanRecordAtNotice =
        CsvPlanRecord.Replace(",6/2,781,LOG,00000,", ",6/2,781,NOTICE,00000,", StringComparison.Ordinal);

    private static List<DescribeDBLogFilesDetails> PlanFileListing() => new()
    {
        new() { LogFileName = StderrFile, LastWritten = 9999 },
        new() { LogFileName = CsvFile, LastWritten = 10000 },
    };

    private sealed class CsvFakeRds : AmazonRDSClient
    {
        public CsvFakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1) { }

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();
        public List<DescribeDBLogFilesDetails> Files { get; set; } = PlanFileListing();
        public string CsvBody { get; set; } = string.Empty;
        public string? NextCsvMarker { get; set; } = "MARKER-1";

        /// <summary>#4053 part c3, case 5: one queued <c>.csv</c> download per entry, a fresh marker per
        /// call, mirroring RdsDeadlockIngestorTests' own CsvPortions.</summary>
        public Queue<(string Body, bool AdditionalDataPending)>? CsvPortions { get; set; }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse { DescribeDBLogFiles = Files });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);

            if (!request.LogFileName.EndsWith(".csv", StringComparison.Ordinal))
            {
                return Task.FromResult(new DownloadDBLogFilePortionResponse
                {
                    LogFileData = "stderr body",
                    Marker = "MARKER-1",
                    AdditionalDataPending = false,
                });
            }

            if (CsvPortions is { Count: > 0 })
            {
                var (body, pending) = CsvPortions.Dequeue();

                return Task.FromResult(new DownloadDBLogFilePortionResponse
                {
                    LogFileData = body,
                    Marker = "MARKER-" + Downloads.Count,
                    AdditionalDataPending = pending,
                });
            }

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = CsvBody,
                Marker = NextCsvMarker,
                AdditionalDataPending = false,
            });
        }
    }

    /// <summary>
    /// #4053 part c3, case 1: csvlog on names the <c>.csv</c> file, and a portion holding one real
    /// auto_explain record reaches the write — proven the way every other test in this file proves it: the
    /// dead store turns a non-empty batch into a throw that is not <see cref="RdsLogUnavailableException"/>.
    /// </summary>
    [Fact]
    public async Task CsvlogOn_NamesTheCsvFile_AndAPortionHoldingOnePlanReachesTheWrite()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var client = new CsvFakeRds { CsvBody = CsvPlanRecord };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsPlanIngestor(store, logs);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.Single(client.Downloads);
        Assert.Equal(CsvFile, client.Downloads[0].LogFileName);
    }

    /// <summary>
    /// #4053 part c3, case 2: a plan-shaped look-alike inside another record's quoted field must not be read
    /// as its own capture. No write, zero rows: the dead store is never opened.
    /// </summary>
    [Fact]
    public async Task ALookAlikeInsideAQuotedField_ProducesNoWrite_AndZeroRows()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var client = new CsvFakeRds { CsvBody = CsvRecordWithForgedPlanLookAlike };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsPlanIngestor(store, logs);

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);

        Assert.Equal(0, outcome.Rows);
        /* Not dropped as forged: the look-alike never became a plan record at all (review round 1). */
        Assert.Equal(0, outcome.ForgedCaptures);

        /* Lane 4053-rds-tests item 7: the look-alike stays inside its one record — the csv parser
           discards nothing, because there is nothing outside a complete record here for it to discard. */
        Assert.Equal(0, outcome.CsvRecordsDiscarded);
    }

    /// <summary>
    /// #4053 c3 review: the RDS csv route counts a forged capture the way the self-hosted route does. A
    /// LOG-severity plan record whose duration has the wrong shape is skipped and counted, and nothing is
    /// written, so no store is opened.
    /// </summary>
    [Fact]
    public async Task APlanRecordWithAMalformedDuration_IsCountedAsForged_AndNotWritten()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var forged = CsvPlanRecord.Replace("duration: 0.020 ms  plan:", "duration: 1.2.3 ms  plan:", StringComparison.Ordinal);
        Assert.NotEqual(CsvPlanRecord, forged);
        var client = new CsvFakeRds { CsvBody = forged };
        var ingestor = new RdsPlanIngestor(store, new RdsLogSource(_ => client));

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);

        Assert.Equal(0, outcome.Rows);
        Assert.Equal(1, outcome.ForgedCaptures);
    }

    /// <summary>
    /// #4053 part c3, case 3: a plan record at NOTICE severity is not a capture — auto_explain writes at LOG
    /// only, and the csv route requires the LOG label exactly as the stderr route's regex does. No write.
    /// </summary>
    [Fact]
    public async Task APlanShapedRecordAtNoticeSeverity_ProducesNoWrite()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var client = new CsvFakeRds { CsvBody = CsvPlanRecordAtNotice };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsPlanIngestor(store, logs);

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);

        Assert.Equal(0, outcome.Rows);

        /* Lane 4053-rds-tests item 7: a NOTICE-severity record is not read as a forged capture either —
           it is simply not a plan candidate at all, the same distinction the look-alike test above draws. */
        Assert.Equal(0, outcome.ForgedCaptures);
        Assert.Equal(0, outcome.CsvRecordsDiscarded);
    }

    /// <summary>
    /// #4053 part c3, case 4: csvlog off requests the stderr file, as before — asserted against a listing
    /// that also carries a <c>.csv</c> sibling so the choice is a real one rather than the sibling being
    /// absent.
    /// </summary>
    [Fact]
    public async Task CsvlogOff_StillRequestsTheStderrFile()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var client = new CsvFakeRds();
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsPlanIngestor(store, logs);

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: false);

        Assert.Equal(0, outcome.Rows);
        Assert.Single(client.Downloads);
        Assert.Equal(StderrFile, client.Downloads[0].LogFileName);
    }

    /// <summary>
    /// #4053 part c3, case 5: a plan record split across two portions comes out once, from the second — the
    /// zero-event-first-portion shape RdsDeadlockIngestorTests' own split test uses. Portion 1 is pending and
    /// holds only the record's head (no events, no write); portion 2 holds the tail and reaches the write.
    /// </summary>
    [Fact]
    public async Task APlanRecordSplitAcrossTwoPortions_ComesOutOnce_FromTheSecondPortion()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var full = CsvPlanRecord;
        var cut = full.IndexOf("duration: 0.020", StringComparison.Ordinal);
        var client = new CsvFakeRds
        {
            CsvPortions = new Queue<(string, bool)>(new[]
            {
                (full[..cut], true),
            }),
        };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsPlanIngestor(store, logs);

        /* Portion 1: pending, holds only the record's head. No events, so no write and no throw. */
        var first = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);
        Assert.Equal(0, first.Rows);

        /* Portion 2: the tail, queued as the single-shot CsvBody/NextCsvMarker so this read resumes with the
           carry the first call recorded. Reaches the write — the dead store turns that into a throw. */
        client.CsvBody = full[cut..];
        client.NextCsvMarker = "MARKER-2";
        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }

    /// <summary>
    /// Lane 4053-rds-tests item 5: the plan route's copy of the deadlock suite's store-failure replay. Portion
    /// 1 (pending) holds only the record's head, so there is no write and the carry's own marker commits.
    /// Portion 2 holds the tail and throws on the dead store — a write was attempted. Re-serving portion 2
    /// (the same bytes, unresumed past it) presents the SAME marker as the first attempt and throws again,
    /// because the record is still whole rather than half-consumed by the failed attempt.
    /// </summary>
    [Fact]
    public async Task AStoreFailureOnTheSecondPortion_PresentsTheSameMarkerAgain_AndThrowsAgain()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var full = CsvPlanRecord;
        var cut = full.IndexOf("duration: 0.020", StringComparison.Ordinal);
        var client = new CsvFakeRds
        {
            CsvPortions = new Queue<(string, bool)>(new[]
            {
                (full[..cut], true),
            }),
        };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsPlanIngestor(store, logs);

        var first = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);
        Assert.Equal(0, first.Rows);

        client.CsvBody = full[cut..];
        client.NextCsvMarker = "MARKER-2";
        await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true));

        /* Serve the SAME bytes again — client.CsvBody/NextCsvMarker untouched — exactly the request a
           resumed-but-not-yet-advanced caller would make. The third download must present the SAME marker
           as the second, and the record must throw again rather than being silently dropped or split. */
        var third = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true));

        Assert.IsNotType<RdsLogUnavailableException>(third);
        Assert.Equal(3, client.Downloads.Count);
        Assert.Equal(client.Downloads[1].Marker, client.Downloads[2].Marker);
    }

    /// <summary>
    /// Lane 4053-rds-tests item 6: the plan route's copy of
    /// <c>RdsLogEventIngestorCsvlogTests.AReplayWithAnEmptyMarker_LeavesTheCarryUntouched</c>. Built so it
    /// FAILS if the carry is wrongly committed on an empty (replay) marker: the first read has no marker
    /// and holds only a straddling record's head, so it has no events, no write, and must not record a
    /// carry. The second read holds only the tail; without a carry it is a cut head with no events, no
    /// write, no throw. Had the first read's carry been wrongly recorded, the two would glue into one
    /// complete record, reach the write, and throw on the dead store.
    /// </summary>
    [Fact]
    public async Task AReplayWithAnEmptyMarker_LeavesTheCarryUntouched()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var full = CsvPlanRecord;
        var cut = full.IndexOf("duration: 0.020", StringComparison.Ordinal);

        var client = new CsvFakeRds { CsvBody = full[..cut], NextCsvMarker = null };
        var ingestor = new RdsPlanIngestor(store, new RdsLogSource(_ => client));

        var outcome1 = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);
        Assert.Equal(0, outcome1.Rows);

        client.CsvBody = full[cut..];
        client.NextCsvMarker = "MARKER-2";
        var outcome2 = await ingestor.IngestAsync(1, "target-a", Host, pgLogUsesCsvlog: true);
        Assert.Equal(0, outcome2.Rows);
        Assert.Equal(2, client.Downloads.Count);
    }
}
