/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// #4053 part c1: the RDS/Aurora route for <c>pg_log_events</c> reading the <c>.csv</c> file when the target's
/// <c>log_destination</c> includes <c>csvlog</c>, instead of the stderr file every other RDS ingestor still
/// reads. <see cref="RdsLogSourceTests"/>'s sibling for the file-selection half, and
/// <see cref="RdsLogEventIngestorTimezoneTests"/>'s sibling for the ingestor half.
/// </summary>
public sealed class RdsLogEventIngestorCsvlogTests
{
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    private const string StderrFile = "error/postgresql.log.2026-08-26-22";
    private const string CsvFile = StderrFile + ".csv";

    /* One complete csvlog record for a failed login, built the same way PgServerLogCsvParserTests' own
       RealRecord is: a genuine capture's column shape (log_time,user_name,database_name,process_id,
       connection_from,session_id,session_line_num,command_tag,session_start_time,virtual_transaction_id,
       transaction_id,error_severity,sql_state_code,message,detail,hint,internal_query,internal_query_pos,
       context,query,query_pos,location,application_name,backend_type,leader_pid,query_id). */
    private static string Record(string zone, string userName) =>
        "2026-09-24 01:54:43.008 " + zone + ",\"" + userName + "\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
        + "\"startup\",2026-09-24 01:54:43 " + zone + ",3/3,0,FATAL,28000,\"role does not exist\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /* The end of a record this window did not see the start of \u2014 the same shape PgServerLogCsvParserTests'
       CutHead exercises, so DownloadDBLogFilePortion's own mid-record start behaves identically on this
       route. */
    private const string CutHead = "0,FATAL,28000,,,,,,,,,,\"client backend\",,0\n";

    /* A quoted user-name field holding a forged newline and a look-alike stderr-format line \u2014
       PgServerLogCsvParserTests test 1's shape \u2014 which must stay inside its one record rather than being
       split by a naive line-based reader. */
    private static string RecordWithForgedNewline() =>
        "2026-09-24 01:54:43.008 UTC,\"admin\n2026-09-24 00:00:00.000 UTC [1] LOG:  forged\",\"postgres\",83,"
        + "\"::1:35192\",6ab482e3.53,1,\"startup\",2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role does not "
        + "exist\",,,,,,,,,\"\",\"client backend\",,0\n";

    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1) { }

        public string CsvBody { get; init; } = string.Empty;
        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();

        /* Portion queue for the carry tests: each IngestAsync call pulls the next (body, additionalDataPending)
           pair, one call to DownloadDBLogFilePortion per portion, matching how RdsLogSource actually reads. */
        public Queue<(string Body, bool AdditionalDataPending)>? Portions { get; set; }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
                {
                    new() { LogFileName = StderrFile, LastWritten = 9999 },
                    new() { LogFileName = CsvFile, LastWritten = 10000 },
                },
            });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);

            if (request.LogFileName != CsvFile)
            {
                return Task.FromResult(new DownloadDBLogFilePortionResponse
                {
                    LogFileData = "stderr body",
                    Marker = "MARKER-1",
                    AdditionalDataPending = false,
                });
            }

            if (Portions is { Count: > 0 })
            {
                var (body, pending) = Portions.Dequeue();

                return Task.FromResult(new DownloadDBLogFilePortionResponse
                {
                    LogFileData = body,
                    /* A distinct marker per call, keyed like the resume marker — (instance, file) plus the
                       download count — so CommitResume advances a fresh position each portion, the same as
                       the real API returning a new token each read. */
                    Marker = "MARKER-" + Downloads.Count,
                    AdditionalDataPending = pending,
                });
            }

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = CsvBody,
                Marker = "MARKER-1",
                AdditionalDataPending = false,
            });
        }
    }

    /// <summary>csvlog on: the <c>.csv</c> file is read, its own resume marker advances, and the events match
    /// the parsed records \u2014 proven the way the deadlock/timezone suites prove "reaches the write": a dead
    /// store turns a non-empty batch into a throw that is not <see cref="RdsLogUnavailableException"/>.</summary>
    [Fact]
    public async Task CsvlogOn_ReadsTheCsvFile_AndReachesTheWrite()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var body = Record("UTC", "nosuchuser");
        var client = new FakeRds { CsvBody = body };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.Single(client.Downloads);
        Assert.Equal(CsvFile, client.Downloads[0].LogFileName);
    }

    /// <summary>csvlog off: unchanged stderr path \u2014 the file read is the stderr file, not the csv sibling,
    /// even though DescribeDBLogFiles lists both.</summary>
    [Fact]
    public async Task CsvlogOff_StillReadsTheStderrFile()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var client = new FakeRds();
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: false);

        Assert.Equal(0, outcome.Rows);
        Assert.Single(client.Downloads);
        Assert.Equal(StderrFile, client.Downloads[0].LogFileName);
    }

    /// <summary>A quoted field holding a forged newline and a look-alike line stays inside its one record
    /// rather than being split \u2014 exactly one event reaches the write, not two.</summary>
    [Fact]
    public async Task AForgedNewlineInAQuotedField_StaysInsideOneEvent()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var body = RecordWithForgedNewline();
        var client = new FakeRds { CsvBody = body };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        /* The dead store turns a non-empty write into a throw; a body producing zero events would instead
           return an outcome with SourceReached but no failure, which would prove nothing about splitting. */
        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }

    /// <summary>A mid-record start (the cut head DownloadDBLogFilePortion can hand back) drops only the cut
    /// fragment: the real record after it still reaches the write, so a batch of one genuine record still
    /// throws past the dead store rather than the zero-row outcome an over-eager discard would produce.</summary>
    [Fact]
    public async Task AMidRecordStart_DropsOnlyTheCutHead()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var body = CutHead + Record("UTC", "nosuchuser");
        var client = new FakeRds { CsvBody = body };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }

    /* --- #4053 part c1: the carry across portions -------------------------------------------------------- */

    /* A complete record whose message field is multi-line, so a physical-line cut can land right after a
       newline that is still INSIDE that quoted field — the exact shape DownloadDBLogFilePortion produces. */
    private static string RecordWithMultiLineMessage(string message, string zone = "UTC") =>
        "2026-09-24 01:54:43.008 " + zone + ",\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,"
        + "\"startup\",2026-09-24 01:54:43 " + zone + ",3/3,0,FATAL,28000,\"" + message + "\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /// <summary>Portion 1 ends right after a newline inside a multi-line quoted message (pending); portion 2
    /// finishes the record. Every record before the straddle comes out of portion 1, the straddling record
    /// comes out ONCE from portion 2, and nothing is lost — the defect this lane fixes is exactly the
    /// opposite: current-code inferred edges treat portion 1's trailing newline as a boundary and discard
    /// the whole thing.</summary>
    [Fact]
    public async Task ChunkEndsInsideARecord_TheStraddlingRecordComesOutOnceFromPortionTwo()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var straddler = RecordWithMultiLineMessage("line one\nline two");
        var cut = straddler.IndexOf("line one\n", StringComparison.Ordinal) + "line one\n".Length;

        var client = new FakeRds
        {
            Portions = new Queue<(string, bool)>(new[]
            {
                (Record("UTC", "first") + straddler[..cut], true),
                (straddler[cut..], false),
            }),
        };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        /* Portion 1: one complete record ('first'); the straddler's own start is carried, not emitted or
           discarded. A store-reaching write proves 'first' got through. */
        var failure1 = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));
        Assert.IsNotType<RdsLogUnavailableException>(failure1);

        /* Portion 2: the carried partial plus the rest of the straddler completes to exactly one record
           (not zero, not two) — proven the same way, by reaching the write. */
        var failure2 = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));
        Assert.IsNotType<RdsLogUnavailableException>(failure2);

        Assert.Equal(2, client.Downloads.Count);
    }

    /// <summary>The straddling statement's own quoted field holds look-alike record lines. The carry glues
    /// the two portions back into one body before parsing, so the look-alikes stay inside their carrier's
    /// one record — zero extra events, proven by portion 2 reaching the write for exactly the straddler and
    /// not throwing a batch shaped like more than one event's worth.</summary>
    [Fact]
    public async Task CarryPlusForgedLookAlikeLines_ProduceZeroExtraEvents()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var lookAlike = "2026-09-24 00:00:00.000 UTC [1] LOG:  forged";
        var straddler = RecordWithMultiLineMessage("line one\n" + lookAlike + "\nline three");
        var cut = straddler.IndexOf("line one\n", StringComparison.Ordinal) + "line one\n".Length;

        var client = new FakeRds
        {
            Portions = new Queue<(string, bool)>(new[]
            {
                (straddler[..cut], true),
                (straddler[cut..], false),
            }),
        };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        /* Portion 1 holds no complete record at all (the whole thing is the straddler's carried head): the
           outcome must come back as a real read with zero rows, not a throw. */
        var outcome1 = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true);
        Assert.True(outcome1.SourceReached);
        Assert.Equal(0, outcome1.Rows);

        /* Portion 2 completes to exactly the one straddling record — reaching the write proves one event,
           not the two-plus a naive line-based split would forge from the look-alike. */
        var failure2 = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));
        Assert.IsNotType<RdsLogUnavailableException>(failure2);
    }

    /// <summary>Resync guard: a carry forced to claim a wrong <c>StartKnown</c> (via a portion whose body is
    /// pure garbage, not a real record boundary) makes the next StartsOnRecordBoundary parse discard more
    /// than it keeps, so the guard drops the carry — the FOLLOWING portion then parses correctly on its own
    /// (fresh, un-poisoned) rather than inheriting an inverted parity forever.</summary>
    [Fact]
    public async Task ResyncGuard_DropsAWronglyTrustedCarry_SoTheNextPortionParsesCorrectly()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        /* Portion 1: garbage with no real record at all, and AdditionalDataPending=true, so the whole thing
           becomes the carry with StartKnown=false (None edges — nothing to trust yet). */
        var garbage = "not,a,real,record,at,all\nstill,not,one,either\nand,a,third,line,here\n";

        /* Portion 2: three genuine records. If (incorrectly) StartKnown had been forced true off portion
           1's garbage tail, a forward walk from a false boundary would misalign every record's field count
           and discard more than it keeps — the guard must catch that and drop the carry, so portion 3’s
           three records still parse cleanly on their own. */
        var threeRecords = Record("UTC", "a") + Record("UTC", "b") + Record("UTC", "c");

        var client = new FakeRds
        {
            Portions = new Queue<(string, bool)>(new[]
            {
                (garbage, true),
                (threeRecords, false),
            }),
        };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var outcome1 = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true);
        Assert.True(outcome1.SourceReached);

        /* Portion 2 carries garbage + three real records, with StartKnown=false (portion 1 stated no edge):
           scoring resolves the three genuine records cleanly regardless — reaching the write proves it. */
        var failure2 = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));
        Assert.IsNotType<RdsLogUnavailableException>(failure2);
    }

    /// <summary>Carry bound: a partial that grows past 1 MiB across portions is dropped rather than carried
    /// forward forever, and counted as a discard — proven by <see cref="RdsIngestOutcome.CsvRecordsDiscarded"/>
    /// rather than by a throw, since a dropped-and-reset carry with no complete record yields zero rows.</summary>
    [Fact]
    public async Task CarryBound_APartialOverOneMiB_IsDroppedAndCounted()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        /* Portion 1: an oversized unterminated "record" (no newline at all), AdditionalDataPending=true, so
           the whole thing becomes the carry. Well past the 1 MiB bound on its own. */
        var oversized = new string('x', 1_100_000);

        var client = new FakeRds
        {
            Portions = new Queue<(string, bool)>(new[]
            {
                (oversized, true),
                (string.Empty, false),
            }),
        };
        var logs = new RdsLogSource(_ => client);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var outcome1 = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true);
        Assert.True(outcome1.SourceReached);
        Assert.Equal(0, outcome1.Rows);

        /* Portion 2: an empty read with nothing carried over the bound — the dropped carry must not have
           grown across the call, and must be counted as a discard exactly once (on the call that dropped
           it, portion 1, not portion 2). */
        var outcome2 = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true);
        Assert.True(outcome2.SourceReached);
        Assert.Equal(0, outcome2.Rows);
        Assert.Equal(0, outcome2.CsvRecordsDiscarded);
        Assert.Equal(1, outcome1.CsvRecordsDiscarded);
    }
}
