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

    private static RdsLogEventIngestor.CsvPortion Step(RdsLogEventIngestor.CsvCarry carry, string text, bool pending) =>
        RdsLogEventIngestor.ParseCsvPortion(carry, text, pending);

    /// <summary>Portion 1 (pending) ends right after a newline inside a multi-line quoted message; portion 2
    /// finishes that record. The record before the straddle comes out of portion 1, and the straddling record
    /// comes out once, from portion 2. The inferred-edge parse #4135 used to call throws portion 1 away
    /// (asserted first), which the committed resume marker then made permanent.</summary>
    [Fact]
    public void AChunkEndingInsideARecord_LosesNothing_AndTheStraddlerComesOutOnce()
    {
        var straddler = RecordWithMultiLineMessage("line one\nline two");
        var cut = straddler.IndexOf("line one\n", StringComparison.Ordinal) + "line one\n".Length;
        var portion1 = Record("UTC", "first") + straddler[..cut];

        Assert.Empty(PgServerLogCsvParser.Parse(portion1, out _));

        var p1 = Step(RdsLogEventIngestor.CsvCarry.Empty, portion1, pending: true);
        Assert.Equal(new[] { "first" }, p1.Entries.ConvertAll(e => e.UserName ?? ""));
        Assert.Equal(0, p1.RecordsDiscarded);
        Assert.Equal(straddler[..cut], p1.Next.Partial);

        var p2 = Step(p1.Next, straddler[cut..], pending: false);
        var only = Assert.Single(p2.Entries);
        Assert.Equal("line one\nline two", only.Message);
        Assert.Equal(string.Empty, p2.Next.Partial);

        /* #4053 review round 1, item A: this portion ran with no known start (Step's carry above never
           passed through a file-start read), so the forward-only route's rule is that an unknown-start
           mode's next carry NEVER hands on a known start — it stays unknown until the next file's own
           offset-0 read. */
        Assert.False(p2.Next.StartKnown);
    }

    /// <summary>After a portion that ends at the file's end, the next portion's start is known, so a
    /// straddling statement whose quoted field holds look-alike record lines is walked FORWARD with exact
    /// parity across both later portions: the look-alikes stay inside their one record, with no extra
    /// entries at any step.</summary>
    [Fact]
    public void AKnownStart_KeepsLookAlikeLinesInsideTheStraddlingRecord()
    {
        /* csvlog doubles every quote inside a quoted field, so a planted look-alike reaches the file that way. */
        var lookAlikeBody = Record("UTC", "planted").TrimEnd('\n').Replace("\"", "\"\"", StringComparison.Ordinal);
        var straddler = RecordWithMultiLineMessage("line one\n" + lookAlikeBody + "\n" + lookAlikeBody + "\nline four");
        var cut = straddler.IndexOf("line one\n", StringComparison.Ordinal) + "line one\n".Length;

        /* #4053 review round 1, item A: the known start a real file-start read gives (CsvCarry with an
           empty partial and StartKnown true) — not derived from an end-of-file portion's own end, which the
           forward-only design no longer trusts. */
        var knownStart = new RdsLogEventIngestor.CsvCarry(string.Empty, true);

        var p1 = Step(knownStart, straddler[..cut], pending: true);
        Assert.Empty(p1.Entries);
        Assert.Equal(0, p1.RecordsDiscarded);
        Assert.True(p1.Next.StartKnown);

        var p2 = Step(p1.Next, straddler[cut..] + Record("UTC", "after"), pending: false);
        Assert.Equal(new[] { "nosuchuser", "after" }, p2.Entries.ConvertAll(e => e.UserName ?? ""));
        Assert.DoesNotContain(p2.Entries, e => e.UserName == "planted");
    }

    /// <summary>Bound: a pending portion with no boundary carries whole until the carry passes 1 MiB, when it is
    /// dropped, counted once, and the next start is unknown.</summary>
    [Fact]
    public void ACarryOverOneMiB_IsDroppedAndCountedOnce()
    {
        var p1 = Step(RdsLogEventIngestor.CsvCarry.Empty, new string('x', 600_000), pending: true);
        Assert.Equal(0, p1.RecordsDiscarded);
        Assert.Equal(600_000, p1.Next.Partial.Length);

        var p2 = Step(p1.Next, new string('x', 600_000), pending: true);
        Assert.Empty(p2.Entries);
        Assert.Equal(1, p2.RecordsDiscarded);
        Assert.Equal(string.Empty, p2.Next.Partial);
        Assert.False(p2.Next.StartKnown);
    }

    /// <summary>The wiring: the ingestor feeds each csv portion through the step with its carry. Portion 1
    /// (pending, one complete record plus a straddler's head) reaches the store for its one record.</summary>
    [Fact]
    public async Task TheIngestor_ReachesTheWriteForAPendingPortionsCompleteRecords()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var straddler = RecordWithMultiLineMessage("line one\nline two");
        var cut = straddler.IndexOf("line one\n", StringComparison.Ordinal) + "line one\n".Length;
        var client = new FakeRds
        {
            Portions = new Queue<(string, bool)>(new[] { (Record("UTC", "first") + straddler[..cut], true) }),
        };
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => client));

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true, pgLogUsesCsvlog: true));
        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }
}
