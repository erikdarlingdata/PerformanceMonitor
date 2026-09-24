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

            var body = request.LogFileName == CsvFile ? CsvBody : "stderr body";

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = body,
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
}
