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
using System.Linq;
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
/// The RDS stderr carry (#7186): <c>DownloadDBLogFilePortion</c> is consume-once, so a plan or a multi-line log event that
/// the end of a portion cuts was lost. The plan route and the log-event route each hold the unfinished tail and
/// prepend it to the next portion, as the deadlock route does (#4735) and the csvlog route does for a partial record.
/// A fake RDS client scripts the portions, and a row writer stands in for the store, so neither AWS nor a database is used.
/// </summary>
public sealed class RdsStderrCarryTests
{
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    private static readonly string PlanText =
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "auto_explain_real_block.txt"));

    /* The fixture is the plan (its first 33 lines) followed by other log lines. This is the plan alone, ending on its
       closing brace, so a portion can end exactly where the plan does. */
    private static readonly string WholePlan = string.Join('\n', PlanText.Split('\n').Take(33)) + "\n";

    private const string QuietLine =
        "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n";

    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds(params (string Body, bool Pending)[] portions)
            : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1)
        {
            Portions = new Queue<(string Body, bool Pending)>(portions);
        }

        public Queue<(string Body, bool Pending)> Portions { get; }

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();

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

            var (body, pending) = Portions.Count > 0 ? Portions.Dequeue() : (string.Empty, false);

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = body,
                Marker = "MARKER-" + Downloads.Count,
                AdditionalDataPending = pending,
            });
        }
    }

    /// <summary>The plan text cut after <paramref name="lines"/> lines.</summary>
    private static (string Head, string Tail) SplitPlanAfterLines(int lines)
    {
        var parts = PlanText.Split('\n');
        var head = string.Join('\n', parts.Take(lines)) + "\n";
        var tail = string.Join('\n', parts.Skip(lines));
        return (head, tail);
    }

    [Fact]
    public async Task APlanCutAtThePortionEnd_IsStoredOnce_AndWhole()
    {
        var (head, tail) = SplitPlanAfterLines(12);

        Assert.Empty(PgPlanLogParser.Extract(head));
        Assert.Empty(PgPlanLogParser.Extract(tail));

        var client = new FakeRds((QuietLine + head, true), (tail + QuietLine, false));
        var stored = new List<PgPlanCaptureCollector.Row>();

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var ingestor = new RdsPlanIngestor(store, new RdsLogSource(_ => client))
        {
            RowWriter = (rows, _) =>
            {
                stored.AddRange(rows);
                return Task.FromResult(rows.Count);
            },
        };

        await ingestor.IngestAsync(1, "target-a", Host);

        Assert.Empty(stored);

        await ingestor.IngestAsync(1, "target-a", Host);

        var whole = Assert.Single(stored);
        var expected = Assert.Single(PgPlanLogParser.Extract(PlanText));
        Assert.Equal(expected.PlanHash, whole.PlanHash);
        Assert.Equal(expected.PlanJson, whole.PlanJson);

        /* The carry moved with the marker: the second read presented the first read's marker. */
        Assert.Equal("MARKER-1", client.Downloads[1].Marker);

        /* And nothing is held once the plan is whole: a third read stores nothing more. */
        await ingestor.IngestAsync(1, "target-a", Host);
        Assert.Single(stored);
    }

    [Fact]
    public async Task APlanCutMidLine_IsStoredOnce_AndWhole()
    {
        var cut = PlanText.IndexOf("\"Node Type\": \"Index Scan\"", StringComparison.Ordinal) + 8;
        var client = new FakeRds((QuietLine + PlanText[..cut], true), (PlanText[cut..] + QuietLine, false));
        var stored = new List<PgPlanCaptureCollector.Row>();

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var ingestor = new RdsPlanIngestor(store, new RdsLogSource(_ => client))
        {
            RowWriter = (rows, _) =>
            {
                stored.AddRange(rows);
                return Task.FromResult(rows.Count);
            },
        };

        await ingestor.IngestAsync(1, "target-a", Host);
        await ingestor.IngestAsync(1, "target-a", Host);

        Assert.Single(stored);
    }

    [Fact]
    public async Task APlanThatEndsAPortionWhole_IsStoredAtOnce_WithNothingHeld()
    {
        var client = new FakeRds((QuietLine + WholePlan, true));
        var stored = new List<PgPlanCaptureCollector.Row>();

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var ingestor = new RdsPlanIngestor(store, new RdsLogSource(_ => client))
        {
            RowWriter = (rows, _) =>
            {
                stored.AddRange(rows);
                return Task.FromResult(rows.Count);
            },
        };

        await ingestor.IngestAsync(1, "target-a", Host);

        Assert.Single(stored);
    }

    [Fact]
    public void UnfinishedTailStart_FindsThePlanTheTextEndsInside()
    {
        var (head, _) = SplitPlanAfterLines(12);
        var lead = QuietLine;

        Assert.Equal(lead.Length, PgPlanLogParser.UnfinishedTailStart(lead + head));
        Assert.Equal(lead.Length, PgPlanLogParser.UnfinishedTailStart(lead + WholePlan[..^3]));
        Assert.Equal(lead.Length, PgPlanLogParser.UnfinishedTailStart(lead + SplitPlanAfterLines(1).Head));

        /* Cut inside the header line itself. */
        Assert.Equal(lead.Length, PgPlanLogParser.UnfinishedTailStart(lead + PlanText[..30]));

        Assert.Equal(-1, PgPlanLogParser.UnfinishedTailStart(lead + WholePlan));
        Assert.Equal(-1, PgPlanLogParser.UnfinishedTailStart(lead + PlanText));
        Assert.Equal(-1, PgPlanLogParser.UnfinishedTailStart(lead + PlanText + QuietLine));
        Assert.Equal(-1, PgPlanLogParser.UnfinishedTailStart(lead));
        Assert.Equal(-1, PgPlanLogParser.UnfinishedTailStart(null));
    }

    private const string Pfx = "2026-08-26 22:25:24.100 UTC:192.0.2.10(52345):app_rw@app_db:[1549]:";

    private static readonly string Primary = Pfx + "ERROR:  duplicate key value violates unique constraint \"t_pkey\"\n";
    private static readonly string DetailLine = Pfx + "DETAIL:  Key (id)=(7) already exists.\n";
    private static readonly string StatementLine = Pfx + "STATEMENT:  INSERT INTO t (id) VALUES (7)\n";

    private static readonly string NextEvent =
        "2026-08-26 22:25:25.100 UTC:192.0.2.10(52345):app_rw@app_db:[1550]:ERROR:  relation \"nope\" does not exist\n";

    private static async Task<List<PgLogEvent>> IngestTwiceAsync(string first, string second)
    {
        var client = new FakeRds((first, true), (second, false));
        var stored = new List<PgLogEvent>();

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => client))
        {
            RowWriter = (rows, _) =>
            {
                stored.AddRange(rows);
                return Task.FromResult(rows.Count);
            },
        };

        await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true);
        await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true);

        Assert.Equal("MARKER-1", client.Downloads[1].Marker);

        return stored;
    }

    [Fact]
    public async Task AnEventCutBeforeItsDetailAndStatement_KeepsBothLines()
    {
        var stored = await IngestTwiceAsync(Primary, DetailLine + StatementLine + NextEvent);

        Assert.Equal(2, stored.Count);

        var duplicate = Assert.Single(stored, e => e.Message.Contains("duplicate key", StringComparison.Ordinal));
        Assert.Contains("already exists", duplicate.Detail, StringComparison.Ordinal);
        Assert.False(string.IsNullOrEmpty(duplicate.StatementFingerprint));
    }

    [Fact]
    public async Task AnEventCutBetweenItsDetailAndStatement_KeepsTheStatement()
    {
        var stored = await IngestTwiceAsync(Primary + DetailLine, StatementLine + NextEvent);

        var duplicate = Assert.Single(stored, e => e.Message.Contains("duplicate key", StringComparison.Ordinal));
        Assert.Contains("already exists", duplicate.Detail, StringComparison.Ordinal);
        Assert.False(string.IsNullOrEmpty(duplicate.StatementFingerprint));
    }

    [Fact]
    public async Task AnEventCutMidLine_IsStoredOnce_AndWhole()
    {
        var cut = Primary.Length + 20;
        var all = Primary + DetailLine + StatementLine;
        var stored = await IngestTwiceAsync(all[..cut], all[cut..] + NextEvent);

        Assert.Equal(2, stored.Count);

        var duplicate = Assert.Single(stored, e => e.Message.Contains("duplicate key", StringComparison.Ordinal));
        Assert.Contains("already exists", duplicate.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task APortionWithNothingMorePending_StoresItsLastEventAtOnce()
    {
        var client = new FakeRds((Primary + DetailLine + StatementLine, false));
        var stored = new List<PgLogEvent>();

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => client))
        {
            RowWriter = (rows, _) =>
            {
                stored.AddRange(rows);
                return Task.FromResult(rows.Count);
            },
        };

        await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true);

        Assert.Single(stored);
    }

    [Fact]
    public void AssemblerUnfinishedTailStart_IsTheLastPrimaryLine()
    {
        var text = Primary + DetailLine + NextEvent + StatementLine.Replace("[1549]", "[1550]");

        Assert.Equal((Primary + DetailLine).Length, PgLogEntryAssembler.UnfinishedTailStart(text));
        Assert.Equal(0, PgLogEntryAssembler.UnfinishedTailStart(Primary));
        Assert.Equal(-1, PgLogEntryAssembler.UnfinishedTailStart(DetailLine));
        Assert.Equal(-1, PgLogEntryAssembler.UnfinishedTailStart(null));
    }
}
