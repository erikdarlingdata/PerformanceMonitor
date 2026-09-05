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
            () => ingestor.IngestAsync(1, "target-a", Host, cancelled.Token));

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
}
