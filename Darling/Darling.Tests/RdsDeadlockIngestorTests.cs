/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// The RDS deadlock ingestor's resume-marker discipline (#3008): the marker advances only after the chunk's
/// rows are in the store, so a cycle that dies between the fetch and a committed COPY leaves the window to
/// be read again instead of consuming it.
///
/// <para><b>Why this needs its own suite rather than an assertion in
/// <c>RdsLogSourceTests</c>.</b> <see cref="RdsLogSource"/> can only be asked whether it advanced when
/// told to. Whether anything ever tells it to — and whether that happens before or after the write — is the
/// INGESTOR's decision, and it is the half that was wrong. A transport-level test passes just as well over
/// a caller that never commits at all, which would be a different permanent bug (every cycle re-reading one
/// window forever while looking like progress).</para>
///
/// <para><b>What makes the failures real rather than simulated.</b> The store is a data source pointed at a
/// closed loopback port, the form <c>DarlingMcpAlertToolsTests</c> already uses: the COPY genuinely fails,
/// at the connection, with no Postgres needed — so these run on the Windows <c>build</c> job alongside the
/// live-store suites rather than only where a real store exists. The log text is the verbatim PostgreSQL
/// 17.11 capture <c>PgDeadlockLogParserTests</c> is built on, parsed by the real
/// <see cref="PgDeadlockLogParser"/>, so the rows reaching the write are the rows production would
/// build.</para>
/// </summary>
public sealed class RdsDeadlockIngestorTests
{
    /* A store that cannot be written to. Port 1 on loopback refuses immediately, and Timeout=1 bounds the
       attempt, so the write fails deterministically and fast rather than hanging a CI job. */
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    /* Verbatim from a PostgreSQL 17.11 target, via PgDeadlockLogParserTests — including the %Q query id
       running straight into ERROR: with no separator, which a hand-written fixture would not have. */
    private const string DeadlockText =
        "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151ERROR:  deadlock detected\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
        + "\tProcess 1549: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n"
        + "\tProcess 1556: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151HINT:  See server log for query details.\n";

    /* A window with log traffic but no deadlock in it — the ordinary quiet cycle, and the positive control
       for the commit firing at all. */
    private const string QuietText =
        "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n"
        + "2026-08-26 22:30:12.000 UTC [1600] LOG:  checkpoint complete: wrote 42 buffers\n";

    /// <summary>
    /// An RDS client that models the transport's ONE load-bearing property: what you get back depends on
    /// the marker you send, and a marker is only handed out once. A fake that answered the same text
    /// regardless of the marker could not tell a resumed read from a repeated one, which is the entire
    /// distinction under test.
    /// </summary>
    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"),
            Amazon.RegionEndpoint.USEast1) { }

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();

        /// <summary>What the FIRST (unresumed) read returns.</summary>
        public string FirstBody { get; init; } = DeadlockText;

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
                {
                    new() { LogFileName = "error/postgresql.log.2026-08-26-22", LastWritten = 9999 },
                },
            });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);

            /* Consume-once: the unresumed read yields the window under test, and only a read that
               presented MARKER-1 has moved past it. */
            var resumed = request.Marker == "MARKER-1";

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = resumed ? QuietText : FirstBody,
                Marker = resumed ? "MARKER-2" : "MARKER-1",
                AdditionalDataPending = false,
            });
        }
    }

    private static (RdsDeadlockIngestor Ingestor, FakeRds Client, RdsLogSource Logs) Build(
        NpgsqlDataSource store, FakeRds? client = null)
    {
        var fake = client ?? new FakeRds();
        var logs = new RdsLogSource(_ => fake);
        return (new RdsDeadlockIngestor(store, logs), fake, logs);
    }

    /// <summary>
    /// The precondition every failure test below rests on, named so a broken fixture reports itself
    /// instead of turning those tests into vacuous passes: this text really does parse to one deadlock, so
    /// a cycle over it really does reach the COPY. A fixture that stopped parsing would make the ingestor
    /// store nothing, which is a legitimate commit — the failure tests would go red for a reason that has
    /// nothing to do with the marker.
    /// </summary>
    [Fact]
    public void TheFixtureReallyParsesToOneDeadlock()
    {
        var found = PgDeadlockLogParser.Extract(DeadlockText);

        Assert.Single(found);
        Assert.Equal(1549, found[0].VictimPid);

        /* And the control text really does not, so the quiet-cycle test below is testing a quiet cycle
           rather than a second copy of the deadlock case. */
        Assert.Empty(PgDeadlockLogParser.Extract(QuietText));
    }

    /// <summary>
    /// <b>The assertion this fix exists for.</b> The store write fails, so the marker must not move: the
    /// next cycle's request carries no marker, which on this transport is the only way the same bytes can
    /// be fetched twice.
    ///
    /// <para>Moving the commit back inside the fetch — the pre-#3008 order — fails
    /// <c>Assert.Null(client.Downloads[1].Marker)</c> below with <c>MARKER-1</c>, because the fetch would
    /// have recorded the marker before the COPY ever ran.</para>
    /// </summary>
    [Fact]
    public async Task AFailedStoreWriteDoesNotAdvanceTheMarker()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host));

        /* The WRITE failed, not the read. RdsLogUnavailableException is the read's failure wrapper, and if
           it arrived here the fetch never produced a chunk and nothing below would mean anything. */
        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.Single(client.Downloads);

        /* Second cycle. The marker never advanced, so this is a fresh unresumed read of the same window. */
        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));

        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[1].Marker);
        Assert.Equal(client.Downloads[0].Marker, client.Downloads[1].Marker);
    }

    /// <summary>
    /// The point of not advancing, stated as the consequence rather than the mechanism: the deadlock text
    /// is offered to the parser AGAIN on the next cycle. This is what "nothing was lost" actually means,
    /// and it is a claim about the bytes rather than about a dictionary key.
    /// </summary>
    [Fact]
    public async Task TheSameLogTextIsOfferedAgainAfterAFailedWrite()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, logs) = Build(store);

        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));
        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));

        /* Read the transport directly for the third pass: the ingestor cannot report what it fetched when
           its write throws, and what matters is that the bytes are still reachable. */
        var third = await logs.ReadNewestAsync(Host);

        Assert.NotNull(third);
        Assert.Equal(DeadlockText, third!.Value.Text);
        Assert.Single(PgDeadlockLogParser.Extract(third.Value.Text));

        /* Three fetches, none of them resumed — the window was never consumed. */
        Assert.Equal(3, client.Downloads.Count);
        Assert.All(client.Downloads, d => Assert.Null(d.Marker));
    }

    /// <summary>
    /// A cancelled cycle is the same category as a failed write, and the reason the fix is placed at the
    /// marker rather than at any one exception type: shutdown mid-COPY loses the window just as
    /// permanently as a parse fault, and nothing about it is an error anyone would investigate.
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

        /* The read happened - the fake ignores the token - and the write did not. */
        Assert.Single(client.Downloads);

        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));

        Assert.Null(client.Downloads[1].Marker);
    }

    /// <summary>
    /// <b>The control that stops all of the above passing over a no-op.</b> A commit that never fires would
    /// satisfy every "did not advance" assertion here while re-reading one window forever — so a cycle that
    /// succeeds has to be shown advancing.
    ///
    /// <para>A window with log traffic but no deadlock in it is the honest way to show it without a live
    /// store: nothing is stored, nothing CAN be lost, so the marker is free to move — and it must, or a
    /// quiet target would ask RDS for the same bounded tail every cycle forever. The dead store below is
    /// never opened, which is itself part of the assertion.</para>
    /// </summary>
    [Fact]
    public async Task AQuietCycleAdvancesTheMarker()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store, new FakeRds { FirstBody = QuietText });

        var rows = await ingestor.IngestAsync(1, "target-a", Host);
        Assert.Equal(0, rows);

        var more = await ingestor.IngestAsync(1, "target-a", Host);
        Assert.Equal(0, more);

        /* Advanced: the second read resumed from the first read's marker instead of asking for the tail
           again. */
        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[0].Marker);
        Assert.Equal("MARKER-1", client.Downloads[1].Marker);
        Assert.Equal(0, client.Downloads[1].NumberOfLines);
    }

    /// <summary>
    /// A non-RDS host is skipped without touching the store or the marker — that target reads its log
    /// through <c>pg_read_file</c>, and there is nothing here to report or to resume.
    /// </summary>
    [Fact]
    public async Task ANonRdsHostIsSkipped()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store);

        Assert.Equal(0, await ingestor.IngestAsync(1, "target-a", "db.internal.example.com"));
        Assert.Empty(client.Downloads);
    }
}
