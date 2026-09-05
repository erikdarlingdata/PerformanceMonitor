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
using PerformanceMonitor.Darling.Service;
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

    /* The same report with a non-zero-offset zone in the prefix. PgDeadlockLogParser refuses the whole
       read on this rather than storing local timestamps as UTC (#2993). */
    private static readonly string NonUtcDeadlockText =
        DeadlockText.Replace(" UTC ", " EST ", StringComparison.Ordinal);

    /* The same report as a MANAGED target renders it, whose parameter group carries the system default
       log_line_prefix = '%t:%r:%u@%d:[%p]:'. This suite is the managed transport's, so this is the shape
       it is actually handed: %t renders no fractional seconds, and the prefix's own ':' sits where the
       self-hosted default puts a space.

       Identifiers synthesised, shape verbatim: %r renders host(port) and %u@%d the connected user and
       database, so a real line carries a real user and database name there. 192.0.2.10 is RFC 5737
       documentation space, which exists to be written down. */
    private const string ManagedPrefix =
        "2026-08-26 22:25:24 UTC:192.0.2.10(52345):app_user@app_db:[1549]:";

    private const string SelfHostedPrefix = "2026-08-26 22:25:24.100 UTC [1549] ";

    private static readonly string ManagedDeadlockText =
        DeadlockText.Replace(SelfHostedPrefix, ManagedPrefix, StringComparison.Ordinal);

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
        Assert.Equal(RdsIngestOutcome.Read(0), rows);

        var more = await ingestor.IngestAsync(1, "target-a", Host);
        Assert.Equal(RdsIngestOutcome.Read(0), more);

        /* Advanced: the second read resumed from the first read's marker instead of asking for the tail
           again. */
        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[0].Marker);
        Assert.Equal("MARKER-1", client.Downloads[1].Marker);
        Assert.Equal(0, client.Downloads[1].NumberOfLines);
    }

    /// <summary>
    /// The parse-refusal case, and the one the defect was found through: a log stamped in a non-UTC zone
    /// makes <see cref="PgDeadlockLogParser"/> throw <see cref="PgLogTimezoneUnsupportedException"/>
    /// (#2993), which abandons the whole read — siblings included. The marker must not move, because
    /// <c>log_timezone</c> is a setting somebody can fix, and every report in that window is worth still
    /// being there when they do.
    ///
    /// <para>This is the shape the pre-#3008 order was worst for: the refusal is not a fault anybody
    /// caused at collection time, it recurs every cycle until the GUC changes, and each occurrence consumed
    /// a fresh window on the way past.</para>
    /// </summary>
    [Fact]
    public async Task ARefusedLogTimezoneDoesNotAdvanceTheMarker()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, logs) = Build(store, new FakeRds { FirstBody = NonUtcDeadlockText });

        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(
            () => ingestor.IngestAsync(1, "target-a", Host));

        /* Still reachable, byte for byte — so the reports survive the setting being corrected. */
        var again = await logs.ReadNewestAsync(Host);

        Assert.Equal(NonUtcDeadlockText, again!.Value.Text);
        Assert.Equal(2, client.Downloads.Count);
        Assert.All(client.Downloads, d => Assert.Null(d.Marker));
    }

    /// <summary>
    /// A non-RDS host is skipped without touching the store or the marker — that target reads its log
    /// through <c>pg_read_file</c>, and there is nothing here to report or to resume.
    ///
    /// <para><b>Skipped is reported as NOT REACHED, and that is a different value from an empty log</b>
    /// (#3017). No log file was listed and none was downloaded (<c>client.Downloads</c> is empty), so
    /// "no new deadlocks in the RDS log window" would be a claim about the contents of a log this cycle
    /// never opened. The pairing with <see cref="AQuietCycleAdvancesTheMarker"/> is the point: that test
    /// reaches the log and finds nothing, this one does not reach it, and the two now carry different
    /// outcomes rather than the same zero.</para>
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

    /// <summary>
    /// The runner's rendering of both, side by side — the assertion that actually closes #3017, because the
    /// two ingest outcomes above are only worth distinguishing if the note distinguishes them.
    ///
    /// <para>Pure: <see cref="DarlingCollectorRunner.RdsIngestNote"/> takes the outcome and the two
    /// sentences, so no store, <c>ServerRuntime</c> or AWS client is needed to pin the three-way choice.</para>
    /// </summary>
    [Fact]
    public void TheRunnerRendersADifferentNote_ForAnUnreachedLogThanForAnEmptyOne()
    {
        var notReached = DarlingCollectorRunner.RdsIngestNote(
            RdsIngestOutcome.NotReached,
            DarlingCollectorRunner.RdsDeadlockLogNotReachedNote,
            DarlingCollectorRunner.RdsDeadlockLogEmptyNote);

        var readNothing = DarlingCollectorRunner.RdsIngestNote(
            RdsIngestOutcome.Read(0),
            DarlingCollectorRunner.RdsDeadlockLogNotReachedNote,
            DarlingCollectorRunner.RdsDeadlockLogEmptyNote);

        var productive = DarlingCollectorRunner.RdsIngestNote(
            RdsIngestOutcome.Read(3),
            DarlingCollectorRunner.RdsDeadlockLogNotReachedNote,
            DarlingCollectorRunner.RdsDeadlockLogEmptyNote);

        Assert.Equal(DarlingCollectorRunner.RdsDeadlockLogNotReachedNote, notReached);
        Assert.Equal(DarlingCollectorRunner.RdsDeadlockLogEmptyNote, readNothing);

        /* The defect, stated directly: these two must never be the same sentence. */
        Assert.NotEqual(readNothing, notReached);

        /* A productive cycle still leaves no note — the rows are the statement. */
        Assert.Null(productive);
    }

    /// <summary>
    /// What the two sentences have to SAY, not merely that they differ. A pair of distinct strings that both
    /// described the log's contents would satisfy the inequality above and still be the defect.
    ///
    /// <para>All three collectors' pairs are asserted here rather than one, because the three notes were
    /// three independent copies of the same mistake and a fix that reached two of them would look
    /// complete.</para>
    /// </summary>
    [Fact]
    public void EveryNotReachedNote_SaysTheCycleDidNotLook_AndNamesWhy()
    {
        foreach (var note in new[]
        {
            DarlingCollectorRunner.RdsDeadlockLogNotReachedNote,
            DarlingCollectorRunner.RdsPlanLogNotReachedNote,
            DarlingCollectorRunner.PiCpuNotReachedNote,
        })
        {
            /* #2633's own closing sentence, as an outcome rather than a rule: a cycle that could not look
               must not claim it looked. An operator who learned the distinction from #2633's PERMISSIONS
               rows meets the same words arriving through the one door that is not a failure. */
            Assert.Contains("this cycle did not look", note, StringComparison.Ordinal);

            /* And the cause, because "did not look" alone would send someone hunting for a collector to
               restart. Nothing is wrong: this transport does not apply to this target. */
            Assert.Contains("not an RDS or Aurora endpoint", note, StringComparison.Ordinal);
        }

        /* The empty notes are unchanged claims about a source that WAS read, and must stay that way -
           those sentences are correct on their own path and are what the not-reached notes exist to stop
           being borrowed. */
        foreach (var note in new[]
        {
            DarlingCollectorRunner.RdsDeadlockLogEmptyNote,
            DarlingCollectorRunner.RdsPlanLogEmptyNote,
            DarlingCollectorRunner.PiCpuEmptyNote,
        })
        {
            Assert.DoesNotContain("did not look", note, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The managed fixture's precondition, the counterpart of
    /// <see cref="TheFixtureReallyParsesToOneDeadlock"/>: the re-rendering really did replace the prefix
    /// on every prefixed line, and the result really is one report.
    /// </summary>
    [Fact]
    public void TheManagedFixtureCarriesTheManagedPrefix_AndParsesToTheSameReport()
    {
        Assert.DoesNotContain(SelfHostedPrefix, ManagedDeadlockText, StringComparison.Ordinal);
        Assert.Contains(ManagedPrefix, ManagedDeadlockText, StringComparison.Ordinal);

        var managed = Assert.Single(PgDeadlockLogParser.Extract(ManagedDeadlockText));
        var selfHosted = Assert.Single(PgDeadlockLogParser.Extract(DeadlockText));

        Assert.Equal(1549, managed.VictimPid);
        Assert.Equal(2, managed.ParticipantCount);

        /* One report, one identity, whichever prefix rendered it - the prefix is not part of the graph.
           A target that moved between the two transports would otherwise store its history twice. */
        Assert.Equal(selfHosted.DeadlockHash, managed.DeadlockHash);
    }

    /// <summary>
    /// A managed window carrying a report reaches the WRITE, and this is the assertion the managed
    /// transport was missing: every other test here drives it over text a self-hosted target renders, so
    /// the suite proved the plumbing over a shape its own targets do not emit (#3030).
    ///
    /// <para>The store is the dead one, so reaching the write means throwing. An
    /// <c>RdsIngestOutcome.Read(0)</c> here would be the quiet-cycle path in
    /// <see cref="AQuietCycleAdvancesTheMarker"/> taken over a window that is not quiet: the note would
    /// say the window held no deadlock, and the marker would advance and consume it, so the report is
    /// never offered again.</para>
    /// </summary>
    [Fact]
    public async Task AManagedPrefixWindowReachesTheWrite_RatherThanReportingAnEmptyLog()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var (ingestor, client, _) = Build(store, new FakeRds { FirstBody = ManagedDeadlockText });

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host));

        /* The WRITE failed, not the read: RdsLogUnavailableException would mean the fetch produced no
           chunk and nothing here would mean anything. */
        Assert.IsNotType<RdsLogUnavailableException>(failure);
        Assert.Single(client.Downloads);

        /* And the window survives the failed write, exactly as the self-hosted one does. */
        await Assert.ThrowsAnyAsync<Exception>(() => ingestor.IngestAsync(1, "target-a", Host));

        Assert.Equal(2, client.Downloads.Count);
        Assert.Null(client.Downloads[1].Marker);
    }
}
