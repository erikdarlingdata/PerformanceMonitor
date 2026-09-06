/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3099: a collector's store write that fails on a transport fault gets ONE re-attempt on a fresh
/// connection, and the acceptance criterion is that the failure costs no sample.
///
/// <para>The policy is exercised through <see cref="StoreWriteReattempt.RunAsync"/> with attempt delegates
/// that move rows into a sink, because the COPY it wraps is reachable only through a live store and a
/// constructed runner. What the delegates cannot see — that the shipped write routes through this at all,
/// and that the re-attempt takes a FRESH connection rather than the caller's dead one — is pinned against
/// the source below. Neither half is sufficient alone: #2213's whole lesson here was a feature whose SQL
/// was proven for weeks and whose call sites had never learned the seam existed.</para>
/// </summary>
public class StoreWriteReattemptTests
{
    /* The shipped failure, constructed the way the repo's other pins construct it: an NpgsqlException
       wrapping the inner fault, because that is what Npgsql hands back and the outer type and message are
       identical for a client deadline and a lost connection. */
    private static NpgsqlException StreamFault(Exception? inner = null)
        => new("Exception while reading from stream", inner ?? new IOException("connection reset"));

    /* A server REPLY. The public constructor takes (message, severity, invariantSeverity, sqlState). */
    private static PostgresException ServerReply(string sqlState)
        => new("no", "ERROR", "ERROR", sqlState);

    private static readonly string[] Batch = { "row-1", "row-2", "row-3" };

    // ── the acceptance criterion ──

    /// <summary>
    /// A failed first write followed by a successful re-attempt STORES THE ROWS and reports success.
    ///
    /// <para>The sink is asserted rather than the return value alone: a helper that returned the right
    /// count while writing nothing would satisfy a count-only assertion, and "no sample is lost" is a claim
    /// about the store's contents, not about an integer.</para>
    /// </summary>
    [Fact]
    public async Task AFailedFirstWriteFollowedByASuccessfulReattempt_StoresTheRows_AndReportsSuccess()
    {
        var stored = new List<string>();
        var attempts = 0;

        var outcome = await StoreWriteReattempt.RunAsync(
            write: _ =>
            {
                attempts++;
                throw StreamFault();
            },
            rewrite: _ =>
            {
                attempts++;
                stored.AddRange(Batch);
                return Task.FromResult(Batch.Length);
            },
            onReattempt: _ => { },
            CancellationToken.None);

        Assert.Equal(2, attempts);
        Assert.Equal(Batch, stored);
        Assert.Equal(Batch.Length, outcome.RowsWritten);
        Assert.True(
            outcome.Reattempted,
            "the outcome must report that the rows arrived on the second attempt — a successful-on-retry " +
            "cycle that looks identical to a first-attempt success is the signal #3099 asks to keep.");
    }

    /// <summary>
    /// A failure on BOTH attempts still reports the error rather than silently succeeding. A retry that
    /// swallowed a persistent fault would trade one lost sample for a store nobody can tell is refusing
    /// writes.
    /// </summary>
    [Fact]
    public async Task AFailureOnBothAttempts_ReportsTheError_AndStoresNothing()
    {
        var stored = new List<string>();
        var attempts = 0;
        Exception? loggedFirstAttempt = null;

        var thrown = await Assert.ThrowsAsync<NpgsqlException>(async () =>
            await StoreWriteReattempt.RunAsync(
                write: _ =>
                {
                    attempts++;
                    throw StreamFault(new TimeoutException("first"));
                },
                rewrite: _ =>
                {
                    attempts++;
                    throw StreamFault(new TimeoutException("second"));
                },
                onReattempt: ex => loggedFirstAttempt = ex,
                CancellationToken.None));

        Assert.Equal(2, attempts);
        Assert.Empty(stored);
        Assert.Equal("second", thrown.InnerException?.Message);

        /* The first attempt is not chained onto the thrown exception, so the ONLY thing that keeps it from
           vanishing is the callback — which is where the host logs it. */
        Assert.Equal("first", (loggedFirstAttempt as NpgsqlException)?.InnerException?.Message);
    }

    // ── what does and does not earn a second attempt ──

    /// <summary>
    /// A server reply is never re-attempted. It arrives as a <see cref="PostgresException"/>: the backend
    /// received the statement and answered, so an identical second attempt gets an identical answer, and
    /// re-attempting past a SQLSTATE turns a legible error into a silent one. 57014 is in the list because
    /// it is the store cancelling us — #3099 observed it on the store's read paths, and a
    /// <c>statement_timeout</c> on the write side is a configured limit, not a dropped connection.
    /// </summary>
    [Theory]
    [InlineData("57014")] /* query_canceled — the store's own statement_timeout */
    [InlineData("23505")] /* unique violation */
    [InlineData("42P01")] /* undefined table */
    [InlineData("22P02")] /* invalid text representation */
    [InlineData("53100")] /* disk full */
    public async Task AServerReplyIsNeverReattempted(string sqlState)
    {
        var attempts = 0;

        await Assert.ThrowsAsync<PostgresException>(async () =>
            await StoreWriteReattempt.RunAsync(
                write: _ =>
                {
                    attempts++;
                    throw ServerReply(sqlState);
                },
                rewrite: _ =>
                {
                    attempts++;
                    return Task.FromResult(0);
                },
                onReattempt: _ => { },
                CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    /// <summary>
    /// A server reply wrapped in transport machinery is still a server reply. The chain is walked and the
    /// <see cref="PostgresException"/> test wins at every level, so wrapping cannot smuggle a SQLSTATE past
    /// the predicate.
    /// </summary>
    [Fact]
    public async Task AWrappedServerReplyIsNeverReattempted()
    {
        var attempts = 0;

        await Assert.ThrowsAsync<NpgsqlException>(async () =>
            await StoreWriteReattempt.RunAsync(
                write: _ =>
                {
                    attempts++;
                    throw new NpgsqlException("Exception while reading from stream", ServerReply("57014"));
                },
                rewrite: _ =>
                {
                    attempts++;
                    return Task.FromResult(0);
                },
                onReattempt: _ => { },
                CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    /// <summary>Every transport shape the store write can fault with earns the second attempt.</summary>
    [Fact]
    public async Task EveryTransportShapeEarnsTheSecondAttempt()
    {
        foreach (var fault in new Exception[]
        {
            StreamFault(new TimeoutException("client-side command deadline")),
            StreamFault(new IOException("connection to server lost")),
            StreamFault(new SocketException(104)),
            new NpgsqlException("Exception while writing to stream"),
        })
        {
            var reattempted = false;

            var outcome = await StoreWriteReattempt.RunAsync(
                write: _ => throw fault,
                rewrite: _ =>
                {
                    reattempted = true;
                    return Task.FromResult(1);
                },
                onReattempt: _ => { },
                CancellationToken.None);

            Assert.True(reattempted, $"{fault.GetType().Name}/{fault.InnerException?.GetType().Name} must be re-attempted");
            Assert.True(outcome.Reattempted);
        }
    }

    // ── cancellation ──

    /// <summary>
    /// A cancelled token suppresses the re-attempt, and the transport fault still propagates as itself. The
    /// re-attempt must not outlive an orderly stop holding a sweep permit and a store connection against a
    /// store the same process is shutting down; and the write must not be relabelled a cancellation on the
    /// way out, or the fault arms record the wrong thing about the cycle that lost the sample.
    /// </summary>
    [Fact]
    public async Task ACancelledTokenSuppressesTheReattempt_AndTheTransportFaultStillPropagates()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var attempts = 0;

        await Assert.ThrowsAsync<NpgsqlException>(async () =>
            await StoreWriteReattempt.RunAsync(
                write: _ =>
                {
                    attempts++;
                    throw StreamFault();
                },
                rewrite: _ =>
                {
                    attempts++;
                    return Task.FromResult(1);
                },
                onReattempt: _ => { },
                cts.Token));

        Assert.Equal(1, attempts);
    }

    /// <summary>An actual cancellation is never reclassified as a transport fault.</summary>
    [Fact]
    public async Task ACancellationIsNeverReattempted()
    {
        var attempts = 0;

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await StoreWriteReattempt.RunAsync(
                write: _ =>
                {
                    attempts++;
                    throw new OperationCanceledException();
                },
                rewrite: _ =>
                {
                    attempts++;
                    return Task.FromResult(1);
                },
                onReattempt: _ => { },
                CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    // ── how the cycle records it ──

    /// <summary>
    /// The note is a COUNT, and it is absent when nothing needed a second attempt. Zero re-attempts must
    /// leave the collection_log message column exactly as an ordinary cycle leaves it, or every row in the
    /// fleet gains a sentence that says nothing.
    /// </summary>
    [Fact]
    public void TheNoteIsAbsentWithoutAReattemptAndCountsThemWhenThereAre()
    {
        Assert.Null(DarlingCollectorRunner.StoreWriteReattemptNote(0));
        Assert.Null(DarlingCollectorRunner.StoreWriteReattemptNote(-1));

        var one = DarlingCollectorRunner.StoreWriteReattemptNote(1);
        Assert.NotNull(one);
        Assert.Contains("1 store write", one, StringComparison.Ordinal);
        Assert.Contains("#3099", one, StringComparison.Ordinal);

        /* A fan-out writes once per database, so the count is what separates a blip from a store in
           trouble — the reason this is not a bool. */
        Assert.Contains("30 store write", DarlingCollectorRunner.StoreWriteReattemptNote(30)!, StringComparison.Ordinal);
    }

    /// <summary>
    /// The note composes with the channels it can co-occur with rather than displacing them. A per-database
    /// cycle can lose databases AND re-attempt a write; whichever note were assigned second would erase the
    /// other.
    /// </summary>
    [Fact]
    public void TheNoteMergesWithTheOtherNoteChannelsInsteadOfReplacingThem()
    {
        var merged = EnumeratedCollectorDriver.MergeNotes(
            "3 of 30 database(s) failed", DarlingCollectorRunner.StoreWriteReattemptNote(2));

        Assert.NotNull(merged);
        Assert.Contains("3 of 30 database(s) failed", merged, StringComparison.Ordinal);
        Assert.Contains("2 store write", merged, StringComparison.Ordinal);
    }

    /// <summary>
    /// The context field starts at zero, so an ordinary cycle composes no note without the host having to
    /// reset anything.
    /// </summary>
    [Fact]
    public void AFreshContextReportsNoReattempts()
        => Assert.Equal(0, new CollectorContext
        {
            ServerId = 1,
            ServerName = "store-write-reattempt",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo(),
        }.StoreWriteReattempts);

    // ── the wiring the delegates above cannot see ──

    /// <summary>
    /// That the SHIPPED store write routes through the re-attempt at all, that the re-attempt opens a FRESH
    /// connection instead of reusing the caller's dead one, and that the count reaches the run's note.
    ///
    /// <para>Pinned against the source because <c>WriteBatchAsync</c> is private and needs a constructed
    /// runner plus a live store to call. Every assertion here is a call site rather than logic: the policy
    /// tests above would all stay green if the write simply stopped using it.</para>
    /// </summary>
    [Fact]
    public void TheShippedStoreWriteRoutesThroughTheReattemptOnAFreshConnection()
    {
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")));

        var write = source.IndexOf("private async Task<int> WriteBatchAsync<TRow>(", StringComparison.Ordinal);
        Assert.True(write >= 0, "expected DarlingCollectorRunner.WriteBatchAsync");

        var copyOnce = source.IndexOf("private async Task<int> CopyBatchOnceAsync<TRow>(", StringComparison.Ordinal);
        Assert.True(
            copyOnce > write,
            "the COPY must live in its own method, so the failed attempt's importer and transaction are " +
            "disposed before the re-attempt runs — a retry entered with an aborted transaction still on " +
            "the connection fails on 25P02 rather than on anything to do with the store.");

        /* Scope every assertion below to WriteBatchAsync's own body. */
        var body = source[write..copyOnce];

        Assert.Contains("StoreWriteReattempt.RunAsync(", body, StringComparison.Ordinal);

        Assert.Contains(
            "await _postgres.OpenConnectionAsync(token)", body, StringComparison.Ordinal);

        var rewrite = body.IndexOf("rewrite:", StringComparison.Ordinal);
        var freshConnection = body.IndexOf("_postgres.OpenConnectionAsync(token)", StringComparison.Ordinal);
        Assert.True(
            rewrite >= 0 && freshConnection > rewrite,
            "the re-attempt must open its OWN store connection. The first attempt's connector is dead, so " +
            "a second COPY on the caller's handle fails on the protocol and the sample is lost anyway.");

        Assert.Contains("context.StoreWriteReattempts++", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// That the cycle's re-attempt count reaches the <c>collection_log</c> row, merged into the note the
    /// success return already carries.
    ///
    /// <para><b>Recording it is the point, not a courtesy.</b> Before the re-attempt a transport fault
    /// wrote an ERROR row, and those rows are the entire measurement behind #3099's in-window versus
    /// out-window error-rate ratio. A silent retry removes the lost sample and the only instrument that can
    /// check whether the association the fix was diagnosed from is real.</para>
    /// </summary>
    [Fact]
    public void TheReattemptCountReachesTheCollectionLogNote()
    {
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")));

        var merge = source.IndexOf(
            "StoreWriteReattemptNote(context.StoreWriteReattempts)", StringComparison.Ordinal);
        Assert.True(
            merge >= 0,
            "the cycle's re-attempt count must be composed into the run's collection_log note. Without it " +
            "a write that faulted and then succeeded writes a row indistinguishable from one that never " +
            "faulted at all.");

        var successReturn = source.IndexOf(
            "rowsWritten, sqlMs, storageMs, collectionNote", StringComparison.Ordinal);
        Assert.True(
            successReturn > merge,
            "the merge must happen BEFORE the success return that carries the note, and after every write " +
            "on all three dispatch paths.");
    }

    /// <summary>
    /// That the re-attempt does NOT introduce a new collection_log status. Eight readers across the
    /// service, the MCP, both viewers and Lite treat <c>status IN ('SUCCESS', 'SKIPPED')</c> as success; a
    /// sixth value would read as a failure in every one of them and suppress <c>last_success</c> for a
    /// cycle that stored every row and advanced its watermark. That is #2673's defect with the sign
    /// flipped.
    /// </summary>
    [Fact]
    public void TheReattemptDoesNotInventANewCollectionLogStatus()
    {
        foreach (var file in new[] { "DarlingCollectorRunner.cs", "StoreWriteReattempt.cs" })
        {
            var source = File.ReadAllText(FindRepoFile(
                Path.Combine("Darling", "PerformanceMonitor.Darling.Service", file)));

            foreach (var invented in new[] { "\"RETRIED\"", "\"REATTEMPTED\"", "\"SUCCESS_ON_RETRY\"", "\"DEGRADED\"" })
            {
                Assert.DoesNotContain(invented, source, StringComparison.Ordinal);
            }
        }

        Assert.Equal("SUCCESS", EnumeratedCollectorDriver.ClassifyReturnedRun(abandoned: false));
    }

    /* ── helpers ── */

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 10 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
