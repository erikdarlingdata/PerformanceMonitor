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
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2936: the collection loop's first store interaction triages its failures instead of treating a
/// two-second outage and a rung that can never apply as the same terminal event.
///
/// <para>The classification is the substance, so most of this file is the boundary rather than the loop.
/// A retry policy that retries EVERYTHING passes any test that only counts attempts — it looks exactly
/// like a working one until the day a broken rung is retried into silence instead of producing the one
/// critical line that says what is wrong. So the terminal half is pinned as hard as the retryable half,
/// state by state, and <see cref="Class57IsSplitStateByState_NotSweptInByClass"/> pins the case where a
/// plausible shortcut (classify on the two-character class) gets it wrong.</para>
/// </summary>
public class StoreStartupFailureTriageTests
{
    /* Observed against PostgreSQL 17.11 / TimescaleDB 2.29.2 rather than reasoned about: each of these is
       a shape a real failure arrived in, with the scenario that produced it. */

    [Fact]
    public void NothingListening_IsRetryable()
        => Assert.True(StoreStartupFailureTriage.IsRetryable(
            new NpgsqlException("Failed to connect to 127.0.0.1:5432", new SocketException(61))));

    [Fact]
    public void ConnectTimeout_IsRetryable()
        => Assert.True(StoreStartupFailureTriage.IsRetryable(
            new NpgsqlException("Failed to connect to 10.0.0.1:5432", new TimeoutException("Timeout during connection attempt"))));

    [Fact]
    public void StoreDyingMidStatement_IsRetryable()
        => Assert.True(StoreStartupFailureTriage.IsRetryable(
            new NpgsqlException(
                "Exception while reading from stream",
                new IOException("Unable to read data from the transport connection.", new SocketException(54)))));

    /// <summary>
    /// A rung overrunning its own <c>CommandTimeout</c> and a blocking <c>pg_advisory_lock</c> overrunning
    /// its lock-wait budget are BYTE-IDENTICAL here (both
    /// <c>NpgsqlException -&gt; TimeoutException("Timeout during reading attempt")</c>), which is why
    /// #2935 stopped spelling the lock wait that way. Retried either way: this classification does not
    /// depend on whether that change has landed.
    /// </summary>
    [Fact]
    public void CommandTimeoutExpiry_IsRetryable()
        => Assert.True(StoreStartupFailureTriage.IsRetryable(
            new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"))));

    /// <summary>#2935's polling waiter throws a plain TimeoutException carrying both schema versions.</summary>
    [Fact]
    public void BareTimeoutException_IsRetryable()
        => Assert.True(StoreStartupFailureTriage.IsRetryable(
            new TimeoutException("Timed out after 1500s waiting for the migration advisory lock.")));

    public static TheoryData<string, string> RetryableStates => new()
    {
        /* Observed: SIGKILL the store and the in-flight session gets this. */
        { "57P01", "store restarting / backend terminated" },
        { "57P02", "the cluster crashed and is coming back" },
        /* Observed: 74 consecutive attempts during crash recovery of a 1.7 GB store. */
        { "57P03", "the store is not yet accepting connections" },
        { "08000", "connection exception" },
        { "08001", "could not establish the connection" },
        { "08003", "connection does not exist" },
        { "08004", "connection rejected" },
        { "08006", "connection failure" },
        /* The rung's DDL and its version stamp commit in ONE transaction, so a concurrency rollback left
           nothing applied and nothing stamped and re-entering re-attempts that same rung. */
        { "40001", "serialization failure" },
        { "40P01", "deadlock detected" },
        { "55P03", "lock not available" },
        { "55006", "object in use" },
    };

    [Theory]
    [MemberData(nameof(RetryableStates))]
    public void RetryableSqlStates_AreRetryable(string sqlState, string why)
        => Assert.True(
            StoreStartupFailureTriage.IsRetryable(new PostgresException(why, "FATAL", "FATAL", sqlState)),
            $"{sqlState} ({why}) must be retryable");

    public static TheoryData<string, string> TerminalStates => new()
    {
        /* A rung that cannot apply against this store — the case the old uniform behaviour was right about. */
        { "42601", "syntax error in the rung" },
        { "42P01", "the rung references a relation that does not exist" },
        { "42703", "the rung references a column that does not exist" },
        { "42501", "the login cannot create objects" },
        /* V62's exact shape: a CHECK the store's existing rows violate. Waiting does not change rows. */
        { "23514", "check constraint violated by existing rows" },
        { "23505", "unique violation" },
        { "28P01", "wrong password" },
        { "28000", "invalid authorization" },
        { "3D000", "the database does not exist" },
        { "55000", "datallowconn is false on this database" },
        /* Same class 57 as three retryable states, and both terminal — see the class-split test. */
        { "57014", "somebody else's statement_timeout cancelled the rung" },
        { "57P04", "the database was dropped" },
        /* Capacity findings an operator has to see now; two minutes clears none of them. */
        { "53100", "disk full" },
        { "53200", "out of memory" },
        { "53300", "too many connections" },
        { "53400", "configuration limit exceeded" },
        { "58030", "I/O error under the store" },
        { "58P01", "undefined file" },
        /* Deliberately absent from the class-08 allowlist. */
        { "08P01", "protocol violation — a defect that recurs identically" },
        { "08007", "transaction resolution unknown — worth saying out loud" },
        /* Not a state this code knows: default-deny. */
        { "XX999", "an unrecognised state" },
    };

    [Theory]
    [MemberData(nameof(TerminalStates))]
    public void TerminalSqlStates_AreTerminal(string sqlState, string why)
        => Assert.False(
            StoreStartupFailureTriage.IsRetryable(new PostgresException(why, "ERROR", "ERROR", sqlState)),
            $"{sqlState} ({why}) must NOT be retried — retrying it replaces the one diagnostic line with warnings forever");

    /// <summary>
    /// The sharpest boundary in the classifier, and the reason it classifies on individual states rather
    /// than on the two-character class: class 57 (operator_intervention) holds three states that mean
    /// "not yet" and two that mean "no", and the cheap shortcut of matching <c>57</c> sweeps in both of
    /// the latter.
    /// </summary>
    [Fact]
    public void Class57IsSplitStateByState_NotSweptInByClass()
    {
        foreach (var retryable in new[] { "57P01", "57P02", "57P03" })
        {
            Assert.True(
                StoreStartupFailureTriage.IsRetryable(new PostgresException("not yet", "FATAL", "FATAL", retryable)),
                $"{retryable} is the store going down or coming up");
        }

        foreach (var terminal in new[] { "57014", "57P04" })
        {
            Assert.False(
                StoreStartupFailureTriage.IsRetryable(new PostgresException("no", "ERROR", "ERROR", terminal)),
                $"{terminal} shares class 57 with the three above and is still terminal");
        }
    }

    /// <summary>
    /// The server's verdict decides however deeply it sits. A single chain walk that returned on the first
    /// transport type it met would call this retryable, because the IOException is OUTSIDE the
    /// PostgresException — and a rung that can never apply would then be retried.
    /// </summary>
    [Fact]
    public void APostgresVerdictDecides_EvenWrappedInATransportFault()
    {
        var terminalUnderTransport = new NpgsqlException(
            "Exception while reading from stream",
            new IOException("transport", new PostgresException("relation does not exist", "ERROR", "ERROR", "42P01")));

        Assert.False(
            StoreStartupFailureTriage.IsRetryable(terminalUnderTransport),
            "a 42P01 verdict wrapped in transport noise is still a rung that cannot apply");

        /* Positive control through the IDENTICAL wrapping, so the assertion above cannot be passing
           because the shape is unreachable or because IsRetryable rejects every nested exception. */
        var retryableUnderTransport = new NpgsqlException(
            "Exception while reading from stream",
            new IOException("transport", new PostgresException("not yet", "FATAL", "FATAL", "57P03")));

        Assert.True(
            StoreStartupFailureTriage.IsRetryable(retryableUnderTransport),
            "control: the same nesting with a retryable state must be retryable");
    }

    /// <summary>
    /// The malformed-connection-string case. SqlClient/Npgsql reject a bad keyword while still PARSING,
    /// raising an ArgumentException that is neither an NpgsqlException nor a PostgresException — the exact
    /// exception that missed both fault-classification arms in #2213. Default-deny puts it on the terminal
    /// side without it having to be enumerated, which is the property being pinned.
    /// </summary>
    [Fact]
    public void AnExceptionThatIsNeitherNpgsqlNorTransport_IsTerminal()
    {
        Assert.False(StoreStartupFailureTriage.IsRetryable(new ArgumentException("Keyword not supported: 'host'")));
        Assert.False(StoreStartupFailureTriage.IsRetryable(new InvalidOperationException("nonsense")));
        Assert.False(StoreStartupFailureTriage.IsRetryable(null));
    }

    /// <summary>
    /// A bare NpgsqlException with nothing recognisable under it is still Npgsql saying the connection
    /// did not work, so it is retryable — the last arm of the classifier, and the one that carries the
    /// pre-#2935 lock-expiry spelling if Npgsql ever stops nesting the TimeoutException.
    /// </summary>
    [Fact]
    public void ABareNpgsqlException_IsRetryable()
        => Assert.True(StoreStartupFailureTriage.IsRetryable(new NpgsqlException("connection went away")));

    /// <summary>
    /// Drift tripwire on the budget. Two minutes is derived in the constant's own remarks from
    /// <c>DarlingManagedPostgres.PgCtlWaitSeconds</c> = 60 below and
    /// <c>DarlingWorker.ColdStartSpreadSeconds</c> = 150 above; a change to either attempt count or delay
    /// that moves the total off two minutes has to come back through that derivation.
    /// </summary>
    [Fact]
    public void RetryBudget_IsTwoMinutes()
    {
        Assert.Equal(25, StoreStartupFailureTriage.MigrateAttempts);
        Assert.Equal(5, (int)StoreStartupFailureTriage.MigrateRetryDelay.TotalSeconds);

        var totalWaitSeconds =
            (StoreStartupFailureTriage.MigrateAttempts - 1) * (int)StoreStartupFailureTriage.MigrateRetryDelay.TotalSeconds;

        Assert.Equal(120, totalWaitSeconds);
        Assert.True(totalWaitSeconds >= 2 * 60, "must outlast the 60s this repo allows a PostgreSQL start (PgCtlWaitSeconds)");
        Assert.True(totalWaitSeconds <= 150, "must stay inside DarlingWorker.ColdStartSpreadSeconds so a spent budget still lands in a normal cold start");
    }

    /// <summary>
    /// The retry arm's filter, read off the shipped source. Three conjuncts, and every one of them is
    /// load-bearing: drop the cancellation exclusion and shutdown becomes a two-minute retry; drop the
    /// attempt bound and a permanent failure never reaches its critical line; drop
    /// <see cref="StoreStartupFailureTriage.IsRetryable"/> and everything is retried, which is the
    /// failure mode #2936 exists to avoid and the one that looks like success.
    /// </summary>
    [Fact]
    public void TheRetryArmFiltersOnCancellation_AttemptCount_AndTheClassifier()
    {
        var source = ReadWorkerSource();
        var retryArm = ExtractMigrateRetryArm(source);

        Assert.Contains("ex is not OperationCanceledException", retryArm, StringComparison.Ordinal);
        Assert.Contains("attempt < StoreStartupFailureTriage.MigrateAttempts", retryArm, StringComparison.Ordinal);
        Assert.Contains("StoreStartupFailureTriage.IsRetryable(ex)", retryArm, StringComparison.Ordinal);

        /* Negative control, run through the IDENTICAL slice and the identical comparison, so
           "the phrase is absent" cannot be the slice having come back empty or misaligned. */
        Assert.DoesNotContain("PLANTED-PHRASE-THAT-IS-NOT-THERE", retryArm, StringComparison.Ordinal);
        Assert.Contains("_logger.LogWarning", retryArm, StringComparison.Ordinal);
    }

    /// <summary>
    /// The terminal arm still exists and still does exactly what it did before #2936. Everything the
    /// classifier declines lands here, so this is the pin that says the change only ADDED a path.
    /// </summary>
    [Fact]
    public void TheTerminalArmStillLogsCriticalAndStandsDown()
    {
        var source = ReadWorkerSource();

        var terminalArm = Slice(
            source,
            "catch (Exception ex) when (ex is not OperationCanceledException)\n            {\n                _logger.LogCritical(\"Cannot reach or migrate the Postgres store: {Message}\"",
            "}");

        Assert.Contains("return;", terminalArm, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every attempt opens a FRESH connection, because a connector that died on a transport failure cannot
    /// carry the next attempt — the same reason <c>DarlingManagedPostgres.EnsureDatabaseAsync</c> retries
    /// the whole unit rather than just the open.
    ///
    /// <para>Asserted by OFFSET, not by occurrence count. Hoisting the open above the loop is a
    /// RELOCATION: it leaves every count in this file invariant (one loop, one open, one migrate call), so
    /// a count-based pin reports clean on the one edit that breaks the property.</para>
    /// </summary>
    [Fact]
    public void EveryAttemptOpensAFreshStoreConnection()
    {
        var source = ReadWorkerSource();

        var loopAt = source.IndexOf("for (var attempt = 1; ; attempt++)", StringComparison.Ordinal);
        var openAt = source.IndexOf("await postgres.OpenConnectionAsync(stoppingToken)", StringComparison.Ordinal);
        var migrateAt = source.IndexOf("await PgMigrations.MigrateAsync(migrateConnection, _logger, stoppingToken)", StringComparison.Ordinal);

        Assert.True(loopAt > 0, "the retry loop around the migrate is gone");
        Assert.True(openAt > 0, "the store connection open is gone");
        Assert.True(migrateAt > 0, "the migrate call is gone");

        Assert.True(loopAt < openAt, "the connection open must be INSIDE the retry loop, not hoisted above it");
        Assert.True(openAt < migrateAt, "the open still precedes the migrate within the loop body");
    }

    /// <summary>
    /// The mechanism that makes re-entering <c>MigrateAsync</c> safe, pinned on the applier itself rather
    /// than assumed by its caller: each rung's DDL and its <c>darling_schema_version</c> stamp go through
    /// ONE transaction, committed once. That is what makes a rung that failed part-way leave nothing
    /// applied AND nothing stamped, so the retry resumes at the rung that failed. Split the two into
    /// separate transactions and a rolled-back rung could be stamped as done, at which point every retry
    /// skips it forever and the store is permanently short of an object nothing will ever create.
    /// </summary>
    [Fact]
    public void TheApplierCommitsEachRungsDdlAndItsStampInOneTransaction()
    {
        var applier = Slice(
            ReadRepoFile("Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs"),
            "private static async Task<int> MigrateLockedAsync",
            "\n    }\n");

        var beginAt = applier.IndexOf("await connection.BeginTransactionAsync(cancellationToken)", StringComparison.Ordinal);
        var applyAt = applier.IndexOf("new NpgsqlCommand(migration.Sql, connection, transaction)", StringComparison.Ordinal);
        var stampAt = applier.IndexOf("INSERT INTO darling_schema_version", StringComparison.Ordinal);
        var commitAt = applier.IndexOf("await transaction.CommitAsync(cancellationToken)", StringComparison.Ordinal);

        Assert.True(beginAt > 0, "the per-rung transaction is gone");
        Assert.True(applyAt > beginAt, "the rung must be applied inside the transaction");
        Assert.True(stampAt > applyAt, "the stamp must be written after the rung, inside the same transaction");
        Assert.True(commitAt > stampAt, "the commit must come after BOTH the rung and its stamp");

        /* Both commands have to be bound to that same transaction OBJECT, not merely sit between the
           begin and the commit — an NpgsqlCommand constructed without it runs in its own implicit
           transaction and commits even when the rung around it rolled back, which would stamp a rung
           that did not apply and make every later retry skip it forever. Asserted per command rather
           than as one substring anywhere in the slice: the rung-apply command carries the same
           `connection, transaction` tail, so a single Contains is satisfied by the apply alone and
           reports clean when the STAMP is the one that lost its binding. */
        Assert.Contains(
            "new NpgsqlCommand(migration.Sql, connection, transaction)",
            applier,
            StringComparison.Ordinal);
        Assert.Contains(
            "VALUES ($1, $2, $3)\", connection, transaction)",
            applier,
            StringComparison.Ordinal);
        Assert.True(
            applier.IndexOf("continue;", StringComparison.Ordinal) < beginAt,
            "the already-applied skip must precede the transaction, which is what makes a retry resume rather than redo");
    }

    private static string ExtractMigrateRetryArm(string workerSource)
        => Slice(
            workerSource,
            "catch (Exception ex) when (ex is not OperationCanceledException\n                && attempt <",
            "await Task.Delay(StoreStartupFailureTriage.MigrateRetryDelay, stoppingToken);");

    /// <summary>
    /// Substring from <paramref name="start"/> through the first <paramref name="end"/> after it, failing
    /// loudly when either anchor has moved. A slice that silently came back empty makes every
    /// <c>DoesNotContain</c> over it pass, so the anchors are asserted before the slice is used.
    /// </summary>
    private static string Slice(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found, this pin is reading nothing: {start}");

        var to = source.IndexOf(end, from + start.Length, StringComparison.Ordinal);
        Assert.True(to > from, $"closing anchor not found after the opening one: {end}");

        var slice = source[from..(to + end.Length)];
        Assert.False(string.IsNullOrWhiteSpace(slice), "empty slice");
        return slice;
    }

    private static string ReadWorkerSource()
        => ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

    private static string ReadRepoFile(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path), $"source not found, this pin would read nothing: {path}");
        /* Normalised to LF. The checkout is CRLF (.gitattributes is eol=crlf) but an anchor that
           embeds the wrong newline matches NOTHING and reads as clean, so no anchor in this file
           carries a line ending at all. */
        return File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
