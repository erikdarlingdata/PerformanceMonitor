/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins how a PostgreSQL fault becomes a collection_log outcome. The point of the mapping is that a
/// PERSISTENT, operator-actionable condition must not log ERROR every cycle forever — and that the ones
/// which genuinely are errors still do.
/// </summary>
public class PostgresFaultOutcomeTests
{
    /// <summary>
    /// PostgresException's public constructor takes the fields the classifier reads. SqlState is the only
    /// one that decides anything; the message text just has to survive into the explanation.
    /// </summary>
    private static PostgresException Pg(string sqlState, string message = "boom")
        => new(message, "ERROR", "ERROR", sqlState);

    /* A collector that does NOT opt into the lock-timeout yield, so 55P03 stays an error for it. */
    private const string PlainCollector = "pg_wait_stats";

    [Fact]
    public void PermissionDeniedIsRecordedAsPermissionsAndNamesTheRoleThatFixesIt()
    {
        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(Pg("42501"), PlainCollector);

        Assert.Equal("PERMISSIONS", status);
        Assert.Contains("pg_monitor", explanation, StringComparison.Ordinal);
        Assert.Contains("42501", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The case this wiring exists for. pg_statement_stats against a database where the extension was
    /// never created raises 42P01 on every single cycle; before this it logged ERROR every minute forever.
    /// It must degrade quietly AND say plainly that it is not a grant problem, or the PERMISSIONS status
    /// sends someone hunting for a GRANT that will not help.
    /// </summary>
    [Theory]
    [InlineData("42P01")]
    [InlineData("42883")]
    public void AMissingObjectDegradesQuietlyAndSaysItIsNotAGrant(string sqlState)
    {
        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(Pg(sqlState), "pg_statement_stats");

        Assert.Equal("PERMISSIONS", status);
        Assert.Contains("NOT a missing grant", explanation, StringComparison.Ordinal);
        Assert.Contains("CREATE EXTENSION", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2638: and it names WHICH database, because extensions are per-database and "create it somewhere" is
    /// not an instruction.
    ///
    /// <para>Measured on the fleet: <c>pg_buffercache</c> was installed on the cluster — in a different
    /// database from the one the collector connects to — while the collector reported it missing. Both
    /// statements were true. An operator who checked the obvious database would have found the extension
    /// already there and concluded the collector was broken.</para>
    /// </summary>
    [Theory]
    [InlineData("42P01")]
    [InlineData("42883")]
    public void AMissingObjectNamesTheDatabaseItIsMissingFrom(string sqlState)
    {
        var (_, explanation) = DarlingWorker.PostgresFaultOutcome(Pg(sqlState), "pg_buffer_usage", "appdb");

        Assert.Contains("database 'appdb'", explanation, StringComparison.Ordinal);
        Assert.Contains("CREATE EXTENSION in 'appdb'", explanation, StringComparison.Ordinal);

        /* The half that makes the name worth printing: it says the extension may exist elsewhere on the
           same cluster, which is the situation the message used to leave someone to discover alone. */
        Assert.Contains("DIFFERENT database on the same cluster", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// And an unknown database degrades to what the sentence always said, rather than inventing a name — a
    /// message that confidently names the wrong database is worse than one that names none.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AnUnknownDatabaseFallsBackRatherThanGuessing(string? connectedDatabase)
    {
        var (_, explanation) = DarlingWorker.PostgresFaultOutcome(Pg("42P01"), "pg_buffer_usage", connectedDatabase);

        Assert.Contains("in the connected database", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("database ''", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// Aurora does not implement some community sources at all (0A000 for pg_stat_wal) and gates others by
    /// parameter group (55006). Neither will change until the platform or the parameter group does, so
    /// neither is worth an error every cycle — and neither is a grant.
    /// </summary>
    [Theory]
    [InlineData("0A000")]
    [InlineData("55000")]
    [InlineData("55006")]
    public void AnUnsupportedOrDisabledFeatureDegradesQuietly(string sqlState)
    {
        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(Pg(sqlState), PlainCollector);

        Assert.Equal("PERMISSIONS", status);
        Assert.Contains("NOT a missing grant", explanation, StringComparison.Ordinal);
        Assert.Contains("parameter group", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// A lock timeout is a YIELD only for a collector that deliberately set a short lock timeout. For any
    /// other collector it is a genuine error and must reach the general handler.
    /// </summary>
    [Fact]
    public void LockTimeoutYieldsOnlyForACollectorThatOptedIn()
    {
        Assert.Equal("ERROR", DarlingWorker.PostgresFaultOutcome(Pg("55P03"), PlainCollector).Status);

        /* Whichever collectors declare the guard, at least one must yield — otherwise the branch is dead
           and the classifier's yieldsOnLockTimeout argument is being ignored. */
        var yielding = Array.Find(
            System.Linq.Enumerable.ToArray(CollectorCatalog.All),
            d => CollectorCatalog.YieldsOnLockTimeout(d.Name));

        if (yielding is not null)
        {
            var (status, explanation) = DarlingWorker.PostgresFaultOutcome(Pg("55P03"), yielding.Name);
            Assert.Equal("YIELDED", status);
            Assert.Contains("lock contention", explanation, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A statement timeout is a slow query, not a dead socket. It must fall through to the general handler
    /// as an ERROR — and, critically, must NOT be classified ConnectionFatal, because the general handler
    /// drops and reprobes the connection for that, turning a tuning problem into a reconnect storm.
    /// </summary>
    [Fact]
    public void AStatementTimeoutIsAnErrorButNotAConnectionFailure()
    {
        Assert.Equal("ERROR", DarlingWorker.PostgresFaultOutcome(Pg("57014"), PlainCollector).Status);

        Assert.Equal(
            CollectorTargetFault.CommandTimeout,
            PostgresTargetProvider.Instance.Classify(Pg("57014"), yieldsOnLockTimeout: false));
        Assert.NotEqual(
            CollectorTargetFault.ConnectionFatal,
            PostgresTargetProvider.Instance.Classify(Pg("57014"), yieldsOnLockTimeout: false));
    }

    /// <summary>
    /// Connection-level failures DO belong to the general handler, and are the cases that force the
    /// reconnect. Pinned through the provider, which is what the handler's own filter consults.
    /// </summary>
    [Theory]
    [InlineData("08006")]
    [InlineData("08003")]
    [InlineData("57P01")]
    [InlineData("57P02")]
    [InlineData("57P03")]
    public void ConnectionLevelFailuresReachTheGeneralHandlerAndForceAReprobe(string sqlState)
    {
        Assert.Equal("ERROR", DarlingWorker.PostgresFaultOutcome(Pg(sqlState), PlainCollector).Status);

        Assert.Equal(
            CollectorTargetFault.ConnectionFatal,
            PostgresTargetProvider.Instance.Classify(Pg(sqlState), yieldsOnLockTimeout: false));
    }

    /// <summary>An unrecognized SQLSTATE stays loud rather than being quietly bucketed as a skip.</summary>
    [Fact]
    public void AnUnknownSqlStateStaysAnError()
    {
        var (status, explanation) = DarlingWorker.PostgresFaultOutcome(Pg("XX000", "internal error"), PlainCollector);

        Assert.Equal("ERROR", status);
        Assert.Contains("internal error", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every non-ERROR status the mapper can emit must be one the store already understands. Inventing a
    /// sixth would break the dashboards, health bands and self-alerts that read this column.
    /// </summary>
    [Theory]
    [InlineData("42501")]
    [InlineData("42P01")]
    [InlineData("0A000")]
    [InlineData("55006")]
    [InlineData("57014")]
    [InlineData("08006")]
    [InlineData("XX000")]
    public void OnlyEverEmitsAStatusTheStoreAlreadyUnderstands(string sqlState)
    {
        var status = DarlingWorker.PostgresFaultOutcome(Pg(sqlState), PlainCollector).Status;

        Assert.Contains(status, new[] { "SUCCESS", "PERMISSIONS", "ERROR", "SESSION_MISSING", "YIELDED" });
    }

    /// <summary>
    /// The gap #2997 is about, stated as an assertion: a client-side command deadline is NOT a
    /// <see cref="PostgresException"/>, so no amount of widening the SQLSTATE map above could ever
    /// classify it.
    ///
    /// <para>Npgsql raises an <c>NpgsqlException</c> wrapping a <c>TimeoutException</c> and carrying no
    /// SQLSTATE at all — the transport's "Exception while reading from stream". The provider already
    /// classifies that shape correctly; what could not reach it was <see cref="DarlingWorker"/>'s
    /// PostgreSQL arm, whose parameter type excludes it. Eleven consecutive <c>pg_index_bloat</c>
    /// failures were logged as raw transport text for exactly this reason, and two separate
    /// investigations read them as a connection dying at open.</para>
    /// </summary>
    [Fact]
    public void AClientSideDeadlineIsNotAPostgresException_SoTheSqlStateMapCannotReachIt()
    {
        var timeout = new NpgsqlException("Exception while reading from stream", new TimeoutException());

        Assert.IsNotType<PostgresException>(timeout);
        Assert.Equal(
            CollectorTargetFault.CommandTimeout,
            PostgresTargetProvider.Instance.Classify(timeout, yieldsOnLockTimeout: false));
    }

    /// <summary>
    /// The authored sentence carries the four things the raw seven words did not: which collector, which
    /// database, how long it actually ran, and that nothing was collected.
    /// </summary>
    [Fact]
    public void TheTimeoutExplanationNamesTheCollectorTheDatabaseAndTheMeasuredElapsedTime()
    {
        var explanation = DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", "appdb", elapsedMs: 300_142, serverCancelled: false);

        Assert.Contains("pg_index_bloat", explanation, StringComparison.Ordinal);
        Assert.Contains("appdb", explanation, StringComparison.Ordinal);
        Assert.Contains("300,142 ms", explanation, StringComparison.Ordinal);

        /* The house rule on honest empties: a run that read nothing must never be mistakable for a run
           that found nothing. */
        Assert.Contains("Nothing was collected this cycle", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// Two deadlines both classify as <see cref="CollectorTargetFault.CommandTimeout"/> and they are
    /// fixed in different places — SQLSTATE 57014 is the TARGET's <c>statement_timeout</c>, the other is
    /// ours. A single sentence covering both would send an operator to the wrong knob half the time.
    /// </summary>
    [Fact]
    public void TheTimeoutExplanationDistinguishesTheServersDeadlineFromOurs()
    {
        var client = DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", "appdb", elapsedMs: 300_000, serverCancelled: false);
        var server = DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", "appdb", elapsedMs: 300_000, serverCancelled: true);

        Assert.Contains("CLIENT-SIDE", client, StringComparison.Ordinal);
        Assert.DoesNotContain("57014", client, StringComparison.Ordinal);

        Assert.Contains("CANCELLED BY THE SERVER", server, StringComparison.Ordinal);
        Assert.Contains("57014", server, StringComparison.Ordinal);
        Assert.Contains("statement_timeout", server, StringComparison.Ordinal);
    }

    /// <summary>
    /// An unknown connected database degrades to a phrase rather than inventing a name — the same rule
    /// <see cref="DarlingWorker.PostgresFaultOutcome"/>'s ObjectMissing arm follows via WhereToCreateIt.
    /// </summary>
    [Fact]
    public void TheTimeoutExplanationDegradesWhenTheDatabaseIsUnknown()
    {
        var explanation = DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", connectedDatabase: null, elapsedMs: 1, serverCancelled: false);

        Assert.Contains("the connected database", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("''", explanation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The ERROR arms record the run's REAL elapsed time, not the literal zeros they used to store
    /// (#2997 item 3).
    ///
    /// <para>A wiring invariant, so it is pinned in the source the way this suite already pins the
    /// #1648 middleware order and the #1706 upgrade catch order: the value is written by
    /// <c>LogCollectionAsync</c> inside a live sweep, so no pure test can observe it, and the defect is
    /// an argument list rather than a logic error. Three literal zeros made a 300-second death and an
    /// instantly-refused connection store byte-identical rows — and <c>duration_ms</c> is
    /// <c>sqlMs + storageMs</c>, so it destroyed the one number that separates them.</para>
    /// </summary>
    [Fact]
    public void TheErrorArmsRecordRealElapsedTimeRatherThanLiteralZeros()
    {
        var worker = ReadSource(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        /* The stopwatch the fault arms read. */
        Assert.Contains("var runClock = System.Diagnostics.Stopwatch.StartNew();", worker, StringComparison.Ordinal);

        /* No ERROR row may be written with the old three-zero argument list. This is the assertion that
           fails if the fix is reverted: the arms once read `"ERROR", 0, 0, 0`. */
        Assert.DoesNotContain("collectorName, \"ERROR\", 0, 0, 0", worker, StringComparison.Ordinal);

        /* Both ERROR writes — the authored timeout arm and the general catch — now pass a measured
           figure in the sqlMs slot that feeds duration_ms. */
        Assert.Equal(2, Regex.Matches(
            worker,
            @"collectorName, ""ERROR"", 0, (?:elapsedMs|runClock\.ElapsedMilliseconds), 0").Count);
    }

    /// <summary>
    /// The timeout arm must NOT force a reconnect. The provider classifies a deadline as
    /// <see cref="CollectorTargetFault.CommandTimeout"/> rather than
    /// <see cref="CollectorTargetFault.ConnectionFatal"/> specifically so a slow statement cannot turn a
    /// tuning problem into a reconnect storm, and an arm that nulled the runtime would undo that while
    /// also taking the reprobe away from the faults that genuinely need it.
    /// </summary>
    [Fact]
    public void TheTimeoutArmDoesNotStealTheReprobeFromConnectionFatal()
    {
        var timeout = new NpgsqlException("Exception while reading from stream", new TimeoutException());
        var dead = new NpgsqlException("connection was closed");

        Assert.Equal(
            CollectorTargetFault.CommandTimeout,
            PostgresTargetProvider.Instance.Classify(timeout, yieldsOnLockTimeout: false));
        Assert.Equal(
            CollectorTargetFault.ConnectionFatal,
            PostgresTargetProvider.Instance.Classify(dead, yieldsOnLockTimeout: false));
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath));
    }
}
