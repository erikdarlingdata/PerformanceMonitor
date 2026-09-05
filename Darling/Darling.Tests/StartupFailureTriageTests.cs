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
using System.Text.RegularExpressions;
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
public class StartupFailureTriageTests
{
    /* Observed against PostgreSQL 17.11 / TimescaleDB 2.29.2 rather than reasoned about: each of these is
       a shape a real failure arrived in, with the scenario that produced it. */

    [Fact]
    public void NothingListening_IsRetryable()
        => Assert.True(StartupFailureTriage.IsRetryable(
            new NpgsqlException("Failed to connect to 127.0.0.1:5432", new SocketException(61))));

    [Fact]
    public void ConnectTimeout_IsRetryable()
        => Assert.True(StartupFailureTriage.IsRetryable(
            new NpgsqlException("Failed to connect to 10.0.0.1:5432", new TimeoutException("Timeout during connection attempt"))));

    [Fact]
    public void StoreDyingMidStatement_IsRetryable()
        => Assert.True(StartupFailureTriage.IsRetryable(
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
        => Assert.True(StartupFailureTriage.IsRetryable(
            new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"))));

    /// <summary>#2935's polling waiter throws a plain TimeoutException carrying both schema versions.</summary>
    [Fact]
    public void BareTimeoutException_IsRetryable()
        => Assert.True(StartupFailureTriage.IsRetryable(
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
            StartupFailureTriage.IsRetryable(new PostgresException(why, "FATAL", "FATAL", sqlState)),
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
        /* Deliberately absent from the class 08 allowlist. */
        { "08P01", "protocol violation — a defect that recurs identically" },
        { "08007", "transaction resolution unknown — worth saying out loud" },
        /* Not a state this code knows: default-deny. */
        { "XX999", "an unrecognised state" },
    };

    [Theory]
    [MemberData(nameof(TerminalStates))]
    public void TerminalSqlStates_AreTerminal(string sqlState, string why)
        => Assert.False(
            StartupFailureTriage.IsRetryable(new PostgresException(why, "ERROR", "ERROR", sqlState)),
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
                StartupFailureTriage.IsRetryable(new PostgresException("not yet", "FATAL", "FATAL", retryable)),
                $"{retryable} is the store going down or coming up");
        }

        foreach (var terminal in new[] { "57014", "57P04" })
        {
            Assert.False(
                StartupFailureTriage.IsRetryable(new PostgresException("no", "ERROR", "ERROR", terminal)),
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
            StartupFailureTriage.IsRetryable(terminalUnderTransport),
            "a 42P01 verdict wrapped in transport noise is still a rung that cannot apply");

        /* Positive control through the IDENTICAL wrapping, so the assertion above cannot be passing
           because the shape is unreachable or because IsRetryable rejects every nested exception. */
        var retryableUnderTransport = new NpgsqlException(
            "Exception while reading from stream",
            new IOException("transport", new PostgresException("not yet", "FATAL", "FATAL", "57P03")));

        Assert.True(
            StartupFailureTriage.IsRetryable(retryableUnderTransport),
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
        Assert.False(StartupFailureTriage.IsRetryable(new ArgumentException("Keyword not supported: 'host'")));
        Assert.False(StartupFailureTriage.IsRetryable(new InvalidOperationException("nonsense")));
        Assert.False(StartupFailureTriage.IsRetryable(null));
    }

    /// <summary>
    /// A bare NpgsqlException with nothing recognisable under it is still Npgsql saying the connection
    /// did not work, so it is retryable — the last arm of the classifier, and the one that carries the
    /// pre-#2935 lock-expiry spelling if Npgsql ever stops nesting the TimeoutException.
    /// </summary>
    [Fact]
    public void ABareNpgsqlException_IsRetryable()
        => Assert.True(StartupFailureTriage.IsRetryable(new NpgsqlException("connection went away")));

    /// <summary>
    /// Drift tripwire on the budget. Two minutes is derived in the constant's own remarks from
    /// <c>DarlingManagedPostgres.PgCtlWaitSeconds</c> = 60 below and
    /// <c>DarlingWorker.ColdStartSpreadSeconds</c> = 150 above; a change to either attempt count or delay
    /// that moves the total off two minutes has to come back through that derivation.
    /// </summary>
    [Fact]
    public void RetryBudget_IsTwoMinutes()
    {
        Assert.Equal(25, StartupFailureTriage.Attempts);
        Assert.Equal(5, (int)StartupFailureTriage.RetryDelay.TotalSeconds);
        Assert.Equal(120, (int)StartupFailureTriage.RetryBudget.TotalSeconds);

        Assert.True(
            StartupFailureTriage.RetryBudget.TotalSeconds >= 2 * 60,
            "must outlast the 60s this repo allows a PostgreSQL start (DarlingManagedPostgres.PgCtlWaitSeconds)");
        Assert.True(
            StartupFailureTriage.RetryBudget.TotalSeconds <= 150,
            "must stay inside DarlingWorker.ColdStartSpreadSeconds so even a spent budget lands in a normal cold start");

        /* The two caps must AGREE on the ordinary case, which is a failure that returns immediately: the
           pauses the attempt count allows have to fill the wall-clock budget rather than expire early
           (retrying for less than the derived two minutes) or overrun it (an attempt count that can never
           be reached, making the log line's "of 25" a number nothing counts to). */
        var pausesAllowed =
            (StartupFailureTriage.Attempts - 1) * StartupFailureTriage.RetryDelay.TotalSeconds;

        Assert.Equal(StartupFailureTriage.RetryBudget.TotalSeconds, pausesAllowed);
    }

    /// <summary>
    /// The wall-clock cap is in the retry filter, not just declared.
    ///
    /// <para>Without it the attempt count is not a bound at all: an attempt that blocks behind a peer's
    /// migration advisory lock can spend <c>MigrationLockWaitTimeoutSeconds</c> = 1500 s, so 25 of them is
    /// over ten hours of retrying rather than the two minutes the constant is derived for, and the one
    /// definitive line arrives most of a day after the failure it describes. Measured on a live store: a
    /// second session holding that lock made a single attempt wait 38 s, silently.</para>
    /// </summary>
    [Fact]
    public void TheRetryArmAlsoStopsOnAWallClockDeadline()
    {
        var source = ReadWorkerSource();
        var retryArm = ExtractRetrySite(source, "storeRetryBudget");

        Assert.Contains(
            "storeRetryBudget.Elapsed < StartupFailureTriage.RetryBudget",
            retryArm,
            StringComparison.Ordinal);

        /* The stopwatch has to start OUTSIDE the loop, or every attempt resets the deadline and the cap
           measures one attempt instead of the whole retry. A relocation, so asserted by offset. */
        var startedAt = source.IndexOf("var storeRetryBudget = System.Diagnostics.Stopwatch.StartNew();", StringComparison.Ordinal);
        var loopAt = source.IndexOf("for (var attempt = 1; ; attempt++)", startedAt + 1, StringComparison.Ordinal);

        Assert.True(startedAt > 0, "the retry deadline's stopwatch is gone");
        Assert.True(loopAt > 0, "the retry loop is gone");
        Assert.True(startedAt < loopAt, "the stopwatch must start BEFORE the loop, not per attempt");
    }

    /// <summary>
    /// The retry arm's filter, read off the shipped source. Four conjuncts, and every one of them is
    /// load-bearing: drop the cancellation exclusion and shutdown becomes a two-minute retry; drop the
    /// attempt bound or the wall-clock deadline and a permanent failure reaches its critical line late or
    /// never (see <see cref="TheRetryArmAlsoStopsOnAWallClockDeadline"/> for why one cap is not two); drop
    /// <see cref="StartupFailureTriage.IsRetryable"/> and everything is retried, which is the
    /// failure mode #2936 exists to avoid and the one that looks like success.
    /// </summary>
    [Fact]
    public void TheRetryArmFiltersOnCancellation_AttemptCount_AndTheClassifier()
    {
        var source = ReadWorkerSource();
        var retryArm = ExtractRetrySite(source, "storeRetryBudget");

        Assert.Contains("ex is not OperationCanceledException", retryArm, StringComparison.Ordinal);
        Assert.Contains("attempt < StartupFailureTriage.Attempts", retryArm, StringComparison.Ordinal);
        Assert.Contains("storeRetryBudget.Elapsed < StartupFailureTriage.RetryBudget", retryArm, StringComparison.Ordinal);
        Assert.Contains("StartupFailureTriage.IsRetryable(ex)", retryArm, StringComparison.Ordinal);

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

        var storeBudgetAt = source.IndexOf("var storeRetryBudget = System.Diagnostics.Stopwatch.StartNew();", StringComparison.Ordinal);
        Assert.True(storeBudgetAt > 0, "the store site's retry budget is gone");
        var loopAt = source.IndexOf("for (var attempt = 1; ; attempt++)", storeBudgetAt + 1, StringComparison.Ordinal);
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

    /// <summary>
    /// The retry warning's message template names each thing ONCE.
    ///
    /// <para>Not style. <c>LogValuesFormatter</c> numbers placeholders by OCCURRENCE, not by name, so a
    /// template that mentions <c>{Total}</c> twice needs five arguments for the four things it names and
    /// <c>String.Format</c> throws <see cref="FormatException"/> on the fifth. That escapes the logger,
    /// escapes this <c>BackgroundService</c>, and <c>BackgroundServiceExceptionBehavior.StopHost</c> then
    /// stops the process - so the retry path would kill the service harder than the transient failure it
    /// was added to survive, and only on the attempt that actually retries. This was a real defect in the
    /// first cut of #2936, found by running the service against a store that was down and not by any
    /// assertion, which is precisely why it is pinned.</para>
    /// </summary>
    [Fact]
    public void TheRetryWarningNamesEachPlaceholderExactlyOnce()
    {
        var retryArm = ExtractRetryWarning(ReadWorkerSource(), "storeRetryBudget");

        /* Read the template out of the arm: the concatenated string literals, minus the C# glue. */
        var template = string.Concat(
            Regex.Matches(retryArm, "\"(?<text>[^\"]*)\"").Select(m => m.Groups["text"].Value));

        var placeholders = Regex.Matches(template, "\\{(?<name>[A-Za-z][A-Za-z0-9]*)\\}")
            .Select(m => m.Groups["name"].Value)
            .ToList();

        Assert.NotEmpty(placeholders);
        Assert.Equal(
            new[] { "Message", "Attempt", "Total", "Delay" },
            placeholders);

        var repeated = placeholders.GroupBy(n => n, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.Empty(repeated);
    }

    /* ---- the file-I/O half: the config-load and bootstrap sites (#2936, widened) ---- */

    /// <summary>
    /// A sharing violation on a file another process is mid-write. This is the config-load site's whole
    /// reason for existing: <c>DarlingConfig.Load</c> is <c>File.Exists</c> then <c>File.ReadAllText</c>,
    /// and an installer, the Viewer's Settings save or a config-management tool holding
    /// <c>darling.json</c> open for a moment used to end collection for the process lifetime.
    /// </summary>
    [Fact]
    public void AFileSharingViolation_IsRetryable()
        => Assert.True(StartupFailureTriage.IsRetryable(
            new IOException("The process cannot access the file 'darling.json' because it is being used by another process.")));

    /// <summary>
    /// The carve-out, and the reason it is not optional. <see cref="FileNotFoundException"/> and
    /// <see cref="DirectoryNotFoundException"/> BOTH derive from <see cref="IOException"/>, so the
    /// transport arm would sweep them in — and <c>DarlingConfig.Load</c> throws
    /// <see cref="FileNotFoundException"/> for a config that is not there, which on a first install is the
    /// likeliest way it ever fails. Retried, the operator would get warnings for two minutes and then the
    /// line naming the missing path, instead of the line immediately.
    /// </summary>
    [Fact]
    public void TheNotFoundSubtypes_AreTerminal_DespiteBeingIOExceptions()
    {
        Assert.True(typeof(IOException).IsAssignableFrom(typeof(FileNotFoundException)),
            "control: FileNotFoundException must really be an IOException, or this carve-out guards nothing");
        Assert.True(typeof(IOException).IsAssignableFrom(typeof(DirectoryNotFoundException)),
            "control: DirectoryNotFoundException must really be an IOException");

        Assert.False(
            StartupFailureTriage.IsRetryable(new FileNotFoundException("Configuration file not found: darling.json", "darling.json")),
            "a config that is not there does not appear by waiting");
        Assert.False(
            StartupFailureTriage.IsRetryable(new DirectoryNotFoundException("install directory missing")),
            "a missing directory does not appear by waiting");

        /* Nested too, since Load() resolves env:/file: references and can wrap. */
        Assert.False(StartupFailureTriage.IsRetryable(
            new InvalidOperationException("resolving postgres.connectionString", new FileNotFoundException("secret file missing"))));

        /* Positive control through the IDENTICAL wrapping, so the three assertions above cannot be
           passing because IsRetryable rejects everything file-shaped or everything nested. */
        Assert.True(StartupFailureTriage.IsRetryable(
            new InvalidOperationException("resolving postgres.connectionString", new IOException("file is locked"))));
    }

    /// <summary>
    /// A malformed config is terminal. <see cref="System.Text.Json.JsonException"/> and
    /// <c>InvalidDataException</c> are not <see cref="IOException"/>s, so default-deny already covers them
    /// — pinned because that is a property of the type hierarchy rather than of anything written here,
    /// and a future carve-out could break it without touching the classifier.
    /// </summary>
    [Fact]
    public void AMalformedConfig_IsTerminal()
    {
        Assert.False(StartupFailureTriage.IsRetryable(new System.Text.Json.JsonException("bad token at line 4")));
        Assert.False(StartupFailureTriage.IsRetryable(new InvalidDataException("Configuration file parsed to null.")));
    }

    /// <summary>
    /// A config this account cannot read is terminal, asserted in the shape the runtime ACTUALLY raises.
    ///
    /// <para><b>This is the fixture that hid a real bug.</b> The first version of this test constructed a
    /// bare <see cref="UnauthorizedAccessException"/>, which is terminal for a trivial reason — it matches
    /// no arm — and so it passed while the classifier was wrong. On Unix .NET raises that exception
    /// WRAPPING an <c>IOException("Permission denied")</c>, measured against a <c>chmod 000</c> file and
    /// against a directory in place of the config, and the transport pass therefore called an ACL problem
    /// transient. Both shapes are asserted now, and the wrapped one is the one that matters.</para>
    /// </summary>
    [Fact]
    public void AnUnreadableConfig_IsTerminal_InTheShapeTheRuntimeActuallyRaises()
    {
        /* The bare shape: terminal, but for a reason that proves nothing on its own. */
        Assert.False(StartupFailureTriage.IsRetryable(new UnauthorizedAccessException("Access to the path is denied.")));

        /* The REAL shape, measured: chmod 000 and a directory-as-config both produce this. */
        Assert.False(
            StartupFailureTriage.IsRetryable(new UnauthorizedAccessException(
                "Access to the path '/etc/darling.json' is denied.",
                new IOException("Permission denied"))),
            "an ACL problem wrapping an IOException must not be read as a transient file lock");

        /* Positive control through the identical wrapping: swap only the OUTER type for one that is not
           carved out, and the same inner IOException must make it retryable. Without this, the assertion
           above could be passing because any nested IOException is rejected. */
        Assert.True(
            StartupFailureTriage.IsRetryable(new InvalidOperationException(
                "reading darling.json", new IOException("Resource temporarily unavailable"))),
            "control: the same inner IOException under a non-carved-out outer type is retryable");
    }

    /// <summary>
    /// All THREE collection-blocking startup steps are triaged, and they all call the SAME predicate.
    ///
    /// <para>The point is the shared classifier. Three sites each classifying for themselves is three
    /// places for the boundary to drift, and the one genuine per-site difference — the not-found carve-out
    /// — is expressed as a type check inside <see cref="StartupFailureTriage.IsRetryable"/> rather than as a
    /// second predicate. So this asserts each site is present, bounded both ways, and delegates.</para>
    /// </summary>
    [Theory]
    [InlineData("configRetryBudget", "Cannot load configuration yet")]
    [InlineData("bootstrapRetryBudget", "Managed Postgres bootstrap failed, retrying")]
    [InlineData("storeRetryBudget", "Cannot reach or migrate the Postgres store yet")]
    public void EveryTriagedStartupSiteSharesTheOneClassifierAndBothCaps(string budgetVariable, string warningOpening)
    {
        var site = ExtractRetrySite(ReadWorkerSource(), budgetVariable);

        Assert.Contains("attempt < StartupFailureTriage.Attempts", site, StringComparison.Ordinal);
        Assert.Contains(budgetVariable + ".Elapsed < StartupFailureTriage.RetryBudget", site, StringComparison.Ordinal);
        Assert.Contains("StartupFailureTriage.IsRetryable(ex)", site, StringComparison.Ordinal);
        Assert.Contains(warningOpening, site, StringComparison.Ordinal);

        /* Negative control through the identical slice, so "the phrase is absent" below cannot be the
           slice having come back empty or misaligned. */
        Assert.DoesNotContain("PLANTED-PHRASE-THAT-IS-NOT-THERE", site, StringComparison.Ordinal);

        /* Each site's warning template names each thing once, for the same LogValuesFormatter reason the
           store site's own pin documents at length. */
        var warning = ExtractRetryWarning(ReadWorkerSource(), budgetVariable);
        var template = string.Concat(
            Regex.Matches(warning, "\"(?<text>[^\"]*)\"").Select(m => m.Groups["text"].Value));
        var placeholders = Regex.Matches(template, "\\{(?<name>[A-Za-z][A-Za-z0-9]*)\\}")
            .Select(m => m.Groups["name"].Value)
            .ToList();

        Assert.Equal(new[] { "Message", "Attempt", "Total", "Delay" }, placeholders);
    }

    /// <summary>
    /// The three budget variables are distinct AND none is a substring of another, which is what lets
    /// <see cref="ExtractRetrySite"/> address one site without matching its siblings. A rename that
    /// collapsed two of them would make every per-site pin in this file read the wrong site and keep
    /// passing, because the three arms are deliberately the same shape.
    /// </summary>
    [Fact]
    public void TheThreeSitesHaveDistinctBudgetVariables()
    {
        var source = ReadWorkerSource();
        var names = new[] { "storeRetryBudget", "configRetryBudget", "bootstrapRetryBudget" };

        foreach (var n in names)
        {
            Assert.Equal(1, CountOf(source, "var " + n + " = System.Diagnostics.Stopwatch.StartNew();"));
            Assert.Equal(1, CountOf(source, "&& " + n + ".Elapsed < StartupFailureTriage.RetryBudget"));
        }

        foreach (var a in names)
        {
            foreach (var b in names.Where(b => b != a))
            {
                Assert.False(b.Contains(a, StringComparison.Ordinal), $"'{a}' is a substring of '{b}'");
            }
        }

        /* And exactly three triaged sites, so a fourth cannot be added without coming through here.

           The stopwatch count is spelled with the RetryBudget suffix all three share rather than as a
           bare Stopwatch.StartNew(): every retry budget here is a stopwatch, but not every stopwatch is
           a retry budget. The bare form made this pin fail for ANY unrelated timing added anywhere in
           DarlingWorker - #2997 added a per-run clock to the collector fault arms, hundreds of lines and
           one concern away - which tells nobody whether a fourth retry site appeared. Suffixed, the pin
           measures the thing its own sentence claims. */
        Assert.Equal(3, CountOf(source, "StartupFailureTriage.IsRetryable(ex)"));
        Assert.Equal(3, CountOf(source, "RetryBudget = System.Diagnostics.Stopwatch.StartNew();"));
    }

    /// <summary>
    /// The two new sites keep their terminal arms exactly as they were, so everything the classifier
    /// declines still lands on the same critical line and the same stand-down.
    /// </summary>
    [Theory]
    [InlineData("_logger.LogCritical(\"Cannot load configuration: {Message}\", ex.Message);")]
    [InlineData("_logger.LogCritical(\"Managed Postgres bootstrap failed: {Message}\", ex.Message);")]
    public void TheNewSitesTerminalArmsStillLogCriticalAndStandDown(string criticalLine)
    {
        var terminalArm = Slice(ReadWorkerSource(), criticalLine, "}");

        Assert.Contains("return;", terminalArm, StringComparison.Ordinal);
    }

    /// <summary>
    /// The three steps this PR deliberately left terminal, asserted so the boundary is visible as a
    /// decision rather than an omission: a config that fails <c>Validate()</c> (a named per-field problem
    /// no amount of waiting fixes), <c>postgres.managed = true</c> on a non-Windows host (the bundled
    /// runtime and DPAPI do not exist there), and — in <c>Program.cs</c> — the single-instance guard, which
    /// notably EXITS non-zero rather than idling, so it is already the one startup refusal an operator or
    /// a service manager can actually see.
    /// </summary>
    [Fact]
    public void TheDeliberatelyTerminalStartupStepsAreStillTerminal()
    {
        var worker = ReadWorkerSource();

        var validate = Slice(worker, "_logger.LogCritical(\"Configuration problem: {Problem}\", problem);", "}");
        Assert.DoesNotContain("StartupFailureTriage", validate, StringComparison.Ordinal);

        var nonWindows = Slice(worker, "\"postgres.managed = true requires Windows", "}");
        Assert.Contains("return;", nonWindows, StringComparison.Ordinal);
        Assert.DoesNotContain("StartupFailureTriage", nonWindows, StringComparison.Ordinal);

        /* Positive control for both DoesNotContain calls above, through the identical Slice form: a slice
           that DOES mention the classifier proves an absent mention is a real absence. */
        var storeSite = ExtractRetrySite(worker, "storeRetryBudget");
        Assert.Contains("StartupFailureTriage", storeSite, StringComparison.Ordinal);

        var guard = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/Program.cs");
        var guardArm = Slice(guard, "Another PerformanceMonitor Darling service instance already holds", "return 4;");
        Assert.DoesNotContain("StartupFailureTriage", guardArm, StringComparison.Ordinal);
    }

    private static int CountOf(string haystack, string needle)
    {
        var n = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            n++;
        }

        return n;
    }

    /// <summary>
    /// The retry site whose wall-clock budget is <paramref name="budgetVariable"/>, from its
    /// <c>Stopwatch.StartNew()</c> through its <c>Task.Delay</c>.
    ///
    /// <para><b>Keyed on the budget variable because the three sites are otherwise IDENTICAL in shape.</b>
    /// The obvious anchor — the <c>catch ... when (ex is not OperationCanceledException &amp;&amp; attempt &lt;</c>
    /// line — now matches all three at the same indentation, and <see cref="Slice"/> takes the FIRST match,
    /// so every store-site pin in this file would have silently retargeted the config site when the second
    /// site landed and gone on passing, because the arms are deliberately the same shape. The three
    /// variables are named so that none is a substring of another
    /// (<c>storeRetryBudget</c> / <c>configRetryBudget</c> / <c>bootstrapRetryBudget</c>), which
    /// <see cref="TheThreeSitesHaveDistinctBudgetVariables"/> pins.</para>
    /// </summary>
    private static string ExtractRetrySite(string workerSource, string budgetVariable)
        => Slice(
            workerSource,
            "var " + budgetVariable + " = System.Diagnostics.Stopwatch.StartNew();",
            "await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);");

    /// <summary>
    /// Just the retry warning's <c>LogWarning</c> call within a site.
    ///
    /// <para>Narrower than <see cref="ExtractRetrySite"/> on purpose: that slice opens at the site's
    /// <c>Stopwatch.StartNew()</c>, which is ABOVE the <c>try</c>, so it also contains the SUCCESS log
    /// line — <c>{Version}</c>/<c>{Applied}</c> at the store site, <c>{Path}</c>/<c>{ServerCount}</c> at
    /// the config site. A placeholder census over the whole site therefore counts template holes that
    /// belong to a different message, which is what this exists to avoid.</para>
    /// </summary>
    private static string ExtractRetryWarning(string workerSource, string budgetVariable)
        => Slice(
            ExtractRetrySite(workerSource, budgetVariable),
            "_logger.LogWarning(",
            "await Task.Delay(");

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
