/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Database;

/// <summary>Whether Lite can use its own DuckDB file, as far as a fatal error is concerned.</summary>
public enum LocalDatabaseState
{
    /// <summary>No fatal error, or the last one was followed by a reopen that worked.</summary>
    Healthy,

    /// <summary>A fatal error invalidated the database and Lite is reopening it. Nothing can be stored meanwhile.</summary>
    Reopening,

    /// <summary>Lite stopped reopening the database. Nothing can be stored until Lite is restarted.</summary>
    Failed,
}

/// <summary>
/// The local database's state and the times that go with it, plus the line the status bar and Collection Health
/// show for it.
/// </summary>
public readonly record struct LocalDatabaseHealth(LocalDatabaseState State, DateTime? FailedAtUtc, DateTime? ReopenedAtUtc)
{
    /// <summary>True while collection and alerting cannot store or read anything.</summary>
    public bool CollectionStopped => State != LocalDatabaseState.Healthy;

    /// <summary>One line for the status bar and Collection Health, or null when there is nothing to report.</summary>
    public string? StatusLine => State switch
    {
        LocalDatabaseState.Reopening =>
            $"Lite's local database failed at {LocalTime(FailedAtUtc)}. Collection and alerts are stopped while Lite reopens it.",
        LocalDatabaseState.Failed =>
            $"Lite's local database failed at {LocalTime(FailedAtUtc)} and Lite stopped reopening it. Collection and alerts are stopped until Lite is restarted.",
        _ when ReopenedAtUtc is not null =>
            $"Lite reopened its local database at {LocalTime(ReopenedAtUtc)} after a fatal error.",
        _ => null,
    };

    internal static string LocalTime(DateTime? utc)
    {
        return utc is { } value
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc).ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)
            : "an unknown time";
    }
}

public partial class DuckDbInitializer
{
    /// <summary>
    /// How many times Lite tries to reopen its database after one fatal error. When every attempt fails, Lite stops
    /// trying and says so, rather than reopening a broken file over and over.
    /// </summary>
    internal const int MaxReopenAttempts = 3;

    /// <summary>
    /// How many fatal errors one run of Lite reopens its database after. A database that reopens and then hits a
    /// fatal error again, time after time, will not be fixed by another reopen: after this many, the next fatal error
    /// leaves it failed until Lite is restarted.
    /// </summary>
    internal const int MaxReopenCycles = 5;

    /// <summary>
    /// The wait before each reopen attempt. The first wait lets the callers that are failing leave the lock; the
    /// longer ones give a file that something else holds time to come free. Not const, so a test can shorten them.
    /// </summary>
    internal TimeSpan[] ReopenDelays { get; set; } = [TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(60)];

    /// <summary>How often a reopen attempt checks again while a collection is still running.</summary>
    internal TimeSpan ReopenGateRetryDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// How long a reopen attempt waits for running collections before it logs a Warning, and again after each further
    /// wait this long. The collection gate's drain timeout: an archive reset gives up after that long, but a reopen
    /// keeps waiting. Not const, so a test can shorten it.
    /// </summary>
    internal TimeSpan ReopenGateWarningInterval { get; set; } = CollectionResetGate.DrainTimeout;

    /* Test seam: runs inside each reopen attempt, after it holds the collection gate and before it opens the file. */
    internal Action? BeforeReopenAttemptForTests { get; set; }

    private readonly object _healthGate = new();
    private LocalDatabaseHealth _health = new(LocalDatabaseState.Healthy, null, null);
    private Task? _reopen;
    private int _reopenAttempts;
    private int _reopenCycles;
    private volatile bool _disposed;

    /// <summary>The local database's state, read by the status bar and Collection Health.</summary>
    public LocalDatabaseHealth LocalDatabaseHealth
    {
        get
        {
            lock (_healthGate)
            {
                return _health;
            }
        }
    }

    /// <summary>The reopen that a fatal error started, or null when none has started yet. A test awaits it.</summary>
    internal Task? ReopenForTests
    {
        get
        {
            lock (_healthGate)
            {
                return _reopen;
            }
        }
    }

    /// <summary>The reopen attempts the last fatal error started.</summary>
    internal int ReopenAttemptsForTests => Volatile.Read(ref _reopenAttempts);

    /// <summary>Whether the sentinel connection is open.</summary>
    internal bool SentinelOpenForTests => Volatile.Read(ref _sentinel) is not null;

    /// <summary>Whether this initializer has been disposed. Set under the health gate, so a reopen sees it.</summary>
    private void MarkDisposed()
    {
        lock (_healthGate)
        {
            _disposed = true;
        }
    }

    /// <summary>
    /// True when the exception, or one it wraps, is DuckDB's FATAL error. A fatal error invalidates the whole
    /// database instance: every later statement on every connection to the file fails with "database has been
    /// invalidated" until all of them are closed and the file is opened again. DuckDB.NET reports it as
    /// <see cref="DuckDBErrorType.Fatal"/>, so the type is checked and the message is not.
    /// </summary>
    internal static bool IsDatabaseInvalidated(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is DuckDBException { ErrorType: DuckDBErrorType.Fatal })
            {
                return true;
            }

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (IsDatabaseInvalidated(inner))
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// Called where a database operation failed. Any error other than DuckDB's FATAL error is ignored. A fatal error
    /// marks the database as reopening and starts one reopen in the background, unless a reopen is already running
    /// or the database has already failed. After <see cref="MaxReopenCycles"/> reopens in this run of Lite, a fatal
    /// error marks it failed instead.
    ///
    /// <para>Takes no database lock, so a caller that holds the read lock may call it: the reopen runs on its own
    /// thread and waits for every caller to let go of the database (see <see cref="ReopenAfterFatalErrorAsync"/>).</para>
    /// </summary>
    public void ReportFailure(Exception exception)
    {
        if (!IsDatabaseInvalidated(exception))
        {
            return;
        }

        bool reopening;

        lock (_healthGate)
        {
            if (_disposed || _health.State != LocalDatabaseState.Healthy)
            {
                return;
            }

            reopening = _reopenCycles < MaxReopenCycles;

            if (reopening)
            {
                _reopenCycles++;
                _health = _health with { State = LocalDatabaseState.Reopening, FailedAtUtc = DateTime.UtcNow };
                _reopenAttempts = 0;
                _reopen = Task.Run(ReopenAfterFatalErrorAsync);
            }
            else
            {
                _health = _health with { State = LocalDatabaseState.Failed, FailedAtUtc = DateTime.UtcNow };
            }
        }

        if (reopening)
        {
            _logger?.LogError(exception,
                "Lite's local database hit a fatal error and cannot be used until it is reopened. Collection and alerting are stopped");
        }
        else
        {
            _logger?.LogError(exception,
                "Lite's local database hit a fatal error again after {Count} reopens in this run. Lite stops reopening it, "
                + "so collection and alerting stay stopped until Lite is restarted",
                MaxReopenCycles);
        }
    }

    /// <summary>
    /// Closes every connection to the database file and opens it again, with the same steps as a start. Never the
    /// reset path: the file and its rows stay as they are.
    ///
    /// <para><b>What makes an attempt work.</b> No connection to the invalidated database may be open when the
    /// attempt opens the file: DuckDB.NET would hand the attempt that same invalidated instance, and its first
    /// statement would fail. Three rules give that guarantee. The write lock alone does not, because collections take
    /// no lock.</para>
    ///
    /// <para>1. Collections hold their connections across their reads from the monitored server, so they take no
    /// database lock. Instead, each one registers with <see cref="CollectionResetGate"/>: the scheduled sweep, the
    /// sweep when a server tab opens, and the tab's refresh. An attempt takes that gate first. The gate waits until
    /// every registered collection has finished, which is the point right after a sweep's last collector, and it keeps
    /// a new one from starting until the attempt ends. The size-triggered reset takes the same gate before it deletes
    /// the file.</para>
    ///
    /// <para>2. Every other connection to the file is opened under <see cref="s_dbLock"/>, the Query Store backfill's
    /// included. The attempt runs <see cref="InitializeAsync"/>, whose write lock waits for those connections to
    /// close and keeps new ones out until the open, the index rebuild included, is done.</para>
    ///
    /// <para>3. While the database is not healthy, collections, the backfill and the collection loop's housekeeping
    /// skip (<c>RemoteCollectorService.LocalDatabaseIsDown</c>). So between attempts nothing new opens a connection to
    /// the invalidated database. A collection that was already running ends at its first failed write, and a
    /// per-database loop ends at once instead of reading every remaining database.</para>
    ///
    /// <para>With no collection running (no servers, every server offline, collection paused), the gate is free at
    /// once and the attempt runs right after its wait. A wait for the gate is not an attempt and does not count
    /// toward <see cref="MaxReopenAttempts"/>. An attempt that fails anyway counts. After
    /// <see cref="MaxReopenAttempts"/> failed attempts the state is <see cref="LocalDatabaseState.Failed"/>, and
    /// nothing starts another reopen.</para>
    /// </summary>
    private async Task ReopenAfterFatalErrorAsync()
    {
        for (var attempt = 1; attempt <= MaxReopenAttempts; attempt++)
        {
            /* The wait is outside the gate and the lock: neither is held while nothing is being done with it. */
            await Task.Delay(ReopenDelays[Math.Min(attempt, ReopenDelays.Length) - 1]);

            using var collectionsStopped = await WaitForCollectionsToFinishAsync();

            if (collectionsStopped is null)
            {
                return;
            }

            Interlocked.Increment(ref _reopenAttempts);

            try
            {
                BeforeReopenAttemptForTests?.Invoke();
                await InitializeAsync();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Reopen attempt {Attempt} of {Max} for Lite's local database failed", attempt, MaxReopenAttempts);
                continue;
            }

            if (!MarkReopened())
            {
                /* Disposed while the attempt ran: Dispose found no sentinel to close, because the open had released
                   it, so the one the open just made is closed here, under the write lock Dispose would have taken. */
                using (AcquireWriteLock())
                {
                    ReleaseSentinelWithoutLock();
                }

                return;
            }

            _logger?.LogInformation(
                "Reopened Lite's local database after a fatal error (attempt {Attempt} of {Max}). Collection and alerting carry on",
                attempt, MaxReopenAttempts);
            return;
        }

        lock (_healthGate)
        {
            _health = _health with { State = LocalDatabaseState.Failed };
        }

        _logger?.LogError(
            "Lite could not reopen its local database after {Max} attempts. Collection and alerting stay stopped until Lite is restarted",
            MaxReopenAttempts);
    }

    /// <summary>
    /// Waits until no registered collection is running and returns the collection gate, held, so none starts until
    /// the caller disposes it. Returns null once this initializer is disposed. Checks the count before taking the
    /// gate, so it can notice the dispose while a collection runs, rather than wait out the gate's own drain
    /// timeout. Logs an Information line when it has to wait. The wait has no time limit, since an attempt while a
    /// collection holds its connection would fail. So a collection that does not end (a remote read with a long
    /// command timeout) keeps the database down, and a Warning with the count of running collections says so after
    /// each <see cref="ReopenGateWarningInterval"/>.
    /// </summary>
    private async Task<IDisposable?> WaitForCollectionsToFinishAsync()
    {
        var waitLogged = false;
        var waited = Stopwatch.StartNew();
        var nextWarning = ReopenGateWarningInterval;

        while (!_disposed)
        {
            if (CollectionResetGate.CollectionsInFlight == 0 && await CollectionResetGate.TryBeginResetAsync() is { } gate)
            {
                return gate;
            }

            if (!waitLogged)
            {
                waitLogged = true;
                _logger?.LogInformation(
                    "Reopening Lite's local database waits for {Count} running collections to finish",
                    CollectionResetGate.CollectionsInFlight);
            }

            if (waited.Elapsed >= nextWarning)
            {
                nextWarning = waited.Elapsed + ReopenGateWarningInterval;
                _logger?.LogWarning(
                    "Reopening Lite's local database is still waiting after {Minutes} minutes. Collections still running: {Count}. "
                    + "Collection and alerts stay stopped until they finish",
                    (int)waited.Elapsed.TotalMinutes, CollectionResetGate.CollectionsInFlight);
            }

            await Task.Delay(ReopenGateRetryDelay);
        }

        return null;
    }

    /// <summary>
    /// Marks the database healthy after a reopen, unless this initializer was disposed meanwhile. Checked under the
    /// same gate <see cref="Dispose"/> sets the flag under, so either the dispose comes first and this returns false,
    /// or it comes after and closes the new sentinel itself.
    /// </summary>
    private bool MarkReopened()
    {
        lock (_healthGate)
        {
            if (_disposed)
            {
                return false;
            }

            _health = _health with { State = LocalDatabaseState.Healthy, ReopenedAtUtc = DateTime.UtcNow };
            return true;
        }
    }
}
