/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    /// <summary>Every reopen attempt failed. Nothing can be stored until Lite is restarted.</summary>
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
            $"Lite's local database failed at {LocalTime(FailedAtUtc)} and could not be reopened. Collection and alerts are stopped until Lite is restarted.",
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
    /// How many times Lite tries to reopen its database after a fatal error. When every attempt fails, Lite stops
    /// trying and says so, rather than reopening a broken file over and over.
    /// </summary>
    internal const int MaxReopenAttempts = 3;

    /// <summary>
    /// The wait before each reopen attempt. The first wait lets the callers that are failing leave the lock; the
    /// longer ones give a file that something else holds time to come free. Not const, so a test can shorten them.
    /// </summary>
    internal TimeSpan[] ReopenDelays { get; set; } = [TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(60)];

    private readonly object _healthGate = new();
    private LocalDatabaseHealth _health = new(LocalDatabaseState.Healthy, null, null);
    private Task? _reopen;
    private int _reopenAttempts;
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
    /// starts one reopen in the background, unless a reopen is already running or every attempt has already failed.
    ///
    /// <para>Takes no database lock, so a caller that holds the read lock may call it: the reopen runs on its own
    /// thread and waits for the write lock until every caller has let go of the database.</para>
    /// </summary>
    public void ReportFailure(Exception exception)
    {
        if (!IsDatabaseInvalidated(exception))
        {
            return;
        }

        lock (_healthGate)
        {
            if (_disposed || _health.State != LocalDatabaseState.Healthy)
            {
                return;
            }

            _health = _health with { State = LocalDatabaseState.Reopening, FailedAtUtc = DateTime.UtcNow };
            _reopenAttempts = 0;
            _reopen = Task.Run(ReopenAfterFatalErrorAsync);
        }

        _logger?.LogError(exception,
            "Lite's local database hit a fatal error and cannot be used until it is reopened. Collection and alerting are stopped");
    }

    /// <summary>
    /// Closes every connection to the database file and opens it again, with the same steps as a start:
    /// <see cref="InitializeAsync"/> takes the write lock, which waits until no other caller holds a connection,
    /// releases the sentinel, the last connection that keeps the invalidated instance alive, and then runs the open
    /// steps and opens a new sentinel. Never the reset path: the file and its rows stay as they are.
    ///
    /// <para>A connection that is still open keeps the invalidated instance alive, and then the open steps fail on
    /// their first statement. That counts as a failed attempt. After <see cref="MaxReopenAttempts"/> failed
    /// attempts the state is <see cref="LocalDatabaseState.Failed"/>, and nothing starts another reopen.</para>
    /// </summary>
    private async Task ReopenAfterFatalErrorAsync()
    {
        for (var attempt = 1; attempt <= MaxReopenAttempts; attempt++)
        {
            /* The wait is outside the lock: the write lock is never held while nothing is being done with it. */
            await Task.Delay(ReopenDelays[Math.Min(attempt, ReopenDelays.Length) - 1]);

            if (_disposed)
            {
                return;
            }

            Interlocked.Increment(ref _reopenAttempts);

            try
            {
                await InitializeAsync();

                lock (_healthGate)
                {
                    _health = _health with { State = LocalDatabaseState.Healthy, ReopenedAtUtc = DateTime.UtcNow };
                }

                _logger?.LogInformation(
                    "Reopened Lite's local database after a fatal error (attempt {Attempt} of {Max}). Collection and alerting carry on",
                    attempt, MaxReopenAttempts);
                return;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Reopen attempt {Attempt} of {Max} for Lite's local database failed", attempt, MaxReopenAttempts);
            }
        }

        lock (_healthGate)
        {
            _health = _health with { State = LocalDatabaseState.Failed };
        }

        _logger?.LogError(
            "Lite could not reopen its local database after {Max} attempts. Collection and alerting stay stopped until Lite is restarted",
            MaxReopenAttempts);
    }
}
