/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Writes this service start's row, and a row for each migration rung it tried, to the store's start history (#9415).
///
/// <para><b>Held in memory until the table exists.</b> The migration that creates the history tables is itself a rung, so the rungs the runner
/// reports before it cannot be written when they finish. This hears them as an <see cref="IMigrationRungObserver"/>, keeps them, and
/// <see cref="FlushAsync"/> writes what it holds as soon as the tables exist. A flush that finds no tables (a store still below V307) keeps
/// everything and tries again, quietly. A rung that fails is reported by the runner too, so a start that dies in a rung still shows it.</para>
///
/// <para><b>The counts</b> come from <see cref="ServiceLogTally"/>. <see cref="RunAsync"/> flushes them once a minute and writes the two fixed
/// snapshots (<see cref="FirstSnapshotAfter"/> and <see cref="SecondSnapshotAfter"/> after the process start) the first time a flush finds each one
/// due, so a snapshot is taken within one flush interval after its time. A final flush runs when the service stops, which is what a later start
/// reads as the time this one stopped: the service writes nothing else on a clean stop.</para>
///
/// <para>Nothing here throws except for a cancellation, and nothing here fails or delays the start.</para>
/// </summary>
internal sealed class ServiceStartRecorder : IMigrationRungObserver, IDisposable
{
    /// <summary>How often the counts are written.</summary>
    internal static readonly TimeSpan FlushInterval = TimeSpan.FromMinutes(1);

    /// <summary>The first fixed snapshot of the counts.</summary>
    internal static readonly TimeSpan FirstSnapshotAfter = TimeSpan.FromMinutes(10);

    /// <summary>The second fixed snapshot of the counts.</summary>
    internal static readonly TimeSpan SecondSnapshotAfter = TimeSpan.FromMinutes(30);

    /// <summary>The longest the final flush at shutdown may take.</summary>
    private static readonly TimeSpan FinalFlushBudget = TimeSpan.FromSeconds(5);

    private readonly object _gate = new();
    private readonly SemaphoreSlim _flushGate = new(1, 1);
    private readonly List<ServiceStartRung> _pending = new();
    private readonly ServiceLogTally _tally;
    private readonly Func<DateTime> _clock;
    private readonly string _serviceHost;
    private readonly string _productVersion;
    private readonly string? _commitId;
    private readonly DateTime _processStartedUtc;
    private int _attempt;
    private int? _schemaBefore;
    private int? _schemaAfter;
    private DateTime? _storeReadyUtc;
    private (long Errors, long Warns)? _snapshot10;
    private (long Errors, long Warns)? _snapshot30;
    private bool _flushFailed;

    internal ServiceStartRecorder(
        ServiceLogTally tally, Guid startId, string serviceHost, string productVersion, string? commitId,
        DateTime processStartedUtc, Func<DateTime>? clock = null)
    {
        _tally = tally;
        StartId = startId;
        _serviceHost = serviceHost;
        _productVersion = productVersion;
        _commitId = commitId;
        _processStartedUtc = processStartedUtc;
        _clock = clock ?? (() => DateTime.UtcNow);
    }

    /// <summary>Releases the flush gate. Call after the flush loop has ended.</summary>
    public void Dispose() => _flushGate.Dispose();

    /// <summary>This start's id.</summary>
    internal Guid StartId { get; }

    /// <summary>The recorder for this process: this machine, this build, a new start id.</summary>
    internal static ServiceStartRecorder ForThisProcess(ServiceLogTally tally, DateTime processStartedUtc)
    {
        var (version, commit) = SplitBuild(
            typeof(DarlingCliCommands).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
            DarlingCliCommands.ProductVersion());
        return new ServiceStartRecorder(tally, Guid.NewGuid(), Environment.MachineName, version, commit, processStartedUtc);
    }

    /// <summary>
    /// The full build string and the commit from an informational version (<c>3.14.0-nightly.20261124.730+0993a1441...</c>): everything before the
    /// <c>+</c> is the build, everything after it is the commit. A version with no <c>+</c> has no commit.
    /// </summary>
    internal static (string Version, string? Commit) SplitBuild(string? informationalVersion, string fallbackVersion)
    {
        if (string.IsNullOrWhiteSpace(informationalVersion))
        {
            return (fallbackVersion, null);
        }

        var raw = informationalVersion.Trim();
        var plus = raw.IndexOf('+', StringComparison.Ordinal);
        if (plus < 0)
        {
            return (raw, null);
        }

        var version = raw[..plus].Trim();
        var commit = raw[(plus + 1)..].Trim();
        return (version.Length == 0 ? fallbackVersion : version, commit.Length == 0 ? null : commit);
    }

    /// <inheritdoc />
    public void LadderStarted(int storeVersion)
    {
        lock (_gate)
        {
            _attempt++;
            _schemaBefore ??= storeVersion;
        }
    }

    /// <inheritdoc />
    public void RungFinished(int version, string name, DateTime startedUtc, long elapsedMilliseconds, string outcome, string? errorText)
    {
        var text = errorText is null
            ? null
            : SecretTextGuard.RedactText(errorText.Replace('\r', ' ').Replace('\n', ' '));
        if (text is { Length: > ServiceStartHistory.LineLength })
        {
            text = text[..ServiceStartHistory.LineLength];
        }

        lock (_gate)
        {
            _pending.Add(new ServiceStartRung(StartId, version, name, Math.Max(_attempt, 1), startedUtc, elapsedMilliseconds, outcome, text));
        }
    }

    /// <summary>
    /// The migrations are done and the store is ready. A start that took the lock-timeout shortcut (the store already at this build's version, so no
    /// ladder ran) has no "before" yet: the store was at this build's top rung.
    /// </summary>
    internal void StoreReady()
    {
        lock (_gate)
        {
            _schemaBefore ??= StorageVersion.SchemaVersion;
            _schemaAfter = Math.Max(StorageVersion.SchemaVersion, _schemaBefore.Value);
            _storeReadyUtc = _clock();
        }
    }

    /// <summary>
    /// Writes the start row and the rungs held so far. Returns true when the row was written. A store without the tables is not a failure: false,
    /// and everything is kept for the next flush. Any other failure logs one warning for the run and debug lines after, and keeps everything.
    /// </summary>
    internal async Task<bool> FlushAsync(NpgsqlDataSource postgres, ILogger logger, CancellationToken cancellationToken)
    {
        await _flushGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var now = _clock();
            var counts = _tally.Snapshot();
            ServiceStartRow row;
            List<ServiceStartRung> rungs;
            lock (_gate)
            {
                var elapsed = now - _processStartedUtc;
                _snapshot10 ??= elapsed >= FirstSnapshotAfter ? (counts.Errors, counts.Warns) : null;
                _snapshot30 ??= elapsed >= SecondSnapshotAfter ? (counts.Errors, counts.Warns) : null;
                row = new ServiceStartRow(
                    StartId, _serviceHost, _productVersion, _commitId, _processStartedUtc, _schemaBefore, _schemaAfter, _storeReadyUtc,
                    PreviousStopUtc: null, now, counts.Errors, counts.Warns, counts.FirstErrors, counts.FirstWarns,
                    _snapshot10?.Errors, _snapshot10?.Warns, _snapshot30?.Errors, _snapshot30?.Warns);
                rungs = new List<ServiceStartRung>(_pending);
            }

            try
            {
                await ServiceStartHistory.UpsertStartAsync(postgres, row, cancellationToken).ConfigureAwait(false);
                if (rungs.Count > 0)
                {
                    await ServiceStartHistory.InsertRungsAsync(postgres, rungs, cancellationToken).ConfigureAwait(false);
                    lock (_gate)
                    {
                        _pending.RemoveRange(0, rungs.Count);
                    }
                }

                _flushFailed = false;
                return true;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                /* A store below V307: the table is made by a rung that has not run yet. Everything is kept. */
                return false;
            }
#pragma warning disable CA1031 // A failed flush keeps what it holds and tries again: the start history never fails or delays the service.
            catch (Exception ex) when (ex is not OperationCanceledException)
#pragma warning restore CA1031
            {
                if (_flushFailed)
                {
                    logger.LogDebug(ex, "The service start history still could not be written");
                }
                else
                {
                    logger.LogWarning(
                        "Could not write this start to the service start history ({Reason}). The service keeps what it holds and tries again every minute.",
                        ex.GetType().Name);
                    _flushFailed = true;
                }

                return false;
            }
        }
        finally
        {
            _flushGate.Release();
        }
    }

    /// <summary>Starts <see cref="RunAsync"/> and returns the handle that owns it: stopping or disposing the handle ends the loop, runs its last flush and waits for it.</summary>
    internal FlushLoop StartFlushLoop(NpgsqlDataSource postgres, ILogger logger, CancellationToken stoppingToken, TimeSpan? interval = null) =>
        new(this, postgres, logger, interval, stoppingToken);

    /// <summary>
    /// A running flush loop and the means to end it. Declare it with <c>await using</c> AFTER the recorder and the data source the loop uses: every way out
    /// of the scope then ends the loop and awaits its last flush before those two are disposed, so the loop never touches a released gate or data source
    /// and its task never faults unobserved.
    /// </summary>
    internal sealed class FlushLoop : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop;
        private int _disposed;

        internal FlushLoop(ServiceStartRecorder recorder, NpgsqlDataSource postgres, ILogger logger, TimeSpan? interval, CancellationToken stoppingToken)
        {
            _stop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            Completion = recorder.RunAsync(postgres, logger, _stop.Token, interval);
        }

        /// <summary>The loop's task. It ends after the last flush and never throws.</summary>
        internal Task Completion { get; }

        /// <summary>Ends the loop and waits for its last flush (bounded at <see cref="FinalFlushBudget"/>).</summary>
        internal async Task StopAsync()
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            await Completion.ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            await StopAsync().ConfigureAwait(false);
            _stop.Dispose();
        }
    }

    /// <summary>
    /// The flush loop: every <paramref name="interval"/> until <paramref name="stoppingToken"/> is cancelled, then one last flush bounded by
    /// <see cref="FinalFlushBudget"/>. Never throws.
    /// </summary>
    internal async Task RunAsync(NpgsqlDataSource postgres, ILogger logger, CancellationToken stoppingToken, TimeSpan? interval = null)
    {
        var wait = interval ?? FlushInterval;
        try
        {
            while (true)
            {
                await Task.Delay(wait, stoppingToken).ConfigureAwait(false);
                await FlushAsync(postgres, logger, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown: the last flush says when this run was last known to be alive. */
        }

        using var budget = new CancellationTokenSource(FinalFlushBudget);
        try
        {
            await FlushAsync(postgres, logger, budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            /* The budget ran out: the last minute's flush stands. */
        }
    }
}
