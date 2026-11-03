/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Bounds how long a failing restart-seed read can hold an alert family back (#7295 round 3).
///
/// <para>A seed step (the saved count watermark, the failed-job watermark, a persistence record) is only done once its load
/// worked, a row or no row. A load that cannot read the store leaves the step open and the check that depends on it skips
/// the sweep, so the next sweep can read again: without that, a store that was briefly unreadable looks like an empty one, the
/// watermark starts at 0, and a page that already went out is sent again. The cost of that rule is that a store which STAYS
/// unreadable while the collected data is readable (a grant revoked on the state table, a Lite table a bad upgrade left missing)
/// would hold the check back for as long as the failure lasts, and the only sign would be a count in <c>alert_read_health</c>.
/// A gate that cannot read its counts must not silence the alert, so this caps the wait.</para>
///
/// <para>The cap is <see cref="MaxFailedLoads"/> failed loads in a row for one server key and step. The failed load that reaches
/// it seeds the step as if no row had been found, which is what every sweep did before #7295: at worst one duplicate page.
/// Once seeded the step is done for the life of the process, the same as after a load that worked, so a later working store never
/// undoes a page that went out in between. A success also ends the step, so the failures counted here are always consecutive and
/// need no reset.</para>
///
/// <para>One cap per host (the engine and the worker each hold one). It holds a count per key and step and nothing else:
/// no clock, so the same code answers in the engine's tests (which drive sweeps, not time) and in a live worker.</para>
///
/// <para>Counting in <c>alert_read_health</c> stays as it was. A failed load that is rethrown is counted by the caller's own
/// catch, and the engine counts every failed seed read in <c>SeedStepAsync</c>. The one failure the cap swallows is counted
/// here, when the caller hands in the read name, so the worker's count does not lose the failure that reached the cap.</para>
/// </summary>
public sealed class SeedLoadCap
{
    private readonly AlertReadFailureCounter? _readFailures;

    /// <param name="readFailures">Where the swallowed failure is counted when a caller passes a read name; null counts nothing.</param>
    public SeedLoadCap(AlertReadFailureCounter? readFailures = null)
    {
        _readFailures = readFailures;
    }

    /// <summary>
    /// Failed loads in a row, for one key and step, after which the step seeds empty. Three: a single failed read is an ordinary
    /// blip (a dropped connection, a statement cancelled for a lock), and the retry that follows it is the reason for the rule, so
    /// one failure alone never seeds empty. A fault that is specific to the state table fails every sweep, so three in a row
    /// tell it from a blip. The sweep is 30 seconds in Darling, so the longest an alert family waits behind a standing fault is the
    /// two sweeps before the third (about a minute), against hours without the cap. A longer wait buys fewer duplicate pages
    /// after a long outage and costs a longer silence; a duplicate page is the safe side, and silence is not.
    /// </summary>
    public const int MaxFailedLoads = 3;

    private readonly ConcurrentDictionary<(string Key, string Step), int> _failedLoads = new();

    /// <summary>
    /// Counts one failed load for <paramref name="key"/> and <paramref name="step"/>. Returns true when it is the
    /// <see cref="MaxFailedLoads"/>th in a row: the caller then marks the step done and carries on with an empty seed. Logs one
    /// warning when it returns true, which can only happen once for a key and step because the step is done after that.
    /// </summary>
    /// <param name="logger">Where the warning goes; null logs nothing.</param>
    /// <param name="store">The state store the read went to, named in the warning.</param>
    /// <param name="key">The server key.</param>
    /// <param name="step">The seed step, named in the warning.</param>
    /// <param name="error">The exception of the load that just failed.</param>
    /// <param name="readName">The name this failure is counted under in <c>alert_read_health</c> when it is the one that reaches the cap.
    /// Null counts nothing, for a caller that counts every failure itself.</param>
    /// <param name="elapsedMilliseconds">How long the failed load ran, for that count.</param>
    public bool RecordFailedLoad(
        ILogger? logger, string store, string key, string step, Exception error, string? readName = null, long elapsedMilliseconds = 0)
    {
        var failed = _failedLoads.AddOrUpdate((key, step), 1, static (_, count) => count + 1);
        if (failed < MaxFailedLoads)
        {
            return false;
        }

        if (failed > MaxFailedLoads)
        {
            return true;
        }

        logger?.LogWarning(
            "The {Step} for {ServerKey} could not be read from {Store} on {Failed} sweeps in a row, so it is seeded empty now and the alert it holds back resumes. " +
            "A page that went out before the restart may be sent once more. Last error: {Message}",
            step, key, store, failed, error.Message);
        if (readName is not null)
        {
            _readFailures?.RecordReadFailure(key, readName, elapsedMilliseconds);
        }

        return true;
    }

    /// <summary>
    /// Runs a seed load for a caller that lets a failure propagate to its own catch (the worker's PostgreSQL passes). A load that
    /// worked answers its value. A load that failed rethrows, so the caller's catch counts it and the pass ends, until the failure
    /// is the <see cref="MaxFailedLoads"/>th in a row: that one is swallowed after it is counted under <paramref name="readName"/>
    /// (the rethrow would have been), and the answer is <c>default</c>, the same as a store with no row, so <typeparamref name="T"/> is a
    /// nullable value type (the seeds answer <c>int?</c> or a nullable record). Cancellation always propagates.
    /// </summary>
    /// <param name="load">The load.</param>
    /// <param name="logger">Where the warning goes.</param>
    /// <param name="store">The state store the read goes to, named in the warning.</param>
    /// <param name="key">The server key.</param>
    /// <param name="step">The seed step.</param>
    /// <param name="readName">The name the swallowed failure is counted under in <c>alert_read_health</c>.</param>
    public async Task<T> LoadOrSeedEmptyAsync<T>(
        Func<Task<T>> load, ILogger? logger, string store, string key, string step, string readName)
    {
        var clock = Stopwatch.StartNew();
        try
        {
            return await load();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (!RecordFailedLoad(logger, store, key, step, ex, readName, clock.ElapsedMilliseconds))
            {
                throw;
            }

            return default!;
        }
    }
}
