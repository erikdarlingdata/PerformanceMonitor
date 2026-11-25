/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The time one AWS-API ingest spends in the monitoring store (#9523): the saved-position restore, the watermark
/// read, the COPY and the position save. Each of those calls runs through <see cref="TimeAsync(Func{Task})"/>, and
/// the sum goes out on <see cref="RdsIngestOutcome.StoreMs"/>.
///
/// <para>The store calls are the ones timed, and the AWS calls are not, because the runner books the rest of the
/// run as the fetch: <c>storage_duration_ms</c> is this sum and <c>sql_duration_ms</c> is the run less this sum, so
/// the two add up to the run with no term measured twice. One instance serves one ingest call. The calls it times
/// run one after another, so it needs no lock.</para>
/// </summary>
internal sealed class RdsStorePhase
{
    private readonly Func<long> _timestamp;

    private long _ticks;

    /// <param name="timestamp">The tick source, in <see cref="Stopwatch.Frequency"/> ticks a second. Null is
    /// <see cref="Stopwatch.GetTimestamp"/>. A test passes its own so a duration is a stated number.</param>
    public RdsStorePhase(Func<long>? timestamp = null)
    {
        _timestamp = timestamp ?? Stopwatch.GetTimestamp;
    }

    /// <summary>Milliseconds spent inside the timed store calls so far.</summary>
    public long ElapsedMs => (long)(_ticks * 1000.0 / Stopwatch.Frequency);

    /// <summary>Runs one store call and adds its duration, whether it returns or throws.</summary>
    public async Task TimeAsync(Func<Task> storeCall)
    {
        ArgumentNullException.ThrowIfNull(storeCall);

        var started = _timestamp();

        try
        {
            await storeCall();
        }
        finally
        {
            _ticks += _timestamp() - started;
        }
    }

    /// <summary>Runs one store call that returns a value and adds its duration, whether it returns or throws.</summary>
    public async Task<T> TimeAsync<T>(Func<Task<T>> storeCall)
    {
        ArgumentNullException.ThrowIfNull(storeCall);

        var started = _timestamp();

        try
        {
            return await storeCall();
        }
        finally
        {
            _ticks += _timestamp() - started;
        }
    }
}
