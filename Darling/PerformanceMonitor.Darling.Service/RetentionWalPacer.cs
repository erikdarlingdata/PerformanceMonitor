/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Holds the daily retention purge's WAL rate to half of what the store's own checkpoint schedule absorbs
/// (#4823). One instance per purge run.
///
/// <para>The cause it answers, as measured on an install monitoring 43 servers: the purge ran 13:04:44 to
/// 13:11:56 UTC and deleted 743,502 rows, 386,718 of them from <c>query_plan_dim</c>. The checkpoint interval
/// that covered it carried 6.89 GB of WAL against 0.6-1.5 GB normally, and the checkpoint that was running
/// ended at 13:16:31 with a 23.5 s sync, one file taking 11.8 s. Collector runs fell from about 850 a minute
/// to between 76 and 677 from 13:13 to 13:17, and every purge since (11 runs in 6 days) showed the same dip.
/// A checkpoint paces its writes against elapsed time AND the WAL written since it started, so the
/// remedy is at the source: the WAL the purge writes, not a server setting.</para>
///
/// <para>A token bucket counted in bytes. The refill rate is
/// <c>max_wal_size / (1 + checkpoint_completion_target) / checkpoint_timeout / 2</c> from the store's own
/// <c>pg_settings</c>, read once when the run starts and clamped to [<see cref="MinRateBytesPerSecond"/>,
/// <see cref="MaxRateBytesPerSecond"/>]. The bucket holds <see cref="BurstSeconds"/> of that rate and starts
/// full, so a small steady-state purge never waits. After each batch the caller reports the WAL the batch
/// wrote (<see cref="AfterBatchAsync"/>); a batch that overdraws the bucket waits for the refill, in waits of
/// at most <see cref="MaxWaitSeconds"/>. The waits themselves are not measured, so the purge always
/// progresses.</para>
///
/// <para>Time and sleeping are injected, so no test needs a real sleep.</para>
/// </summary>
internal sealed class RetentionWalPacer
{
    /// <summary>The slowest rate the pacer will use, whatever the settings say.</summary>
    internal const long MinRateBytesPerSecond = 1_048_576;

    /// <summary>The fastest rate the pacer will use, whatever the settings say.</summary>
    internal const long MaxRateBytesPerSecond = 67_108_864;

    /// <summary>The rate used when the settings cannot be read or cannot be used.</summary>
    internal const long FallbackRateBytesPerSecond = 4_194_304;

    /// <summary>How many seconds of the rate the bucket holds, and starts with.</summary>
    internal const double BurstSeconds = 10;

    /// <summary>The longest a single wait may be; a larger debt is repaid in several waits.</summary>
    internal const double MaxWaitSeconds = 30;

    /// <summary>
    /// How many seconds of the rate one batch may write before the plan dimension's next batch cap shrinks
    /// (<see cref="DarlingRetention.NextPlanDimBatchCap"/>).
    /// </summary>
    internal const double BatchTargetSeconds = 30;

    private const long BytesPerMegabyte = 1_048_576;
    private const int ReadTimeoutSeconds = 30;

    private const string SettingsSql =
        "SELECT name, setting, unit FROM pg_settings "
        + "WHERE name IN ('max_wal_size', 'checkpoint_completion_target', 'checkpoint_timeout')";

    /* pg_lsn minus pg_lsn is numeric; an LSN of 64 bits fits a bigint until the log reaches 8 EiB. */
    private const string WalPositionSql = "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint";

    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<double> _secondsClock;
    private readonly ILogger? _logger;
    private double _tokens;
    private double _lastSeconds;
    private bool _unpaced;

    /// <param name="rateBytesPerSecond">The refill rate. Not clamped here: <see cref="RateFromSettings"/> clamps.</param>
    /// <param name="delay">Waits for the given time; defaults to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.</param>
    /// <param name="secondsClock">Monotonic seconds; defaults to <see cref="Stopwatch"/>.</param>
    /// <param name="logger">Receives the one line logged if the WAL position cannot be read.</param>
    internal RetentionWalPacer(
        long rateBytesPerSecond,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<double>? secondsClock = null,
        ILogger? logger = null)
    {
        RateBytesPerSecond = Math.Max(1, rateBytesPerSecond);
        _delay = delay ?? ((wait, cancellationToken) => Task.Delay(wait, cancellationToken));
        _secondsClock = secondsClock ?? (() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency);
        _logger = logger;
        _tokens = BurstBytes;
        _lastSeconds = _secondsClock();
    }

    /// <summary>The refill rate in bytes per second.</summary>
    internal long RateBytesPerSecond { get; }

    /// <summary>What the bucket holds when full: <see cref="BurstSeconds"/> of the rate.</summary>
    internal long BurstBytes => (long)(RateBytesPerSecond * BurstSeconds);

    /// <summary>
    /// The WAL one batch may write before the plan dimension's cap shrinks: <see cref="BatchTargetSeconds"/> of
    /// the rate.
    /// </summary>
    internal long BatchWalTargetBytes => (long)(RateBytesPerSecond * BatchTargetSeconds);

    /// <summary>The WAL reported by every batch so far.</summary>
    internal long TotalWalBytes { get; private set; }

    /// <summary>The seconds spent waiting so far.</summary>
    internal double TotalWaitSeconds { get; private set; }

    /// <summary>True once a WAL position read failed: the rest of the run neither measures nor waits.</summary>
    internal bool IsUnpaced => _unpaced;

    /// <summary>
    /// The rate before clamping: <c>max_wal_size / (1 + checkpoint_completion_target) / checkpoint_timeout / 2</c>,
    /// with <c>max_wal_size</c> in MB (as <c>pg_settings</c> reports it) and the timeout in seconds. Zero when an
    /// input cannot give a rate.
    /// </summary>
    internal static long RawRateFromSettings(long maxWalSizeMb, double checkpointCompletionTarget, long checkpointTimeoutSeconds)
    {
        if (maxWalSizeMb <= 0 || checkpointTimeoutSeconds <= 0 || checkpointCompletionTarget < 0
            || double.IsNaN(checkpointCompletionTarget))
        {
            return 0;
        }

        return (long)(maxWalSizeMb * (double)BytesPerMegabyte / (1 + checkpointCompletionTarget) / checkpointTimeoutSeconds / 2);
    }

    /// <summary>The rate for these settings, clamped to [<see cref="MinRateBytesPerSecond"/>, <see cref="MaxRateBytesPerSecond"/>].</summary>
    internal static long RateFromSettings(long maxWalSizeMb, double checkpointCompletionTarget, long checkpointTimeoutSeconds) =>
        Math.Clamp(
            RawRateFromSettings(maxWalSizeMb, checkpointCompletionTarget, checkpointTimeoutSeconds),
            MinRateBytesPerSecond,
            MaxRateBytesPerSecond);

    /// <summary>
    /// Builds the pacer from the rows of <c>pg_settings</c> for <c>max_wal_size</c>, <c>checkpoint_completion_target</c>
    /// and <c>checkpoint_timeout</c>. A null result (the read failed), a missing row, an unexpected unit or an
    /// unusable number uses <see cref="FallbackRateBytesPerSecond"/> and logs that once at Information.
    /// </summary>
    internal static RetentionWalPacer FromSettingRows(
        IReadOnlyList<(string Name, string Setting, string? Unit)>? rows,
        ILogger? logger,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<double>? secondsClock = null,
        string? readFailure = null)
    {
        if (TryRateFromRows(rows, out var rate, out var reason))
        {
            return new RetentionWalPacer(rate, delay, secondsClock, logger);
        }

        logger?.LogInformation(
            "Retention purge WAL pacing: no rate could be taken from pg_settings ({Reason}); using the default {Rate} bytes/s",
            readFailure ?? reason, FallbackRateBytesPerSecond);
        return new RetentionWalPacer(FallbackRateBytesPerSecond, delay, secondsClock, logger);
    }

    /// <summary>
    /// Reads the three checkpoint settings once, through a pooled connection, and builds the pacer. A failed
    /// read is not an error: the run paces at <see cref="FallbackRateBytesPerSecond"/> and says so once. Only
    /// a cancellation propagates.
    /// </summary>
    internal static async Task<RetentionWalPacer> CreateAsync(
        NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(SettingsSql, connection) { CommandTimeout = ReadTimeoutSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var rows = new List<(string Name, string Setting, string? Unit)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add((reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }

            return FromSettingRows(rows, logger);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return FromSettingRows(null, logger, readFailure: "the read failed: " + ex.Message);
        }
    }

    /// <summary>
    /// The WAL position (bytes since the start of the log) on the batch's own connection, or null when this run
    /// is unpaced. A failed read logs once, marks the run unpaced, and returns null: the rest of the purge runs
    /// without measuring or waiting.
    /// </summary>
    internal async Task<long?> ReadWalPositionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        if (_unpaced)
        {
            return null;
        }

        try
        {
            await using var command = new NpgsqlCommand(WalPositionSql, connection) { CommandTimeout = ReadTimeoutSeconds };
            var value = await command.ExecuteScalarAsync(cancellationToken);
            if (value is long position)
            {
                return position;
            }

            throw new InvalidOperationException("the WAL position came back as " + (value?.ToString() ?? "NULL"));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _unpaced = true;
            _logger?.LogWarning(
                "Retention purge WAL pacing stopped: the WAL position could not be read ({Failure}); the rest of this purge runs unpaced",
                ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The WAL the store wrote since <paramref name="before"/> (a <see cref="ReadWalPositionAsync"/> result), read
    /// on the same connection. Zero when the run is unpaced or nothing was measured before. This counts the whole
    /// store's WAL across the batch, collection included, because that is what the checkpoint sees.
    /// </summary>
    internal async Task<long> WalWrittenSinceAsync(NpgsqlConnection connection, long? before, CancellationToken cancellationToken)
    {
        if (before is null)
        {
            return 0;
        }

        var after = await ReadWalPositionAsync(connection, cancellationToken);
        return after is null ? 0 : Math.Max(0, after.Value - before.Value);
    }

    /// <summary>
    /// Charges a finished batch's WAL to the bucket and, when that overdraws it, waits for the refill: each wait
    /// at most <see cref="MaxWaitSeconds"/>, as many as the debt needs. Returns at once for a batch inside the
    /// bucket, for no WAL, and for an unpaced run. Honors <paramref name="cancellationToken"/> between and
    /// during waits.
    /// </summary>
    internal async Task AfterBatchAsync(long walBytes, CancellationToken cancellationToken)
    {
        if (walBytes <= 0 || _unpaced)
        {
            return;
        }

        TotalWalBytes += walBytes;
        Refill();
        _tokens -= walBytes;

        /* Under one byte of debt is rounding, not debt. */
        while (_tokens < -1)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var waitSeconds = Math.Min(-_tokens / RateBytesPerSecond, MaxWaitSeconds);
            await _delay(TimeSpan.FromSeconds(waitSeconds), cancellationToken);
            TotalWaitSeconds += waitSeconds;

            /* Credit the wait itself rather than re-reading the clock, so the refill is exactly what the wait
               bought however the delay was implemented. */
            _tokens += waitSeconds * RateBytesPerSecond;
            _lastSeconds = _secondsClock();
        }
    }

    private void Refill()
    {
        var now = _secondsClock();
        var elapsed = now - _lastSeconds;
        _lastSeconds = now;
        if (elapsed > 0)
        {
            _tokens = Math.Min(BurstBytes, _tokens + elapsed * RateBytesPerSecond);
        }
    }

    private static bool TryRateFromRows(
        IReadOnlyList<(string Name, string Setting, string? Unit)>? rows, out long rate, out string reason)
    {
        rate = 0;
        if (rows is null)
        {
            reason = "the settings could not be read";
            return false;
        }

        string? maxWalSize = null, completionTarget = null, timeout = null;
        foreach (var (name, setting, unit) in rows)
        {
            switch (name)
            {
                case "max_wal_size" when unit == "MB":
                    maxWalSize = setting;
                    break;
                case "checkpoint_completion_target":
                    completionTarget = setting;
                    break;
                case "checkpoint_timeout" when unit == "s":
                    timeout = setting;
                    break;
            }
        }

        if (!long.TryParse(maxWalSize, NumberStyles.Integer, CultureInfo.InvariantCulture, out var maxWalSizeMb)
            || !double.TryParse(completionTarget, NumberStyles.Float, CultureInfo.InvariantCulture, out var target)
            || !long.TryParse(timeout, NumberStyles.Integer, CultureInfo.InvariantCulture, out var timeoutSeconds))
        {
            reason = "max_wal_size in MB, checkpoint_completion_target and checkpoint_timeout in seconds were not all present and numeric";
            return false;
        }

        if (RawRateFromSettings(maxWalSizeMb, target, timeoutSeconds) <= 0)
        {
            reason = "the settings give no positive rate";
            return false;
        }

        rate = RateFromSettings(maxWalSizeMb, target, timeoutSeconds);
        reason = string.Empty;
        return true;
    }
}
