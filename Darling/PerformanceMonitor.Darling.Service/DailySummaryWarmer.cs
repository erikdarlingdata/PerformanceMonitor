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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8370: builds the closed-day block of <see cref="DailySummaryRangeCache{TRow}"/> for every monitored SQL Server before
/// a visitor asks for it. The Overview's Daily Summary tile and its Daily Health Calendar both read
/// <c>get_daily_summary_range</c> over <see cref="DaysBack"/> days ending today, and the cache (#4232, #6809) serves the
/// closed days of that range from one block until the next UTC day boundary plus
/// <see cref="DailySummaryRangeCache{TRow}.ClosedGrace"/>. The block lives in the service process, so every service start
/// and every new UTC day otherwise put each server's first visitor on a cold read of about thirty compressed days of
/// <c>wait_stats</c>: 7 to 13 seconds on a large store, against under a second from the block.
///
/// <para><b>Schedule.</b> One pass <see cref="StartDelay"/> after the service starts, so the collectors' first connects
/// and the start-up repairs go first, then one pass at each boundary (<see cref="NextPassUtc"/>), when the blocks the
/// previous pass built stop being served. A pass with a failed server, or one the heavy-read gate refused, is tried
/// again after <see cref="RetryDelayFor"/>, never later than the next boundary. No pass runs while the service is
/// paused, while a materialization repair is rebuilding the rollups, or while neither the web host nor the MCP host is
/// on, because then nothing reads the block.</para>
///
/// <para><b>Load.</b> One server at a time, each read under the read-admission gate (<see cref="ReadAdmission"/>) as the
/// <c>7d+</c> web read it stands in for, so it takes one of <c>reads.maxHeavyReads</c> slots and queues behind a
/// visitor's read like any other. A refusal (the store is busy) ends the pass. A server already warm costs the open
/// day's read only, so a retry pass re-reads only what failed.</para>
/// </summary>
public sealed class DailySummaryWarmer
{
    /// <summary>The range the Overview reads: the tile and the calendar's current month both ask for 31 days ending today.</summary>
    public const int DaysBack = 31;

    /// <summary>The route the warmer admits under: the read it builds the block for.</summary>
    internal const string Route = "get_daily_summary_range";

    /// <summary>How long after the service starts the first pass runs.</summary>
    internal static readonly TimeSpan StartDelay = TimeSpan.FromMinutes(2);

    /// <summary>Added to a block's expiry so the pass lands after it, never a clock tick before.</summary>
    internal static readonly TimeSpan PassMargin = TimeSpan.FromMinutes(1);

    /// <summary>The wait after a failed or refused pass, doubled per consecutive failure up to <see cref="MaxRetryDelay"/>;
    /// also how often an idle warmer looks again while nothing reads the cache.</summary>
    internal static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(15);

    internal static readonly TimeSpan MaxRetryDelay = TimeSpan.FromHours(2);

    private readonly Func<IReadOnlyList<(int ServerId, string Name)>> _servers;
    private readonly Func<int, string, CancellationToken, Task> _warm;
    private readonly Func<bool> _enabled;
    private readonly ReadAdmission? _readAdmission;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _clock;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    /// <param name="servers">The servers to warm, read at each pass so a reconcile shows on the next one.</param>
    /// <param name="warm">Builds one server's block; <see cref="WarmAsync"/> in the service.</param>
    /// <param name="enabled">False while no pass should run (paused, repairing, or no host reads the cache).</param>
    /// <param name="readAdmission">The heavy-read gate, or null for a service without one.</param>
    /// <param name="clock">UTC now; tests pass one that moves.</param>
    /// <param name="delay">Waits the given span; tests pass one that records and advances the clock.</param>
    public DailySummaryWarmer(
        Func<IReadOnlyList<(int ServerId, string Name)>> servers,
        Func<int, string, CancellationToken, Task> warm,
        Func<bool> enabled,
        ReadAdmission? readAdmission,
        ILogger logger,
        Func<DateTime>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _servers = servers ?? throw new ArgumentNullException(nameof(servers));
        _warm = warm ?? throw new ArgumentNullException(nameof(warm));
        _enabled = enabled ?? throw new ArgumentNullException(nameof(enabled));
        _readAdmission = readAdmission;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _clock = clock ?? (static () => DateTime.UtcNow);
        _delay = delay ?? (static (span, ct) => Task.Delay(span, ct));
    }

    /// <summary>When the blocks built at <paramref name="nowUtc"/> stop being served, plus <see cref="PassMargin"/>: the
    /// next UTC day boundary plus the cache's closed grace, the same arithmetic as the cache's own block expiry.</summary>
    internal static DateTime NextPassUtc(DateTime nowUtc) =>
        nowUtc.Date.AddDays(1).Add(DailySummaryRangeCache<DarlingHealthReader.DailySummaryReadRow>.ClosedGrace).Add(PassMargin);

    /// <summary><see cref="RetryDelay"/> doubled per consecutive incomplete pass, capped at <see cref="MaxRetryDelay"/>.</summary>
    internal static TimeSpan RetryDelayFor(int consecutiveFailures)
    {
        var delay = RetryDelay;
        for (var i = 1; i < consecutiveFailures && delay < MaxRetryDelay; i++)
        {
            delay += delay;
        }

        return delay < MaxRetryDelay ? delay : MaxRetryDelay;
    }

    /// <summary>The service's warm read for one server: the Overview's own range (<see cref="DaysBack"/> days ending
    /// today, as of now) through <see cref="DarlingHealthReader.GetDailySummaryRangeAsync"/>, with the same scope the
    /// tool resolves, so the block it builds is the one the tool's first call looks up. The rows are discarded; the
    /// block is the result.</summary>
    public static async Task WarmAsync(
        NpgsqlDataSource postgres, int serverId, string name, MonitoredServerRegistryState? registryState, ILogger? logger, CancellationToken cancellationToken)
    {
        var today = DateTime.UtcNow.Date;
        var fromDate = today.AddDays(-(DaysBack - 1));
        var toDate = today.AddDays(1);
        var separatelyMonitored = await DarlingMcpHealthTools.ResolveSeparatelyMonitoredAsync(postgres, serverId, registryState, logger, cancellationToken).ConfigureAwait(false);
        await DarlingHealthReader.GetDailySummaryRangeAsync(
            postgres, serverId, fromDate, toDate, referenceUtc: null, asOfNow: true,
            separatelyMonitored: separatelyMonitored, logger: logger, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The loop: wait, pass, wait for the boundary (or a retry), until <paramref name="stoppingToken"/> cancels.</summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var delay = StartDelay;
        var consecutiveFailures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            var now = _clock();
            if (!_enabled())
            {
                delay = Min(RetryDelay, NextPassUtc(now) - now);
                continue;
            }

            DailySummaryWarmPass pass;
            try
            {
                pass = await RunPassAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            now = _clock();
            var untilBoundary = NextPassUtc(now) - now;
            if (pass.Complete)
            {
                consecutiveFailures = 0;
                delay = untilBoundary;
            }
            else
            {
                consecutiveFailures++;
                delay = Min(RetryDelayFor(consecutiveFailures), untilBoundary);
            }
        }
    }

    /// <summary>One pass over the servers, each under the gate. A refusal ends the pass; a failed server is logged and
    /// skipped; a cancellation propagates.</summary>
    internal async Task<DailySummaryWarmPass> RunPassAsync(CancellationToken cancellationToken)
    {
        var targets = _servers();
        var started = Stopwatch.GetTimestamp();
        var warmed = 0;
        var failed = 0;
        var refused = false;

        foreach (var (serverId, name) in targets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var ticket = _readAdmission is null
                ? ReadAdmissionTicket.Passed
                : await _readAdmission.AdmitAsync(ReadSurface.Web, Route, ReadWindowBand.Longer, cancellationToken).ConfigureAwait(false);
            if (!ticket.MayRun)
            {
                refused = true;
                _logger.LogInformation("Daily summary warm pass stopped at {Server}: the store is busy with other long reads; it is tried again later", name);
                break;
            }

            try
            {
                await _warm(serverId, name, cancellationToken).ConfigureAwait(false);
                warmed++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                failed++;
                _logger.LogWarning(ex, "Daily summary warm for {Server} failed; it is tried again later", name);
            }
        }

        var pass = new DailySummaryWarmPass(targets.Count, warmed, failed, refused);
        _logger.LogInformation(
            "Daily summary warm pass: {Warmed} of {Servers} servers in {ElapsedMs} ms (failed {Failed}, refused {Refused})",
            warmed, targets.Count, Stopwatch.GetElapsedTime(started).TotalMilliseconds, failed, refused);
        return pass;
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}

/// <summary>What one warm pass did. <see cref="Complete"/> is false when a server failed or the gate refused the pass,
/// which schedules a retry before the next boundary.</summary>
internal readonly record struct DailySummaryWarmPass(int Servers, int Warmed, int Failed, bool Refused)
{
    internal bool Complete => Failed == 0 && !Refused;
}
