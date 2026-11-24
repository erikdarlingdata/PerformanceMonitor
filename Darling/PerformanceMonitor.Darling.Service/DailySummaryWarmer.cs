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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
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
/// and the start-up repairs go first, then one pass just after each UTC day boundary (<see cref="NextPassUtc"/>), when
/// the Overview's 31-days-ending-today key moves to the new day. The cache key carries the range, so a block built the
/// day before sits under yesterday's key until it expires and serves nobody; the pass at 00:01 builds the new day's
/// block while <see cref="DailySummaryRangeCache{TRow}.OpenStartUtc"/> is still yesterday, so it holds the days before
/// yesterday, a visitor before 02:00 hits with a one-day tail read, and a visitor after 02:00 hits with a two-day tail
/// read that also grows the block. A pass that straddles midnight is followed by the new day's pass at its 00:01, or at
/// once, because the servers it warmed before midnight hold a block under the old day's key. A pass with a failed
/// server, or one the heavy-read gate refused, is tried again after <see cref="RetryDelayFor"/>, never later than the
/// next due pass. No pass runs while the service is paused,
/// while a materialization repair is rebuilding the rollups, while neither the web host nor the MCP host is on (nothing
/// reads the block then), or while <c>dailySummaryWarmerEnabled</c> is off in the config file; it looks again every
/// <see cref="DeferredRecheck"/> and logs one line when it first defers (#8554).</para>
///
/// <para><b>Load.</b> One server at a time, each read under the read-admission gate (<see cref="ReadAdmission"/>) as the
/// <c>7d+</c> web read it stands in for, so it takes one of <c>reads.maxHeavyReads</c> slots and queues behind a
/// visitor's read like any other. A refusal (the store is busy) ends the pass. A server already warm costs the days before
/// the retention horizon and the trend read (#9039: not the open day's read, whose rows the warm read throws away), and a retry pass re-reads only what failed.</para>
///
/// <para><b>Refresh (#8824).</b> A pass repeats every <see cref="RefreshInterval"/>, not once a day, because a block serves the days
/// before the retention horizon for one hour only and a visitor after the hour paid their read, with the open day's, on pages
/// no one had read since the pass. Each repeat re-reads them (the warm read passes <c>refreshSubHorizon</c>) and the 24-hour
/// <c>total_waits</c> trend the Overview's wait strip reads. A failed or refused pass backs off 15, 30, 60, then 120 minutes, never the 30-minute interval.</para>
/// </summary>
public sealed class DailySummaryWarmer
{
    /// <summary>The range the Overview reads: the tile and the calendar's current month both ask for 31 days ending today.</summary>
    public const int DaysBack = 31;

    /// <summary>The route the warmer admits under: the read it builds the block for.</summary>
    internal const string Route = "get_daily_summary_range";

    /// <summary>How long after the service starts the first pass runs.</summary>
    internal static readonly TimeSpan StartDelay = TimeSpan.FromMinutes(2);

    /// <summary>Added to the day boundary so the pass lands after the Overview's key has moved, never a clock tick before.</summary>
    internal static readonly TimeSpan PassMargin = TimeSpan.FromMinutes(1);

    /// <summary>The wait after a failed or refused pass, doubled per consecutive failure up to <see cref="MaxRetryDelay"/>.</summary>
    internal static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(15);

    /// <summary>How often a warmer that is switched off for now (the service is paused, a materialization repair is rebuilding the
    /// rollups, no host reads the cache, or the knob is off) looks again (#8554). It was <see cref="RetryDelay"/>, so a start whose
    /// repair outlasted the two-minute start delay found its first pass up to 15 minutes after the repair ended, and a server opened
    /// in that gap paid the cold read. The check is one delegate call.</summary>
    internal static readonly TimeSpan DeferredRecheck = TimeSpan.FromMinutes(1);

    internal static readonly TimeSpan MaxRetryDelay = TimeSpan.FromHours(2);

    /// <summary>#8824: the longest the warmer waits between passes. Each pass re-reads every server's days before the retention
    /// horizon (a block serves them for <see cref="DailySummaryRangeCache{TRow}.SubHorizonTtl"/>, one hour) and its open day, and
    /// reads the 24-hour wait trend, so a visitor an hour or more after the start no longer pays for those reads, the first of
    /// which found the store's pages cold. Half the hour, so a pass that runs long still lands inside it, and the pages the
    /// reads touched are recent when the visitor comes.</summary>
    internal static readonly TimeSpan RefreshInterval = TimeSpan.FromMinutes(30);

    private readonly Func<IReadOnlyList<(int ServerId, string Name)>> _servers;
    private readonly Func<int, string, CancellationToken, Task> _warm;
    private readonly Func<bool> _enabled;
    private readonly ReadAdmission? _readAdmission;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _clock;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<CancellationToken, Task<IReadOnlySet<int>>>? _registered;

    /// <param name="servers">The servers to warm, read at each pass so a reconcile shows on the next one.</param>
    /// <param name="warm">Builds one server's block; <see cref="WarmAsync"/> in the service.</param>
    /// <param name="enabled">False while no pass should run (paused, repairing, or no host reads the cache).</param>
    /// <param name="readAdmission">The heavy-read gate, or null for a service without one.</param>
    /// <param name="clock">UTC now; tests pass one that moves.</param>
    /// <param name="delay">Waits the given span; tests pass one that records and advances the clock.</param>
    /// <param name="registered">#9334: the ids of the servers the store's registry holds, read once per pass. A server not in it is
    /// skipped at Debug and tried again on a later pass, because the reads resolve the server through that registry and answer
    /// "no servers are registered yet" until the first connection writes the row. Null warms every server (the old behaviour).</param>
    public DailySummaryWarmer(
        Func<IReadOnlyList<(int ServerId, string Name)>> servers,
        Func<int, string, CancellationToken, Task> warm,
        Func<bool> enabled,
        ReadAdmission? readAdmission,
        ILogger logger,
        Func<DateTime>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<CancellationToken, Task<IReadOnlySet<int>>>? registered = null)
    {
        _servers = servers ?? throw new ArgumentNullException(nameof(servers));
        _warm = warm ?? throw new ArgumentNullException(nameof(warm));
        _enabled = enabled ?? throw new ArgumentNullException(nameof(enabled));
        _readAdmission = readAdmission;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _clock = clock ?? (static () => DateTime.UtcNow);
        _delay = delay ?? (static (span, ct) => Task.Delay(span, ct));
        _registered = registered;
    }

    /// <summary>#9334: the ids of the enabled servers in the registry the warm reads resolve through
    /// (<see cref="DarlingServerResolver.LoadEnabledAsync"/>), so "not registered" is decided from the same rows the reads use.</summary>
    public static async Task<IReadOnlySet<int>> RegisteredServerIdsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var servers = await DarlingServerResolver.LoadEnabledAsync(postgres, cancellationToken).ConfigureAwait(false);
        return servers.Select(server => server.ServerId).ToHashSet();
    }

    /// <summary>The next UTC day boundary after <paramref name="nowUtc"/> plus <see cref="PassMargin"/>: the first
    /// moment the new day's Overview key is in use, so the block built then serves every visitor of that day.</summary>
    internal static DateTime NextPassUtc(DateTime nowUtc) => nowUtc.Date.AddDays(1).Add(PassMargin);

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
            separatelyMonitored: separatelyMonitored, logger: logger, cancellationToken: cancellationToken,
            refreshSubHorizon: true, skipOpenDayTail: true).ConfigureAwait(false);

        /* #8824: the Overview's wait strip reads the 24-hour total_waits trend, which has no cache of its own, so its first read
           of a server after the pages left the store's buffers paid 4 to 8 seconds against about half a second for the next.
           The same read here, answer discarded, keeps those pages warm. A failed read comes back as an error or refusal answer, not
           a throw: it is logged like a failed server read, and the pass stays complete (the daily summary block is built). */
        var trend = await DarlingMcpServerTrendTools.GetServerTrend(
            postgres, "total_waits", name, 24, null, null, null, null, TrendBudget.ChartFor(null), cancellationToken).ConfigureAwait(false);
        if (McpHelpers.IsErrorEnvelope(trend) || McpHelpers.IsRefusalEnvelope(trend))
        {
            logger?.LogWarning("Daily summary warm for {Server}: the 24-hour wait trend read failed ({Reason}); the daily summary block is built, the trend pages are not warmed", name, McpHelpers.ErrorMessageOf(trend));
        }
    }

    /// <summary>The loop: wait, pass, wait for the boundary (or a retry), until <paramref name="stoppingToken"/> cancels.</summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var delay = StartDelay;
        var consecutiveFailures = 0;
        var deferredLogged = false;
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
                /* #8554: said once per stretch, so the log shows why no pass line follows the start delay. */
                if (!deferredLogged)
                {
                    deferredLogged = true;
                    _logger.LogInformation(
                        "Daily summary warm pass deferred: the service is paused, a materialization repair is rebuilding the rollups, no host reads the cache, or the warmer is off. It is checked again every minute");
                }

                delay = Min(DeferredRecheck, NextPassUtc(now) - now);
                continue;
            }

            deferredLogged = false;
            var passDay = now.Date;
            DailySummaryWarmPass pass;
            try
            {
                pass = await RunPassAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            /* A pass that straddles UTC midnight warmed its earlier servers under the old day's key (WarmAsync reads the
               day per server), where their block serves nobody. The new day's own pass is then due at its 00:01, at
               once if that has already gone by, not a day later. */
            now = _clock();
            var dueUtc = now.Date > passDay ? now.Date.Add(PassMargin) : NextPassUtc(now);
            var untilDue = dueUtc > now ? dueUtc - now : TimeSpan.Zero;
            if (pass.Complete)
            {
                /* #8824: the refresh interval is the cadence of a healthy pass only. A failed or refused pass backs off by
                   RetryDelayFor, so a failing server is not retried every 30 minutes, each try holding a heavy slot. */
                consecutiveFailures = 0;
                delay = Min(untilDue, RefreshInterval);
            }
            else
            {
                consecutiveFailures++;
                delay = Min(RetryDelayFor(consecutiveFailures), untilDue);
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
        var skipped = 0;
        var refused = false;
        var registered = await ReadRegisteredAsync(targets.Count, cancellationToken).ConfigureAwait(false);

        foreach (var (serverId, name) in targets)
        {
            cancellationToken.ThrowIfCancellationRequested();

            /* #9334: on a fresh install the first pass runs before the first connection registers the server, and the wait trend
               read answers "no servers are registered yet". Not a failure: skipped before it takes a heavy slot, warmed on a later pass. */
            if (registered is not null && !registered.Contains(serverId))
            {
                skipped++;
                _logger.LogDebug("Daily summary warm for {Server} skipped: it is not registered in the store yet; it is warmed on a later pass", name);
                continue;
            }

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

        var pass = new DailySummaryWarmPass(targets.Count, warmed, failed, refused, skipped);
        _logger.LogInformation(
            "Daily summary warm pass: {Warmed} of {Servers} servers in {ElapsedMs} ms (failed {Failed}, refused {Refused}, not registered yet {Skipped})",
            warmed, targets.Count, Stopwatch.GetElapsedTime(started).TotalMilliseconds, failed, refused, skipped);
        return pass;
    }

    /// <summary>The registry's server ids for this pass, or null when there is no registry seam, nothing to warm, or the registry
    /// read failed: every server is then warmed as before, and a store that cannot answer fails each read with its own Warning.</summary>
    private async Task<IReadOnlySet<int>?> ReadRegisteredAsync(int targetCount, CancellationToken cancellationToken)
    {
        if (_registered is null || targetCount == 0)
        {
            return null;
        }

        try
        {
            return await _registered(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Daily summary warm: the server registry could not be read; every server is warmed as if registered");
            return null;
        }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}

/// <summary>What one warm pass did. <see cref="Complete"/> is false when a server failed or the gate refused the pass,
/// which schedules a retry before the next boundary. <see cref="Skipped"/> counts servers the registry does not hold yet (#9334):
/// neither warmed nor failed, so they do not make the pass incomplete.</summary>
internal readonly record struct DailySummaryWarmPass(int Servers, int Warmed, int Failed, bool Refused, int Skipped = 0)
{
    internal bool Complete => Failed == 0 && !Refused;
}
