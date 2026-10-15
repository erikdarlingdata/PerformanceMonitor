/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #5820: Lite's investigation expiry sweep, the twin of Darling's <c>RunInvestigationSweepLoopAsync</c>. Every
/// <see cref="Interval"/> it runs the shared sweep pass (<see cref="InvestigationDispatchCore.SweepOnceAsync"/>): a lapsed
/// claim is journaled once and goes back to open, and a request past its expiry is expired; nothing is ever reopened. It runs
/// whatever the MCP settings say, because requests open from alerts whether or not an agent can reach Lite. A pass that
/// swept something logs one line (the shared pass writes it); an empty pass logs nothing. A failed pass is logged and tried
/// again next tick; only the stopping token ends the loop.
/// </summary>
public sealed class InvestigationSweepService
{
    /// <summary>How often the sweep runs: 60 seconds, Darling's <c>s_investigationSweepInterval</c>.</summary>
    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(60);

    private readonly IInvestigationRequestStore _store;
    private readonly ILogger _logger;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    public InvestigationSweepService(IInvestigationRequestStore store, ILogger logger)
        : this(store, logger, static (wait, token) => Task.Delay(wait, token))
    {
    }

    /// <summary>Test seam: the wait between passes.</summary>
    internal InvestigationSweepService(IInvestigationRequestStore store, ILogger logger, Func<TimeSpan, CancellationToken, Task> delay)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _delay = delay ?? throw new ArgumentNullException(nameof(delay));
    }

    /// <summary>Runs one pass now, then one every <see cref="Interval"/>, until <paramref name="stoppingToken"/> fires.</summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await InvestigationDispatchCore.SweepOnceAsync(_store, _logger, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Investigation sweep failed; it is tried again next pass: {Message}", ex.Message);
            }

            try
            {
                await _delay(Interval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
