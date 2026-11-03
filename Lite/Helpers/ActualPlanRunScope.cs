/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Owns the cancellation of a server tab's "Get Actual Plan" re-run. The re-run has no command timeout
/// (#7181), so the only thing that stops it on the monitored server is its token. The tab cancels through
/// <see cref="CancelForClose"/> when it is closed or its server is removed, which also lets the late
/// completion see <see cref="IsClosed"/> and skip the message box or plan tab it would otherwise raise for a
/// tab that no longer exists. <see cref="Cancel"/> is the Cancel button: the tab stays open and says so.
/// </summary>
internal sealed class ActualPlanRunScope
{
    private CancellationTokenSource? _cts;

    /// <summary>True once the owning tab was closed; a run that ends after this must not touch the UI.</summary>
    public bool IsClosed { get; private set; }

    /// <summary>Starts a run and returns its token. After the tab closed, the token is already cancelled.</summary>
    public CancellationToken Begin()
    {
        _cts?.Dispose();
        _cts = new CancellationTokenSource();
        if (IsClosed) _cts.Cancel();
        return _cts.Token;
    }

    /// <summary>The Cancel button: stops the run in flight, if any.</summary>
    public void Cancel() => _cts?.Cancel();

    /// <summary>The tab was closed: stops the run in flight and marks the scope closed.</summary>
    public void CancelForClose()
    {
        IsClosed = true;
        _cts?.Cancel();
    }
}
