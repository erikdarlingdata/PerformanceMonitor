/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #9194: the cancellation a server tab's load hands to every store read it starts, without a token parameter on each of the
/// reads. A sub-tab load runs inside a scope (<see cref="Enter"/>), and a store read that was given no token of its own takes the
/// scope's, so picking another sub-tab cancels the reads of the one being left and the picked tab's own read starts at once instead
/// of queueing behind them. A read given a token of its own keeps it. Shared work that serves more than one caller, such as the
/// single-flight caches, clears the scope first (<see cref="Clear"/>) so one tab's pick never cancels what others wait on.
/// </summary>
internal static class ViewerReadScope
{
    private static readonly AsyncLocal<CancellationToken> Ambient = new();

    /// <summary>The scope the calling flow runs in, or a token that never cancels when there is none.</summary>
    internal static CancellationToken Current => Ambient.Value;

    /// <summary>Runs the rest of the calling async flow (and what it starts) in <paramref name="token"/>'s scope. The change stays
    /// inside the calling async method and does not reach its caller.</summary>
    internal static void Enter(CancellationToken token) => Ambient.Value = token;

    /// <summary>Takes the calling flow out of any scope.</summary>
    internal static void Clear() => Ambient.Value = default;

    /// <summary>The token a read uses: its own when it has one, otherwise the scope's.</summary>
    internal static CancellationToken Effective(CancellationToken own) => own.CanBeCanceled ? own : Ambient.Value;
}
