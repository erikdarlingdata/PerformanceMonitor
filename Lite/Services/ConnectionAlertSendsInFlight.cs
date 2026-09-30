/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Which servers have a "Server Unreachable" send still running (#4795), so the connection loop does not send the
/// same retry twice. The loop hands a send off and moves on: it never waits on SMTP or a webhook, and a send can take
/// the whole SMTP timeout. The retry's due time only moves when the send's answer has been recorded, so until then
/// every 30 second tick would still find the retry due and send it again, and if the slow send then got through,
/// both would.
///
/// <para>The loop calls <see cref="Begin"/> right before it hands a send to the step that records its answer, and
/// that step calls <see cref="End"/> in a <c>finally</c>, so no exit leaves a server held. Its retry time for the
/// policy comes from <see cref="RetryDueUtc"/>: none while a send is running, the tracker's due time otherwise. The
/// tracker itself is left alone (no clear at dispatch), because clearing it would also end the failed-send streak,
/// and the next failure would wait a minute again instead of doubling.</para>
///
/// <para>Counted rather than a plain set, so two sends for one server that overlap (it went down, came back and went
/// down again inside one slow send) hold the retry until the last of them ends. Touched only from the UI thread, like
/// the connection dictionaries beside it in <c>MainWindow</c>.</para>
/// </summary>
public sealed class ConnectionAlertSendsInFlight
{
    private readonly Dictionary<string, int> _running = new(StringComparer.Ordinal);

    /// <summary>A send for the server has been handed off and has not answered yet.</summary>
    public void Begin(string serverId) =>
        _running[serverId] = _running.TryGetValue(serverId, out var count) ? count + 1 : 1;

    /// <summary>One send for the server has answered, failed or been given up on. An <see cref="End"/> nobody
    /// began is nothing.</summary>
    public void End(string serverId)
    {
        if (!_running.TryGetValue(serverId, out var count))
        {
            return;
        }

        if (count <= 1)
        {
            _running.Remove(serverId);
        }
        else
        {
            _running[serverId] = count - 1;
        }
    }

    /// <summary>When the server's retry is due, for
    /// <see cref="PerformanceMonitor.Common.ConnectionAlertPolicy.Decide"/>: null while a send for it is running (the
    /// retry is already on its way), otherwise what <paramref name="retries"/> holds as of <paramref name="nowUtc"/>
    /// (#4732: a due time a backward clock step left too far ahead comes back as <paramref name="nowUtc"/>, so the
    /// caller passes the same clock reading it gives the policy).</summary>
    public DateTime? RetryDueUtc(string serverId, FailedSendRetryTracker retries, DateTime nowUtc) =>
        _running.ContainsKey(serverId) ? null : retries.DueUtc(serverId, nowUtc);
}
