/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The time a read waits for a store connection, apart from its run time (#5506). Npgsql has no per-open wait value, and
/// most reads never call <c>OpenAsync</c> themselves (a data-source command takes its connection inside the execute), so
/// the wait is taken from the two measurements Npgsql's pool reports on the caller's own async flow: the pending-request
/// counter (+1 when an open starts waiting for a pooled connection, -1 when it stops) and the physical connection create
/// time. A <see cref="MeterListener"/> receives both synchronously, on the flow that is waiting, so the wait lands in that
/// flow's <see cref="ReadScope"/>. A create that happens inside a pending wait is already part of that wait and is not
/// added twice. Outside a read scope (collectors, the worker) nothing is recorded. The callbacks never throw.
/// </summary>
internal static class ReadConnectionWait
{
    internal const string MeterName = "Npgsql";
    internal const string PendingRequestsInstrument = "db.client.connection.npgsql.pending_requests";
    internal const string CreateTimeInstrument = "db.client.connection.npgsql.create_time";

    private static readonly object s_gate = new();
    private static MeterListener? s_listener;

    /* The timestamp the current flow's pending open started, or 0. Set and read inside Npgsql's own rent method, so a read
       that opens several connections at once keeps each wait apart. */
    private static readonly AsyncLocal<long> s_pendingStart = new();

    /// <summary>Registers the listener once per process; later calls do nothing.</summary>
    internal static void Register()
    {
        lock (s_gate)
        {
            if (s_listener is not null)
            {
                return;
            }

            var listener = new MeterListener
            {
                InstrumentPublished = static (instrument, l) =>
                {
                    if (instrument.Meter.Name == MeterName
                        && instrument.Name is PendingRequestsInstrument or CreateTimeInstrument)
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                },
            };
            listener.SetMeasurementEventCallback<int>(static (instrument, value, _, _) => OnPending(value));
            listener.SetMeasurementEventCallback<double>(static (instrument, value, _, _) => OnCreateSeconds(value));
            listener.Start();
            s_listener = listener;
        }
    }

    /// <summary>One pending-request change on the current flow: +1 starts a wait, -1 ends it.</summary>
    internal static void OnPending(int delta)
    {
        try
        {
            if (delta > 0)
            {
                s_pendingStart.Value = ReadScope.Current is null ? 0 : Stopwatch.GetTimestamp();
                return;
            }

            var started = s_pendingStart.Value;
            if (started == 0)
            {
                return;
            }

            s_pendingStart.Value = 0;
            ReadScope.Current?.AddWait(Stopwatch.GetTimestamp() - started);
        }
        catch
        {
            /* A wait measurement fault must never reach the read. */
        }
    }

    /// <summary>One physical connection create on the current flow, in seconds. Counted only outside a pending wait.</summary>
    internal static void OnCreateSeconds(double seconds)
    {
        try
        {
            if (s_pendingStart.Value != 0 || ReadScope.Current is not { } scope || double.IsNaN(seconds) || seconds <= 0)
            {
                return;
            }

            scope.AddWait((long)(seconds * Stopwatch.Frequency));
        }
        catch
        {
            /* A wait measurement fault must never reach the read. */
        }
    }
}
