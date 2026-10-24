/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6629: the bytes one collector run received from, and sent to, the monitored SQL Server.
/// </summary>
public readonly record struct CollectorBytes(long Received, long Sent);

/// <summary>
/// #6629: counts the network bytes one collector run used on its SQL Server connections, from SqlClient's own connection
/// statistics (<c>StatisticsEnabled</c>, <c>RetrieveStatistics()</c>), so <c>collection_log</c> can say which collector pulls the
/// traffic a monitoring host sees.
///
/// <para><b>How a run is bracketed.</b> <see cref="Begin"/> sets an ambient meter for the run (an <see cref="AsyncLocal{T}"/>, so
/// it follows the run into every awaited call and every task the run starts, and is invisible to a run on another thread of
/// the same server loop). <c>SqlServerTargetProvider.CreateConnection</c> and the Azure master listing hand each connection
/// they create to <see cref="Track"/>, which switches statistics on and registers it with the run's meter. A fan-out collector
/// opens one connection per database, so its total is the sum over all of them. A connection made with no meter in scope (the
/// connect probe, XE session upkeep, the viewer) is left alone, with statistics off.</para>
///
/// <para><b>Per run, not per connection.</b> Each connection is registered with the figures it already holds (zero for a fresh
/// one), and the run's total is the growth since that point, so a connection that is shared across runs would still count only
/// the run in hand. That is the per-run reset <c>ResetStatistics()</c> would give, without touching a connection the next run
/// may be reading.</para>
///
/// <para><b>NULL, not 0.</b> <see cref="Snapshot"/> returns null when no tracked connection ever opened: a run on a PostgreSQL
/// or RDS target, or one whose connection failed at open, measured nothing, and a stored 0 would read as "this collector is
/// free". A connection that opened and then failed mid-run counts what it moved.</para>
/// </summary>
public sealed class CollectorByteMeter
{
    private static readonly AsyncLocal<CollectorByteMeter?> s_current = new();

    private readonly object _gate = new();
    private readonly List<Entry> _entries = new();

    /// <summary>The meter of the run in scope, or null outside one.</summary>
    public static CollectorByteMeter? Current => s_current.Value;

    /// <summary>Starts a run's meter in the calling async flow. Dispose clears it.</summary>
    public static Scope Begin()
    {
        var meter = new CollectorByteMeter();
        s_current.Value = meter;
        return new Scope(meter);
    }

    /// <summary>Registers a connection with the run in scope (and turns its statistics on). A no-op with no run in scope or for a
    /// non-SQL-Server connection.</summary>
    public static void Track(DbConnection connection)
    {
        if (s_current.Value is { } meter && connection is SqlConnection sql)
        {
            meter.Add(new SqlSource(sql));
        }
    }

    /// <summary>Registers a source of figures. Internal so a test can stand in for a connection it cannot open.</summary>
    internal void Add(ByteSource source)
    {
        lock (_gate)
        {
            _entries.Add(new Entry(source));
        }
    }

    /// <summary>The run's total so far, or null when no tracked connection ever opened.</summary>
    public CollectorBytes? Snapshot()
    {
        long received = 0;
        long sent = 0;
        var measured = false;
        lock (_gate)
        {
            foreach (var entry in _entries)
            {
                if (entry.Source.Read() is { } now)
                {
                    measured |= entry.Source.EverOpened;
                    received += Math.Max(0, now.Received - entry.Baseline.Received);
                    sent += Math.Max(0, now.Sent - entry.Baseline.Sent);
                }
            }
        }

        return measured ? new CollectorBytes(received, sent) : null;
    }

    /// <summary>One source of figures: a connection's statistics, or a test's stand-in.</summary>
    internal abstract class ByteSource
    {
        /// <summary>The cumulative figures, or null when they cannot be read.</summary>
        public abstract CollectorBytes? Read();

        /// <summary>Whether the connection ever reached Open.</summary>
        public abstract bool EverOpened { get; }
    }

    private sealed class Entry
    {
        public Entry(ByteSource source)
        {
            Source = source;
            Baseline = source.Read() ?? default;
        }

        public ByteSource Source { get; }

        public CollectorBytes Baseline { get; }
    }

    private sealed class SqlSource : ByteSource
    {
        private readonly SqlConnection _connection;
        private CollectorBytes? _final;
        private volatile bool _opened;

        public SqlSource(SqlConnection connection)
        {
            _connection = connection;
            connection.StatisticsEnabled = true;
            _opened = connection.State == ConnectionState.Open;
            connection.StateChange += OnStateChange;
        }

        public override bool EverOpened => _opened;

        public override CollectorBytes? Read() => _final ?? ReadLive();

        private CollectorBytes? ReadLive()
        {
            try
            {
                var statistics = _connection.RetrieveStatistics();
                return new CollectorBytes(ToLong(statistics, "BytesReceived"), ToLong(statistics, "BytesSent"));
            }
            catch (Exception ex) when (ex is InvalidOperationException or ObjectDisposedException)
            {
                return null;
            }
        }

        /* The figures are taken as the connection closes, because a disposed connection may no longer answer. */
        private void OnStateChange(object sender, StateChangeEventArgs e)
        {
            if (e.CurrentState == ConnectionState.Open)
            {
                _opened = true;
            }
            else if (e.CurrentState == ConnectionState.Closed && ReadLive() is { } last)
            {
                _final = last;
            }
        }

        private static long ToLong(IDictionary statistics, string key) =>
            statistics.Contains(key) && statistics[key] is { } value ? Convert.ToInt64(value) : 0;
    }

    /// <summary>The run's scope: dispose to take the meter out of the async flow.</summary>
    public readonly struct Scope : IDisposable
    {
        public Scope(CollectorByteMeter meter) => Meter = meter;

        public CollectorByteMeter Meter { get; }

        public void Dispose()
        {
            if (ReferenceEquals(s_current.Value, Meter))
            {
                s_current.Value = null;
            }
        }
    }
}
