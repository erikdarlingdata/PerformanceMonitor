/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The memory peaks of the MCP calls that are slow enough to be recorded (#9424). DuckDB keeps no peak for a call, so the way
/// <c>ParquetCompaction.DuckDbMemorySampler</c> takes a merge's peak is used here too: read the figure on an interval and keep the highest.
///
/// <para><b>Cost on a fast call.</b> A call is not known to be slow until it ends, and most calls are fast. A call therefore pays one
/// small <see cref="Registration"/> object and one one-shot timer that never fires when the call ends first. No thread starts and no DuckDB
/// read happens until a call has been running for <see cref="McpCallTimingRecorder.SamplingStartDelay"/>. From then on ONE shared
/// loop runs for as long as at least one call is attached, reads the process working set and DuckDB's memory once per tick, and hands
/// the reading to every attached call. The loop ends when the last call detaches and starts again on the next attach.</para>
///
/// <para>What it can miss: the first <see cref="McpCallTimingRecorder.SamplingStartDelay"/> of every call is covered by one working-set reading at the
/// start and none of DuckDB's memory, and a spike shorter than the interval between two ticks can fall between them.</para>
///
/// <para>Both figures are for the whole process. Calls that overlap share the readings taken while they overlap, so a call's peak can
/// hold another call's memory.</para>
/// </summary>
internal sealed class McpMemorySampler
{
    private readonly Func<long> _readWorkingSet;
    private readonly Func<long?> _readDuckDbMemory;
    private readonly Func<TimeSpan> _interval;
    private readonly object _gate = new();
    private readonly List<Registration> _attached = new();
    private Thread? _loop;
    private int _starts;

    internal McpMemorySampler(Func<long> readWorkingSet, Func<long?> readDuckDbMemory, Func<TimeSpan> interval)
    {
        _readWorkingSet = readWorkingSet;
        _readDuckDbMemory = readDuckDbMemory;
        _interval = interval;
    }

    /// <summary>How many times the sampling loop has started since the process began. A fast call adds none. For tests.</summary>
    internal int Starts => Volatile.Read(ref _starts);

    /// <summary>One call's peaks. Created for every call, attached to the loop only once the call has run past the start delay.</summary>
    internal sealed class Registration
    {
        private long _workingSetPeak;
        private long _duckDbPeak;
        private long _duckDbSeen;

        internal bool Attached { get; set; }
        internal bool Closed { get; set; }

        /// <summary>The highest working set read while the call ran, null when none was read.</summary>
        internal long? WorkingSetPeak => Volatile.Read(ref _workingSetPeak) is var v and > 0 ? v : null;

        /// <summary>The highest DuckDB memory total read while the call ran, null when every read found the store lock busy.</summary>
        internal long? DuckDbPeak => Volatile.Read(ref _duckDbSeen) == 1 ? Volatile.Read(ref _duckDbPeak) : null;

        internal void NoteWorkingSet(long bytes) => RaiseTo(ref _workingSetPeak, bytes);

        internal void NoteDuckDb(long bytes)
        {
            RaiseTo(ref _duckDbPeak, bytes);
            Volatile.Write(ref _duckDbSeen, 1);
        }

        private static void RaiseTo(ref long field, long value)
        {
            long seen;
            while (value > (seen = Interlocked.Read(ref field)))
            {
                if (Interlocked.CompareExchange(ref field, value, seen) == seen) break;
            }
        }
    }

    /// <summary>
    /// Joins a call to the loop, takes one reading for it now and starts the loop when it was not running. A call that has
    /// already ended (<see cref="Registration.Closed"/>) is not joined.
    /// </summary>
    internal void Attach(Registration call)
    {
        Thread? start = null;
        lock (_gate)
        {
            if (call.Closed || call.Attached) return;
            call.Attached = true;
            _attached.Add(call);
            if (_loop is null && _interval() > TimeSpan.Zero)
            {
                _loop = start = new Thread(Run) { IsBackground = true, Name = "McpCallMemory" };
                Interlocked.Increment(ref _starts);
            }
        }

        ReadInto(new[] { call });
        start?.Start();
    }

    /// <summary>Takes a call out of the loop. The call's peaks stay readable. Safe to call twice and on a call that never attached.</summary>
    internal void Detach(Registration call)
    {
        lock (_gate)
        {
            call.Closed = true;
            if (call.Attached)
            {
                call.Attached = false;
                _attached.Remove(call);
            }
        }
    }

    /// <summary>One reading of the working set and DuckDB's memory, handed to <paramref name="calls"/>. A reading that fails is a missing sample.</summary>
    internal void ReadInto(IReadOnlyList<Registration> calls)
    {
        try
        {
            var ws = _readWorkingSet();
            foreach (var c in calls) c.NoteWorkingSet(ws);
        }
        catch (Exception)
        {
            /* A reading that fails is a missing sample, never a failed call. */
        }

        try
        {
            if (_readDuckDbMemory() is { } duck)
            {
                foreach (var c in calls) c.NoteDuckDb(duck);
            }
        }
        catch (Exception)
        {
            /* The same for DuckDB's memory. */
        }
    }

    private void Run()
    {
        try
        {
            while (true)
            {
                Thread.Sleep((int)Math.Max(1, _interval().TotalMilliseconds));
                Registration[] now;
                lock (_gate)
                {
                    if (_attached.Count == 0)
                    {
                        _loop = null;
                        return;
                    }

                    now = _attached.ToArray();
                }

                ReadInto(now);
            }
        }
        finally
        {
            lock (_gate)
            {
                if (ReferenceEquals(_loop, Thread.CurrentThread)) _loop = null;
            }
        }
    }
}
