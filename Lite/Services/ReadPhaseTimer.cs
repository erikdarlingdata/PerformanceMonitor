/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using PerformanceMonitorLite.Helpers;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Where one store read spent its time (#5371): the read-lock wait, the connection open, the statement execute and the row
/// read, each timed apart. The field trace of a 248 s Collection Health read could not say which of those it was, and the
/// answer decides the fix: a lock wait is a writer parked ahead of us, an execute is the engine's own work (or its pool and
/// threads shared with an archive scan), a row read is the client side.
///
/// <para>There is no separate "prepare" phase. DuckDB.NET's <c>Prepare()</c> parses and binds nothing (0.09 ms on the
/// shipped statement, which then took about 25 ms to execute), so the statement's parse, bind and plan cost, which grows
/// with the number of archive files, is part of the execute phase, and the log line says so.</para>
///
/// <para>The total is the SUM of the phases, which cover the read end to end, so a test can hand it fake phase times and
/// get a deterministic total. <see cref="Report"/> logs one <c>SLOW METHOD</c> block, through the profiler's own writer
/// and under the profiler's own threshold, carrying all four numbers in its context line. A read under the threshold logs
/// nothing. A read a newer request superseded logs one plain line saying so instead.</para>
/// </summary>
internal sealed class ReadPhaseTimer
{
    internal const string LockWait = "lock wait";
    internal const string Open = "open";
    internal const string Execute = "execute";
    internal const string RowRead = "row read";

    private static readonly string[] s_order = { LockWait, Open, Execute, RowRead };

    private readonly Dictionary<string, double> _phasesMs = new();
    private readonly DateTime _startedAt = DateTime.Now;

    /// <summary>Times the scope that follows, under <paramref name="phase"/>. Dispose ends it.</summary>
    internal PhaseScope Measure(string phase) => new(this, phase);

    /// <summary>Adds <paramref name="ms"/> to a phase. A phase measured twice adds up.</summary>
    internal void Add(string phase, double ms)
    {
        _phasesMs[phase] = (_phasesMs.TryGetValue(phase, out var soFar) ? soFar : 0) + ms;
    }

    internal double PhaseMs(string phase) => _phasesMs.TryGetValue(phase, out var ms) ? ms : 0;

    internal double TotalMs
    {
        get
        {
            double total = 0;
            foreach (var ms in _phasesMs.Values) total += ms;
            return total;
        }
    }

    /// <summary>The four numbers, in read order, as one line: <c>lock wait 12 ms, open 3 ms, execute 900 ms (prepare and
    /// bind included), row read 2 ms</c>.</summary>
    internal string Describe()
    {
        var sb = new StringBuilder();
        foreach (var phase in s_order)
        {
            if (sb.Length > 0) sb.Append(", ");
            sb.Append(CultureInfo.InvariantCulture, $"{phase} {PhaseMs(phase):F0} ms");
            if (phase == Execute) sb.Append(" (prepare and bind included)");
        }
        return sb.ToString();
    }

    /// <summary>
    /// Logs the read as a slow method when its total passes the profiler's threshold. <paramref name="sink"/> replaces the
    /// profiler's writer (a test's seam); it is called only for a read that passed the threshold.
    ///
    /// <para>A <paramref name="cancelled"/> read, one a newer request stopped on purpose, is not a slow method: it logs ONE
    /// plain line, <c>cancelled after N ms (superseded)</c> with the phases it got through, and no <c>SLOW METHOD</c> block.
    /// It still has to pass the threshold, since a read cut off in a few milliseconds is not worth a line. The sink, when
    /// given, receives that same line as its context.</para>
    /// </summary>
    internal void Report(string readName, Action<string, double, string>? sink = null, bool cancelled = false, [CallerFilePath] string filePath = "", [CallerLineNumber] int lineNumber = 0)
    {
        var total = TotalMs;
        if (total < MethodProfiler.ThresholdMs) return;

        if (cancelled)
        {
            var line = $"{readName} cancelled after {total.ToString("F0", CultureInfo.InvariantCulture)} ms (superseded); {Describe()}";
            if (sink is not null)
            {
                sink(readName, total, line);
                return;
            }

            AppLogger.Info("ReadPhaseTimer", line);
            return;
        }

        var context = $"{readName} phases: {Describe()}";
        if (sink is not null)
        {
            sink(readName, total, context);
            return;
        }

        MethodProfiler.LogSlowMethod(_startedAt, DateTime.Now, total, context, readName, filePath, lineNumber);
    }

    internal sealed class PhaseScope : IDisposable
    {
        private readonly ReadPhaseTimer _owner;
        private readonly string _phase;
        private readonly long _start;
        private bool _done;

        internal PhaseScope(ReadPhaseTimer owner, string phase)
        {
            _owner = owner;
            _phase = phase;
            _start = Stopwatch.GetTimestamp();
            _done = false;
        }

        public void Dispose()
        {
            if (_done) return;
            _done = true;
            _owner.Add(_phase, Stopwatch.GetElapsedTime(_start).TotalMilliseconds);
        }
    }
}
