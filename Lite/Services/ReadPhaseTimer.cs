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
/// Where one store read spent its time (#5371): the read-lock wait, the connection open, the statement prepare, the
/// statement execute and the result drain, each timed apart. The field trace of a 248 s Collection Health read could not
/// say which of those it was, and the answer decides the fix: a lock wait is a writer parked ahead of us, an execute is
/// the engine's own work (or its pool and threads shared with an archive scan), a drain is the client side.
///
/// <para>The total is the SUM of the phases, which cover the read end to end, so a test can hand it fake phase times and
/// get a deterministic total. <see cref="Report"/> logs one <c>SLOW METHOD</c> block, through the profiler's own writer
/// and under the profiler's own threshold, carrying all five numbers in its context line. A read under the threshold logs
/// nothing.</para>
/// </summary>
internal sealed class ReadPhaseTimer
{
    internal const string LockWait = "lock wait";
    internal const string Open = "open";
    internal const string Prepare = "prepare";
    internal const string Execute = "execute";
    internal const string Drain = "drain";

    private static readonly string[] s_order = { LockWait, Open, Prepare, Execute, Drain };

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

    /// <summary>The five numbers, in read order, as one line: <c>lock wait 12 ms, open 3 ms, ...</c>.</summary>
    internal string Describe()
    {
        var sb = new StringBuilder();
        foreach (var phase in s_order)
        {
            if (sb.Length > 0) sb.Append(", ");
            sb.Append(CultureInfo.InvariantCulture, $"{phase} {PhaseMs(phase):F0} ms");
        }
        return sb.ToString();
    }

    /// <summary>
    /// Logs the read as a slow method when its total passes the profiler's threshold. <paramref name="sink"/> replaces the
    /// profiler's writer (a test's seam); it is called only for a read that passed the threshold.
    /// </summary>
    internal void Report(string readName, Action<string, double, string>? sink = null, [CallerFilePath] string filePath = "", [CallerLineNumber] int lineNumber = 0)
    {
        var total = TotalMs;
        if (total < MethodProfiler.ThresholdMs) return;

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
