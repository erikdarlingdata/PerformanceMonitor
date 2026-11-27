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
using System.Threading;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// #9195: how long one MCP call spent waiting for the store's read lock, added up over every read lock it took. A call opens
/// several store connections (the archive span, the top read, the window floor, the tag scope, the database state), each waits
/// for the lock, and a pending collector write holds every one of them up. Nothing in the log said how long that wait was, so a
/// slow field read could not be told from a slow statement.
///
/// <para>A call opens a tally with <see cref="Begin"/> and the read lock's own acquire adds to whichever tally is current, so
/// no read has to pass anything down. The current tally flows with the async call (an <see cref="AsyncLocal{T}"/>), and a
/// call outside any tally pays one null check. The tally is a reference, so reads that run in parallel under the same call add
/// to the same one. Locking itself is unchanged.</para>
/// </summary>
internal sealed class ReadLockWaitTally : IDisposable
{
    /// <summary>
    /// A read that takes longer than this logs its timing line. Two seconds, the threshold the collector pass line uses. The
    /// setter is a test seam, so a test can make any read slow or fast without depending on the clock.
    /// </summary>
    internal static TimeSpan SlowReadThreshold { get; set; } = TimeSpan.FromSeconds(2);

    private static readonly AsyncLocal<ReadLockWaitTally?> s_current = new();

    /// <summary>The tally of the call that is running now, or null outside one.</summary>
    internal static ReadLockWaitTally? Current => s_current.Value;

    private readonly ReadLockWaitTally? _outer;
    private long _waitTicks;
    private long _archiveSpanTicks;
    private long _archiveFiles;
    private int _hasSplit;
    private string? _answeredFrom;
    private string? _phaseSplit;
    private int _connections;
    private bool _disposed;

    private ReadLockWaitTally()
    {
        _outer = s_current.Value;
    }

    /// <summary>Starts a tally for the call that follows and makes it current. Dispose ends it.</summary>
    internal static ReadLockWaitTally Begin()
    {
        var tally = new ReadLockWaitTally();
        s_current.Value = tally;
        return tally;
    }

    /// <summary>Adds one read lock acquire that waited <paramref name="waitedTicks"/> stopwatch ticks.</summary>
    internal void RecordAcquire(long waitedTicks)
    {
        Interlocked.Add(ref _waitTicks, waitedTicks);
        Interlocked.Increment(ref _connections);
    }

    /// <summary>Adds time spent reading an archive span. It is inside whatever phase asked for the span.</summary>
    internal void RecordArchiveSpan(long ticks) => Interlocked.Add(ref _archiveSpanTicks, ticks);

    /// <summary>Adds the archive files an archive span read opened (#9423).</summary>
    internal void RecordArchiveFiles(int files) => Interlocked.Add(ref _archiveFiles, files);

    /// <summary>Archive files the call's archive span reads opened (#9423).</summary>
    internal long ArchiveFiles => Interlocked.Read(ref _archiveFiles);

    /// <summary>
    /// Says the read keeps a phase split (#9423): a tool that times its own phases, as get_query_store_top does, calls this so the
    /// slow-call row stores the hot against archive figures. A read that never calls it stores NULL for them, since "no split
    /// kept" and "no archive time" are different answers. The flag passes to the outer tally when this one ends.
    /// </summary>
    internal void MarkSplitKept() => Interlocked.Exchange(ref _hasSplit, 1);

    internal bool SplitKept => Volatile.Read(ref _hasSplit) == 1;

    /// <summary>
    /// Records which path answered the call and the call's own phase text (#9885), for the slow-call row. <c>get_object_usage</c> calls it
    /// once it knows whether the chain or the full fold read the tables. Both values pass to the outer tally when this one ends, the way
    /// <see cref="MarkSplitKept"/> does, so the row of the call that wraps the tool carries them.
    /// </summary>
    internal void NoteAnswer(string? answeredFrom, string? phaseSplit)
    {
        if (answeredFrom is not null) Volatile.Write(ref _answeredFrom, answeredFrom);
        if (phaseSplit is not null) Volatile.Write(ref _phaseSplit, phaseSplit);
    }

    /// <summary>The path that answered the call, as <see cref="NoteAnswer"/> recorded it. Null when the tool records none.</summary>
    internal string? AnsweredFrom => Volatile.Read(ref _answeredFrom);

    /// <summary>The call's phases as <see cref="ReadPhaseSplit.Describe"/> wrote them. Null when the tool records none.</summary>
    internal string? PhaseSplit => Volatile.Read(ref _phaseSplit);

    internal TimeSpan LockWait => Elapsed(Interlocked.Read(ref _waitTicks));
    internal TimeSpan ArchiveSpan => Elapsed(Interlocked.Read(ref _archiveSpanTicks));
    internal int Connections => Volatile.Read(ref _connections);

    private static TimeSpan Elapsed(long ticks) => Stopwatch.GetElapsedTime(0, ticks);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        s_current.Value = _outer;
        /* A call inside a call (an MCP tool that runs the facts read) is part of the outer call's wait. */
        if (_outer is not null)
        {
            Interlocked.Add(ref _outer._waitTicks, Interlocked.Read(ref _waitTicks));
            Interlocked.Add(ref _outer._archiveSpanTicks, Interlocked.Read(ref _archiveSpanTicks));
            Interlocked.Add(ref _outer._archiveFiles, Interlocked.Read(ref _archiveFiles));
            if (SplitKept) _outer.MarkSplitKept();
            _outer.NoteAnswer(AnsweredFrom, PhaseSplit);
            Interlocked.Add(ref _outer._connections, Volatile.Read(ref _connections));
        }
    }

    /// <summary><c>read lock wait 120 ms over 5 connections</c>.</summary>
    internal string DescribeLockWait() =>
        string.Create(CultureInfo.InvariantCulture, $"read lock wait {(long)LockWait.TotalMilliseconds} ms over {Connections} connections");
}

/// <summary>
/// The phases of one MCP read in the order they ran, each timed apart, for the slow-read line. Whatever part of the total no
/// phase accounts for is "other".
/// </summary>
internal sealed class ReadPhaseSplit
{
    private readonly List<(string Name, long Ticks)> _phases = new();
    private readonly long _startedAt = Stopwatch.GetTimestamp();

    /// <summary>Awaits <paramref name="read"/> and adds its time to <paramref name="phase"/>. A phase run twice adds up.</summary>
    internal async System.Threading.Tasks.Task<T> TimeAsync<T>(string phase, Func<System.Threading.Tasks.Task<T>> read)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            return await read();
        }
        finally
        {
            Add(phase, Stopwatch.GetTimestamp() - started);
        }
    }

    /// <summary>Adds <paramref name="ticks"/> of stopwatch time to <paramref name="phase"/> for a phase a read measures inside its own loop (#9885).</summary>
    internal void Add(string phase, long ticks)
    {
        var i = _phases.FindIndex(p => p.Name == phase);
        if (i < 0) _phases.Add((phase, ticks));
        else _phases[i] = (phase, _phases[i].Ticks + ticks);
    }

    internal TimeSpan Total => Stopwatch.GetElapsedTime(_startedAt);

    /// <summary><c>tag scope 3 ms, top read 900 ms, other 12 ms</c>, in the order the phases first ran.</summary>
    internal string Describe(TimeSpan total)
    {
        long accounted = 0;
        var parts = new List<string>();
        foreach (var (name, ticks) in _phases)
        {
            accounted += ticks;
            parts.Add(string.Create(CultureInfo.InvariantCulture, $"{name} {(long)Stopwatch.GetElapsedTime(0, ticks).TotalMilliseconds} ms"));
        }

        var other = total - Stopwatch.GetElapsedTime(0, accounted);
        parts.Add(string.Create(CultureInfo.InvariantCulture, $"other {(long)Math.Max(0, other.TotalMilliseconds)} ms"));
        return string.Join(", ", parts);
    }
}
