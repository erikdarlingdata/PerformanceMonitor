/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The ambient scope one recorded read runs inside (#5097). A recorder (the MCP tool filter, the
/// <c>/api/read/*</c> loop, the composed-panel runner) opens it around the read; a reader that quietly falls
/// back to raw calls <see cref="NoteFallback"/>; the recorder then resolves the measured outcome through
/// <see cref="Resolve"/> so an otherwise-<c>Ok</c> read is recorded as <c>fallback_raw</c> / <c>gate_failed</c>.
/// The scope also carries the recorder's logger, so a fallback reaches the service log instead of a Trace
/// listener the service never wires.
///
/// <para>The <see cref="AsyncLocal{T}"/> holds a mutable object, not a value: a child async method's
/// assignment to an AsyncLocal never flows back to its caller, but a mutation of the object the caller placed
/// there does. <see cref="Open"/> restores the previous scope on dispose, so nesting is safe (the inner scope
/// wins while it is open).</para>
/// </summary>
internal sealed class ReadScope
{
    private static readonly AsyncLocal<ReadScope?> s_current = new();

    private readonly object _gate = new();
    private ReadFallback? _fallback;

    /// <summary>The most statements one scope keeps (#6795): the slowest this many by time, whatever order they ran in. A
    /// faster one that arrives once the scope is full is only counted, in <see cref="StatementsDropped"/>. The slow-read row
    /// stores them all, so this is the same number.</summary>
    internal const int MaxStatements = SlowReadLog.MaxStatementsStored;

    /// <summary>The fixed vocabulary <see cref="NoteSource"/> accepts.</summary>
    internal const string SourceRaw = ComposeReadSources.Raw;
    internal const string SourceIntervalTable = ComposeReadSources.IntervalTable;
    internal const string SourceHourlyEdges = ComposeReadSources.HourlyEdges;

    /* #6795: a min-heap on (duration, then the later arrival), so the root is the statement to evict: the fastest kept, and the
       latest-arriving of any tie. Guarded by _statementGate; a statement arrives on whatever thread closed its reader. */
    private readonly PriorityQueue<StatementTiming, (double DurationMs, int NegatedOrdinal)> _statements = new();
    private readonly object _statementGate = new();
    private int _statementCount;
    private string? _source;
    private string? _sourceReason;
    private long _rows = -1;
    /* #6834: a server id is a hash of the name, so half of them are negative; "none noted" is a value no int can hold. */
    private const long NoServer = long.MinValue;
    private long _serverId = NoServer;
    private long _waitTicks;
    private DateTime? _windowStart;
    private DateTime? _windowEnd;
    private bool _noWindow;

    /* #5506: the reads in progress on the service right now. Only an outermost scope counts, so a composed panel the MCP
       run_custom_view_panel tool runs (a scope inside the tool's scope) is one read, not two. */
    private static int s_inFlight;

    private ReadScope(ILogger? logger, ReadScope? parent, int inFlightAtStart)
    {
        Logger = logger;
        StartedUtc = DateTime.UtcNow;
        _parent = parent;
        InFlightAtStart = inFlightAtStart;
        _counting = parent is null ? 1 : 0;
    }

    private readonly ReadScope? _parent;

    /* 1 while this outermost scope is counted in s_inFlight; 0 for a nested scope, and while the read waits for an
       admission slot (#5506 part 3). Exchanged, so a scope is uncounted exactly once. */
    private int _counting;

    /// <summary>The reads in progress on the service right now (#5506).</summary>
    internal static int InFlight => Volatile.Read(ref s_inFlight);

    /// <summary>How many other reads were already in progress on the service when this read started (#5506). A nested
    /// scope carries its outermost scope's count.</summary>
    internal int InFlightAtStart { get; private set; }

    /// <summary>Whether this scope counts in <see cref="InFlight"/> now: an outermost scope does, except while its read waits
    /// in the admission queue (<see cref="AdmitAsync"/>).</summary>
    internal bool IsCounted => Volatile.Read(ref _counting) == 1;

    /// <summary>
    /// #5506 part 3: admits a read with this flow's scope uncounted while it waits. A read that queues for an admission
    /// slot is not running, so it leaves <see cref="InFlight"/> until the slot is granted (or refused, or the caller
    /// leaves), and its <see cref="InFlightAtStart"/> is taken again then: <c>max_in_flight</c> counts reads running,
    /// as it did before admission existed. Not async itself: <see cref="ReadAdmission.AdmitAsync"/> sets its nesting
    /// marker in the caller's flow, and an async wrapper would undo that marker on return.
    /// </summary>
    internal static Task<ReadAdmissionTicket> AdmitAsync(ReadAdmission admission, ReadSurface surface, string route, string? band,
        CancellationToken cancellationToken)
    {
        var admitting = admission.AdmitAsync(surface, route, band, cancellationToken);
        if (admitting.IsCompleted || s_current.Value is not { } current)
        {
            return admitting;
        }

        var outermost = current;
        while (outermost._parent is { } parent)
        {
            outermost = parent;
        }

        if (Interlocked.Exchange(ref outermost._counting, 0) == 0)
        {
            return admitting;
        }

        Interlocked.Decrement(ref s_inFlight);
        return ResumeAfterAsync(admitting, outermost);

        static async Task<ReadAdmissionTicket> ResumeAfterAsync(Task<ReadAdmissionTicket> admitting, ReadScope scope)
        {
            try
            {
                return await admitting.ConfigureAwait(false);
            }
            finally
            {
                scope.InFlightAtStart = Interlocked.Increment(ref s_inFlight) - 1;
                Volatile.Write(ref scope._counting, 1);
            }
        }
    }

    /// <summary>
    /// #5506 part 3 (round 2, L3): this flow's outermost scope stops counting in <see cref="InFlight"/> for the rest of its
    /// life. A caller answered from the read cache, or one that joined a read another caller runs, runs no read of its
    /// own, so it must not count in <c>max_in_flight</c> or in another read's <see cref="InFlightAtStart"/> while it waits
    /// (round 1, L7: reads running only), as a read waiting for an admission slot does not (<see cref="AdmitAsync"/>).
    /// Exchanged, so the scope is uncounted exactly once whatever else uncounts it.
    /// </summary>
    internal void Uncount()
    {
        var outermost = this;
        while (outermost._parent is { } parent)
        {
            outermost = parent;
        }

        if (Interlocked.Exchange(ref outermost._counting, 0) == 1)
        {
            Interlocked.Decrement(ref s_inFlight);
        }
    }

    /// <summary>The time this read waited for a store connection, in milliseconds, apart from its run time (#5506): the
    /// pool's queue wait plus the time to open a new physical connection, summed over every connection the read took.
    /// <see cref="ReadConnectionWait"/> adds it.</summary>
    internal long WaitMs => (long)(Interlocked.Read(ref _waitTicks) * 1000.0 / System.Diagnostics.Stopwatch.Frequency);

    /// <summary>Adds connection wait to this scope and every scope it is nested in.</summary>
    internal void AddWait(long stopwatchTicks)
    {
        if (stopwatchTicks <= 0)
        {
            return;
        }

        for (var scope = this; scope is not null; scope = scope._parent)
        {
            Interlocked.Add(ref scope._waitTicks, stopwatchTicks);
        }
    }

    /// <summary>When the scope opened.</summary>
    internal DateTime StartedUtc { get; }

    /// <summary>True when the recorder wants per-statement timings: the process-wide Npgsql listener samples
    /// commands only inside a scope with this set.</summary>
    internal bool CaptureStatements { get; set; }

    /// <summary>Statements the scope saw but does not hold: the ones too fast to stay among the slowest
    /// <see cref="MaxStatements"/>.</summary>
    internal int StatementsDropped
    {
        get
        {
            lock (_statementGate)
            {
                return _statementCount - _statements.Count;
            }
        }
    }

    /// <summary>The source a reader noted (<c>raw</c>, <c>interval_table</c>, <c>hourly_edges</c>), or null.</summary>
    internal string? Source { get { lock (_gate) { return _source; } } }

    /// <summary>The short reason noted with <see cref="Source"/>, or null.</summary>
    internal string? SourceReason { get { lock (_gate) { return _sourceReason; } } }

    /// <summary>The row count a reader noted, or null.</summary>
    internal long? Rows { get { var r = Interlocked.Read(ref _rows); return r < 0 ? null : r; } }

    /// <summary>The monitored server id the read resolved, or null for a store-wide or fleet read.</summary>
    internal int? ServerId { get { var id = Interlocked.Read(ref _serverId); return id == NoServer ? null : (int)id; } }

    /// <summary>Notes the server a read resolved. Ignored outside a scope; the first one noted stays.</summary>
    internal static void NoteServer(int serverId)
    {
        if (s_current.Value is { } scope)
        {
            Interlocked.CompareExchange(ref scope._serverId, serverId, NoServer);
        }
    }

    /// <summary>The start of the window the read ran with (#5814), UTC, or null when the read noted none.</summary>
    internal DateTime? WindowStartUtc { get { lock (_gate) { return _windowStart; } } }

    /// <summary>The end of the window the read ran with (#5814), UTC, or null when the read noted none.</summary>
    internal DateTime? WindowEndUtc { get { lock (_gate) { return _windowEnd; } } }

    /// <summary>True when the read noted that it ran with no window at all (#5844); see <see cref="NoteNoWindow"/>.</summary>
    internal bool NotedNoWindow { get { lock (_gate) { return _noWindow; } } }

    /// <summary>
    /// Notes the window a read ran with (#5814): the window after the tool resolved its arguments, so a defaulted
    /// <c>hours_back</c> is noted as the 24 hours it read, and a route whose window is fixed in code notes that window.
    /// The slow-read row records it in place of a window lifted from the arguments. Ignored outside a scope, and for an
    /// empty or inverted window. The first window noted stays: a tool notes its own window before any reader it calls
    /// can note one.
    /// </summary>
    internal static void NoteWindow(DateTime startUtc, DateTime endUtc)
    {
        if (s_current.Value is not { } scope || endUtc <= startUtc)
        {
            return;
        }

        lock (scope._gate)
        {
            if (scope._windowEnd is null && !scope._noWindow)
            {
                scope._windowStart = startUtc;
                scope._windowEnd = endUtc;
            }
        }
    }

    /// <summary>Notes a window <paramref name="hours"/> wide ending at <paramref name="endUtc"/>; see
    /// <see cref="NoteWindow(DateTime, DateTime)"/>. Zero or negative hours note nothing.</summary>
    internal static void NoteWindow(double hours, DateTime endUtc)
    {
        if (hours > 0 && !double.IsNaN(hours))
        {
            NoteWindow(endUtc.AddHours(-hours), endUtc);
        }
    }

    /// <summary>
    /// Notes that the read ran with no window (#5844): a read of one request by id spans its whole history, so the
    /// slow-read row records no window, not one lifted from an <c>hours_back</c> the caller sent and the read ignored.
    /// Silence still means "fall back to the arguments" for every read that notes nothing. Ignored outside a scope; like
    /// <see cref="NoteWindow(DateTime, DateTime)"/>, the first note stays.
    /// </summary>
    internal static void NoteNoWindow()
    {
        if (s_current.Value is not { } scope)
        {
            return;
        }

        lock (scope._gate)
        {
            if (scope._windowEnd is null)
            {
                scope._noWindow = true;
            }
        }
    }

    /// <summary>Notes which source answered. Ignored outside a scope or for a name outside the fixed vocabulary.</summary>
    internal static void NoteSource(string source, string? reason = null)
    {
        var scope = s_current.Value;
        if (scope is null || (source != SourceRaw && source != SourceIntervalTable && source != SourceHourlyEdges))
        {
            return;
        }

        lock (scope._gate)
        {
            scope._source = source;
            scope._sourceReason = reason is { Length: > 64 } ? reason[..64] : reason;
        }
    }

    /// <summary>Notes the row count the read returned. Ignored outside a scope.</summary>
    internal static void NoteRows(long n)
    {
        if (s_current.Value is { } scope)
        {
            Interlocked.Exchange(ref scope._rows, Math.Max(0, n));
        }
    }

    /// <summary>
    /// Adds one statement timing (#6795). The scope keeps the slowest <see cref="MaxStatements"/> by time, each with its arrival
    /// ordinal, and counts the rest: a read that opens with many small statements keeps the big ones that come later. This runs
    /// for every statement of every read, so a statement too fast to stay is counted and dropped under one short lock and its
    /// text is never labelled or hashed. A statement that stays takes the lock twice: once to take its ordinal and see whether
    /// it beats the fastest kept one, and once, after its text is labelled and hashed outside the lock, to join the kept set,
    /// where the bound is enforced again because other statements may have arrived between the two. A tie keeps the earlier
    /// statement.
    /// </summary>
    internal void AddStatement(string? sql, double durationMs, long? rows)
    {
        int ordinal;
        lock (_statementGate)
        {
            ordinal = ++_statementCount;
            if (_statements.Count >= MaxStatements && _statements.TryPeek(out _, out var fastestKept) && durationMs <= fastestKept.DurationMs)
            {
                return;
            }
        }

        var timing = new StatementTiming(ordinal, StatementLabel.Label(sql), StatementLabel.Hash(sql), durationMs, rows);
        lock (_statementGate)
        {
            _statements.Enqueue(timing, (durationMs, -ordinal));
            if (_statements.Count > MaxStatements)
            {
                _statements.Dequeue();
            }
        }
    }

    /// <summary>Every statement the scope saw, including the ones it counted but did not keep.</summary>
    internal int StatementTotal
    {
        get
        {
            lock (_statementGate)
            {
                return _statementCount;
            }
        }
    }

    /// <summary>The statements the scope kept, the slowest ones, in arrival order.</summary>
    internal IReadOnlyList<StatementTiming> Snapshot()
    {
        lock (_statementGate)
        {
            var kept = new List<StatementTiming>(_statements.Count);
            foreach (var (timing, _) in _statements.UnorderedItems)
            {
                kept.Add(timing);
            }

            kept.Sort(static (x, y) => x.Ordinal.CompareTo(y.Ordinal));
            return kept;
        }
    }

    /// <summary>The scope the calling async flow runs inside, or null outside any recorder.</summary>
    internal static ReadScope? Current => s_current.Value;

    /* #6390 S2c: the composed-panel run lives in Storage and cannot name this class; it notes through ComposeReadNotes, which is
       pointed at this scope here, before any service type is used. */
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void InstallComposeReadNotes()
    {
        ComposeReadNotes.NoteSourceSink = NoteSource;
        ComposeReadNotes.NoteSink = Note;
        ComposeReadNotes.NoteFallbackSink = NoteFallback;
        ComposeReadNotes.WarnSink = Warn;
        ComposeReadNotes.CurrentLoggerSource = static () => Current?.Logger;
    }

    /// <summary>The recorder's logger, or null.</summary>
    internal ILogger? Logger { get; }

    /// <summary>The strongest fallback noted so far (<see cref="ReadFallback.GateFailed"/> outranks
    /// <see cref="ReadFallback.FallbackRaw"/>, which outranks <see cref="ReadFallback.LedgerUncovered"/>), or null.</summary>
    internal ReadFallback? Fallback
    {
        get
        {
            lock (_gate)
            {
                return _fallback;
            }
        }
    }

    /// <summary>Opens a scope for the current async flow. Dispose restores the previous scope.</summary>
    internal static Opened Open(ILogger? logger)
    {
        var previous = s_current.Value;
        var counted = previous is null;
        var inFlightAtStart = counted ? Interlocked.Increment(ref s_inFlight) - 1 : previous!.InFlightAtStart;
        var scope = new ReadScope(logger, previous, inFlightAtStart);
        s_current.Value = scope;
        return new Opened(scope, previous, counted);
    }

    /// <summary>Notes a fallback and logs it: a Warning through the scope's logger (the exception object, never
    /// its message text), or, with no scope or no logger, the Trace line unscoped callers have always had.</summary>
    internal static void NoteFallback(ReadFallback kind, string site, Exception? ex)
    {
        var scope = s_current.Value;
        if (scope is not null)
        {
            lock (scope._gate)
            {
                scope.Keep(kind);
            }
        }

        if (scope?.Logger is { } logger)
        {
            logger.LogWarning(ex, "{Site} fell back to raw ({Kind})", site, kind);
        }
        else
        {
            System.Diagnostics.Trace.TraceWarning(ex is null
                ? $"{site} fell back to raw ({kind})"
                : $"{site} fell back to raw ({kind}): {ex.GetType().Name}");
        }
    }

    /// <summary>Notes a fallback with no log line: for a caller whose callee already logged the fault, or for an expected
    /// state that is not a fault (<see cref="ReadFallback.LedgerUncovered"/>, a store before V164).</summary>
    internal static void Note(ReadFallback kind)
    {
        var scope = s_current.Value;
        if (scope is null)
        {
            return;
        }

        lock (scope._gate)
        {
            scope.Keep(kind);
        }
    }

    /// <summary>The order the reasons rank in: a bigger number is the bigger fact. Written out, not taken from the enum's
    /// declaration order, so adding a reason cannot quietly reorder them.</summary>
    private static int Rank(ReadFallback kind) => kind switch
    {
        ReadFallback.GateFailed => 3,
        ReadFallback.FallbackRaw => 2,
        ReadFallback.LedgerUncovered => 1,
        _ => 0,
    };

    /// <summary>Holds <paramref name="kind"/> unless a stronger reason is already held; an equal one is replaced by the later
    /// note, as it always was. Call under <c>_gate</c>.</summary>
    private void Keep(ReadFallback kind)
    {
        if (_fallback is not { } held || Rank(kind) >= Rank(held))
        {
            _fallback = kind;
        }
    }

    /// <summary>Logs a degraded read that is not a source decision, so it has no outcome (log only).</summary>
    internal static void Warn(string message, Exception? ex)
    {
        if (s_current.Value?.Logger is { } logger)
        {
            logger.LogWarning(ex, "{Message}", message);
        }
        else
        {
            System.Diagnostics.Trace.TraceWarning(ex is null ? message : $"{message}: {ex.GetType().Name}");
        }
    }

    /// <summary>The recorded outcome: a noted fallback replaces ONLY <see cref="ReadOutcome.Ok"/>; a timeout,
    /// cancel, error or limit is the bigger fact and wins. <see cref="ReadFallback.GateFailed"/> becomes
    /// <see cref="ReadOutcome.GateFailed"/>; every other reason, <see cref="ReadFallback.LedgerUncovered"/> included,
    /// becomes <see cref="ReadOutcome.FallbackRaw"/>.</summary>
    internal static ReadOutcome Resolve(ReadOutcome measured, ReadFallback? noted)
    {
        if (measured != ReadOutcome.Ok || noted is null)
        {
            return measured;
        }

        return noted == ReadFallback.GateFailed ? ReadOutcome.GateFailed : ReadOutcome.FallbackRaw;
    }

    /// <summary>The disposable <see cref="Open"/> returns.</summary>
    internal readonly struct Opened : IDisposable
    {
        private readonly ReadScope? _previous;
        private readonly bool _counted;

        internal Opened(ReadScope scope, ReadScope? previous, bool counted)
        {
            Scope = scope;
            _previous = previous;
            _counted = counted;
        }

        /// <summary>The scope this handle opened.</summary>
        internal ReadScope Scope { get; }

        /// <summary>The fallback noted into <see cref="Scope"/>.</summary>
        internal ReadFallback? Fallback => Scope.Fallback;

        public void Dispose()
        {
            s_current.Value = _previous;
            if (_counted && Interlocked.Exchange(ref Scope._counting, 0) == 1)
            {
                Interlocked.Decrement(ref s_inFlight);
            }
        }
    }
}
