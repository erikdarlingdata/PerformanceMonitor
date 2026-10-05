/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>A silent read fallback a reader noted into the current <see cref="ReadScope"/> (#5097).</summary>
internal enum ReadFallback
{
    /// <summary>The gate said the interval table serves, the table read faulted, and raw answered.</summary>
    FallbackRaw,

    /// <summary>The source decision itself (or the connection / transaction it needs) faulted, so raw answered
    /// without a decision. Outranks <see cref="FallbackRaw"/> and <see cref="LedgerUncovered"/> when more than one was
    /// noted in one call.</summary>
    GateFailed,

    /// <summary>The hourly-edges count guard found the hour ledger does not cover the window yet (#4605), so the
    /// route was not taken and raw answered. An expected state, not a fault: it is noted with
    /// <see cref="ReadScope.Note"/> (no log line) and shows as <c>fallback_raw</c> in <c>get_read_latency</c>. Ranks below
    /// <see cref="GateFailed"/> and <see cref="FallbackRaw"/>, which are the bigger facts when noted in the same call.</summary>
    LedgerUncovered,
}

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

    /// <summary>The most statements one scope keeps; later ones are counted in <see cref="StatementsDropped"/>.</summary>
    internal const int MaxStatements = 64;

    /// <summary>The fixed vocabulary <see cref="NoteSource"/> accepts.</summary>
    internal const string SourceRaw = "raw";
    internal const string SourceIntervalTable = "interval_table";
    internal const string SourceHourlyEdges = "hourly_edges";

    private readonly ConcurrentQueue<StatementTiming> _statements = new();
    private int _statementCount;
    private int _statementsDropped;
    private string? _source;
    private string? _sourceReason;
    private long _rows = -1;
    private int _serverId = -1;

    private ReadScope(ILogger? logger)
    {
        Logger = logger;
        StartedUtc = DateTime.UtcNow;
    }

    /// <summary>When the scope opened.</summary>
    internal DateTime StartedUtc { get; }

    /// <summary>True when the recorder wants per-statement timings: the process-wide Npgsql listener samples
    /// commands only inside a scope with this set.</summary>
    internal bool CaptureStatements { get; set; }

    /// <summary>Statements that arrived after the queue held <see cref="MaxStatements"/>.</summary>
    internal int StatementsDropped => Volatile.Read(ref _statementsDropped);

    /// <summary>The source a reader noted (<c>raw</c>, <c>interval_table</c>, <c>hourly_edges</c>), or null.</summary>
    internal string? Source { get { lock (_gate) { return _source; } } }

    /// <summary>The short reason noted with <see cref="Source"/>, or null.</summary>
    internal string? SourceReason { get { lock (_gate) { return _sourceReason; } } }

    /// <summary>The row count a reader noted, or null.</summary>
    internal long? Rows { get { var r = Interlocked.Read(ref _rows); return r < 0 ? null : r; } }

    /// <summary>The monitored server id the read resolved, or null for a store-wide or fleet read.</summary>
    internal int? ServerId { get { var id = Volatile.Read(ref _serverId); return id < 0 ? null : id; } }

    /// <summary>Notes the server a read resolved. Ignored outside a scope; the first one noted stays.</summary>
    internal static void NoteServer(int serverId)
    {
        if (s_current.Value is { } scope && serverId >= 0)
        {
            Interlocked.CompareExchange(ref scope._serverId, serverId, -1);
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

    /// <summary>Appends one statement timing; past <see cref="MaxStatements"/> it is only counted.</summary>
    internal void AddStatement(string? sql, double durationMs, long? rows)
    {
        var ordinal = Interlocked.Increment(ref _statementCount);
        if (ordinal > MaxStatements)
        {
            Interlocked.Increment(ref _statementsDropped);
            return;
        }

        _statements.Enqueue(new StatementTiming(ordinal, StatementLabel.Label(sql), StatementLabel.Hash(sql), durationMs, rows));
    }

    /// <summary>Every statement the scope saw, including the ones past <see cref="MaxStatements"/> that were only counted.</summary>
    internal int StatementTotal => Volatile.Read(ref _statementCount);

    /// <summary>The captured statements in arrival order (for tests).</summary>
    internal IReadOnlyList<StatementTiming> Snapshot() => _statements.ToArray();

    /// <summary>The scope the calling async flow runs inside, or null outside any recorder.</summary>
    internal static ReadScope? Current => s_current.Value;

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
        var scope = new ReadScope(logger);
        s_current.Value = scope;
        return new Opened(scope, previous);
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

    /// <summary>Notes a fallback with no log line, for a caller whose callee already logged the fault.</summary>
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

        internal Opened(ReadScope scope, ReadScope? previous)
        {
            Scope = scope;
            _previous = previous;
        }

        /// <summary>The scope this handle opened.</summary>
        internal ReadScope Scope { get; }

        /// <summary>The fallback noted into <see cref="Scope"/>.</summary>
        internal ReadFallback? Fallback => Scope.Fallback;

        public void Dispose() => s_current.Value = _previous;
    }
}
