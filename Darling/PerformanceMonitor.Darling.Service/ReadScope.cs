/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>A silent read fallback a reader noted into the current <see cref="ReadScope"/> (#5097).</summary>
internal enum ReadFallback
{
    /// <summary>The gate said the interval table serves, the table read faulted, and raw answered.</summary>
    FallbackRaw,

    /// <summary>The source decision itself (or the connection / transaction it needs) faulted, so raw answered
    /// without a decision. Outranks <see cref="FallbackRaw"/> when both were noted in one call.</summary>
    GateFailed,
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

    private ReadScope(ILogger? logger) => Logger = logger;

    /// <summary>The scope the calling async flow runs inside, or null outside any recorder.</summary>
    internal static ReadScope? Current => s_current.Value;

    /// <summary>The recorder's logger, or null.</summary>
    internal ILogger? Logger { get; }

    /// <summary>The strongest fallback noted so far (<see cref="ReadFallback.GateFailed"/> outranks
    /// <see cref="ReadFallback.FallbackRaw"/>), or null.</summary>
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
                if (scope._fallback is not ReadFallback.GateFailed)
                {
                    scope._fallback = kind;
                }
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
            if (scope._fallback is not ReadFallback.GateFailed)
            {
                scope._fallback = kind;
            }
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
    /// cancel, error or limit is the bigger fact and wins.</summary>
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
