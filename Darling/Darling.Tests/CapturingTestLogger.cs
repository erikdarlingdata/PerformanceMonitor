/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Darling.Tests;

/// <summary>
/// A capturing <see cref="ILogger"/> for gated live tests. Born from #1564: the purge E2Es passed
/// <c>logger: null</c>, so when a per-table statement failed against the shared store the failure was
/// isolated AND silenced — the test then failed downstream on a surviving row with zero evidence of why.
/// Passing this logger and folding <see cref="Joined"/> into assertion messages turns the next such
/// failure into a self-diagnosing artifact (the CI log carries the actual per-table warning text).
/// </summary>
internal sealed class CapturingTestLogger : ILogger
{
    private readonly List<string> _lines = new();
    private readonly List<Exception?> _exceptions = new();

    /// <summary>Every line logged so far, joined for embedding in an assertion message.</summary>
    public string Joined
    {
        get
        {
            lock (_lines)
            {
                return _lines.Count == 0 ? "(no log lines captured)" : string.Join(" | ", _lines);
            }
        }
    }

    /// <summary>How many lines were logged at exactly this level — #4276's tests count Warning/Error lines
    /// rather than parsing <see cref="Joined"/>.</summary>
    public int CountAtLevel(LogLevel level)
    {
        lock (_lines)
        {
            return _lines.Count(line => line.StartsWith(level.ToString() + ":", StringComparison.Ordinal));
        }
    }

    /// <summary>A snapshot of every line logged so far, in log order, for a test that needs to count matches
    /// against a substring rather than a level.</summary>
    public IReadOnlyList<string> Lines
    {
        get
        {
            lock (_lines)
            {
                return _lines.ToList();
            }
        }
    }

    /// <summary>The <see cref="Exception"/> argument of every line logged at exactly this level, in log
    /// order, non-null entries only -- lets a test assert an entry's real exception reached the sink
    /// (the <c>logger.LogError(ex, ...)</c> overload), not just that its message text was baked into
    /// the formatted string by a <c>{Message}</c> template placeholder.</summary>
    public IReadOnlyList<Exception> ExceptionsAtLevel(LogLevel level)
    {
        lock (_lines)
        {
            var matches = new List<Exception>();
            for (var i = 0; i < _lines.Count; i++)
            {
                if (_lines[i].StartsWith(level.ToString() + ":", StringComparison.Ordinal) && _exceptions[i] is not null)
                {
                    matches.Add(_exceptions[i]!);
                }
            }
            return matches;
        }
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (_lines)
        {
            _lines.Add($"{logLevel}: {formatter(state, exception)}");
            _exceptions.Add(exception);
        }
    }
}
