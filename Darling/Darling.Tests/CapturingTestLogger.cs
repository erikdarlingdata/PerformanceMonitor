/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (_lines)
        {
            _lines.Add($"{logLevel}: {formatter(state, exception)}");
        }
    }
}
