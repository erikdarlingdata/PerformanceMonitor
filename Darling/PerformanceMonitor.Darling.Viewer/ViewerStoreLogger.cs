/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The <see cref="ILogger"/> the Viewer hands to the storage readers that take one (#6654 review), so a best-effort step that gives up
/// (the dropped-database read, <c>CurrentDatabaseScope</c>) lands in the Viewer's own log as a warning instead of vanishing.
/// Warnings and worse only; the exception text rides the formatter's message.
/// </summary>
internal sealed class ViewerStoreLogger : ILogger
{
    public static readonly ViewerStoreLogger Instance = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (IsEnabled(logLevel))
        {
            ViewerLogger.Warn("ViewerDataService", formatter(state, exception));
        }
    }
}
