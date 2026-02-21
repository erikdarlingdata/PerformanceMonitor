/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Bridges the static AppLogger to the ILogger&lt;T&gt; interface so services
/// that accept ILogger&lt;T&gt; can log to the same file as the rest of the app.
/// </summary>
public sealed class AppLoggerAdapter<T> : ILogger<T>
{
    private readonly string _categoryName = typeof(T).Name;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        var message = formatter(state, exception);

        switch (logLevel)
        {
            case LogLevel.Trace:
            case LogLevel.Debug:
                AppLogger.Debug(_categoryName, message);
                break;
            case LogLevel.Information:
                AppLogger.Info(_categoryName, message);
                break;
            case LogLevel.Warning:
                AppLogger.Warn(_categoryName, message);
                break;
            case LogLevel.Error:
            case LogLevel.Critical:
                AppLogger.Error(_categoryName, message, exception);
                break;
        }
    }
}
