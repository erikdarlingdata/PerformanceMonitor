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
///
/// <para><b>The gate is <see cref="AppLogger.IsEnabled"/>, not a rule of this type's own (#3104).</b> Lite
/// constructs these directly rather than resolving them from a logger factory, so there is no
/// <c>LoggerFilterOptions</c> upstream to filter on and this <see cref="IsEnabled"/> is the only thing a
/// caller's level is ever compared against. Answering it from a level fixed here rather than from the sink
/// puts the app's verbosity decision in two places: a caller lowering a site to <c>Debug</c> would move it
/// from one admitted level to another admitted level while the sink dropped it anyway, so the level a site
/// carries and the level that decides its fate could disagree with nothing to reveal it. Deferring leaves
/// one answer, which is also the answer <see cref="AppLogger"/>'s own static callers get.</para>
/// </summary>
public sealed class AppLoggerAdapter<T> : ILogger<T>
{
    private readonly string _categoryName = typeof(T).Name;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => AppLogger.IsEnabled(logLevel);

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
