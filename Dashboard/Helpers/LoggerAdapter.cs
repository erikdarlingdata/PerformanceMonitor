/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorDashboard.Helpers
{
    /// <summary>
    /// Bridges the static <see cref="Logger"/> to the ILogger&lt;T&gt; interface so
    /// services that accept ILogger&lt;T&gt; can log to the same file as the rest of
    /// the app. Parallel to Lite's <c>AppLoggerAdapter&lt;T&gt;</c>.
    /// </summary>
    public sealed class LoggerAdapter<T> : ILogger<T>
    {
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
                case LogLevel.Information:
                    Logger.Info(message);
                    break;
                case LogLevel.Warning:
                    Logger.Warning(message);
                    break;
                case LogLevel.Error:
                case LogLevel.Critical:
                    Logger.Error(message, exception);
                    break;
            }
        }
    }
}
