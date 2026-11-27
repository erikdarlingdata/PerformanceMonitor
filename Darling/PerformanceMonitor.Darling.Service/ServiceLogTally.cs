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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Counts the ERROR and WARN lines the service itself logs, and keeps the first <see cref="ServiceStartHistory.FirstLinesKept"/> of each (#9415),
/// for the start history row. In process, from the service's own logging, so it needs no read of the log file and starts counting before the
/// store is reachable. ERROR counts <see cref="LogLevel.Error"/> and <see cref="LogLevel.Critical"/>. A kept line goes through
/// <see cref="SecretTextGuard.RedactText"/> and is cut to <see cref="ServiceStartHistory.LineLength"/> characters, because the text can carry what
/// an exception message quotes.
/// </summary>
internal sealed class ServiceLogTally
{
    /// <summary>The tally of this process, which <see cref="ServiceLogTallyProvider"/> feeds in the service.</summary>
    internal static ServiceLogTally Shared { get; } = new();

    private readonly object _gate = new();
    private readonly List<string> _firstErrors = new();
    private readonly List<string> _firstWarns = new();
    private long _errors;
    private long _warns;

    /// <summary>What the tally holds now.</summary>
    internal readonly record struct Counts(long Errors, long Warns, IReadOnlyList<string> FirstErrors, IReadOnlyList<string> FirstWarns);

    /// <summary>Counts one line. Levels below Warning are ignored.</summary>
    internal void Record(LogLevel level, Func<string> text)
    {
        if (level < LogLevel.Warning || level == LogLevel.None)
        {
            return;
        }

        var isError = level >= LogLevel.Error;
        lock (_gate)
        {
            var first = isError ? _firstErrors : _firstWarns;
            if (isError)
            {
                _errors++;
            }
            else
            {
                _warns++;
            }

            if (first.Count < ServiceStartHistory.FirstLinesKept)
            {
                first.Add(Mask(text));
            }
        }
    }

    /// <summary>The counts and kept lines as of now.</summary>
    internal Counts Snapshot()
    {
        lock (_gate)
        {
            return new Counts(_errors, _warns, _firstErrors.ToArray(), _firstWarns.ToArray());
        }
    }

    private static string Mask(Func<string> text)
    {
        string raw;
        try
        {
            raw = text();
        }
#pragma warning disable CA1031 // A message that cannot be formatted is kept as a placeholder: logging never throws into the caller.
        catch (Exception)
#pragma warning restore CA1031
        {
            raw = "(the log message could not be formatted)";
        }

        var masked = SecretTextGuard.RedactText(raw.Replace('\r', ' ').Replace('\n', ' '));
        return masked.Length <= ServiceStartHistory.LineLength ? masked : masked[..ServiceStartHistory.LineLength];
    }
}

/// <summary>Feeds <see cref="ServiceLogTally"/> from the host's logging. It writes nothing anywhere.</summary>
internal sealed class ServiceLogTallyProvider : ILoggerProvider
{
    private readonly ServiceLogTally _tally;

    internal ServiceLogTallyProvider(ServiceLogTally tally)
    {
        _tally = tally;
    }

    public ILogger CreateLogger(string categoryName) => new TallyLogger(_tally, categoryName);

    public void Dispose()
    {
    }

    private sealed class TallyLogger : ILogger
    {
        private readonly ServiceLogTally _tally;
        private readonly string _category;

        internal TallyLogger(ServiceLogTally tally, string category)
        {
            _tally = tally;
            _category = category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning && logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            _tally.Record(logLevel, () =>
            {
                var message = formatter(state, exception);
                if (exception is not null && !message.Contains(exception.Message, StringComparison.Ordinal))
                {
                    message += " | " + exception.GetType().Name + ": " + exception.Message;
                }

                var dot = _category.LastIndexOf('.');
                return (dot >= 0 ? _category[(dot + 1)..] : _category) + ": " + message;
            });
        }
    }
}
