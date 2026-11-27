/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// What the service's stop-time store writes left unwritten because the store itself was going down (#9794).
///
/// <para>A store restarted together with the service (<c>docker compose restart</c> stops both at once) is already refusing or
/// closing connections when the service makes its last writes: the query-tag sightings flush, the last update of the start
/// history's row and the collector-cost and read-latency flush. Each of those is a routine loss the next start makes good, not
/// a failure of its own, so a writer handed a notice adds what it could not write here instead of logging a Warning, and the
/// worker reports the lot as ONE Information line once the last of them has run. Each writer still tries once and inside its own
/// budget: nothing is retried.</para>
///
/// <para>Only a failure <see cref="IsStoreUnavailable"/> recognises is taken here. Every other failure (another SQLSTATE, a budget
/// that ran out) keeps the writer's own Warning.</para>
/// </summary>
public sealed class StoreStopNotice : IDisposable
{
    private readonly object _gate = new();
    private readonly List<string> _notWritten = new();
    private readonly ILogger? _logger;

    /// <param name="logger">Where <see cref="Report"/> writes the line. Null reports nothing.</param>
    public StoreStopNotice(ILogger? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    /// Whether <paramref name="ex"/> is the store saying it is not there to be written to: it answered that it is shutting down
    /// or not yet accepting connections (SQLSTATE 57P01, 57P02 or 57P03, the three a PostgreSQL going down or coming up answers
    /// with), or nothing was listening on its port (a refused connection). A <see cref="PostgresException"/> with any other
    /// SQLSTATE is the store answering about the statement or the login, and is not this.
    /// </summary>
    public static bool IsStoreUnavailable(Exception? ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is PostgresException pg)
            {
                return pg.SqlState is PostgresErrorCodes.AdminShutdown or PostgresErrorCodes.CrashShutdown or PostgresErrorCodes.CannotConnectNow;
            }

            if (e is SocketException { SocketErrorCode: SocketError.ConnectionRefused })
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>How many things are waiting to be reported.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _notWritten.Count;
            }
        }
    }

    /// <summary>Adds one thing a stop-time write did not write, in the words the line names it with. The same words twice are one entry.</summary>
    public void NotWritten(string what)
    {
        ArgumentException.ThrowIfNullOrEmpty(what);
        lock (_gate)
        {
            if (!_notWritten.Contains(what))
            {
                _notWritten.Add(what);
            }
        }
    }

    /// <summary>
    /// Logs the one Information line that names everything added since the last report, and forgets it. Logs nothing when nothing
    /// was added, so a stop whose writes all landed says nothing.
    /// </summary>
    public void Report()
    {
        string[] notWritten;
        lock (_gate)
        {
            if (_notWritten.Count == 0)
            {
                return;
            }

            notWritten = _notWritten.ToArray();
            _notWritten.Clear();
        }

        _logger?.LogInformation(
            "The store was going down while the service stopped, so the service did not write: {NotWritten}. Restarting the store and the service together does this, and nothing was retried.",
            string.Join(", ", notWritten));
    }

    /// <summary>Reports whatever is still waiting, so a way out of the worker that never reaches its own <see cref="Report"/> call still says it.</summary>
    public void Dispose() => Report();
}
