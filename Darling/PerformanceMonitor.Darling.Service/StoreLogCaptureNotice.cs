/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9695: decides when the hourly store-log capture's failure is a Warning and when it is a Debug repeat. A failure the
/// store itself causes and will not change on its own (58P01, no log folder: a stock Debian or Ubuntu PostgreSQL logs to
/// stderr with logging_collector off; 42501, the login lacks the log read) warns once per service start, and the same
/// cause repeats at Debug. A different cause warns again, and so does the same cause after a capture that worked. Any
/// other fault (a timeout, a dropped connection) is not store configuration and warns every time it happens.
/// </summary>
internal sealed class StoreLogCaptureNotice
{
    private string? _warnedCause;

    /// <summary>True when the fault is one the store's own configuration causes, which is what repeats quietly.</summary>
    internal static bool IsStoreSideCause(Exception ex)
        => ex is PostgresException { SqlState: PostgresErrorCodes.UndefinedFile or PostgresErrorCodes.InsufficientPrivilege };

    /// <summary>Records one capture failure and answers whether it is a Warning. A store-side cause already warned for, with no
    /// successful capture since, is not.</summary>
    internal bool ShouldWarn(Exception ex)
    {
        if (!IsStoreSideCause(ex))
        {
            return true;
        }

        var cause = ((PostgresException)ex).SqlState;
        if (string.Equals(cause, _warnedCause, StringComparison.Ordinal))
        {
            return false;
        }

        _warnedCause = cause;
        return true;
    }

    /// <summary>A capture worked, so the next store-side failure is news again.</summary>
    internal void Succeeded() => _warnedCause = null;
}
