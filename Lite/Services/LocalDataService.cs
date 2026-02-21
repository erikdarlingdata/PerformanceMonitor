/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Service for reading collected data from DuckDB.
/// Partial class - individual data type readers are in separate files.
/// </summary>
public partial class LocalDataService
{
    private readonly DuckDbInitializer _duckDb;

    public LocalDataService(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Creates and opens a DuckDB connection wrapped in a read lock.
    /// The lock prevents CHECKPOINT and compaction from reorganizing the database file
    /// while this connection is reading from it.
    /// </summary>
    internal async Task<LockedConnection> OpenConnectionAsync()
    {
        var readLock = _duckDb.AcquireReadLock();
        try
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return new LockedConnection(connection, readLock);
        }
        catch
        {
            readLock.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Safely converts a DuckDB value to double, handling BigInteger from SUM aggregations.
    /// </summary>
    protected static double ToDouble(object value)
    {
        if (value is BigInteger bi)
            return (double)bi;
        return Convert.ToDouble(value);
    }

    /// <summary>
    /// Safely converts a DuckDB value to long, handling BigInteger from SUM/COUNT aggregations.
    /// </summary>
    protected static long ToInt64(object value)
    {
        if (value is BigInteger bi)
            return (long)bi;
        return Convert.ToInt64(value);
    }

    /// <summary>
    /// Gets the time range for queries based on hoursBack or explicit date range.
    /// Returns UTC time for collection_time queries (most tables store collection_time in UTC).
    /// When fromDate/toDate are provided, they should already be in UTC.
    /// </summary>
    protected static (DateTime startTime, DateTime endTime) GetTimeRange(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (fromDate.HasValue && toDate.HasValue)
        {
            /* Custom date range - convert from server time back to UTC for storage lookup */
            var startUtc = fromDate.Value.AddMinutes(-ServerTimeHelper.UtcOffsetMinutes);
            var endUtc = toDate.Value.AddMinutes(-ServerTimeHelper.UtcOffsetMinutes);
            return (startUtc, endUtc);
        }

        /* Use UTC directly since collection_time is stored in UTC */
        return (DateTime.UtcNow.AddHours(-hoursBack), DateTime.UtcNow);
    }

    /// <summary>
    /// Gets the time range in server local time (for tables like cpu_utilization_stats.sample_time).
    /// </summary>
    protected static (DateTime startTime, DateTime endTime) GetTimeRangeServerLocal(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var serverNow = DateTime.UtcNow.AddMinutes(ServerTimeHelper.UtcOffsetMinutes);

        if (fromDate.HasValue && toDate.HasValue)
        {
            /* fromDate/toDate are already in server time from the caller */
            return (fromDate.Value, toDate.Value);
        }

        return (serverNow.AddHours(-hoursBack), serverNow);
    }

}
