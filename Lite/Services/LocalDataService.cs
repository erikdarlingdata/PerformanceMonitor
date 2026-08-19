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
    /// Creates and opens a DuckDB connection wrapped in an exclusive write lock.
    /// Use for UPDATE/DELETE/INSERT operations that must not race with archival or compaction.
    /// A 5-second timeout prevents UI freeze if archival currently holds the lock.
    /// </summary>
    internal async Task<LockedConnection> OpenWriteConnectionAsync()
    {
        var writeLock = _duckDb.AcquireWriteLock(timeout: TimeSpan.FromSeconds(5));
        try
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return new LockedConnection(connection, writeLock);
        }
        catch
        {
            writeLock.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Safely converts a DuckDB value to double, handling BigInteger from SUM aggregations.
    /// </summary>
    private static double ToDouble(object value)
    {
        if (value is BigInteger bi)
            return (double)bi;
        return Convert.ToDouble(value);
    }

    /// <summary>
    /// Safely converts a DuckDB value to long, handling BigInteger from SUM/COUNT aggregations.
    /// </summary>
    private static long ToInt64(object value)
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
    private static (DateTime startTime, DateTime endTime) GetTimeRange(int hoursBack, DateTime? fromDate, DateTime? toDate)
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
    private static (DateTime startTime, DateTime endTime) GetTimeRangeServerLocal(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var serverNow = DateTime.UtcNow.AddMinutes(ServerTimeHelper.UtcOffsetMinutes);

        if (fromDate.HasValue && toDate.HasValue)
        {
            /* fromDate/toDate are already in server time from the caller */
            return (fromDate.Value, toDate.Value);
        }

        return (serverNow.AddHours(-hoursBack), serverNow);
    }

    /// <summary>
    /// Starts query timing for performance logging. Use with 'using' statement.
    /// Only logs queries that exceed the slow query threshold (default 500ms).
    /// </summary>
    private static Helpers.QueryExecutionContext TimeQuery(string context, string sql)
    {
        return Helpers.QueryLogger.StartQuery(context, sql, source: "DuckDB");
    }

}
