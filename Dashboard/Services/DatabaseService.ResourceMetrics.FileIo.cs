/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {

                public async Task<List<FileIoLatencyItem>> GetFileIoLatencyAsync()
                {
                    var items = new List<FileIoLatencyItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            database_name,
                            file_type,
                            file_name,
                            avg_read_latency_ms,
                            avg_write_latency_ms,
                            reads_last_15min,
                            writes_last_15min,
                            latency_issue,
                            recommendation,
                            last_seen
                        FROM report.file_io_latency
                        ORDER BY
                            avg_read_latency_ms DESC,
                            avg_write_latency_ms DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoLatencyItem
                        {
                            DatabaseName = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                            FileType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            AvgReadLatencyMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                            AvgWriteLatencyMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture),
                            ReadsLast15Min = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                            WritesLast15Min = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                            LatencyIssue = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            Recommendation = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                            LastSeen = reader.IsDBNull(9) ? DateTime.MinValue : reader.GetDateTime(9)
                        });
                    }
        
                    return items;
                }

                public async Task<List<FileIoDataPoint>> GetFileIoDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<FileIoDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                fio.collection_time,
                                fio.database_name,
                                fio.file_name,
                                fio.file_type_desc,
                                avg_read_latency_ms =
                                    CASE
                                        WHEN fio.num_of_reads > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_read_ms * 1.0 / fio.num_of_reads)
                                        ELSE 0
                                    END,
                                avg_write_latency_ms =
                                    CASE
                                        WHEN fio.num_of_writes > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_write_ms * 1.0 / fio.num_of_writes)
                                        ELSE 0
                                    END
                            FROM collect.file_io_stats AS fio
                            WHERE fio.collection_time >= @from_date
                            AND   fio.collection_time <= @to_date
                            ORDER BY
                                fio.collection_time ASC,
                                fio.database_name,
                                fio.file_name;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                fio.collection_time,
                                fio.database_name,
                                fio.file_name,
                                fio.file_type_desc,
                                avg_read_latency_ms =
                                    CASE
                                        WHEN fio.num_of_reads > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_read_ms * 1.0 / fio.num_of_reads)
                                        ELSE 0
                                    END,
                                avg_write_latency_ms =
                                    CASE
                                        WHEN fio.num_of_writes > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_write_ms * 1.0 / fio.num_of_writes)
                                        ELSE 0
                                    END
                            FROM collect.file_io_stats AS fio
                            WHERE fio.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                fio.collection_time ASC,
                                fio.database_name,
                                fio.file_name;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoDataPoint
                        {
                            CollectionTime = reader.GetDateTime(0),
                            DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            FileType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            AvgReadLatencyMs = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            AvgWriteLatencyMs = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5)
                        });
                    }
        
                    return items;
                }

                public async Task<List<FileIoLatencyTimeSeriesItem>> GetFileIoLatencyTimeSeriesAsync(bool isTempDb, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<FileIoLatencyTimeSeriesItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND fio.collection_time >= @fromDate AND fio.collection_time <= @toDate"
                        : "AND fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string dbFilter = isTempDb
                        ? "AND fio.database_name = N'tempdb'"
                        : "AND fio.database_name <> N'tempdb'";
        
                    // Get files that have had latency issues (outliers)
                    // Only include files with at least some I/O activity and meaningful latency
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            file_avg_latency AS
        (
            SELECT
                fio.database_name,
                fio.file_name,
                fio.file_type_desc,
                avg_read_latency_ms =
                    CASE
                        WHEN SUM(fio.num_of_reads_delta) > 0
                        THEN CONVERT(decimal(19,2), SUM(fio.io_stall_read_ms_delta) * 1.0 / SUM(fio.num_of_reads_delta))
                        ELSE 0
                    END,
                avg_write_latency_ms =
                    CASE
                        WHEN SUM(fio.num_of_writes_delta) > 0
                        THEN CONVERT(decimal(19,2), SUM(fio.io_stall_write_ms_delta) * 1.0 / SUM(fio.num_of_writes_delta))
                        ELSE 0
                    END,
                total_reads = SUM(ISNULL(fio.num_of_reads_delta, 0)),
                total_writes = SUM(ISNULL(fio.num_of_writes_delta, 0))
            FROM collect.file_io_stats AS fio
            WHERE fio.database_name IS NOT NULL
            {dbFilter}
            {dateFilter}
            GROUP BY
                fio.database_name,
                fio.file_name,
                fio.file_type_desc
            HAVING
                SUM(ISNULL(fio.num_of_reads_delta, 0)) + SUM(ISNULL(fio.num_of_writes_delta, 0)) > 0
        ),
            top_files AS
        (
            SELECT TOP (10)
                fal.database_name,
                fal.file_name,
                fal.file_type_desc
            FROM file_avg_latency AS fal
            ORDER BY
                fal.total_reads + fal.total_writes DESC
        )
        SELECT
            fio.collection_time,
            fio.database_name,
            fio.file_name,
            fio.file_type_desc,
            read_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_reads_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), fio.io_stall_read_ms_delta * 1.0 / fio.num_of_reads_delta)
                    ELSE 0
                END,
            write_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_writes_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), fio.io_stall_write_ms_delta * 1.0 / fio.num_of_writes_delta)
                    ELSE 0
                END,
            read_queued_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_reads_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), ISNULL(fio.io_stall_queued_read_ms_delta, 0) * 1.0 / fio.num_of_reads_delta)
                    ELSE 0
                END,
            write_queued_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_writes_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), ISNULL(fio.io_stall_queued_write_ms_delta, 0) * 1.0 / fio.num_of_writes_delta)
                    ELSE 0
                END,
            read_count = ISNULL(fio.num_of_reads_delta, 0),
            write_count = ISNULL(fio.num_of_writes_delta, 0)
        FROM collect.file_io_stats AS fio
        JOIN top_files AS tf
          ON  tf.database_name = fio.database_name
          AND tf.file_name = fio.file_name
        WHERE fio.database_name IS NOT NULL
        {dbFilter}
        {dateFilter}
        ORDER BY
            fio.collection_time,
            fio.database_name,
            fio.file_name;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoLatencyTimeSeriesItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            FileType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            ReadLatencyMs = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            WriteLatencyMs = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                            ReadQueuedLatencyMs = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            WriteQueuedLatencyMs = reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
                            ReadCount = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                            WriteCount = reader.IsDBNull(9) ? 0 : reader.GetInt64(9)
                        });
                    }
        
                    return items;
                }

                public async Task<List<FileIoLatencyTimeSeriesItem>> GetFileIoThroughputTimeSeriesAsync(bool isTempDb, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<FileIoLatencyTimeSeriesItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND fio.collection_time >= @fromDate AND fio.collection_time <= @toDate"
                        : "AND fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";

                    string dbFilter = isTempDb
                        ? "AND fio.database_name = N'tempdb'"
                        : "AND fio.database_name <> N'tempdb'";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            file_avg_throughput AS
        (
            SELECT
                fio.database_name,
                fio.file_name,
                fio.file_type_desc,
                total_bytes = SUM(ISNULL(fio.num_of_bytes_read_delta, 0)) + SUM(ISNULL(fio.num_of_bytes_written_delta, 0)),
                total_io = SUM(ISNULL(fio.num_of_reads_delta, 0)) + SUM(ISNULL(fio.num_of_writes_delta, 0))
            FROM collect.file_io_stats AS fio
            WHERE fio.database_name IS NOT NULL
            {dbFilter}
            {dateFilter}
            GROUP BY
                fio.database_name,
                fio.file_name,
                fio.file_type_desc
            HAVING
                SUM(ISNULL(fio.num_of_bytes_read_delta, 0)) + SUM(ISNULL(fio.num_of_bytes_written_delta, 0)) > 0
        ),
            top_files AS
        (
            SELECT TOP (10)
                fat.database_name,
                fat.file_name,
                fat.file_type_desc
            FROM file_avg_throughput AS fat
            ORDER BY
                fat.total_bytes DESC
        )
        SELECT
            fio.collection_time,
            fio.database_name,
            fio.file_name,
            fio.file_type_desc,
            read_throughput_mb_per_sec =
                CASE
                    WHEN ISNULL(fio.sample_ms_delta, 0) > 0
                    THEN CONVERT(decimal(19,4), fio.num_of_bytes_read_delta * 1000.0 / fio.sample_ms_delta / 1048576.0)
                    ELSE 0
                END,
            write_throughput_mb_per_sec =
                CASE
                    WHEN ISNULL(fio.sample_ms_delta, 0) > 0
                    THEN CONVERT(decimal(19,4), fio.num_of_bytes_written_delta * 1000.0 / fio.sample_ms_delta / 1048576.0)
                    ELSE 0
                END,
            read_count = ISNULL(fio.num_of_reads_delta, 0),
            write_count = ISNULL(fio.num_of_writes_delta, 0)
        FROM collect.file_io_stats AS fio
        JOIN top_files AS tf
          ON  tf.database_name = fio.database_name
          AND tf.file_name = fio.file_name
        WHERE fio.database_name IS NOT NULL
        {dbFilter}
        {dateFilter}
        ORDER BY
            fio.collection_time,
            fio.database_name,
            fio.file_name;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoLatencyTimeSeriesItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            FileType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            ReadThroughputMbPerSec = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            WriteThroughputMbPerSec = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                            ReadCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            WriteCount = reader.IsDBNull(7) ? 0 : reader.GetInt64(7)
                        });
                    }

                    return items;
                }
    }
}
