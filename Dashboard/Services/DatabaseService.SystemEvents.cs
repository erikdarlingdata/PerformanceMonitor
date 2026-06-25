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
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // System Events & HealthParser Data Access
        // ============================================

                public async Task<List<DefaultTraceEventItem>> GetDefaultTraceEventsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, string? eventNameFilter = null)
                {
                    var items = new List<DefaultTraceEventItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE dte.event_time >= @fromDate AND dte.event_time <= @toDate"
                        : "WHERE dte.event_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string eventFilter = !string.IsNullOrEmpty(eventNameFilter)
                        ? (dateFilter.Contains("WHERE", StringComparison.Ordinal) ? " AND dte.event_name LIKE @eventNameFilter" : "WHERE dte.event_name LIKE @eventNameFilter")
                        : "";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            dte.event_id,
            dte.collection_time,
            dte.event_time,
            dte.event_name,
            dte.event_class,
            dte.spid,
            dte.database_name,
            dte.database_id,
            dte.login_name,
            dte.host_name,
            dte.application_name,
            dte.server_name,
            dte.object_name,
            dte.filename,
            dte.integer_data,
            dte.integer_data_2,
            dte.text_data,
            dte.session_login_name,
            dte.error_number,
            dte.severity,
            dte.state,
            dte.event_sequence,
            dte.is_system,
            dte.request_id,
            dte.duration_us,
            dte.end_time
        FROM collect.default_trace_events AS dte
        {dateFilter}{eventFilter}
        ORDER BY
            dte.event_time DESC;";
        
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
        
                    if (!string.IsNullOrEmpty(eventNameFilter))
                    {
                        command.Parameters.Add(new SqlParameter("@eventNameFilter", SqlDbType.NVarChar, 200) { Value = eventNameFilter });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new DefaultTraceEventItem
                        {
                            EventId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.GetDateTime(2),
                            EventName = reader.GetString(3),
                            EventClass = reader.GetInt32(4),
                            Spid = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                            DatabaseName = reader.IsDBNull(6) ? null : reader.GetString(6),
                            DatabaseId = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            LoginName = reader.IsDBNull(8) ? null : reader.GetString(8),
                            HostName = reader.IsDBNull(9) ? null : reader.GetString(9),
                            ApplicationName = reader.IsDBNull(10) ? null : reader.GetString(10),
                            ServerName = reader.IsDBNull(11) ? null : reader.GetString(11),
                            ObjectName = reader.IsDBNull(12) ? null : reader.GetString(12),
                            Filename = reader.IsDBNull(13) ? null : reader.GetString(13),
                            IntegerData = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            IntegerData2 = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            TextData = reader.IsDBNull(16) ? null : reader.GetString(16),
                            SessionLoginName = reader.IsDBNull(17) ? null : reader.GetString(17),
                            ErrorNumber = reader.IsDBNull(18) ? null : reader.GetInt32(18),
                            Severity = reader.IsDBNull(19) ? null : reader.GetInt32(19),
                            State = reader.IsDBNull(20) ? null : reader.GetInt32(20),
                            EventSequence = reader.IsDBNull(21) ? null : reader.GetInt64(21),
                            IsSystem = reader.IsDBNull(22) ? null : reader.GetBoolean(22),
                            RequestId = reader.IsDBNull(23) ? null : reader.GetInt32(23),
                            DurationUs = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                            EndTime = reader.IsDBNull(25) ? null : reader.GetDateTime(25)
                        });
                    }
        
                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetDefaultTraceSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE dte.event_time >= @fromDate AND dte.event_time <= @toDate"
                        : "WHERE dte.event_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, dte.event_time), 0) AS bucket_hour,
    COUNT(*) AS event_count,
    COUNT(DISTINCT dte.event_name) AS distinct_events,
    COUNT(DISTINCT dte.database_name) AS distinct_databases
FROM collect.default_trace_events AS dte
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, dte.event_time), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var count = Convert.ToInt64(reader.GetValue(1));
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = count,
                            TotalCpu = Convert.ToDouble(reader.GetValue(2)),
                            TotalLogicalReads = Convert.ToDouble(reader.GetValue(3)),
                            Value = count,
                        });
                    }

                    return items;
                }

                public async Task<List<TraceAnalysisItem>> GetTraceAnalysisAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TraceAnalysisItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ta.collection_time >= @fromDate AND ta.collection_time <= @toDate"
                        : "WHERE ta.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ta.analysis_id,
            ta.collection_time,
            ta.trace_file_name,
            ta.event_class,
            ta.event_name,
            ta.database_name,
            ta.login_name,
            ta.nt_user_name,
            ta.application_name,
            ta.host_name,
            ta.spid,
            ta.duration_ms,
            ta.cpu_ms,
            ta.reads,
            ta.writes,
            ta.row_counts,
            ta.start_time,
            ta.end_time,
            ta.sql_text,
            ta.object_id,
            ta.client_process_id,
            ta.session_context
        FROM collect.trace_analysis AS ta
        {dateFilter}
        ORDER BY
            ta.collection_time DESC;";
        
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
                        items.Add(new TraceAnalysisItem
                        {
                            AnalysisId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            TraceFileName = reader.GetString(2),
                            EventClass = reader.GetInt32(3),
                            EventName = reader.GetString(4),
                            DatabaseName = reader.IsDBNull(5) ? null : reader.GetString(5),
                            LoginName = reader.IsDBNull(6) ? null : reader.GetString(6),
                            NtUserName = reader.IsDBNull(7) ? null : reader.GetString(7),
                            ApplicationName = reader.IsDBNull(8) ? null : reader.GetString(8),
                            HostName = reader.IsDBNull(9) ? null : reader.GetString(9),
                            Spid = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                            DurationMs = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            CpuMs = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            Reads = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            Writes = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            RowCounts = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            StartTime = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                            EndTime = reader.IsDBNull(17) ? null : reader.GetDateTime(17),
                            SqlText = reader.IsDBNull(18) ? null : reader.GetString(18),
                            ObjectId = reader.IsDBNull(19) ? null : reader.GetInt64(19),
                            ClientProcessId = reader.IsDBNull(20) ? null : reader.GetInt32(20),
                            SessionContext = reader.IsDBNull(21) ? null : reader.GetString(21)
                        });
                    }
        
                    return items;
                }

                public async Task<List<ServerConfigChangeItem>> GetServerConfigChangesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<ServerConfigChangeItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            scc.change_time,
            scc.configuration_name,
            scc.old_value_configured,
            scc.new_value_configured,
            scc.old_value_in_use,
            scc.new_value_in_use,
            scc.requires_restart,
            scc.change_description,
            scc.description,
            scc.is_dynamic,
            scc.is_advanced
        FROM report.server_configuration_changes AS scc
        WHERE scc.change_time >= @from_date AND scc.change_time <= @to_date
        ORDER BY scc.change_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            scc.change_time,
            scc.configuration_name,
            scc.old_value_configured,
            scc.new_value_configured,
            scc.old_value_in_use,
            scc.new_value_in_use,
            scc.requires_restart,
            scc.change_description,
            scc.description,
            scc.is_dynamic,
            scc.is_advanced
        FROM report.server_configuration_changes AS scc
        WHERE scc.change_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY scc.change_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new ServerConfigChangeItem
                        {
                            ChangeTime = reader.GetDateTime(0),
                            ConfigurationName = reader.GetString(1),
                            OldValueConfigured = reader.IsDBNull(2) ? string.Empty : reader.GetValue(2).ToString() ?? string.Empty,
                            NewValueConfigured = reader.IsDBNull(3) ? string.Empty : reader.GetValue(3).ToString() ?? string.Empty,
                            OldValueInUse = reader.IsDBNull(4) ? string.Empty : reader.GetValue(4).ToString() ?? string.Empty,
                            NewValueInUse = reader.IsDBNull(5) ? string.Empty : reader.GetValue(5).ToString() ?? string.Empty,
                            RequiresRestart = reader.GetInt32(6) == 1,
                            ChangeDescription = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            Description = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                            IsDynamic = reader.GetBoolean(9),
                            IsAdvanced = reader.GetBoolean(10)
                        });
                    }
        
                    return items;
                }

                public async Task<List<DatabaseConfigChangeItem>> GetDatabaseConfigChangesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<DatabaseConfigChangeItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            dcc.change_time,
            dcc.database_name,
            dcc.setting_type,
            dcc.setting_name,
            dcc.old_value,
            dcc.new_value,
            dcc.change_description
        FROM report.database_configuration_changes AS dcc
        WHERE dcc.change_time >= @from_date AND dcc.change_time <= @to_date
        ORDER BY dcc.change_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            dcc.change_time,
            dcc.database_name,
            dcc.setting_type,
            dcc.setting_name,
            dcc.old_value,
            dcc.new_value,
            dcc.change_description
        FROM report.database_configuration_changes AS dcc
        WHERE dcc.change_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY dcc.change_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new DatabaseConfigChangeItem
                        {
                            ChangeTime = reader.GetDateTime(0),
                            DatabaseName = reader.GetString(1),
                            SettingType = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            SettingName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            OldValue = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            NewValue = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            ChangeDescription = reader.IsDBNull(6) ? string.Empty : reader.GetString(6)
                        });
                    }
        
                    return items;
                }

                public async Task<List<TraceFlagChangeItem>> GetTraceFlagChangesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TraceFlagChangeItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            tfc.change_time,
            tfc.trace_flag,
            tfc.previous_status,
            tfc.new_status,
            tfc.scope,
            tfc.change_description,
            tfc.is_global,
            tfc.is_session
        FROM report.trace_flag_changes AS tfc
        WHERE tfc.change_time >= @from_date AND tfc.change_time <= @to_date
        ORDER BY tfc.change_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            tfc.change_time,
            tfc.trace_flag,
            tfc.previous_status,
            tfc.new_status,
            tfc.scope,
            tfc.change_description,
            tfc.is_global,
            tfc.is_session
        FROM report.trace_flag_changes AS tfc
        WHERE tfc.change_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY tfc.change_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TraceFlagChangeItem
                        {
                            ChangeTime = reader.GetDateTime(0),
                            TraceFlag = reader.GetInt32(1),
                            PreviousStatus = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            NewStatus = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            Scope = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            ChangeDescription = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            IsGlobal = reader.GetBoolean(6),
                            IsSession = reader.GetBoolean(7)
                        });
                    }
        
                    return items;
                }

                public async Task<List<CurrentServerConfigItem>> GetCurrentServerConfigAsync()
                {
                    var items = new List<CurrentServerConfigItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            latest AS
        (
            SELECT
                h.configuration_id,
                h.configuration_name,
                h.value_configured,
                h.value_in_use,
                h.value_minimum,
                h.value_maximum,
                h.is_dynamic,
                h.is_advanced,
                h.description,
                h.collection_time,
                rn = ROW_NUMBER() OVER (PARTITION BY h.configuration_id ORDER BY h.collection_time DESC)
            FROM config.server_configuration_history AS h
        )
        SELECT
            l.configuration_name,
            value_configured = CONVERT(nvarchar(100), l.value_configured),
            value_in_use = CONVERT(nvarchar(100), l.value_in_use),
            value_minimum = CONVERT(nvarchar(100), l.value_minimum),
            value_maximum = CONVERT(nvarchar(100), l.value_maximum),
            l.is_dynamic,
            l.is_advanced,
            l.description,
            last_changed = l.collection_time
        FROM latest AS l
        WHERE l.rn = 1
        ORDER BY
            l.configuration_name;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 30;

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CurrentServerConfigItem
                        {
                            ConfigurationName = reader.GetString(0),
                            ValueConfigured = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            ValueInUse = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            ValueMinimum = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            ValueMaximum = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            IsDynamic = reader.GetBoolean(5),
                            IsAdvanced = reader.GetBoolean(6),
                            Description = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            LastChanged = reader.GetDateTime(8)
                        });
                    }

                    return items;
                }

                public async Task<List<CurrentDatabaseConfigItem>> GetCurrentDatabaseConfigAsync()
                {
                    var items = new List<CurrentDatabaseConfigItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            latest AS
        (
            SELECT
                h.database_name,
                h.setting_type,
                h.setting_name,
                h.setting_value,
                h.collection_time,
                rn = ROW_NUMBER() OVER (
                    PARTITION BY h.database_name, h.setting_type, h.setting_name
                    ORDER BY h.collection_time DESC
                )
            FROM config.database_configuration_history AS h
        )
        SELECT
            l.database_name,
            l.setting_type,
            l.setting_name,
            setting_value = CONVERT(nvarchar(500), l.setting_value),
            last_changed = l.collection_time
        FROM latest AS l
        WHERE l.rn = 1
        ORDER BY
            l.database_name,
            l.setting_type,
            l.setting_name;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 30;

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CurrentDatabaseConfigItem
                        {
                            DatabaseName = reader.GetString(0),
                            SettingType = reader.GetString(1),
                            SettingName = reader.GetString(2),
                            SettingValue = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            LastChanged = reader.GetDateTime(4)
                        });
                    }

                    return items;
                }

                public async Task<List<CurrentTraceFlagItem>> GetCurrentTraceFlagsAsync()
                {
                    var items = new List<CurrentTraceFlagItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            latest AS
        (
            SELECT
                h.trace_flag,
                h.status,
                h.is_global,
                h.is_session,
                h.collection_time,
                rn = ROW_NUMBER() OVER (PARTITION BY h.trace_flag ORDER BY h.collection_time DESC)
            FROM config.trace_flags_history AS h
        )
        SELECT
            l.trace_flag,
            l.status,
            l.is_global,
            l.is_session,
            last_changed = l.collection_time
        FROM latest AS l
        WHERE l.rn = 1
        ORDER BY
            l.trace_flag;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 30;

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CurrentTraceFlagItem
                        {
                            TraceFlag = reader.GetInt32(0),
                            Status = reader.GetBoolean(1),
                            IsGlobal = reader.GetBoolean(2),
                            IsSession = reader.GetBoolean(3),
                            LastChanged = reader.GetDateTime(4)
                        });
                    }

                    return items;
                }
    }
}
