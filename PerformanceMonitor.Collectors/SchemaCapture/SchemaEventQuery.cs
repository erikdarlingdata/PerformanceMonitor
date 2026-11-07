/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: the statement that reads the schema-changes session's ring buffer. Two result sets. The first is one row, always: the
/// session's <c>create_time</c> (UTC; null when the session does not run), the target's <c>totalEventsProcessed</c> and
/// <c>eventCount</c>, and the ring-buffer truncation columns (<see cref="RingBufferXml.TruncationColumnsSql"/>). The second is the
/// events after <c>@cutoff_time</c>, lowest sequence first. The time filter sits inside the XQuery (<c>exist()</c>), so an event
/// older than the cutoff is never shredded. The caller passes the stored last event time less a second, so events that share the
/// stored event's timestamp are read again; <see cref="SchemaWorkPlanner"/> drops those by sequence.
/// </summary>
public static class SchemaEventQuery
{
    /// <summary>
    /// The batch for <paramref name="sessionName"/> (a name <see cref="SchemaChangesXeSession"/> made). Takes <c>@cutoff_time datetime2</c>.
    /// <paramref name="databaseScoped"/> reads the database-scoped session of Azure SQL Database.
    /// </summary>
    public static string Build(string sessionName, bool databaseScoped)
    {
        if (!SchemaChangesXeSession.IsInstallSessionName(sessionName))
        {
            throw new ArgumentException("Not a schema-changes session name.", nameof(sessionName));
        }

        var sessions = databaseScoped ? "sys.dm_xe_database_sessions" : "sys.dm_xe_sessions";
        var targets = databaseScoped ? "sys.dm_xe_database_session_targets" : "sys.dm_xe_session_targets";
        var login = databaseScoped ? "username" : "server_principal_name";

        return $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

{RingBufferXml.DeclareSql}

SELECT /* PerformanceMonitorLite */
    @ring_text = CONVERT(nvarchar(max), xet.target_data),
    @ring_session = xes.name
FROM {targets} AS xet
JOIN {sessions} AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{sessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

{RingBufferXml.ParseSql}

{RingBufferXml.TruncationSql}

SELECT
    create_time =
    (
        SELECT
            DATEADD(MINUTE, DATEDIFF(MINUTE, SYSDATETIME(), SYSUTCDATETIME()), dxs.create_time)
        FROM {sessions} AS dxs
        WHERE dxs.name = N'{sessionName}'
    ),
    total_events_processed = @ring_xml.value('(RingBufferTarget/@totalEventsProcessed)[1]', 'bigint'),
    event_count = @ring_xml.value('(RingBufferTarget/@eventCount)[1]', 'bigint'),
    {RingBufferXml.TruncationColumnsSql};

SELECT
    event_name = evt.value('(@name)[1]', 'nvarchar(60)'),
    event_sequence = evt.value('(action[@name=""event_sequence""]/value)[1]', 'bigint'),
    event_time_utc = evt.value('(@timestamp)[1]', 'datetime2'),
    database_name = evt.value('(action[@name=""database_name""]/value)[1]', 'nvarchar(128)'),
    object_id = evt.value('(data[@name=""object_id""]/value)[1]', 'integer'),
    object_type = TRY_CONVERT(integer, evt.value('(data[@name=""object_type""]/value)[1]', 'nvarchar(20)')),
    index_id = evt.value('(data[@name=""index_id""]/value)[1]', 'integer'),
    object_name = evt.value('(data[@name=""object_name""]/value)[1]', 'nvarchar(256)'),
    related_object_id = NULLIF(evt.value('(data[@name=""related_object_id""]/value)[1]', 'integer'), 0),
    login = evt.value('(action[@name=""{login}""]/value)[1]', 'nvarchar(256)'),
    client_app = evt.value('(action[@name=""client_app_name""]/value)[1]', 'nvarchar(256)'),
    client_host = evt.value('(action[@name=""client_hostname""]/value)[1]', 'nvarchar(256)')
FROM @ring_xml.nodes('RingBufferTarget/event') AS e(evt)
WHERE evt.exist('@timestamp[. > sql:variable(""@cutoff_time"")]') = 1
ORDER BY
    evt.value('(action[@name=""event_sequence""]/value)[1]', 'bigint')
OPTION(RECOMPILE);";
    }
}
