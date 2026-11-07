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
/// Schema tracking (#7643): the Extended Events session that records committed table, view, index and trigger DDL, so the schema
/// pass re-reads only the objects that changed. One per install, named from the install's id like the long-query session, so two
/// installs that monitor the same server never share one. Its ring buffer is read by the schema pass; nothing else reads it.
///
/// <para><b>No statement text.</b> The session has no <c>sql_text</c> action: the event says which object changed, who changed it
/// and from where, and the pass reads the new definition from the catalog. Nothing a user typed leaves the monitored server.</para>
///
/// <para><b>What it records.</b> <c>object_created</c>, <c>object_altered</c> and <c>object_deleted</c> at <c>ddl_phase = 1</c>
/// (the commit phase, so a rolled-back ALTER leaves no event), outside the system databases, and never for a statistics object
/// (<c>object_type</c> 21587, which an auto-created statistic would otherwise raise on every query). <c>sp_rename</c> raises none of
/// those events, so <c>module_end</c> of that procedure stands in for it.</para>
/// </summary>
public static class SchemaChangesXeSession
{
    /// <summary>The suffix every session name ends with.</summary>
    public const string SessionSuffix = "_SchemaChanges";

    /// <summary>The ring buffer's event cap. Past it the oldest event is lost, and the pass reconciles by a full read.</summary>
    public const int MaxEventsLimit = 10000;

    /// <summary>The ring buffer's memory in KB.</summary>
    public const int MaxMemoryKb = 4096;

    private const string NamePrefix = "PerformanceMonitor_";
    private const string Product = "Darling";

    /// <summary>
    /// This install's session name: <c>PerformanceMonitor_Darling_{id}_SchemaChanges</c>. Throws for an id that fails
    /// <see cref="InstallId.IsValid"/>; the check also keeps the name safe to put in a statement, because the id is hex only.
    /// </summary>
    public static string NameFor(string? installId)
    {
        if (!InstallId.IsValid(installId))
        {
            throw new ArgumentException("The install id is missing or not eight lowercase hex digits.", nameof(installId));
        }

        return NamePrefix + Product + "_" + installId + SessionSuffix;
    }

    /// <summary><see cref="NameFor"/>, or null when <paramref name="installId"/> does not pass <see cref="InstallId.IsValid"/>.</summary>
    public static string? TryNameFor(string? installId) => InstallId.IsValid(installId) ? NameFor(installId) : null;

    /// <summary>True when <paramref name="name"/> is a name <see cref="NameFor"/> makes, for any install.</summary>
    public static bool IsInstallSessionName(string? name)
    {
        const string head = NamePrefix + Product + "_";
        if (name is null
            || name.Length != head.Length + InstallId.Length + SessionSuffix.Length
            || !name.StartsWith(head, StringComparison.Ordinal)
            || !name.EndsWith(SessionSuffix, StringComparison.Ordinal))
        {
            return false;
        }

        return InstallId.IsValid(name.Substring(head.Length, InstallId.Length));
    }

    /* The statement builders interpolate the name, so each takes only a name this class made. */
    private static string Require(string sessionName)
    {
        if (!IsInstallSessionName(sessionName))
        {
            throw new ArgumentException("Not a schema-changes session name.", nameof(sessionName));
        }

        return sessionName;
    }

    private static string Scope(bool databaseScoped) => databaseScoped ? "DATABASE" : "SERVER";

    /// <summary>
    /// The <c>CREATE EVENT SESSION</c>. Server scope: <c>server_principal_name</c> and <c>MEMORY_PARTITION_MODE = NONE</c> (one
    /// readable ring buffer). Database scope (Azure SQL Database): <c>username</c> in place of <c>server_principal_name</c>, which
    /// the service refuses there (#3753), and no partition mode, which it does not accept. The <c>module_end</c> predicate also drops
    /// <c>source_database_id = 32767</c> there: that is the resource database's id, a fact of the on-premises engine that has not been
    /// checked on Azure SQL Database, and <c>object_name</c> alone still limits the event to <c>sp_rename</c>.
    /// </summary>
    public static string BuildCreateSql(string name, bool databaseScoped)
    {
        Require(name);
        var principal = databaseScoped ? "sqlserver.username" : "sqlserver.server_principal_name";
        var partitionMode = databaseScoped ? "" : "\n    MEMORY_PARTITION_MODE = NONE,";
        var renameWhere = databaseScoped
            ? "object_name = N'sp_rename'"
            : "source_database_id = 32767\n        AND object_name = N'sp_rename'";
        var actions = $@"
        sqlserver.database_name,
        {principal},
        sqlserver.client_app_name,
        sqlserver.client_hostname,
        package0.event_sequence";
        var ddlWhere = "ddl_phase = 1\n        AND database_id > 4\n        AND object_type <> 21587";

        return $@"
CREATE EVENT SESSION [{name}]
ON {Scope(databaseScoped)}
ADD EVENT sqlserver.object_created
(
    ACTION
    ({actions}
    )
    WHERE {ddlWhere}
),
ADD EVENT sqlserver.object_altered
(
    ACTION
    ({actions}
    )
    WHERE {ddlWhere}
),
ADD EVENT sqlserver.object_deleted
(
    ACTION
    ({actions}
    )
    WHERE {ddlWhere}
),
ADD EVENT sqlserver.module_end
(
    ACTION
    ({actions}
    )
    WHERE {renameWhere}
)
ADD TARGET package0.ring_buffer
(
    SET
        max_memory = {MaxMemoryKb},
        MAX_EVENTS_LIMIT = {MaxEventsLimit}
)
WITH
(
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,{partitionMode}
    STARTUP_STATE = OFF
);";
    }

    /// <summary>Starts the session. CREATE does not, and the DDL says <c>STARTUP_STATE = OFF</c>.</summary>
    public static string BuildStartSql(string name, bool databaseScoped) =>
        $"ALTER EVENT SESSION [{Require(name)}] ON {Scope(databaseScoped)} STATE = START;";

    /// <summary>
    /// Stops the database-scoped session where it runs on the connection's replica, as the long-query session's stop does
    /// (<see cref="LongQueryCompletionsCollector.BuildStopSessionSql"/>): the cleanup verb sends it before a drop.
    /// </summary>
    public static string BuildStopSql(string name) => $@"
IF EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions
    WHERE name = N'{Require(name)}'
)
BEGIN
    ALTER EVENT SESSION [{name}] ON DATABASE STATE = STOP;
END;";

    /// <summary>Drops the session when it exists, in the catalog of its scope; a clean no-op otherwise.</summary>
    public static string BuildDropSql(string name, bool databaseScoped)
    {
        Require(name);
        var catalog = databaseScoped ? "sys.database_event_sessions" : "sys.server_event_sessions";
        return $@"
IF EXISTS
(
    SELECT
        1/0
    FROM {catalog}
    WHERE name = N'{name}'
)
BEGIN
    DROP EVENT SESSION [{name}] ON {Scope(databaseScoped)};
END;";
    }

    /// <summary>
    /// One row, always: <c>exists</c> (the definition is in the catalog), <c>is_running</c> (it runs on this connection's server or
    /// replica) and <c>create_time</c> (when it last started, in UTC; null when it does not run). The create time moves on every
    /// start, so a pass that stored the last one sees a restart, and with it a gap in what the session saw. Takes <c>@session_name</c>.
    /// </summary>
    public static string BuildStateProbeSql(bool databaseScoped)
    {
        var (catalog, running) = databaseScoped
            ? ("sys.database_event_sessions", "sys.dm_xe_database_sessions")
            : ("sys.server_event_sessions", "sys.dm_xe_sessions");
        return $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    [exists] = CASE WHEN ses.name IS NOT NULL THEN 1 ELSE 0 END,
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END,
    create_time = DATEADD(MINUTE, DATEDIFF(MINUTE, SYSDATETIME(), SYSUTCDATETIME()), dxs.create_time)
FROM (SELECT name = @session_name) AS want
LEFT JOIN {catalog} AS ses
  ON ses.name = want.name
LEFT JOIN {running} AS dxs
  ON dxs.name = want.name;";
    }
}
