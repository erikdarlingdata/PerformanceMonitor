/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The replica-side reads of Query Store routing (#6254, #6279). Every statement here reads at most ONE
/// <c>sys.query_store_*</c> view (the rule: never join two of them in one statement), each in its own statement,
/// and none of them touches the stats, payload or text statements.
/// </summary>
public sealed partial class QueryStoreCollector
{
    /// <summary>
    /// The flush interval, read for each probed database inside the primary's existing per-database loop, ONLY
    /// when the server has a replica set (so no new per-run statement where the enumeration already visits every
    /// database, and today's text when it is off). Own TRY/CATCH: the fact is advisory, and a failure here must
    /// never drop the database from the enumeration, which its probe already settled. A capturing secondary
    /// (#6279) is NOT read here: it is detected from the replica group of the rows the payload read already
    /// returns (<see cref="QueryStoreCapturingSecondaryTracker"/>).
    /// </summary>
    private const string FactsDeclareText = @"
DECLARE
    @facts TABLE (name sysname, flush_interval_seconds integer NULL);

DECLARE
    @facts_sql nvarchar(2000);";

    private const string FactsReadText = @"

        IF EXISTS
        (
            SELECT
                1
            FROM @result AS r
            WHERE r.name = @db
        )
        BEGIN
            BEGIN TRY
                SET @facts_sql = N'
    DECLARE
        @flush_interval_seconds integer;

    SELECT /* PerformanceMonitorLite */
        @flush_interval_seconds = qso.flush_interval_seconds
    FROM sys.database_query_store_options AS qso;

    SELECT /* PerformanceMonitorLite */ ' + QUOTENAME(@db, '''') + N', @flush_interval_seconds;';

                INSERT @facts (name, flush_interval_seconds)
                EXECUTE @exec_sp @facts_sql;
            END TRY
            BEGIN CATCH
                /* Advisory: no facts for this database means no look-back and today's cutoff. */
            END CATCH;
        END;";

    private const string FactsResultText = @"

/* Third result set (#6254): per probed database, its flush interval. Read after the two sets above by
   ReadPrimaryFactsAsync. */
SELECT /* PerformanceMonitorLite */
    name,
    flush_interval_seconds
FROM @facts
ORDER BY
    name;";

    private static string ApplyEnumerationFacts(string text, CollectorContext context)
    {
        if (!context.QueryStoreReplicaConfigured)
        {
            return text
                .Replace("/*FACTS_DECLARE*/", "", StringComparison.Ordinal)
                .Replace("/*FACTS_READ*/", "", StringComparison.Ordinal)
                .Replace("/*FACTS_RESULT*/", "", StringComparison.Ordinal);
        }

        return text
            .Replace("/*FACTS_DECLARE*/", FactsDeclareText, StringComparison.Ordinal)
            .Replace("/*FACTS_READ*/", FactsReadText, StringComparison.Ordinal)
            .Replace("/*FACTS_RESULT*/", FactsResultText, StringComparison.Ordinal);
    }

    private const string ReplicaDatabaseListQueryText = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @result TABLE
    (
        name sysname,
        actual_state integer NOT NULL,
        readonly_reason integer NOT NULL,
        flush_interval_seconds integer NULL
    );

DECLARE
    @probe_failures TABLE (name sysname, error_text nvarchar(4000));

DECLARE
    @db sysname,
    @sql nvarchar(2000),
    @exec_sp nvarchar(256);

/* The replica-mode enumeration (#6254). The primary's cursor skips every database that is a secondary here
   (primary replicas only) and its probe refuses readonly_reason & 8; this one admits them and
   returns what the routing decision needs to judge each: actual_state and readonly_reason (a readable,
   non-capturing secondary has bit 8 and state 1, or 0 on SQL Server 2025; a capturing one is state 4) and
   flush_interval_seconds. A capturing secondary elsewhere in the group is the PRIMARY's fact. The same screens as the primary's cursor decide which databases are candidates at all. */
DECLARE db_check CURSOR LOCAL FAST_FORWARD FOR
    SELECT /* PerformanceMonitorLite */
        d.name
    FROM sys.databases AS d
    WHERE d.database_id > 4
    AND   d.database_id < 32761
    AND   d.state_desc = N'ONLINE'
    AND   d.name <> N'PerformanceMonitor'
        AND   d.name NOT IN
          (
              N'master', N'model', N'msdb', N'tempdb',
              N'rdsadmin', N'gcloud_cloudsqladmin',
              N'ReportServer', N'ReportServerTempDB',
              N'DWConfiguration', N'DWDiagnostics', N'DWQueue',
              N'DBAUtil', N'DBAUtils', N'Utility'
          )
    /*EXCLUSION_FILTER*/
    OPTION(RECOMPILE);

OPEN db_check;

FETCH NEXT
FROM db_check
INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN" + NoDatabaseAccessNote.LoopGuardDbCheck + @"
    BEGIN TRY
        SET @sql = N'
    DECLARE
        @actual_state integer,
        @readonly_reason integer,
        @flush_interval_seconds integer;

    SELECT /* PerformanceMonitorLite */
        @actual_state = qso.actual_state,
        @readonly_reason = qso.readonly_reason,
        @flush_interval_seconds = qso.flush_interval_seconds
    FROM sys.database_query_store_options AS qso;

    SELECT /* PerformanceMonitorLite */ ' + QUOTENAME(@db, '''') + N', @actual_state, @readonly_reason, @flush_interval_seconds
    WHERE @actual_state IN (0, 1, 2, 4);';

        SET @exec_sp = QUOTENAME(@db) + N'.sys.sp_executesql';

        INSERT @result (name, actual_state, readonly_reason, flush_interval_seconds)
        EXECUTE @exec_sp @sql;
    END TRY
    BEGIN CATCH
        INSERT @probe_failures (name, error_text)
        VALUES (@db, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT
    FROM db_check
    INTO @db;
END;

CLOSE db_check;
DEALLOCATE db_check;

SELECT /* PerformanceMonitorLite */
    name,
    actual_state,
    readonly_reason,
    flush_interval_seconds
FROM @result
ORDER BY
    name;

SELECT /* PerformanceMonitorLite */
    name,
    error_text
FROM @probe_failures
ORDER BY
    name;";

    private const string RedoStateQueryText = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* The redo read (#6254): this server's own row per availability-group database. R, the redo lag, is the
   primary's last_commit_time minus the replica's; group_database_id is the same value on every replica of one
   database, so a mismatch means the replica host holds a different database of that name. One DMV and the
   database catalog, no Query Store view. */
SELECT /* PerformanceMonitorLite */
    database_name = d.name,
    drs.group_database_id,
    last_commit_time = drs.last_commit_time,
    drs.is_primary_replica
FROM sys.dm_hadr_database_replica_states AS drs
JOIN sys.databases AS d
    ON d.database_id = drs.database_id
WHERE drs.is_local = 1
ORDER BY
    d.name;";

    /// <summary>
    /// The replica-mode enumeration (#6254). Run it on the REPLICA connection. Returns two result sets: the
    /// candidate databases with <c>actual_state</c>, <c>readonly_reason</c> and <c>flush_interval_seconds</c> (read
    /// with <see cref="ReadReplicaEnumerationAsync"/>), then the probe failures.
    /// Null on Azure SQL Database, which never reads a replica.
    /// </summary>
    /// <param name="context">The run's context (scope and exclusion filters).</param>
    public CollectorQuery? BuildReplicaEnumerationQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = ReplicaDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    /// <summary>
    /// The redo read (#6254): <c>group_database_id</c>, <c>last_commit_time</c> and <c>is_primary_replica</c> of this
    /// server's own row (<c>is_local = 1</c>) for each availability-group database. Run it on BOTH connections;
    /// read with <see cref="ReadRedoStatesAsync"/>.
    /// </summary>
    public static CollectorQuery BuildRedoStateQuery() => new(RedoStateQueryText);

    /// <summary>
    /// Reads the replica enumeration's two result sets: the databases and the probe failures (same contract as
    /// the primary's enumeration, #1837).
    /// </summary>
    public static async Task<ReplicaEnumeration> ReadReplicaEnumerationAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var states = new List<QueryStoreReplicaDatabaseState>();
        while (await reader.ReadAsync(cancellationToken))
        {
            states.Add(new QueryStoreReplicaDatabaseState(
                reader.GetString(0),
                Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture)));
        }

        var failures = await EnumeratedCollectorDriver.ReadProbeFailuresAsync(reader, cancellationToken);
        return new ReplicaEnumeration(states, failures);
    }

    /// <summary>
    /// Reads the primary enumeration's THIRD result set (<see cref="BuildEnumerationQuery"/>, when the server has a replica set), after <see cref="EnumeratedCollectorDriver.ReadEnumerationAsync"/> has consumed the first two.
    /// An enumeration without the set (no replica set) returns an empty list.
    /// </summary>
    public static async Task<IReadOnlyList<QueryStorePrimaryDatabaseState>> ReadPrimaryFactsAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var facts = new List<QueryStorePrimaryDatabaseState>();
        if (!await reader.NextResultAsync(cancellationToken) || reader.FieldCount < 2)
        {
            return facts;
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            facts.Add(new QueryStorePrimaryDatabaseState(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture)));
        }

        return facts;
    }

    /// <summary>Reads <see cref="BuildRedoStateQuery"/>'s one result set, keyed by database name.</summary>
    public static async Task<IReadOnlyDictionary<string, QueryStoreAgRedoState>> ReadRedoStatesAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var states = new Dictionary<string, QueryStoreAgRedoState>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync(cancellationToken))
        {
            var name = reader.GetString(0);
            states[name] = new QueryStoreAgRedoState(
                name,
                reader.GetGuid(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                !reader.IsDBNull(3) && Convert.ToBoolean(reader.GetValue(3), CultureInfo.InvariantCulture));
        }

        return states;
    }

    /// <summary>The replica enumeration: databases with their facts, and the probe failures.</summary>
    public sealed record ReplicaEnumeration(
        IReadOnlyList<QueryStoreReplicaDatabaseState> Databases,
        IReadOnlyList<EnumerationProbeFailure> ProbeFailures);
}
