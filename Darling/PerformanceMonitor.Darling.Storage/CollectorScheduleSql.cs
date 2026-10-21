/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The two <c>config.config_collector_schedules</c> columns a single-column write can set or clear.</summary>
public enum CollectorScheduleColumn
{
    /// <summary><c>frequency_minutes</c>.</summary>
    FrequencyMinutes,

    /// <summary><c>retention_days</c>.</summary>
    RetentionDays,
}

/// <summary>
/// The write statements for <c>config.config_collector_schedules</c>'s frequency and retention (#6227), kept in Storage so the
/// Viewer's schedule editor and the service's command plane (and so the MCP tools) run the same SQL. The sparse table's two
/// partial unique indexes (V17) are the ON CONFLICT arbiters: a primary key cannot span the nullable <c>server_id</c>.
///
/// <para>The Viewer writes a whole scope at a time (delete the scope's rows, insert them again: <see cref="FleetRowUpsert"/> and
/// <see cref="ServerRowUpsert"/>). The single-column statements here touch ONE column and leave the rest of the row alone, so a
/// change to a collector's frequency keeps its enabled flag, retention and database scope.</para>
/// </summary>
public static class CollectorScheduleSql
{
    /// <summary>Upserts one FLEET-WIDE override row (server_id NULL), every column. $1 collector_name, $2 frequency (nullable),
    /// $3 retention (nullable), $4 enabled, $5 databases (nullable, the V125 scope; NULL and empty are distinct).</summary>
    public const string FleetRowUpsert = @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled, databases)
VALUES (NULL, $1, $2, $3, $4, $5)
ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET
    frequency_minutes = EXCLUDED.frequency_minutes,
    retention_days = EXCLUDED.retention_days,
    enabled = EXCLUDED.enabled,
    databases = EXCLUDED.databases";

    /// <summary>Upserts one PER-SERVER override row, every column. $1 server_id, $2 collector_name, $3 frequency (nullable),
    /// $4 retention (nullable), $5 enabled, $6 databases (nullable, the V125 scope).</summary>
    public const string ServerRowUpsert = @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled, databases)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET
    frequency_minutes = EXCLUDED.frequency_minutes,
    retention_days = EXCLUDED.retention_days,
    enabled = EXCLUDED.enabled,
    databases = EXCLUDED.databases";

    private static string Name(CollectorScheduleColumn column) =>
        column == CollectorScheduleColumn.FrequencyMinutes ? "frequency_minutes" : "retention_days";

    /// <summary>
    /// Sets one column on the fleet-wide row, creating the row when there is none. A NEW row stores <c>enabled</c> as NULL
    /// ("inherit": V206), so setting a frequency on a collector that ships OFF does not switch it on, and a later fleet-wide enable or
    /// disable still decides it. An existing row keeps its enabled flag and every other column. The row is found the way the resolver
    /// finds it, ignoring the case of the collector name, because the unique index does not: a row an older writer left as
    /// <c>Wait_Stats</c> is the one updated, not a second row beside it. $1 collector_name, $2 the value.
    /// </summary>
    public static string SetFleetColumn(CollectorScheduleColumn column) =>
        "WITH changed AS (UPDATE config.config_collector_schedules SET " + Name(column) + " = $2::int " +
        "WHERE server_id IS NULL AND lower(collector_name) = lower($1::text) RETURNING 1) " +
        "INSERT INTO config.config_collector_schedules (server_id, collector_name, " + Name(column) + ", enabled) " +
        "SELECT NULL, $1::text, $2::int, NULL WHERE NOT EXISTS (SELECT 1 FROM changed) " +
        "ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET " + Name(column) + " = EXCLUDED." + Name(column);

    /// <summary>
    /// Sets one column on one server's row, creating the row when there is none. A NEW row stores <c>enabled</c> as NULL ("inherit":
    /// V206): it changes no collector's on/off state, and a later fleet-wide disable still reaches this server. The row is found the way
    /// the resolver finds it, ignoring the case of the collector name (see <see cref="SetFleetColumn"/>). $1 server_id, $2 collector_name,
    /// $3 the value.
    /// </summary>
    public static string SetServerColumn(CollectorScheduleColumn column) =>
        "WITH changed AS (UPDATE config.config_collector_schedules SET " + Name(column) + " = $3::int " +
        "WHERE server_id = $1::int AND lower(collector_name) = lower($2::text) RETURNING 1) " +
        "INSERT INTO config.config_collector_schedules (server_id, collector_name, " + Name(column) + ", enabled) " +
        "SELECT $1::int, $2::text, $3::int, NULL WHERE NOT EXISTS (SELECT 1 FROM changed) " +
        "ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET " + Name(column) + " = EXCLUDED." + Name(column);

    /// <summary>Clears one column on the fleet-wide row (back to the code default). A row that is not there is left alone: there is
    /// nothing to clear, and an INSERT would create a row the operator never asked for. The name is matched ignoring case, as the
    /// resolver does. $1 collector_name.</summary>
    public static string ClearFleetColumn(CollectorScheduleColumn column) =>
        "UPDATE config.config_collector_schedules SET " + Name(column) + " = NULL WHERE server_id IS NULL AND lower(collector_name) = lower($1)";

    /// <summary>Clears one column on one server's row (back to the fleet-wide value, else the code default). The name is matched
    /// ignoring case, as the resolver does. $1 server_id, $2 collector_name.</summary>
    public static string ClearServerColumn(CollectorScheduleColumn column) =>
        "UPDATE config.config_collector_schedules SET " + Name(column) + " = NULL WHERE server_id = $1 AND lower(collector_name) = lower($2)";

    /// <summary>
    /// Puts the fleet-wide row's <c>enabled</c> back to NULL ("inherit": the collector's shipped default applies). A row that is not
    /// there is left alone. The name is matched ignoring case. $1 collector_name.
    /// </summary>
    public const string InheritFleetEnabled =
        "UPDATE config.config_collector_schedules SET enabled = NULL WHERE server_id IS NULL AND lower(collector_name) = lower($1)";

    /// <summary>
    /// Puts one server's row's <c>enabled</c> back to NULL ("inherit": the fleet-wide row's value, else the shipped default), so a
    /// later fleet-wide enable or disable reaches this server again. A row that is not there is left alone. The name is matched ignoring
    /// case. $1 server_id, $2 collector_name.
    /// </summary>
    public const string InheritServerEnabled =
        "UPDATE config.config_collector_schedules SET enabled = NULL WHERE server_id = $1 AND lower(collector_name) = lower($2)";
}
