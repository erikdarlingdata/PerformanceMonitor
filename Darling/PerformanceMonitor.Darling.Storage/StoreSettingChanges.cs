/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One setting of the store, as <c>pg_settings</c> shows it to the service's own connection.</summary>
public sealed record StoreSettingReading(
    string Name,
    string? Setting,
    string? Unit,
    string? Source,
    string? SourceFile,
    int? SourceLine,
    bool? PendingRestart);

/// <summary>What one <see cref="StoreSettingChanges.CaptureAsync"/> did: change rows written, settings stored or refreshed in the last-seen state, settings dropped from it, and the state's size before the pass.</summary>
public readonly record struct StoreSettingCapture(int Changes, int Stored, int Dropped, int StateRowsBefore)
{
    /// <summary>True for the first capture on a store (or the first after the rung): the state was empty, so the pass stored a baseline and wrote no change row.</summary>
    public bool IsBaseline => StateRowsBefore == 0;
}

/// <summary>One recorded change of a store setting, newest first in a read.</summary>
public sealed record StoreSettingChange(
    long ChangeId,
    DateTime SeenAt,
    DateTime? ConfLoadTime,
    string SettingName,
    string? Unit,
    string? OldValue,
    string? NewValue,
    string? OldSource,
    string? NewSource,
    string? OldSourceFile,
    string? NewSourceFile,
    int? OldSourceLine,
    int? NewSourceLine,
    bool? OldPendingRestart,
    bool? NewPendingRestart);

/// <summary>
/// #9427: a row for each change to a setting of the monitoring store between service starts. The store settings check (#9418) reads the
/// settings once at start, so an <c>ALTER SYSTEM</c> plus <c>pg_reload_conf()</c> for a settings trial, or a setting overridden by hand,
/// left no record of when it changed, what it changed from, or where the new value came from.
///
/// <para><b>Shape (V313).</b> <c>collect.store_setting_state</c> is the last captured reading, one row per setting. <c>collect.store_setting_changes</c>
/// has one row per change: the setting, its unit, the old and the new value, source, source file and line, and <c>pending_restart</c>, when the
/// capture saw it (<c>seen_at</c>) and the time the server last loaded its configuration (<c>conf_load_time</c>, from
/// <c>pg_conf_load_time()</c>). Both are plain tables with naive UTC times.</para>
///
/// <para><b>What counts as a change.</b> A setting already in the state whose value, source or <c>pending_restart</c> differs from it, compared
/// NULL-safely: a role that cannot read a setting's value sees NULL, and a source file or line is NULL for a source that is not a file. A move of the
/// source file or line alone, or of the unit, refreshes the state and writes no row. A setting that was not in the state (an extension that
/// loaded and added its own) joins it with no row, and one that left <c>pg_settings</c> leaves it.</para>
///
/// <para><b>Baseline.</b> The first capture on a store, and the first after the rung, finds the state empty: it stores every setting and writes no
/// change row. <see cref="StoreSettingCapture.IsBaseline"/> says so.</para>
///
/// <para><b>Secrets.</b> A value goes through <see cref="PgSettingRedactor.Redact"/> before it is compared or stored, as the collectors do, so a
/// <c>primary_conninfo</c> password never lands here. A change to the secret part alone reads as no change.</para>
///
/// <para><b>Writes.</b> The state is written only for a setting whose reading differs from it, so a minute with no change writes nothing: a full
/// rewrite of the whole setting list (over 500 rows on PostgreSQL 18) every minute would leave that many dead row versions each time.</para>
///
/// <para><b>Retention.</b> <see cref="StoreSelfMetrics.RetentionDays"/>, the self-metrics' own. The delete runs in the capture that wrote a change:
/// rows only arrive with a change, so the table cannot grow without one.</para>
/// </summary>
public static class StoreSettingChanges
{
    /// <summary>The store schema version that adds the two tables. The one place the number is written.</summary>
    public const int Version = 313;

    /// <summary>Per-command timeout, in seconds. The capture runs on the one-minute pass, so it is far short of the hourly sweep's.</summary>
    public const int CaptureTimeoutSeconds = 30;

    /// <summary>The window's ceiling, in days: the retention.</summary>
    public const int RetentionDays = StoreSelfMetrics.RetentionDays;

    /// <summary>
    /// The rung (V313): two empty plain tables, no data moves. An older build never reads them.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_setting_state
(
    setting_name text NOT NULL,
    setting text,
    unit text,
    source text,
    sourcefile text,
    sourceline integer,
    pending_restart boolean,
    tracked_since timestamp(6) without time zone NOT NULL,
    CONSTRAINT pk_store_setting_state PRIMARY KEY (setting_name)
);

CREATE TABLE IF NOT EXISTS collect.store_setting_changes
(
    change_id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
    seen_at timestamp(6) without time zone NOT NULL,
    conf_load_time timestamp(6) without time zone,
    setting_name text NOT NULL,
    unit text,
    old_value text,
    new_value text,
    old_source text,
    new_source text,
    old_sourcefile text,
    new_sourcefile text,
    old_sourceline integer,
    new_sourceline integer,
    old_pending_restart boolean,
    new_pending_restart boolean,
    CONSTRAINT pk_store_setting_changes PRIMARY KEY (change_id)
);

CREATE INDEX IF NOT EXISTS ix_store_setting_changes_seen
    ON collect.store_setting_changes (seen_at DESC, change_id DESC);";

    /// <summary>The server's own settings, one row each. A role that cannot read a value gets NULL for it.</summary>
    public const string ReadSettingsSql = @"
SELECT name, setting, unit, source, sourcefile, sourceline, pending_restart
FROM pg_catalog.pg_settings";

    /// <summary>
    /// One capture, one statement, so the change rows, the state refresh and the drop all see the state as it was before. <c>$1</c> to <c>$7</c> are the
    /// readings as parallel arrays (name, setting, unit, source, sourcefile, sourceline, pending_restart). Returns the change rows written, the state rows
    /// stored or refreshed, the state rows dropped, and the state's size before the pass.
    /// </summary>
    public const string CaptureSql = @"
WITH cur AS MATERIALIZED
(
    SELECT n AS name, s AS setting, u AS unit, o AS source, f AS sourcefile, l AS sourceline, p AS pending_restart
    FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::integer[], $7::boolean[]) AS t(n, s, u, o, f, l, p)
),
changed AS
(
    INSERT INTO collect.store_setting_changes
        (seen_at, conf_load_time, setting_name, unit, old_value, new_value, old_source, new_source,
         old_sourcefile, new_sourcefile, old_sourceline, new_sourceline, old_pending_restart, new_pending_restart)
    SELECT timezone('UTC', clock_timestamp()), timezone('UTC', pg_catalog.pg_conf_load_time()), c.name, c.unit,
           s.setting, c.setting, s.source, c.source, s.sourcefile, c.sourcefile, s.sourceline, c.sourceline,
           s.pending_restart, c.pending_restart
    FROM cur AS c
    JOIN collect.store_setting_state AS s ON s.setting_name = c.name
    WHERE (s.setting, s.source, s.pending_restart) IS DISTINCT FROM (c.setting, c.source, c.pending_restart)
    RETURNING 1
),
stored AS
(
    INSERT INTO collect.store_setting_state AS s
        (setting_name, setting, unit, source, sourcefile, sourceline, pending_restart, tracked_since)
    SELECT c.name, c.setting, c.unit, c.source, c.sourcefile, c.sourceline, c.pending_restart, timezone('UTC', clock_timestamp())
    FROM cur AS c
    LEFT JOIN collect.store_setting_state AS o ON o.setting_name = c.name
    WHERE o.setting_name IS NULL
       OR (o.setting, o.unit, o.source, o.sourcefile, o.sourceline, o.pending_restart)
          IS DISTINCT FROM (c.setting, c.unit, c.source, c.sourcefile, c.sourceline, c.pending_restart)
    ON CONFLICT (setting_name) DO UPDATE
    SET setting = EXCLUDED.setting,
        unit = EXCLUDED.unit,
        source = EXCLUDED.source,
        sourcefile = EXCLUDED.sourcefile,
        sourceline = EXCLUDED.sourceline,
        pending_restart = EXCLUDED.pending_restart
    RETURNING 1
),
dropped AS
(
    DELETE FROM collect.store_setting_state AS s
    WHERE NOT EXISTS (SELECT 1 FROM cur AS c WHERE c.name = s.setting_name)
    RETURNING 1
)
SELECT (SELECT count(*) FROM changed)::integer,
       (SELECT count(*) FROM stored)::integer,
       (SELECT count(*) FROM dropped)::integer,
       (SELECT count(*) FROM collect.store_setting_state)::integer";

    /// <summary>The retention delete. <c>$1</c> the cutoff (naive UTC).</summary>
    public const string RetentionDeleteSql = @"
DELETE FROM collect.store_setting_changes
WHERE seen_at < $1";

    /// <summary>Whether the rung's two tables exist, for a store the service has not migrated yet.</summary>
    public const string ShapeSql = @"
SELECT to_regclass('collect.store_setting_state') IS NOT NULL
   AND to_regclass('collect.store_setting_changes') IS NOT NULL";

    /// <summary>
    /// The state's size and the time the first setting was tracked, the "watching since" a read gives. Zero rows is no baseline yet.
    /// </summary>
    public const string StateSql = @"
SELECT count(*)::integer, min(tracked_since)
FROM collect.store_setting_state";

    /// <summary>
    /// The changes since a time, newest first, for one setting or all of them. <c>$1</c> the window start (naive UTC), <c>$2</c> the setting's name or
    /// NULL, <c>$3</c> the fetch size (the page's limit plus one). The name filter is in the WHERE, ahead of the LIMIT, and matches the whole name in
    /// any case.
    /// </summary>
    public const string ListSql = @"
SELECT change_id, seen_at, conf_load_time, setting_name, unit, old_value, new_value, old_source, new_source,
       old_sourcefile, new_sourcefile, old_sourceline, new_sourceline, old_pending_restart, new_pending_restart
FROM collect.store_setting_changes
WHERE seen_at >= $1
  AND ($2::text IS NULL OR lower(setting_name) = lower($2::text))
ORDER BY seen_at DESC, change_id DESC
LIMIT $3";

    /// <summary>Reads the store's settings and redacts the values a secret can hide in.</summary>
    public static async Task<IReadOnlyList<StoreSettingReading>> ReadSettingsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var readings = new List<StoreSettingReading>();
        await using var command = new NpgsqlCommand(ReadSettingsSql, connection) { CommandTimeout = CaptureTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var name = reader.GetString(0);
            readings.Add(new StoreSettingReading(
                name,
                PgSettingRedactor.Redact(name, reader.IsDBNull(1) ? null : reader.GetString(1)),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetBoolean(6)));
        }

        return readings;
    }

    /// <summary>
    /// One capture: read the settings, compare them with the last captured state, write a change row for each that moved, refresh the state, and prune
    /// rows past the retention when the pass wrote one. The first capture on a store writes only the baseline.
    /// </summary>
    public static async Task<StoreSettingCapture> CaptureAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var readings = await ReadSettingsAsync(connection, cancellationToken);
        return await CaptureAsync(connection, readings, cancellationToken);
    }

    /// <summary>The write half of <see cref="CaptureAsync(NpgsqlConnection, CancellationToken)"/>, over readings the caller took.</summary>
    public static async Task<StoreSettingCapture> CaptureAsync(NpgsqlConnection connection, IReadOnlyList<StoreSettingReading> readings, CancellationToken cancellationToken)
    {
        StoreSettingCapture result;
        await using (var command = new NpgsqlCommand(CaptureSql, connection) { CommandTimeout = CaptureTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = readings.Select(r => r.Name).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = readings.Select(r => r.Setting).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = readings.Select(r => r.Unit).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = readings.Select(r => r.Source).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = readings.Select(r => r.SourceFile).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = readings.Select(r => r.SourceLine).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Boolean, Value = readings.Select(r => r.PendingRestart).ToArray() });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            result = new StoreSettingCapture(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3));
        }

        if (result.Changes > 0)
        {
            await using var retention = new NpgsqlCommand(RetentionDeleteSql, connection) { CommandTimeout = CaptureTimeoutSeconds };
            retention.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-RetentionDays), DateTimeKind.Unspecified) });
            await retention.ExecuteNonQueryAsync(cancellationToken);
        }

        return result;
    }
}
