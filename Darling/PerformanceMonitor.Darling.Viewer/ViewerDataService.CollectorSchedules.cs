/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane writes to <c>config.config_collector_schedules</c> — the SPARSE per-collector
/// override table (absent row / NULL column = the shared <c>CollectorScheduleDefaults</c> code default;
/// <c>server_id</c> NULL = fleet-wide) the Stage-1 <c>StoreConfigProvider.ResolveSchedule</c> layers over the
/// code defaults (per-server override &gt; fleet override &gt; default, per column). This restores Lite's
/// per-server/per-collector schedule editor + presets, now writing the store the running service honors.
///
/// <para>Same discipline as the sibling write partials: public-const SQL (Darling.Tests pin the dialect + the
/// two ON CONFLICT arbiters against V17's partial-unique indexes AND the service's own
/// <c>DarlingCommandExecutor.ResolveCollectorToggle</c> upsert shape), bound <c>$N</c> parameters, routed
/// through <see cref="ExecuteWriteAsync"/> so a read-only seat degrades to <see cref="ViewerReadOnlyException"/>.
/// A PRIMARY KEY cannot span the nullable <c>server_id</c>, so the fleet upsert arbitrates on
/// <c>(collector_name) WHERE server_id IS NULL</c> and the per-server upsert on
/// <c>(server_id, collector_name) WHERE server_id IS NOT NULL</c>.</para>
///
/// <para>A whole-scope Save is atomic: the replace deletes the scope's rows and re-inserts its overrides
/// inside ONE transaction, so a mid-save failure never leaves a half-written schedule, and the many
/// per-collector writes collapse into a single service reload (the worker reads the latest
/// <c>config_version</c> once per sweep). The fleet scope stays sparse (only collectors that differ from the
/// code default get a row); a per-server "custom" scope writes an explicit row per collector, matching Lite's
/// full-snapshot per-server override so the grid is WYSIWYG regardless of the fleet layer.</para>
///
/// <para><b>A collector's run time is not a column of these rows (#4938).</b> It lives in its own table,
/// <c>config.config_collector_run_times</c>, because the whole-scope Save above deletes a scope's rows and inserts them
/// again with a fixed column list: a viewer that did not know the column would clear it on every Save. This partial reads
/// that table with <see cref="GetCollectorRunTimesAsync"/> and writes it with <see cref="SaveCollectorRunTimesAsync"/>,
/// each change its own statement (an upsert of one row, or the delete of the row for "no run time"). The schedule
/// statements never name the run time, so a Save here leaves every run time as it was.</para>
///
/// <para><b>A schedule Reset also deletes that scope's run times, in the same transaction as the schedule rows (#4938).</b> A
/// reset means back to the shipped defaults, and they have no fixed time. The window's "Reset to Defaults" and a server's "Use
/// default schedule" Save through <see cref="SaveCollectorScheduleAsync(int?, IEnumerable{CollectorScheduleRow}, IReadOnlyList{CollectorRunTimeChange}, bool, CancellationToken)"/>
/// with the scope's run-time rows cleared first, and "Apply Default to All Servers" is the single call
/// <see cref="ResetAllServerSchedulesAsync"/>, which deletes every server's schedule rows and run times together. A viewer
/// released before the run-time table existed resets the schedule rows only, so a run time set by this viewer survives a Reset
/// done in an older one.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>All override rows (both scopes), for the editor to overlay on the code defaults. Column order
    /// matches the service's <c>ReadScheduleOverridesAsync</c>.</summary>
    public const string CollectorSchedulesSelectSql =
        "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, databases FROM config_collector_schedules ORDER BY server_id NULLS FIRST, collector_name";

    /// <summary>Upserts one FLEET-WIDE override row (server_id NULL). Arbiter matches V17's
    /// <c>ux_config_collector_schedules_fleet</c>. $1 collector_name, $2 frequency (nullable), $3 retention
    /// (nullable), $4 enabled, $5 databases (nullable — the V125 scope; NULL and empty are distinct,
    /// see <see cref="CollectorScheduleRow.Databases"/>).</summary>
    public const string CollectorScheduleFleetUpsertSql = @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled, databases)
VALUES (NULL, $1, $2, $3, $4, $5)
ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET
    frequency_minutes = EXCLUDED.frequency_minutes,
    retention_days = EXCLUDED.retention_days,
    enabled = EXCLUDED.enabled,
    databases = EXCLUDED.databases";

    /// <summary>Upserts one PER-SERVER override row. Arbiter matches V17's
    /// <c>ux_config_collector_schedules_server</c>. $1 server_id, $2 collector_name, $3 frequency (nullable),
    /// $4 retention (nullable), $5 enabled, $6 databases (nullable — the V125 scope).</summary>
    public const string CollectorScheduleServerUpsertSql = @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled, databases)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET
    frequency_minutes = EXCLUDED.frequency_minutes,
    retention_days = EXCLUDED.retention_days,
    enabled = EXCLUDED.enabled,
    databases = EXCLUDED.databases";

    /// <summary>Deletes every fleet-wide override row (revert the fleet scope to code defaults).</summary>
    public const string CollectorScheduleDeleteFleetScopeSql =
        "DELETE FROM config_collector_schedules WHERE server_id IS NULL";

    /// <summary>Deletes every override row for one server (the "use default schedule" revert). $1 server_id.</summary>
    public const string CollectorScheduleDeleteServerScopeSql =
        "DELETE FROM config_collector_schedules WHERE server_id = $1";

    /// <summary>Deletes EVERY per-server override row in one statement (the fleet-wide "Apply Default to All"
    /// reset), leaving the fleet-wide defaults (<c>server_id IS NULL</c>) untouched.</summary>
    public const string CollectorScheduleDeleteAllServerScopesSql =
        "DELETE FROM config_collector_schedules WHERE server_id IS NOT NULL";

    /// <summary>All run-time rows (both scopes), for the editor to show next to the schedule rows (#4938). Its own read of
    /// <c>config.config_collector_run_times</c>, the V160 table, and not a column of the schedule rows: the Save above deletes
    /// a scope's schedule rows and inserts them again with a fixed column list, so a viewer that did not know a run-time column
    /// would clear it. Schema-qualified, so a 42P01 from it can only mean this table is missing.</summary>
    public const string CollectorRunTimesSelectSql =
        "SELECT server_id, collector_name, run_at_minute FROM config.config_collector_run_times ORDER BY server_id NULLS FIRST, collector_name";

    /// <summary>Upserts one FLEET-WIDE run time (server_id NULL). Arbiter matches V160's
    /// <c>ux_config_collector_run_times_fleet</c>. $1 collector_name, $2 run_at_minute (smallint, minutes after midnight on the
    /// server's clock). A plain INSERT: the table's own statement-level trigger bumps <c>config_version</c>, so a running
    /// service reloads.</summary>
    public const string CollectorRunTimeFleetUpsertSql = @"
INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute)
VALUES (NULL, $1, $2)
ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET
    run_at_minute = EXCLUDED.run_at_minute";

    /// <summary>Upserts one PER-SERVER run time. Arbiter matches V160's <c>ux_config_collector_run_times_server</c>. $1 server_id,
    /// $2 collector_name, $3 run_at_minute (smallint; -1 is "no fixed time on this server").</summary>
    public const string CollectorRunTimeServerUpsertSql = @"
INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute)
VALUES ($1, $2, $3)
ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET
    run_at_minute = EXCLUDED.run_at_minute";

    /// <summary>Deletes the fleet-wide run time of one collector ("no run time", or back to the default). $1 collector_name.</summary>
    public const string CollectorRunTimeFleetDeleteSql =
        "DELETE FROM config.config_collector_run_times WHERE server_id IS NULL AND collector_name = $1";

    /// <summary>Deletes one server's run time of one collector ("use the fleet's time", or back to the default). $1 server_id,
    /// $2 collector_name.</summary>
    public const string CollectorRunTimeServerDeleteSql =
        "DELETE FROM config.config_collector_run_times WHERE server_id = $1 AND collector_name = $2";

    /// <summary>Deletes EVERY per-server run time in one statement, leaving the fleet-wide ones (<c>server_id IS NULL</c>) alone:
    /// the run-time half of the fleet-wide "Apply Default to All" reset, which sends every server back to the fleet's schedule.</summary>
    public const string CollectorRunTimeDeleteAllServerScopesSql =
        "DELETE FROM config.config_collector_run_times WHERE server_id IS NOT NULL";

    /// <summary>Deletes EVERY fleet-wide run time, whatever the collector: the run-time half of a fleet-scope schedule Reset, which
    /// goes back to the shipped defaults, and they have no fixed time.</summary>
    public const string CollectorRunTimeDeleteFleetScopeSql =
        "DELETE FROM config.config_collector_run_times WHERE server_id IS NULL";

    /// <summary>Deletes EVERY run time of one server, whatever the collector: the run-time half of a server-scope schedule Reset.
    /// $1 server_id.</summary>
    public const string CollectorRunTimeDeleteServerScopeSql =
        "DELETE FROM config.config_collector_run_times WHERE server_id = $1";

    /// <summary>Whether the run-time table exists, looked up by name in the catalog, so the answer does not depend on the connecting
    /// role's privileges on it (the connect gate's probe asks the same way). A statement that fails aborts its transaction, so a
    /// delete that has to tolerate a store below V160 asks first and is skipped there.</summary>
    public const string CollectorRunTimeTableExistsSql =
        "SELECT to_regclass('config.config_collector_run_times') IS NOT NULL";

    /// <summary>All override rows in the store (both fleet + per-server), for the editor overlay.</summary>
    public async Task<List<CollectorScheduleRow>> GetCollectorSchedulesAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorScheduleRow>();

        await using var command = _dataSource.CreateCommand(CollectorSchedulesSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CollectorScheduleRow(
                reader.IsDBNull(0) ? null : reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.GetBoolean(4),
                /* V125 (#3477): NULL and empty stay distinct through the round trip — the service's
                   ReadScheduleOverridesAsync says why the collapse would be a semantic change. */
                reader.IsDBNull(5) ? null : reader.GetFieldValue<string[]>(5)));
        }

        return rows;
    }

    /// <summary>
    /// Every run-time row in the store, both scopes (#4938), read from <c>config.config_collector_run_times</c>. A store
    /// below V160 has no such table, and that is "no run times", which is what every collector had before it existed: the
    /// 42P01 is answered with an empty list. The viewer's connect gate normally refuses such a store first, but its schema
    /// probe fails open when it throws, so a store below V160 can still reach the editor, and the editor must open on it.
    /// Matched on the state code, not the message text, because lc_messages is not always English. Any other failure
    /// propagates.
    /// </summary>
    public async Task<List<CollectorRunTimeRow>> GetCollectorRunTimesAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorRunTimeRow>();

        try
        {
            await using var command = _dataSource.CreateCommand(CollectorRunTimesSelectSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new CollectorRunTimeRow(
                    reader.IsDBNull(0) ? null : reader.GetInt32(0),
                    reader.GetString(1),
                    Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTableSqlState)
        {
            return new List<CollectorRunTimeRow>();
        }

        return rows;
    }

    /// <summary>
    /// Writes run-time changes on their own (#4938), each one a statement of its own against
    /// <c>config.config_collector_run_times</c>: a value upserts the row for its scope and collector, and null deletes it.
    /// Never the schedule rows' delete-and-reinsert Save, so a run time is never carried through (or cleared by) that
    /// statement. The changes share one transaction, so a failure leaves none of them written; the table's own
    /// statement-level trigger bumps <c>config_version</c>, so a running service reloads.
    ///
    /// <para>The schedule window's Save does not call this: it calls <see cref="SaveCollectorScheduleAsync"/>, which writes the
    /// run times and the schedule rows in ONE transaction, so a Save that fails part way leaves neither.</para>
    ///
    /// <para>A read-only seat (42501) throws <see cref="ViewerReadOnlyException"/>. A store below V160 (42P01 undefined_table)
    /// throws <see cref="ViewerSchemaSkewException"/>, the message that says the store is older than this viewer and the
    /// service must migrate it, and not the raw error: the connect gate refuses such a store first, but its probe fails open
    /// when it throws, so the save has to say it itself.</para>
    /// </summary>
    public async Task SaveCollectorRunTimesAsync(IReadOnlyList<CollectorRunTimeChange> changes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (changes.Count == 0)
        {
            return;
        }

        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await WriteRunTimeChangesAsync(connection, transaction, changes, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    /// <summary>
    /// The schedule window's Save as ONE transaction on one connection (#4938): the run-time changes first (the statements
    /// <see cref="SaveCollectorRunTimesAsync"/> runs), then the scope's schedule rows, which are the delete-and-reinsert
    /// <see cref="ReplaceFleetSchedulesAsync"/> (<paramref name="serverId"/> null) and <see cref="ReplaceServerSchedulesAsync"/>
    /// run, statement for statement. A schedule write that fails therefore leaves the run times unsaved too, and a run-time
    /// write that fails leaves the schedule rows as they were: a failed Save leaves neither.
    ///
    /// <para>No run-time change means no run-time statement, so a Save that changed no run time still saves its schedules on a
    /// store below V160, which has no run-time table. Errors map as the two writes always did: a read-only seat (42501) throws
    /// <see cref="ViewerReadOnlyException"/>, and a run-time statement that finds the table (42P01) or a column (42703) missing
    /// throws <see cref="ViewerSchemaSkewException"/>. The schedule statements are exactly the ones a released viewer runs, and
    /// a released viewer never writes a run time, so nothing older is affected.</para>
    /// </summary>
    public Task SaveCollectorScheduleAsync(
        int? serverId, IEnumerable<CollectorScheduleRow> rows, IReadOnlyList<CollectorRunTimeChange> runTimeChanges,
        CancellationToken cancellationToken = default) =>
        SaveCollectorScheduleAsync(serverId, rows, runTimeChanges, clearScopeRunTimes: false, cancellationToken);

    /// <summary>
    /// The same ONE-transaction Save for a schedule that is being reset (#4938): with <paramref name="clearScopeRunTimes"/> the scope's
    /// run-time rows are deleted FIRST, in the same transaction, whatever collector they are for and whether or not the window ever
    /// showed them, and then <paramref name="runTimeChanges"/> are written as the statements they are, so a time the grid holds
    /// after the reset is inserted anew. A reset means back to the shipped defaults, which have no fixed time, and the window's
    /// "Reset to Defaults" and a server's "Use default schedule" are that reset. The scope is the fleet's rows when
    /// <paramref name="serverId"/> is null and that server's rows otherwise; no other scope is touched.
    ///
    /// <para>A store below V160 has no run-time table and so no run times to delete: the clear is skipped there (asked of the
    /// catalog first, because a failed statement would abort the transaction), and a Save with no run-time change still saves its
    /// schedules.</para>
    /// </summary>
    public Task SaveCollectorScheduleAsync(
        int? serverId, IEnumerable<CollectorScheduleRow> rows, IReadOnlyList<CollectorRunTimeChange> runTimeChanges,
        bool clearScopeRunTimes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(runTimeChanges);
        return ReplaceScheduleScopeAsync(serverId, rows, runTimeChanges, clearScopeRunTimes, cancellationToken);
    }

    /// <summary>The run-time changes, each one a statement on the caller's connection and transaction (#4938), so the caller
    /// decides what they commit with. A store below V160 throws <see cref="ViewerSchemaSkewException"/> (42P01 undefined_table, or
    /// 42703 undefined_column); every other failure, 42501 included, propagates for the caller's own mapping.</summary>
    private static async Task WriteRunTimeChangesAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, IReadOnlyList<CollectorRunTimeChange> changes, CancellationToken cancellationToken)
    {
        try
        {
            foreach (var change in changes)
            {
                await using var command = BuildRunTimeCommand(change, connection, transaction);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>The one statement for one change (#4938): the upsert of the row when it carries a time, else the delete of the
    /// row, for the fleet or for the server. The time binds as a smallint, the column's type.</summary>
    private static NpgsqlCommand BuildRunTimeCommand(CollectorRunTimeChange change, NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        var fleet = change.ServerId is null;
        var command = new NpgsqlCommand(
            change.RunAtMinute is null
                ? (fleet ? CollectorRunTimeFleetDeleteSql : CollectorRunTimeServerDeleteSql)
                : (fleet ? CollectorRunTimeFleetUpsertSql : CollectorRunTimeServerUpsertSql),
            connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };

        if (change.ServerId is int serverId)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });                         // $1 (server scope)
        }

        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = change.CollectorName });              // $1 fleet / $2 server

        if (change.RunAtMinute is int minute)
        {
            /* checked: a value that does not fit a smallint is a bug in the caller, never a silent wrap. The column's own CHECK
               refuses anything outside 0-1439 (and -1 off a server row), so the store is the last word on the range. */
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = checked((short)minute) });         // $2 fleet / $3 server
        }

        return command;
    }

    /// <summary>
    /// Atomically replaces the FLEET scope's overrides: delete every fleet row, then upsert the supplied ones
    /// (pass only collectors that differ from the code default to keep the table sparse). Read-only seats throw
    /// <see cref="ViewerReadOnlyException"/>.
    /// </summary>
    public Task ReplaceFleetSchedulesAsync(IEnumerable<CollectorScheduleRow> rows, CancellationToken cancellationToken = default) =>
        ReplaceScheduleScopeAsync(serverId: null, rows, Array.Empty<CollectorRunTimeChange>(), clearScopeRunTimes: false, cancellationToken);

    /// <summary>
    /// Atomically replaces one SERVER's overrides: delete the server's rows, then upsert the supplied ones. Pass
    /// an empty sequence to revert the server to the fleet/default schedule (the "use default" case).
    /// </summary>
    public Task ReplaceServerSchedulesAsync(int serverId, IEnumerable<CollectorScheduleRow> rows, CancellationToken cancellationToken = default) =>
        ReplaceScheduleScopeAsync(serverId, rows, Array.Empty<CollectorRunTimeChange>(), clearScopeRunTimes: false, cancellationToken);

    /// <summary>
    /// Reverts EVERY server's per-server schedule override back to the fleet/default schedule (the "Apply Default to All" bulk
    /// reset) — the fleet-scale shortcut over reverting one server at a time. Deletes all per-server schedule rows AND all
    /// per-server run times (#4938), in ONE transaction, and leaves the fleet-wide (<c>server_id IS NULL</c>) rows of both tables
    /// in place: a server's run time is part of its override, and a reset means back to the shipped defaults, which have no
    /// fixed time. Returns the number of rows removed from the two tables together. The schedule rows go first, so a failure
    /// in the run-time delete leaves the schedule rows in place too. The V17 <c>trg_bump_collector_schedules</c> trigger and the
    /// run-time table's own bump <c>config_version</c> on the DELETE, so the service re-resolves schedules on its next sweep
    /// (same reload path as <see cref="ReplaceServerSchedulesAsync"/>).
    ///
    /// <para>A read-only seat (42501) throws <see cref="ViewerReadOnlyException"/>, and a missing table or column (42P01,
    /// 42703) throws <see cref="ViewerSchemaSkewException"/>. A store below V160 has no run-time table and so no run times to
    /// remove: that half is skipped there and the schedule rows are still reset.</para>
    ///
    /// <para>A viewer released before the run-time table existed deletes the schedule rows only, so a run time set by this
    /// viewer survives a reset done in an older one.</para>
    /// </summary>
    public async Task<int> ResetAllServerSchedulesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            int removed;
            await using (var schedules = new NpgsqlCommand(CollectorScheduleDeleteAllServerScopesSql, connection, transaction)
            {
                CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds,
            })
            {
                removed = await schedules.ExecuteNonQueryAsync(cancellationToken);
            }

            removed += await DeleteRunTimesAsync(connection, transaction, CollectorRunTimeDeleteAllServerScopesSql, serverId: null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return removed;
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>One run-time delete on the caller's connection and transaction (#4938), for a scope, one server or every server
    /// (<paramref name="deleteSql"/>, with <paramref name="serverId"/> bound as $1 when the statement takes it), and the rows it
    /// removed. A store below V160 has no run-time table, which is no run times to remove: the table is asked of the catalog first
    /// (<see cref="CollectorRunTimeTableExistsSql"/>), because a failed statement would abort the caller's transaction and the
    /// 42P01 could not be caught and carried on from, and a store without it removes 0. Every other failure propagates for the
    /// caller's own mapping.</summary>
    private static async Task<int> DeleteRunTimesAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string deleteSql, int? serverId, CancellationToken cancellationToken)
    {
        await using (var exists = new NpgsqlCommand(CollectorRunTimeTableExistsSql, connection, transaction)
        {
            CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds,
        })
        {
            if (await exists.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return 0;
            }
        }

        await using var delete = new NpgsqlCommand(deleteSql, connection, transaction)
        {
            CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds,
        };
        if (serverId is int sid)
        {
            delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sid });
        }

        return await delete.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>One scope's schedule rows, and with <paramref name="runTimeChanges"/> the run times too (#4938), in ONE transaction
    /// on one connection. The schedule statements are the released viewers' own, unchanged; the run-time statements run first
    /// and only when there are changes, so a store without the run-time table still saves a Save that changed no run time. A
    /// schedule Reset (<paramref name="clearScopeRunTimes"/>) deletes the scope's run-time rows ahead of those changes, in the same
    /// transaction.</summary>
    private async Task ReplaceScheduleScopeAsync(
        int? serverId, IEnumerable<CollectorScheduleRow> rows, IReadOnlyList<CollectorRunTimeChange> runTimeChanges,
        bool clearScopeRunTimes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(rows);

        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            if (clearScopeRunTimes)
            {
                /* Reset means back to the shipped defaults, which have no fixed time: every run-time row of this scope goes,
                   by scope and not by what the window loaded, so a row for a collector this build does not define, or one
                   another writer added while the window was open, goes too. */
                await DeleteRunTimesAsync(
                    connection, transaction,
                    serverId is null ? CollectorRunTimeDeleteFleetScopeSql : CollectorRunTimeDeleteServerScopeSql,
                    serverId, cancellationToken);
            }

            if (runTimeChanges.Count > 0)
            {
                await WriteRunTimeChangesAsync(connection, transaction, runTimeChanges, cancellationToken);
            }

            /* Clear the scope first so removed overrides don't linger, then re-insert the current set. */
            await using (var delete = new NpgsqlCommand(
                serverId is null ? CollectorScheduleDeleteFleetScopeSql : CollectorScheduleDeleteServerScopeSql, connection, transaction))
            {
                delete.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                if (serverId is int sid)
                {
                    delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sid });
                }

                await delete.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var row in rows)
            {
                await using var upsert = new NpgsqlCommand(
                    serverId is null ? CollectorScheduleFleetUpsertSql : CollectorScheduleServerUpsertSql, connection, transaction);
                upsert.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                if (serverId is int sid)
                {
                    upsert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sid });                    // $1 (server scope)
                }

                upsert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.CollectorName });       // $1 fleet / $2 server
                AddNullableInt(upsert, row.FrequencyMinutes);                                                 // frequency
                AddNullableInt(upsert, row.RetentionDays);                                                    // retention
                upsert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.Enabled });               // enabled
                AddNullableTextArray(upsert, row.Databases);                                                  // databases (V125 scope)
                await upsert.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    private static void AddNullableInt(NpgsqlCommand command, int? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            Value = value.HasValue ? value.Value : DBNull.Value,
        });

    /// <summary>Binds the V125 <c>databases</c> scope: null binds SQL NULL (no scope at this level,
    /// falls through the layering), a list — INCLUDING an empty one — binds a real array, because an
    /// explicit empty array is the "no scope" override that stops the fall-through (#3477).</summary>
    private static void AddNullableTextArray(NpgsqlCommand command, IReadOnlyList<string>? values) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = values is null ? DBNull.Value : values as string[] ?? System.Linq.Enumerable.ToArray(values),
        });
}

/// <summary>
/// One <c>config.config_collector_schedules</c> override row as the viewer authors + reads it — the twin of
/// the service's <c>ScheduleOverride</c>. <see cref="ServerId"/> NULL = fleet-wide; a NULL
/// <see cref="FrequencyMinutes"/>/<see cref="RetentionDays"/> falls through to the next resolution level (the
/// service's per-column layering). The editor works in effective values and only emits rows for collectors
/// that carry an actual override. <see cref="Databases"/> is the V125 per-collector allow-list (#3477):
/// null = column NULL (falls through), empty = the explicit "no scope" that stops the fall-through,
/// non-empty = collect only those databases (<c>excludedDatabases</c> still wins downstream).
/// </summary>
public sealed record CollectorScheduleRow(int? ServerId, string CollectorName, int? FrequencyMinutes, int? RetentionDays, bool Enabled, IReadOnlyList<string>? Databases = null);

/// <summary>
/// One <c>config.config_collector_run_times</c> row as the viewer reads it (#4938): the time of day a collector that runs
/// once a day or less often starts. <see cref="ServerId"/> NULL = fleet-wide. <see cref="RunAtMinute"/> is minutes after
/// midnight on the monitored server's own clock (0-1439), or -1 on a server row for "no fixed time on this server", which
/// stops a fleet-wide time for that server only. No row is no run time at that level.
/// </summary>
public sealed record CollectorRunTimeRow(int? ServerId, string CollectorName, int RunAtMinute);

/// <summary>
/// One change the editor makes to the run-time table (#4938), each written as a statement of its own: a value upserts the
/// row for the scope and collector, and null deletes it (no run time at that level, so the scope falls back to the next one).
/// </summary>
public sealed record CollectorRunTimeChange(int? ServerId, string CollectorName, int? RunAtMinute);
