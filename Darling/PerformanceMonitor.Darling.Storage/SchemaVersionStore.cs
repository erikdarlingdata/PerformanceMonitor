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

/// <summary>
/// #7643: one database's catalog read, ready to be written. <see cref="TargetedIds"/> is null for a full read of the database, and the
/// requested object ids for a read of named objects only. <see cref="Attribution"/> are the session events that led to the read.
/// <see cref="NewState"/> is the capture-state row stored with the same transaction.
/// </summary>
public sealed record SchemaDatabaseWrite(
    SchemaHistoryKey Key,
    int DetectedOnServerId,
    string DatabaseName,
    DateTime DetectedAtUtc,
    string DetectedBy,
    bool BaselineRead,
    bool CompleteRead,
    IReadOnlyList<SchemaObjectDefinition> Read,
    IReadOnlyList<SchemaEvent> Attribution,
    SchemaCaptureStateRow NewState)
{
    public IReadOnlySet<int>? TargetedIds { get; init; }
}

/// <summary>What one <see cref="SchemaVersionStore.ApplyAsync"/> wrote: new versions (drops included) and how many of them were drops.</summary>
public sealed record SchemaWriteCounts(int VersionsWritten, int Dropped);

/// <summary>
/// #7643: writes one database's read into the versioned store. One transaction: a changed object closes its current version and opens a
/// new one with its children, a complete read closes what it no longer finds (a targeted read: what it asked for and no longer finds)
/// with a dropped version, and the capture state is stored last. A read with no difference writes no version. Column lists are stored
/// joined and escaped by SchemaCatalogReader.JoinList (SplitList reads them), and a list with no entries is null.
/// </summary>
public static class SchemaVersionStore
{
    private const string ModuleEnd = "module_end";

    public static async Task<IReadOnlyDictionary<int, StoredSchemaObject>> LoadCurrentAsync(
        NpgsqlConnection c, SchemaHistoryKey key, string databaseName, CancellationToken ct)
    {
        return await LoadCurrentAsync(c, null, key, databaseName, ct);
    }

    private static async Task<IReadOnlyDictionary<int, StoredSchemaObject>> LoadCurrentAsync(
        NpgsqlConnection c, NpgsqlTransaction? tx, SchemaHistoryKey key, string databaseName, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using var cmd = new NpgsqlCommand(@"
SELECT version_id, object_id, schema_name, object_name, definition_hash
FROM collect.schema_objects
WHERE server_id IS NOT DISTINCT FROM $1 AND ag_group_id IS NOT DISTINCT FROM $2 AND database_name = $3 AND valid_to IS NULL", c, tx);
        Param(cmd, NpgsqlDbType.Integer, key.ServerId);
        Param(cmd, NpgsqlDbType.Uuid, key.AgGroupId);
        Param(cmd, NpgsqlDbType.Text, databaseName);

        var result = new Dictionary<int, StoredSchemaObject>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var objectId = reader.GetInt32(1);
            result[objectId] = new StoredSchemaObject(reader.GetInt64(0), objectId, reader.GetString(2), reader.GetString(3), (byte[])reader[4]);
        }

        return result;
    }

    public static async Task<SchemaWriteCounts> ApplyAsync(NpgsqlConnection c, SchemaDatabaseWrite w, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(w);

        await using var tx = await c.BeginTransactionAsync(ct);
        await LockHistoryAsync(c, tx, w, ct);
        var current = await LoadCurrentAsync(c, tx, w.Key, w.DatabaseName, ct);
        var readIds = new HashSet<int>(w.Read.Select(d => d.ObjectId));
        var claimed = new HashSet<int>();
        var versions = 0;
        var dropped = 0;

        /* Current rows the read does not show under their own id: the only ones a read object may be matched to by name. */
        var byName = new Dictionary<string, StoredSchemaObject>(StringComparer.Ordinal);
        foreach (var row in current.Values.Where(r => !readIds.Contains(r.ObjectId)))
        {
            byName[NameKey(row.Schema, row.Name)] = row;
        }

        foreach (var read in w.Read)
        {
            var hash = SchemaCanonicalForm.Hash(read);
            StoredSchemaObject? stored;
            SchemaChangeKind kind;
            if (current.TryGetValue(read.ObjectId, out stored))
            {
                kind = SchemaChangeClassifier.Classify(stored, read, w.BaselineRead);
            }
            else if (byName.TryGetValue(NameKey(read.Schema, read.Name), out stored) && !claimed.Contains(stored.ObjectId))
            {
                /* Dropped and recreated under a new object_id: the same definition is the same version, a different one continues the line. */
                claimed.Add(stored.ObjectId);
                if (stored.Hash.AsSpan().SequenceEqual(hash))
                {
                    await using var move = new NpgsqlCommand("UPDATE collect.schema_objects SET object_id = $1 WHERE version_id = $2", c, tx);
                    Param(move, NpgsqlDbType.Integer, read.ObjectId);
                    Param(move, NpgsqlDbType.Bigint, stored.VersionId);
                    await move.ExecuteNonQueryAsync(ct);
                    continue;
                }

                kind = SchemaChangeKind.Altered;
            }
            else
            {
                stored = null;
                kind = SchemaChangeClassifier.Classify(null, read, w.BaselineRead);
            }

            if (kind == SchemaChangeKind.None)
            {
                continue;
            }

            if (stored != null)
            {
                await CloseAsync(c, tx, stored.VersionId, w.DetectedAtUtc, ct);
            }

            var evt = kind == SchemaChangeKind.Renamed ? RenameEvent(w) ?? EventFor(w, read.ObjectId) : EventFor(w, read.ObjectId);
            await InsertVersionAsync(c, tx, w, read, hash, KindText(kind), evt, ct);
            versions++;
        }

        if (w.CompleteRead)
        {
            foreach (var row in current.Values)
            {
                if (readIds.Contains(row.ObjectId) || claimed.Contains(row.ObjectId))
                {
                    continue;
                }

                if (w.TargetedIds != null && !w.TargetedIds.Contains(row.ObjectId))
                {
                    continue;
                }

                await CloseAsync(c, tx, row.VersionId, w.DetectedAtUtc, ct);
                await InsertDroppedAsync(c, tx, w, row, EventFor(w, row.ObjectId), ct);
                versions++;
                dropped++;
            }
        }

        await UpsertStateAsync(c, tx, w.NewState, ct);
        await tx.CommitAsync(ct);
        return new SchemaWriteCounts(versions, dropped);
    }

    /* One writer per (history key, database) at a time: two passes of one availability group's servers, or overlapping passes, would
       otherwise both load the same current rows and both write the next version. The lock is held to the transaction's end. */
    private static async Task LockHistoryAsync(NpgsqlConnection c, NpgsqlTransaction tx, SchemaDatabaseWrite w, CancellationToken ct)
    {
        var keyText = w.Key.AgGroupId is { } ag
            ? "ag:" + ag.ToString("D") + "|" + w.DatabaseName
            : "server:" + w.Key.ServerId?.ToString(System.Globalization.CultureInfo.InvariantCulture) + "|" + w.DatabaseName;
        await using var cmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", c, tx);
        Param(cmd, NpgsqlDbType.Text, "schema_versions|" + keyText);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string NameKey(string schema, string name) => schema + "\u0001" + name;

    private static string KindText(SchemaChangeKind kind) => kind switch
    {
        SchemaChangeKind.Baseline => "baseline",
        SchemaChangeKind.Created => "created",
        SchemaChangeKind.Altered => "altered",
        SchemaChangeKind.Renamed => "renamed",
        SchemaChangeKind.Dropped => "dropped",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "not a version kind")
    };

    /* Index events carry the table's object_id, so naming the object id covers a change of one of its indexes. */
    private static SchemaEvent? EventFor(SchemaDatabaseWrite w, int objectId)
    {
        return Newest(w.Attribution.Where(e => e.ObjectId == objectId && InDatabase(e, w)));
    }

    private static SchemaEvent? RenameEvent(SchemaDatabaseWrite w)
    {
        return Newest(w.Attribution.Where(e => string.Equals(e.EventName, ModuleEnd, StringComparison.Ordinal) && InDatabase(e, w)));
    }

    private static bool InDatabase(SchemaEvent e, SchemaDatabaseWrite w)
    {
        return string.IsNullOrEmpty(e.DatabaseName) || string.Equals(e.DatabaseName, w.DatabaseName, StringComparison.OrdinalIgnoreCase);
    }

    private static SchemaEvent? Newest(IEnumerable<SchemaEvent> events)
    {
        return events.OrderByDescending(e => e.TimeUtc).ThenByDescending(e => e.Sequence).FirstOrDefault();
    }

    private static async Task CloseAsync(NpgsqlConnection c, NpgsqlTransaction tx, long versionId, DateTime at, CancellationToken ct)
    {
        await using (var close = new NpgsqlCommand("UPDATE collect.schema_objects SET valid_to = $1 WHERE version_id = $2", c, tx))
        {
            Param(close, NpgsqlDbType.Timestamp, Naive(at));
            Param(close, NpgsqlDbType.Bigint, versionId);
            await close.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in new[] { "schema_columns", "schema_indexes", "schema_constraints", "schema_triggers" })
        {
            await using var flag = new NpgsqlCommand($"UPDATE collect.{table} SET is_current = FALSE WHERE version_id = $1 AND is_current", c, tx);
            Param(flag, NpgsqlDbType.Bigint, versionId);
            await flag.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task InsertVersionAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, SchemaDatabaseWrite w, SchemaObjectDefinition d, byte[] hash, string kind,
        SchemaEvent? evt, CancellationToken ct)
    {
        long versionId;
        await using (var cmd = new NpgsqlCommand(@"
INSERT INTO collect.schema_objects (
    server_id, ag_group_id, ag_name, database_name, object_id, schema_name, object_name, object_type, valid_from, valid_to,
    change_kind, detected_by, detected_on_server_id, modify_date, definition_hash, change_login, change_app, change_host,
    temporal_type, history_table, is_memory_optimized, durability, partition_scheme, partition_column, lock_escalation,
    is_schema_bound, is_indexed, view_definition)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
RETURNING version_id", c, tx))
        {
            Param(cmd, NpgsqlDbType.Integer, w.Key.ServerId);
            Param(cmd, NpgsqlDbType.Uuid, w.Key.AgGroupId);
            Param(cmd, NpgsqlDbType.Text, w.Key.AgName);
            Param(cmd, NpgsqlDbType.Text, w.DatabaseName);
            Param(cmd, NpgsqlDbType.Integer, d.ObjectId);
            Param(cmd, NpgsqlDbType.Text, d.Schema);
            Param(cmd, NpgsqlDbType.Text, d.Name);
            Param(cmd, NpgsqlDbType.Char, d.Type.ToString());
            Param(cmd, NpgsqlDbType.Timestamp, Naive(w.DetectedAtUtc));
            Param(cmd, NpgsqlDbType.Text, kind);
            Param(cmd, NpgsqlDbType.Text, w.DetectedBy);
            Param(cmd, NpgsqlDbType.Integer, w.DetectedOnServerId);
            Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(d.ModifyDate));
            Param(cmd, NpgsqlDbType.Bytea, hash);
            Param(cmd, NpgsqlDbType.Text, evt?.Login);
            Param(cmd, NpgsqlDbType.Text, evt?.App);
            Param(cmd, NpgsqlDbType.Text, evt?.Host);
            Param(cmd, NpgsqlDbType.Text, d.Table?.TemporalType);
            Param(cmd, NpgsqlDbType.Text, d.Table?.HistoryTable);
            Param(cmd, NpgsqlDbType.Boolean, d.Table?.IsMemoryOptimized);
            Param(cmd, NpgsqlDbType.Text, d.Table?.Durability);
            Param(cmd, NpgsqlDbType.Text, d.Table?.PartitionScheme);
            Param(cmd, NpgsqlDbType.Text, d.Table?.PartitionColumn);
            Param(cmd, NpgsqlDbType.Text, d.Table?.LockEscalation);
            Param(cmd, NpgsqlDbType.Boolean, d.View?.IsSchemaBound);
            Param(cmd, NpgsqlDbType.Boolean, d.View?.IsIndexed);
            Param(cmd, NpgsqlDbType.Text, d.View?.ViewDefinition);
            versionId = (long)(await cmd.ExecuteScalarAsync(ct))!;
        }

        await InsertColumnsAsync(c, versionId, d, ct);
        await InsertIndexesAsync(c, versionId, d, ct);
        await InsertConstraintsAsync(c, versionId, d, ct);
        await InsertTriggersAsync(c, versionId, d, ct);
    }

    /* A dropped version repeats the last definition's object properties and hash, has no children, and is open and closed at the
       detection time. */
    private static async Task InsertDroppedAsync(
        NpgsqlConnection c, NpgsqlTransaction tx, SchemaDatabaseWrite w, StoredSchemaObject last, SchemaEvent? evt, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.schema_objects (
    server_id, ag_group_id, ag_name, database_name, object_id, schema_name, object_name, object_type, valid_from, valid_to,
    change_kind, detected_by, detected_on_server_id, modify_date, definition_hash, change_login, change_app, change_host,
    temporal_type, history_table, is_memory_optimized, durability, partition_scheme, partition_column, lock_escalation,
    is_schema_bound, is_indexed, view_definition)
SELECT server_id, ag_group_id, ag_name, database_name, object_id, schema_name, object_name, object_type, $2, $2,
    'dropped', $3, $4, modify_date, definition_hash, $5, $6, $7,
    temporal_type, history_table, is_memory_optimized, durability, partition_scheme, partition_column, lock_escalation,
    is_schema_bound, is_indexed, view_definition
FROM collect.schema_objects WHERE version_id = $1", c, tx);
        Param(cmd, NpgsqlDbType.Bigint, last.VersionId);
        Param(cmd, NpgsqlDbType.Timestamp, Naive(w.DetectedAtUtc));
        Param(cmd, NpgsqlDbType.Text, w.DetectedBy);
        Param(cmd, NpgsqlDbType.Integer, w.DetectedOnServerId);
        Param(cmd, NpgsqlDbType.Text, evt?.Login);
        Param(cmd, NpgsqlDbType.Text, evt?.App);
        Param(cmd, NpgsqlDbType.Text, evt?.Host);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertColumnsAsync(NpgsqlConnection c, long versionId, SchemaObjectDefinition d, CancellationToken ct)
    {
        if (d.Columns.Count == 0)
        {
            return;
        }

        await using var import = await c.BeginBinaryImportAsync(
            "COPY collect.schema_columns (version_id, is_current, ordinal, name, type_name, max_length, precision, scale, is_nullable, "
            + "identity_seed, identity_increment, computed_definition, is_persisted, default_name, default_definition, collation_name, "
            + "is_sparse, is_rowguidcol) FROM STDIN (FORMAT BINARY)", ct);
        foreach (var k in d.Columns)
        {
            await import.StartRowAsync(ct);
            await import.WriteAsync(versionId, NpgsqlDbType.Bigint, ct);
            await import.WriteAsync(true, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(k.Ordinal, NpgsqlDbType.Integer, ct);
            await import.WriteAsync(k.Name, NpgsqlDbType.Text, ct);
            await import.WriteAsync(k.TypeName, NpgsqlDbType.Text, ct);
            await WriteValueAsync(import, k.MaxLength, NpgsqlDbType.Integer, ct);
            await WriteValueAsync(import, k.Precision, NpgsqlDbType.Integer, ct);
            await WriteValueAsync(import, k.Scale, NpgsqlDbType.Integer, ct);
            await import.WriteAsync(k.IsNullable, NpgsqlDbType.Boolean, ct);
            await WriteValueAsync(import, k.IdentitySeed, NpgsqlDbType.Numeric, ct);
            await WriteValueAsync(import, k.IdentityIncrement, NpgsqlDbType.Numeric, ct);
            await WriteTextAsync(import, k.ComputedDefinition, ct);
            await WriteValueAsync(import, k.IsPersisted, NpgsqlDbType.Boolean, ct);
            await WriteTextAsync(import, k.DefaultName, ct);
            await WriteTextAsync(import, k.DefaultDefinition, ct);
            await WriteTextAsync(import, k.CollationName, ct);
            await WriteValueAsync(import, k.IsSparse, NpgsqlDbType.Boolean, ct);
            await WriteValueAsync(import, k.IsRowGuidCol, NpgsqlDbType.Boolean, ct);
        }

        await import.CompleteAsync(ct);
    }

    private static async Task InsertIndexesAsync(NpgsqlConnection c, long versionId, SchemaObjectDefinition d, CancellationToken ct)
    {
        if (d.Indexes.Count == 0)
        {
            return;
        }

        await using var import = await c.BeginBinaryImportAsync(
            "COPY collect.schema_indexes (version_id, is_current, index_id, name, type_desc, is_unique, is_primary_key, is_unique_constraint, "
            + "key_columns, included_columns, filter_definition, data_compression, is_disabled) FROM STDIN (FORMAT BINARY)", ct);
        foreach (var i in d.Indexes)
        {
            await import.StartRowAsync(ct);
            await import.WriteAsync(versionId, NpgsqlDbType.Bigint, ct);
            await import.WriteAsync(true, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(i.IndexId, NpgsqlDbType.Integer, ct);
            await WriteTextAsync(import, i.Name, ct);
            await import.WriteAsync(i.TypeDesc, NpgsqlDbType.Text, ct);
            await import.WriteAsync(i.IsUnique, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(i.IsPrimaryKey, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(i.IsUniqueConstraint, NpgsqlDbType.Boolean, ct);
            await WriteTextAsync(import, SchemaCatalogReader.JoinList(i.KeyColumns), ct);
            await WriteTextAsync(import, SchemaCatalogReader.JoinList(i.IncludedColumns), ct);
            await WriteTextAsync(import, i.FilterDefinition, ct);
            await WriteTextAsync(import, i.DataCompression, ct);
            await import.WriteAsync(i.IsDisabled, NpgsqlDbType.Boolean, ct);
        }

        await import.CompleteAsync(ct);
    }

    private static async Task InsertConstraintsAsync(NpgsqlConnection c, long versionId, SchemaObjectDefinition d, CancellationToken ct)
    {
        if (d.Constraints.Count == 0)
        {
            return;
        }

        await using var import = await c.BeginBinaryImportAsync(
            "COPY collect.schema_constraints (version_id, is_current, kind, name, definition, fk_columns, referenced_schema, referenced_table, "
            + "referenced_columns, delete_action, update_action, is_disabled, is_not_trusted, is_system_named) FROM STDIN (FORMAT BINARY)", ct);
        foreach (var k in d.Constraints)
        {
            await import.StartRowAsync(ct);
            await import.WriteAsync(versionId, NpgsqlDbType.Bigint, ct);
            await import.WriteAsync(true, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(k.Kind.ToString(), NpgsqlDbType.Char, ct);
            await import.WriteAsync(k.Name, NpgsqlDbType.Text, ct);
            await WriteTextAsync(import, k.Definition, ct);
            await WriteTextAsync(import, SchemaCatalogReader.JoinList(k.FkColumns), ct);
            await WriteTextAsync(import, k.ReferencedSchema, ct);
            await WriteTextAsync(import, k.ReferencedTable, ct);
            await WriteTextAsync(import, SchemaCatalogReader.JoinList(k.ReferencedColumns), ct);
            await WriteTextAsync(import, k.DeleteAction, ct);
            await WriteTextAsync(import, k.UpdateAction, ct);
            await import.WriteAsync(k.IsDisabled, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(k.IsNotTrusted, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(k.IsSystemNamed, NpgsqlDbType.Boolean, ct);
        }

        await import.CompleteAsync(ct);
    }

    private static async Task InsertTriggersAsync(NpgsqlConnection c, long versionId, SchemaObjectDefinition d, CancellationToken ct)
    {
        if (d.Triggers.Count == 0)
        {
            return;
        }

        await using var import = await c.BeginBinaryImportAsync(
            "COPY collect.schema_triggers (version_id, is_current, name, is_instead_of, events) FROM STDIN (FORMAT BINARY)", ct);
        foreach (var g in d.Triggers)
        {
            await import.StartRowAsync(ct);
            await import.WriteAsync(versionId, NpgsqlDbType.Bigint, ct);
            await import.WriteAsync(true, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(g.Name, NpgsqlDbType.Text, ct);
            await import.WriteAsync(g.IsInsteadOf, NpgsqlDbType.Boolean, ct);
            await import.WriteAsync(g.Events, NpgsqlDbType.Text, ct);
        }

        await import.CompleteAsync(ct);
    }

    private static async Task UpsertStateAsync(NpgsqlConnection c, NpgsqlTransaction tx, SchemaCaptureStateRow s, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.schema_capture_state (
    server_id, database_name, session_create_time, last_event_sequence, last_event_time, total_events_processed, recovery_fork_guid,
    history_server_id, history_ag_group_id, was_primary, baseline_at, last_complete_read_at, last_seen_in_scope_at, last_reconcile_at,
    last_reconcile_reason)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (server_id, database_name) DO UPDATE SET
    session_create_time = EXCLUDED.session_create_time, last_event_sequence = EXCLUDED.last_event_sequence,
    last_event_time = EXCLUDED.last_event_time, total_events_processed = EXCLUDED.total_events_processed,
    recovery_fork_guid = EXCLUDED.recovery_fork_guid, history_server_id = EXCLUDED.history_server_id,
    history_ag_group_id = EXCLUDED.history_ag_group_id, was_primary = EXCLUDED.was_primary, baseline_at = EXCLUDED.baseline_at,
    last_complete_read_at = EXCLUDED.last_complete_read_at, last_seen_in_scope_at = EXCLUDED.last_seen_in_scope_at,
    last_reconcile_at = EXCLUDED.last_reconcile_at, last_reconcile_reason = EXCLUDED.last_reconcile_reason", c, tx);
        Param(cmd, NpgsqlDbType.Integer, s.ServerId);
        Param(cmd, NpgsqlDbType.Text, s.DatabaseName);
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.SessionCreateTime));
        Param(cmd, NpgsqlDbType.Bigint, s.LastEventSequence);
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.LastEventTime));
        Param(cmd, NpgsqlDbType.Bigint, s.TotalEventsProcessed);
        Param(cmd, NpgsqlDbType.Uuid, s.RecoveryForkGuid);
        Param(cmd, NpgsqlDbType.Integer, s.HistoryServerId);
        Param(cmd, NpgsqlDbType.Uuid, s.HistoryAgGroupId);
        Param(cmd, NpgsqlDbType.Boolean, s.WasPrimary);
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.BaselineAt));
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.LastCompleteReadAt));
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.LastSeenInScopeAt));
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.LastReconcileAt));
        Param(cmd, NpgsqlDbType.Text, s.LastReconcileReason);
        await cmd.ExecuteNonQueryAsync(ct);
    }


    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static DateTime? NaiveOrNull(DateTime? value) => value is { } v ? Naive(v) : null;

    private static void Param(NpgsqlCommand cmd, NpgsqlDbType type, object? value)
    {
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = type, Value = value ?? DBNull.Value });
    }

    private static async Task WriteTextAsync(NpgsqlBinaryImporter import, string? value, CancellationToken ct)
    {
        if (value == null)
        {
            await import.WriteNullAsync(ct);
            return;
        }

        await import.WriteAsync(value, NpgsqlDbType.Text, ct);
    }

    private static async Task WriteValueAsync<T>(NpgsqlBinaryImporter import, T? value, NpgsqlDbType type, CancellationToken ct)
        where T : struct
    {
        if (value is not { } v)
        {
            await import.WriteNullAsync(ct);
            return;
        }

        await import.WriteAsync(v, type, ct);
    }
}
