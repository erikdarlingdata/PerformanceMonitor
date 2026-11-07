/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7643: the three reads over the schema history the <c>schema_objects</c> collector keeps (<c>get_object_definition</c>,
/// <c>get_schema_changes</c>, <c>find_schema_objects</c>), served from the store with no hit on the monitored server. Available to every
/// role, the reader included: they carry names, types and structure at every <c>mcp.dataDetail</c> level, and the expression text they do
/// carry (<c>check_definition</c>, <c>computed_definition</c>, <c>default_definition</c>, <c>filter_definition</c>, <c>view_definition</c>)
/// is under keys <see cref="McpDataDetailKeys"/> masks.
///
/// <para>The history of a database that belongs to an availability group is the group's, whichever replica the read names
/// (<see cref="SchemaHistoryKeyResolver"/>); the answer says so with <c>resolved_from_ag</c>. A server the collector has never run for
/// answers <c>not_collected</c>. Lists are fetched at one past their limit, an out-of-range value is refused and never clamped, and the
/// counts of a page are named <c>*_returned</c>.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpSchemaTools
{
    public const int DefaultColumnLimit = 200;
    public const int MaxColumnLimit = 1000;
    public const int DefaultChangesLimit = 50;
    public const int MaxChangesLimit = 100;
    public const int DefaultFindLimit = 50;
    public const int MaxFindLimit = 100;
    public const int MinTextLength = 3;

    /// <summary>The most of a view's text get_object_definition returns: a quarter of the response budget, so its JSON escaping still fits.</summary>
    public const int MaxViewDefinitionChars = McpResponseBudget.DefaultBytes / 4;

    /// <summary>The time a definition text search may run: 4 s, as <c>find_queries_by_text</c> gives each source.</summary>
    public const int BudgetMilliseconds = DarlingMcpFindQueriesByTextTools.BudgetMilliseconds;

    private const string NotCollectedMessage =
        "Schema history has not been collected for this server: the schema_objects collector has never run for it. It is off by default; " +
        "turn it on with enable_collector (collector schema_objects) and read again after its first pass.";

    [McpServerTool(Name = "get_object_definition"), Description(
        "Gets the stored definition of one table or view of a database, from the schema history: its columns, indexes, constraints and triggers, the times of the version shown, versions_kept and history_starts_at. object_name is 'schema.name', or a bare name when only one object has it. Without as_of it is the current version (a dropped object answers with its last definition and dropped_at); with as_of it is the version that held at that instant, and an as_of before the history starts answers with the oldest version and a note (status empty when the first version is its creation). Columns are paged (column_offset, column_limit) and cut to the response budget; columns_returned and next_column_offset say what is left. A view's text longer than 8,192 characters is cut there, with view_definition_truncated and view_definition_length. A database of an availability group answers from the group's history whichever replica server_name names (resolved_from_ag). Expression text (check_definition, computed_definition, default_definition, filter_definition, view_definition) is masked by mcp.dataDetail; names, types and structure are not. status not_collected: the schema_objects collector has never run for the server.")]
    public static Task<string> GetObjectDefinition(
        NpgsqlDataSource postgres,
        [Description("Database name.")] string database_name,
        [Description("The table or view: 'schema.name', or a bare name when one object has it.")] string object_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional ISO-8601 UTC instant (2026-08-18T14:30:00Z; 2026-08-18 = 00:00 UTC): the version that held then. Default now (the current version).")] string? as_of = null,
        [Description("Columns to skip, for a wide table. Default 0.")] int column_offset = 0,
        [Description("Columns per page, 1 to 1000. Default 200.")] int column_limit = DefaultColumnLimit,
        CancellationToken cancellationToken = default)
        => ReadObjectDefinition(postgres, server_name, database_name, object_name, as_of, column_offset, column_limit, cancellationToken);

    internal static async Task<string> ReadObjectDefinition(
        NpgsqlDataSource postgres, string? server_name, string? database_name, string? object_name, string? as_of, int column_offset, int column_limit,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(database_name))
        {
            return McpHelpers.Refusal("database_name", "database_name is required: the schema history is kept per database.");
        }

        if (!TryParseName(object_name, out var schema, out var name))
        {
            return McpHelpers.Refusal("object_name", "object_name is required: 'schema.name', or a bare name.");
        }

        if (column_offset < 0)
        {
            return McpHelpers.Refusal("column_offset", $"Invalid column_offset value '{column_offset}'. Must be zero or a positive integer.");
        }

        var limitError = McpHelpers.ValidateTop(column_limit, MaxColumnLimit, "column_limit");
        if (limitError != null) return limitError;

        var asOfError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (asOfError != null) return asOfError;
        DateTime? at = string.IsNullOrWhiteSpace(as_of) ? null : windowEnd;

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            var resolution = await SchemaHistoryKeyResolver.ResolveAsync(connection, resolved.ServerId, database_name.Trim(), windowEnd, cancellationToken);
            if (!resolution.Collected)
            {
                return McpHelpers.Status("not_collected", NotCollectedMessage);
            }

            var key = resolution.Keys[0];
            var database = resolution.Database!;
            var candidates = at is null
                ? await SchemaHistoryReader.FindByNameAsync(connection, key, database, schema, name, null, false, cancellationToken)
                : await SchemaHistoryReader.FindByNameAsync(connection, key, database, schema, name, at, false, cancellationToken);
            if (candidates.Count == 0)
            {
                candidates = await SchemaHistoryReader.FindByNameAsync(connection, key, database, schema, name, null, true, cancellationToken);
            }

            var lines = candidates.GroupBy(c => (c.Schema.ToUpperInvariant(), c.Name.ToUpperInvariant())).ToList();
            if (lines.Count == 0)
            {
                return McpHelpers.Status("empty", $"The schema history of {database} holds no table or view named {object_name!.Trim()}. Only user tables and views of a tracked database are kept, and a version older than the retention is gone.");
            }

            if (lines.Count > 1)
            {
                var names = string.Join(", ", lines.Select(g => g.First().Schema + "." + g.First().Name).OrderBy(n => n, StringComparer.Ordinal));
                return McpHelpers.Refusal("object_name", $"object_name '{object_name!.Trim()}' matches more than one object ({names}). Give 'schema.name'.");
            }

            var chosen = lines[0].OrderByDescending(c => c.VersionId).First();
            var history = await SchemaHistoryReader.ReadHistoryAsync(connection, key, database, chosen.ObjectId, chosen.Schema, chosen.Name, cancellationToken);
            var pick = PickVersion(history, at, out var droppedAt, out var note);
            if (pick is null)
            {
                return McpHelpers.Status("empty", note ?? "No version of the object held at as_of.");
            }

            var loaded = await SchemaHistoryReader.LoadVersionsAsync(connection, [pick.VersionId], cancellationToken);
            var version = loaded[pick.VersionId];
            var definition = version.Definition;
            var columns = definition.Columns.OrderBy(c => c.Ordinal).Skip(column_offset).Take(column_limit + 1).ToList();
            var (page, hasMoreByLimit) = McpHelpers.BoundPage(columns, column_limit);
            var totalColumns = definition.Columns.Count;
            var agName = version.AgName ?? key.AgName;

            /* A view's text is not paged like its columns: past half the response budget it is cut, and the answer says so with its length. */
            var viewText = definition.View?.ViewDefinition;
            var viewTextCut = viewText is not null && viewText.Length > MaxViewDefinitionChars;

            string Build(int n)
            {
                var shown = page.Take(n).ToList();
                var cut = n < page.Count;
                var more = hasMoreByLimit || cut;
                return JsonSerializer.Serialize(new
                {
                    server = resolved.DisplayName,
                    database_name = version.DatabaseName,
                    ag_name = agName,
                    resolved_from_ag = resolution.ResolvedFromAg,
                    as_of = at is { } t ? Utc(t) : null,
                    state = droppedAt is not null ? "dropped" : at is not null ? "as_of" : "current",
                    dropped_at = droppedAt is { } d ? Utc(d) : null,
                    note,
                    @object = new
                    {
                        schema = definition.Schema,
                        name = definition.Name,
                        type = definition.Type == 'V' ? "view" : "table",
                        object_id = definition.ObjectId,
                    },
                    version = new
                    {
                        change_kind = version.ChangeKind,
                        detected_by = version.DetectedBy,
                        valid_from = Utc(version.ValidFrom),
                        valid_to = version.ValidTo is { } vt ? Utc(vt) : null,
                        login = version.ChangeLogin,
                        application = version.ChangeApp,
                        host_name = version.ChangeHost,
                    },
                    versions_kept = history.Count(h => h.ChangeKind != "dropped"),
                    history_starts_at = Utc(history.Min(h => h.ValidFrom)),
                    properties = definition.Table is { } tp
                        ? (object)new
                        {
                            temporal_type = tp.TemporalType,
                            history_table = tp.HistoryTable,
                            is_memory_optimized = tp.IsMemoryOptimized,
                            durability = tp.Durability,
                            partition_scheme = tp.PartitionScheme,
                            partition_column = tp.PartitionColumn,
                            lock_escalation = tp.LockEscalation,
                        }
                        : definition.View is { } vp
                            ? new
                            {
                                is_schema_bound = vp.IsSchemaBound,
                                is_indexed = vp.IsIndexed,
                                view_definition = viewTextCut ? viewText![..MaxViewDefinitionChars] : viewText,
                                view_definition_truncated = viewTextCut,
                                view_definition_length = viewText?.Length,
                            }
                            : null,
                    column_count = totalColumns,
                    column_offset,
                    columns_returned = shown.Count,
                    columns_has_more = more,
                    next_column_offset = more ? column_offset + shown.Count : (int?)null,
                    truncated = cut,
                    budget_note = cut ? McpResponseBudget.PageCutNote(page.Count, shown.Count, "columns in ordinal order", "columns") : null,
                    columns = shown.Select(c => new
                    {
                        ordinal = c.Ordinal,
                        name = c.Name,
                        type_name = c.TypeName,
                        max_length = c.MaxLength,
                        precision = c.Precision,
                        scale = c.Scale,
                        is_nullable = c.IsNullable,
                        identity_seed = c.IdentitySeed?.ToString(CultureInfo.InvariantCulture),
                        identity_increment = c.IdentityIncrement?.ToString(CultureInfo.InvariantCulture),
                        computed_definition = c.ComputedDefinition,
                        is_persisted = c.IsPersisted,
                        default_name = c.DefaultName,
                        default_definition = c.DefaultDefinition,
                        collation_name = c.CollationName,
                        is_sparse = c.IsSparse,
                        is_rowguidcol = c.IsRowGuidCol,
                    }),
                    index_count = definition.Indexes.Count,
                    indexes = definition.Indexes.Select(i => new
                    {
                        name = i.Name,
                        type_desc = i.TypeDesc,
                        is_unique = i.IsUnique,
                        is_primary_key = i.IsPrimaryKey,
                        is_unique_constraint = i.IsUniqueConstraint,
                        key_columns = i.KeyColumns,
                        included_columns = i.IncludedColumns,
                        filter_definition = i.FilterDefinition,
                        data_compression = i.DataCompression,
                        is_disabled = i.IsDisabled,
                    }),
                    constraint_count = definition.Constraints.Count,
                    constraints = definition.Constraints.Select(k => new
                    {
                        kind = KindName(k.Kind),
                        name = k.Name,
                        check_definition = k.Definition,
                        fk_columns = k.FkColumns,
                        referenced_schema = k.ReferencedSchema,
                        referenced_table = k.ReferencedTable,
                        referenced_columns = k.ReferencedColumns,
                        delete_action = k.DeleteAction,
                        update_action = k.UpdateAction,
                        is_disabled = k.IsDisabled,
                        is_not_trusted = k.IsNotTrusted,
                        is_system_named = k.IsSystemNamed,
                    }),
                    trigger_count = definition.Triggers.Count,
                    triggers = definition.Triggers.Select(g => new { name = g.Name, is_instead_of = g.IsInsteadOf, events = g.Events }),
                }, McpHelpers.JsonOptions);
            }

            return McpResponseBudget.FitPrefix(page.Count, Build, McpResponseBudget.DefaultBytes, 0).Json;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_object_definition", ex);
        }
    }

    [McpServerTool(Name = "get_schema_changes"), Description(
        "Lists the table and view changes the schema history recorded in a window, newest first: change_kind (created, altered, renamed, dropped; baseline only with include_baseline), detected_by (xe_event, reconcile, run_now), valid_from (when the change was detected), the login, application and host the change session recorded when it named one, and the diff against the previous version (columns, indexes, constraints and triggers added, removed or changed, a rename, view text changed). The window is hours_back (1 to 168) ending at as_of or now; an out-of-range value is refused. Optionally one database and one object (object_name 'schema.name' or bare). Paged with limit (1 to 100) and offset; has_more and next_offset say whether there is more. valid_from is the detection time, not the change time: a reconcile finds a change up to its interval after it happened. status not_collected: the schema_objects collector has never run for the server.")]
    public static Task<string> GetSchemaChanges(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Limit to one database. Omit for every database of the server (and of its availability groups).")] string? database_name = null,
        [Description("Limit to one table or view: 'schema.name', or a bare name.")] string? object_name = null,
        [Description("Hours of history, 1 to 168. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("List the baseline versions too (the first read of each object). Default false.")] bool include_baseline = false,
        [Description("Maximum changes per page, 1 to 100. Default 50.")] int limit = DefaultChangesLimit,
        [Description("Changes to skip. Default 0.")] int offset = 0,
        CancellationToken cancellationToken = default)
        => ReadSchemaChanges(postgres, server_name, database_name, object_name, hours_back, as_of, include_baseline, limit, offset, cancellationToken);

    internal static async Task<string> ReadSchemaChanges(
        NpgsqlDataSource postgres, string? server_name, string? database_name, string? object_name, int hours_back, string? as_of, bool include_baseline,
        int limit, int offset, CancellationToken cancellationToken)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit, MaxChangesLimit);
        if (limitError != null) return limitError;
        if (offset < 0)
        {
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be zero or a positive integer.");
        }

        string? schema = null;
        string? name = null;
        if (!string.IsNullOrWhiteSpace(object_name) && !TryParseName(object_name, out schema, out name))
        {
            return McpHelpers.Refusal("object_name", "object_name must be 'schema.name', or a bare name.");
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var resolution = await SchemaHistoryKeyResolver.ResolveAsync(connection, resolved.ServerId, database, windowEnd, cancellationToken);
            if (!resolution.Collected)
            {
                return McpHelpers.Status("not_collected", NotCollectedMessage);
            }

            var fetched = await SchemaHistoryReader.ReadChangesAsync(
                connection, resolution.Keys, resolution.Database, schema, name, windowStart, windowEnd, include_baseline, limit + 1, offset, cancellationToken);
            if (fetched.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No schema change was recorded in the {hours_back} hours ending {Utc(windowEnd)}"
                    + (database is null ? string.Empty : " for " + database)
                    + (include_baseline ? "." : ", apart from baseline versions (include_baseline lists them)."));
            }

            var (rows, hasMore) = McpHelpers.BoundPage(fetched, limit);
            var ids = rows.Select(r => r.VersionId).Concat(rows.Where(r => r.PreviousVersionId.HasValue).Select(r => r.PreviousVersionId!.Value)).ToList();
            var versions = await SchemaHistoryReader.LoadVersionsAsync(connection, ids, cancellationToken);
            var shapedRows = rows.Select(r => ShapeChange(r, versions)).ToList();

            string Build(int n)
            {
                var shown = shapedRows.Take(n).ToList();
                var cut = n < shapedRows.Count;
                var more = hasMore || cut;
                return JsonSerializer.Serialize(new
                {
                    server = resolved.DisplayName,
                    database_name = database,
                    object_name = string.IsNullOrWhiteSpace(object_name) ? null : object_name.Trim(),
                    resolved_from_ag = resolution.ResolvedFromAg,
                    hours_back,
                    window_start = Utc(windowStart),
                    window_end = Utc(windowEnd),
                    include_baseline,
                    offset,
                    limit,
                    changes_returned = shown.Count,
                    has_more = more,
                    next_offset = more ? offset + shown.Count : (int?)null,
                    truncated = more,
                    budget_note = cut ? McpResponseBudget.PageCutNote(shapedRows.Count, shown.Count, "newest first", "changes") : null,
                    changes = shown,
                }, McpHelpers.JsonOptions);
            }

            return McpResponseBudget.FitPrefix(shapedRows.Count, Build).Json;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_schema_changes", ex);
        }
    }

    [McpServerTool(Name = "find_schema_objects"), Description(
        "Finds the current tables and views of a server's schema history by name, column, foreign key or definition text. name_prefix and name_contains match the object name; column_name matches a column exactly; references_table finds the objects with a foreign key to a table ('schema.table' or bare); definition_contains searches the text of view definitions, computed columns, column defaults, check constraints and index filters, needs database_name, runs under a 4 second budget (a run that exceeds it is status stopped), and is refused when mcp.dataDetail masks or hides statement text because a search on hidden text would reveal it. All texts are matched ignoring case, a literal substring for the contains filters; give at least one filter. Current objects only: use get_schema_changes for what an object was. Rows carry database, schema, name, type, object_id and since when the current version holds. status not_collected: the schema_objects collector has never run for the server.")]
    public static Task<string> FindSchemaObjects(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Limit to one database. Required with definition_contains.")] string? database_name = null,
        [Description("Object names that start with this text.")] string? name_prefix = null,
        [Description("Object names that contain this text.")] string? name_contains = null,
        [Description("Objects with a column of exactly this name.")] string? column_name = null,
        [Description("Objects with a foreign key to this table: 'schema.table', or a bare table name.")] string? references_table = null,
        [Description("Objects whose view, computed column, default, check constraint or index filter text contains this text (at least 3 characters). Needs database_name.")] string? definition_contains = null,
        [Description("Maximum objects, 1 to 100. Default 50.")] int limit = DefaultFindLimit,
        McpDataDetailSetting? maskSetting = null,
        CancellationToken cancellationToken = default)
        => FindSchemaObjects(
            postgres, server_name, database_name, name_prefix, name_contains, column_name, references_table, definition_contains, limit,
            maskSetting?.Mode ?? McpDataDetail.Full, BudgetMilliseconds, DateTime.UtcNow, cancellationToken);

    internal static async Task<string> FindSchemaObjects(
        NpgsqlDataSource postgres, string? server_name, string? database_name, string? name_prefix, string? name_contains, string? column_name,
        string? references_table, string? definition_contains, int limit, McpDataDetail detail, int budgetMilliseconds, DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        var text = Blank(definition_contains);
        if (text is not null)
        {
            if (detail != McpDataDetail.Full)
            {
                /* maskLiterals hides the literals in an expression; a search on them would tell the caller what they are. metadataOnly hides the text. */
                return McpHelpers.Refusal(
                    "definition_contains",
                    $"definition_contains is not served at mcp.dataDetail '{McpDataDetailModes.Word(detail)}': it searches expression text, and a search on text this level hides would tell the caller what the text holds. Search by name_prefix, name_contains, column_name or references_table, or ask the operator for the 'full' level.");
            }

            if (Blank(database_name) is null)
            {
                return McpHelpers.Refusal("database_name", "definition_contains needs database_name: the text has no index, so the search is bounded to one database.");
            }

            if (text.Length < MinTextLength)
            {
                return McpHelpers.Refusal("definition_contains", $"definition_contains must be at least {MinTextLength} characters: a shorter one matches most definitions.");
            }
        }

        var limitError = McpHelpers.ValidateTop(limit, MaxFindLimit);
        if (limitError != null) return limitError;

        string? referencesSchema = null;
        string? referencesName = null;
        if (Blank(references_table) is { } reference && !TryParseName(reference, out referencesSchema, out referencesName))
        {
            return McpHelpers.Refusal("references_table", "references_table must be 'schema.table', or a bare table name.");
        }

        if (Blank(name_prefix) is null && Blank(name_contains) is null && Blank(column_name) is null && referencesName is null && text is null)
        {
            return McpHelpers.Refusal("name_prefix", "Give at least one filter: name_prefix, name_contains, column_name, references_table or definition_contains.");
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            var resolution = await SchemaHistoryKeyResolver.ResolveAsync(connection, resolved.ServerId, Blank(database_name), nowUtc, cancellationToken);
            if (!resolution.Collected)
            {
                return McpHelpers.Status("not_collected", NotCollectedMessage);
            }

            var search = new SchemaObjectSearch(
                resolution.Database, Blank(name_prefix), Blank(name_contains), Blank(column_name), referencesSchema, referencesName, text);
            IReadOnlyList<SchemaFoundObject> fetched = [];
            var stopped = false;
            await using (var transaction = await connection.BeginTransactionAsync(cancellationToken))
            {
                try
                {
                    if (text is not null)
                    {
                        var setSql = $"SET LOCAL statement_timeout = {budgetMilliseconds.ToString(CultureInfo.InvariantCulture)}";
                        await using var set = new NpgsqlCommand(setSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
                        await set.ExecuteNonQueryAsync(cancellationToken);
                    }

                    fetched = await SchemaHistoryReader.FindCurrentAsync(connection, transaction, resolution.Keys, search, limit + 1, cancellationToken);
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled)
                {
                    stopped = true;
                }
            }

            if (stopped)
            {
                return McpHelpers.Status(
                    "stopped",
                    $"Stopped: the {budgetMilliseconds / 1000.0:0.#} s budget for a definition text search ran out before it finished, so no rows are returned. Use a longer, more specific definition_contains, or add another filter.");
            }

            var (rows, hasMore) = McpHelpers.BoundPage(fetched, limit);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", "No current table or view matches. Only the objects of tracked databases that exist now are searched.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                database_name = resolution.Database,
                resolved_from_ag = resolution.ResolvedFromAg,
                limit,
                objects_returned = rows.Count,
                has_more = hasMore,
                truncated = hasMore,
                objects = rows.Select(r => new
                {
                    database_name = r.DatabaseName,
                    ag_name = r.AgName,
                    schema = r.Schema,
                    name = r.Name,
                    type = r.Type == 'V' ? "view" : "table",
                    object_id = r.ObjectId,
                    current_since = Utc(r.ValidFrom),
                    matched_in = text is null ? null : MatchedIn(r),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("find_schema_objects", ex);
        }
    }

    /// <summary>Which version a read of <paramref name="history"/> shows: the newest (a dropped one gives way to its last live version, with its drop time), or the one that held at <paramref name="at"/> (the oldest when <paramref name="at"/> is before the history).</summary>
    private static SchemaVersionStamp? PickVersion(IReadOnlyList<SchemaVersionStamp> history, DateTime? at, out DateTime? droppedAt, out string? note)
    {
        droppedAt = null;
        note = null;
        var live = history.Where(h => h.ChangeKind != "dropped").ToList();
        if (live.Count == 0)
        {
            note = "The object has only a drop recorded: no definition of it was kept.";
            return null;
        }

        if (at is null)
        {
            var newest = history[^1];
            if (newest.ChangeKind == "dropped")
            {
                droppedAt = newest.ValidFrom;
                note = "The object was dropped at " + Utc(newest.ValidFrom) + "; this is its last definition.";
                return live[^1];
            }

            return newest;
        }

        var covering = live.LastOrDefault(h => h.ValidFrom <= at && (h.ValidTo is null || h.ValidTo > at));
        if (covering is not null)
        {
            return covering;
        }

        if (at < live[0].ValidFrom && live[0].ChangeKind == "created")
        {
            note = "The object did not exist at as_of: it was created at " + Utc(live[0].ValidFrom) + ".";
            return null;
        }

        if (at < live[0].ValidFrom)
        {
            note = "as_of is before the history of this object starts (" + Utc(live[0].ValidFrom) + "); this is its oldest recorded version. The history is kept for a limited time, and an object that existed before tracking began starts at its first read.";
            return live[0];
        }

        note = "No version of the object held at as_of: it was dropped before then.";
        return null;
    }

    private static object ShapeChange(SchemaChangeRow row, IReadOnlyDictionary<long, SchemaStoredVersion> versions)
    {
        var after = versions.TryGetValue(row.VersionId, out var a)
            ? a.Definition
            : new SchemaObjectDefinition(row.ObjectId, row.Schema, row.Name, row.Type, null, null, null, [], [], [], []);
        var before = row.PreviousVersionId is { } p && versions.TryGetValue(p, out var b) ? b.Definition : null;
        /* A dropped version keeps no children, so everything the object had reads as removed; a baseline version is every object's first read and carries no diff. */
        object? diff = row.ChangeKind == "baseline" ? null : ShapeDiff(SchemaObjectDiff.Compare(before, after));
        return new
        {
            version_id = row.VersionId.ToString(CultureInfo.InvariantCulture),
            database_name = row.DatabaseName,
            ag_name = row.AgName,
            schema = row.Schema,
            name = row.Name,
            type = row.Type == 'V' ? "view" : "table",
            object_id = row.ObjectId,
            change_kind = row.ChangeKind,
            detected_by = row.DetectedBy,
            valid_from = Utc(row.ValidFrom),
            login = row.Login,
            application = row.App,
            host_name = row.Host,
            diff,
        };
    }

    /// <summary>The diff with only the parts that changed, so a one-column change is one short entry.</summary>
    private static Dictionary<string, object?> ShapeDiff(SchemaObjectDiff d)
    {
        var shaped = new Dictionary<string, object?>(StringComparer.Ordinal);
        void Names(string key, IReadOnlyList<string> names)
        {
            if (names.Count > 0)
            {
                shaped[key] = names;
            }
        }

        void Items(string key, IReadOnlyList<SchemaChangedItem> items)
        {
            if (items.Count > 0)
            {
                shaped[key] = items.Select(i => new { name = i.Name, fields = i.Fields });
            }
        }

        if (d.Renamed is { } rename)
        {
            shaped["renamed"] = new { from = rename.From, to = rename.To };
        }

        Names("columns_added", d.ColumnsAdded);
        Names("columns_removed", d.ColumnsRemoved);
        Items("columns_changed", d.ColumnsChanged);
        Names("indexes_added", d.IndexesAdded);
        Names("indexes_removed", d.IndexesRemoved);
        Items("indexes_changed", d.IndexesChanged);
        Names("constraints_added", d.ConstraintsAdded);
        Names("constraints_removed", d.ConstraintsRemoved);
        Items("constraints_changed", d.ConstraintsChanged);
        Names("triggers_added", d.TriggersAdded);
        Names("triggers_removed", d.TriggersRemoved);
        Names("properties_changed", d.PropertiesChanged);
        if (d.ViewTextChanged)
        {
            shaped["view_text_changed"] = true;
        }

        return shaped;
    }

    private static List<string> MatchedIn(SchemaFoundObject r)
    {
        var matched = new List<string>();
        if (r.InViewText) matched.Add("view_definition");
        if (r.InComputedColumn) matched.Add("computed_definition");
        if (r.InDefault) matched.Add("default_definition");
        if (r.InCheck) matched.Add("check_definition");
        if (r.InFilter) matched.Add("filter_definition");
        return matched;
    }

    private static string KindName(char kind) => kind switch
    {
        'P' => "primary_key",
        'U' => "unique",
        'F' => "foreign_key",
        'C' => "check",
        'D' => "default",
        _ => kind.ToString(),
    };

    /// <summary>Splits 'schema.name' at its first dot (brackets around a part are dropped); a bare name has no schema.</summary>
    private static bool TryParseName(string? value, out string? schema, out string name)
    {
        schema = null;
        name = string.Empty;
        var text = value?.Trim();
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var dot = text.IndexOf('.', StringComparison.Ordinal);
        if (dot >= 0)
        {
            schema = Unbracket(text[..dot]);
            name = Unbracket(text[(dot + 1)..]);
            return schema.Length > 0 && name.Length > 0;
        }

        name = Unbracket(text);
        return name.Length > 0;
    }

    private static string Unbracket(string part)
    {
        var p = part.Trim();
        return p.Length >= 2 && p[0] == '[' && p[^1] == ']' ? p[1..^1] : p;
    }

    private static string? Blank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static string Utc(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}
