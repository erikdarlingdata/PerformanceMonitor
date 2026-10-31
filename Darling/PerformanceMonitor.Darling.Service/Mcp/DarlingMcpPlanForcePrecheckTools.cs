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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.PlanAnalysis;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #5632 step A2: <c>get_plan_force_precheck</c>, a store-only read that answers "can this Query Store plan be forced, and what
/// should a person see first". It writes nothing and never connects to the monitored server. Nothing calls it automatically yet.
/// <para><b>The verdict blocks only a force that cannot work.</b> <c>blocked</c> when the plan is not among the query's Query Store
/// rows (<c>plan_not_found</c>) or is already the query's forced plan (<c>plan_already_forced</c>), else <c>not_blocked</c>. Every
/// other fact that fired is a <c>warning</c> the caller weighs: the product sets no limit on a force, each team's agent instructions
/// decide (Erik, 10-18). The read never says a force is safe or will help. A check the store cannot answer is listed in
/// <c>unknown</c>. Executions, the minutes since the query last ran and the rows per call are shown as facts, with no threshold.</para>
/// <para><b>Reads.</b> The query's per-plan history from the grid's own tier (<see cref="DarlingMcpQueryStoreHistoryTools"/>'s helpers),
/// the force gate (<see cref="ForcePlanBotPolicy.Blockers"/> over <see cref="DarlingForcePlanTargetStateReader"/>), the two stored
/// plans, the statement text, the newest <c>query_store_health</c> row, the newest <c>index_object_stats</c> capture and the
/// <c>job_history</c> names. Compiled parameter values are customer values: they ride under <c>compiled_value</c>, which
/// <c>mcp.dataDetail</c> masks or withholds, and are never logged and never put in a warning.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanForcePrecheckTools
{
    /// <summary>Stable warning codes (a consumer API: add freely, never respell). The gate's own blockers keep their own names and ride as warnings.</summary>
    internal const string CodeRecompileHint = "recompile_hint";
    internal const string CodeTempTable = "temp_table";
    internal const string CodeQueryStoreReadOnly = "query_store_read_only";
    internal const string CodePspVariant = "psp_variant";

    internal const string VerdictBlocked = "blocked";
    internal const string VerdictNotBlocked = "not_blocked";

    /// <summary>The only two blockers: a force that cannot do anything.</summary>
    internal const string BlockerPlanNotFound = "plan_not_found";
    internal const string BlockerPlanAlreadyForced = "plan_already_forced";

    /// <summary>The <c>unknown</c> check names this read owns besides the warning codes.</summary>
    internal const string CheckRowsPerCall = "rows_per_call";
    internal const string CheckCpuPerExecution = "cpu_per_execution";
    internal const string CheckDurationPerExecution = "duration_per_execution";

    /// <summary>Most rows each of the shown lists carries.</summary>
    internal const int MaxListed = 20;

    /// <summary>How far back, before <c>as_of</c>, a job run is read for a forcing-named job or step.</summary>
    internal const int JobLookbackDays = 30;

    private static readonly string s_planColumns = """
        plan_id, collection_time, execution_count,
        CAST(avg_duration_us AS double precision), CAST(avg_cpu_time_us AS double precision), CAST(avg_rowcount AS double precision),
        last_execution_time, is_forced_plan, force_failure_count, last_force_failure_reason, plan_forcing_type
        """;

    /// <summary>The raw tier: the history tool's own dedupe (#1841), then the columns this read folds.</summary>
    internal static readonly string RawPlanRowsSql =
        DarlingMcpQueryStoreHistoryTools.HistoryDedupeCte + "\n" + "SELECT " + s_planColumns + "\nFROM deduped WHERE rn = 1 "
        + DarlingMcpQueryStoreHistoryTools.HistoryOrder;

    /// <summary>The interval table: the history tool's own FROM and WHERE (one row per interval identity, #5300).</summary>
    internal static readonly string TablePlanRowsSql =
        "SELECT " + s_planColumns + "\n" + DarlingMcpQueryStoreHistoryTools.HistoryTableFromWhere + " " + DarlingMcpQueryStoreHistoryTools.HistoryOrder;

    /// <summary>$1 server, $2 database, $3 query, $4 window start, $5 window end. The statement text, the text dimension first and the newest inline text second, as the grid resolves it.</summary>
    internal const string QueryTextSql = """
        SELECT COALESCE(
            (SELECT x.query_sql_text FROM query_store_text AS x WHERE x.server_id = $1 AND x.database_name = $2 AND x.query_id = $3),
            (SELECT s.query_text FROM query_store_stats AS s
              WHERE s.server_id = $1 AND s.database_name = $2 AND s.query_id = $3
              AND s.collection_time >= $4 AND s.collection_time <= $5 AND s.query_text IS NOT NULL
              ORDER BY s.collection_time DESC, s.collection_id DESC LIMIT 1))
        """;

    /// <summary>$1 server, $2 database, $3 as_of. The newest health row for the database at or before as_of.</summary>
    internal const string HealthSql = """
        SELECT actual_state, desired_state, readonly_reason, capture_time
        FROM v_query_store_health
        WHERE server_id = $1 AND database_name = $2 AND capture_time <= $3
        ORDER BY capture_time DESC
        LIMIT 1
        """;

    /// <summary>$1 server, $2 database, $3 as_of. Every index the database's newest capture at or before as_of lists, by table.</summary>
    internal const string IndexCaptureSql = """
        SELECT schema_name, table_name, index_name, collection_time
        FROM v_index_object_stats
        WHERE server_id = $1 AND database_name = $2
        AND collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND database_name = $2 AND collection_time <= $3)
        """;

    /// <summary>$1 server, $2 window floor, $3 as_of. Job and step names that mention forcing, as a word.</summary>
    internal const string JobNamesSql = """
        SELECT DISTINCT job_name, step_name
        FROM job_history
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
        AND (job_name ~* '\mforc(e|es|ed|ing)\M' OR step_name ~* '\mforc(e|es|ed|ing)\M')
        ORDER BY job_name, step_name
        LIMIT 20
        """;

    internal sealed record PlanRow(
        long PlanId, DateTime CollectionTime, long Executions, double? DurationUs, double? CpuUs, double? Rows,
        DateTime? LastExecution, bool IsForced, long? FailureCount, string? FailureReason, string? ForcingType);

    internal sealed record PlanFold(
        long PlanId, long Executions, double? CpuPerExecMs, double? DurationPerExecMs, double? AvgRows, DateTime? LastExecution,
        bool IsForced, long? FailureCount, string? FailureReason, string? ForcingType);

    /// <summary>One fact that fired: a blocker (a force that cannot work) or a warning (everything else). Same shape, two lists.</summary>
    internal sealed record Finding(string Code, string Sentence, object? Evidence);

    internal sealed record Unknown(string Check, string Reason);

    private static async Task<List<PlanRow>> ReadPlanRowsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        var rows = new List<PlanRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanRow(
                reader.GetInt64(0), reader.GetDateTime(1), reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetDouble(3), reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDouble(5), reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                !reader.IsDBNull(7) && reader.GetBoolean(7), reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetString(9), reader.IsDBNull(10) ? null : reader.GetString(10)));
        }

        return rows;
    }

    /// <summary>
    /// One line per plan, execution-weighted as <see cref="DarlingMcpQueryStoreHistoryTools.FoldPlans"/> folds a plan. Rows per call is
    /// null when any snapshot of the plan has no <c>avg_rowcount</c>: a missing count never reads as a small one (#5631). The forcing
    /// facts come from the plan's newest snapshot; the failure count is the largest seen.
    /// </summary>
    internal static List<PlanFold> Fold(IEnumerable<PlanRow> rows) =>
        rows.GroupBy(r => r.PlanId).OrderBy(g => g.Key).Select(g =>
        {
            var members = g.ToList();
            var execs = members.Sum(r => r.Executions);
            var newest = members.OrderByDescending(r => r.CollectionTime).First();
            return new PlanFold(
                g.Key, execs,
                WeightedMs(members, r => r.CpuUs),
                WeightedMs(members, r => r.DurationUs),
                execs > 0 && members.All(r => r.Rows is not null) ? members.Sum(r => r.Executions * r.Rows!.Value) / execs : null,
                members.Max(r => r.LastExecution), newest.IsForced, members.Max(r => r.FailureCount),
                members.OrderByDescending(r => r.CollectionTime).Select(r => r.FailureReason).FirstOrDefault(s => !string.IsNullOrEmpty(s)),
                newest.ForcingType);
        }).ToList();

    /// <summary>
    /// The execution-weighted mean of a per-execution microsecond column, in milliseconds. An interval with no value is left out of
    /// both the sum and the weight: a stored null is not a measured zero. Null when no interval has a value (or no execution ran).
    /// </summary>
    private static double? WeightedMs(List<PlanRow> members, Func<PlanRow, double?> column)
    {
        var withValue = members.Where(r => column(r) is not null).ToList();
        var execs = withValue.Sum(r => r.Executions);
        return execs > 0 ? withValue.Sum(r => r.Executions * column(r)!.Value) / execs / 1000.0 : null;
    }

    /// <summary>The larger of two rows-per-call averages over the smaller, or null when the smaller is not above zero (no ratio exists). A fact, with no threshold.</summary>
    internal static double? RowsRatio(double a, double b)
    {
        var (low, high) = a <= b ? (a, b) : (b, a);
        return low > 0 ? high / low : null;
    }

    /// <summary>What a stored plan says about the checks that read it. Null members mean the plan could not be read.</summary>
    internal sealed record PlanFacts(IReadOnlyList<string> TempTables, bool IsPspVariant, string? PspBasis, IReadOnlyList<(string Schema, string Table, string Index)> Indexes);

    /// <summary>Reads the three plan checks from one plan's XML. Null for a missing, withheld or unparseable plan.</summary>
    internal static PlanFacts? ReadPlanFacts(string? planXml, string database)
    {
        if (string.IsNullOrWhiteSpace(planXml) || SensitiveStatements.IsWithheldPlan(planXml)) return null;
        try
        {
            var doc = XDocument.Parse(planXml, LoadOptions.None);
            var temps = new SortedSet<string>(StringComparer.Ordinal);
            var indexes = new List<(string, string, string)>();
            foreach (var o in doc.Descendants().Where(e => e.Name.LocalName == "Object"))
            {
                var table = o.Attribute("Table")?.Value;
                if (string.IsNullOrEmpty(table)) continue;
                if (table.StartsWith("[#", StringComparison.Ordinal)) { temps.Add(table); continue; }
                if (table.StartsWith("[@", StringComparison.Ordinal)) continue;
                var index = o.Attribute("Index")?.Value;
                var db = o.Attribute("Database")?.Value;
                if (string.IsNullOrEmpty(index) || (db is not null && !string.Equals(Unbracket(db), database, StringComparison.OrdinalIgnoreCase))) continue;
                indexes.Add((Unbracket(o.Attribute("Schema")?.Value), Unbracket(table), Unbracket(index)));
            }

            var parsed = ShowPlanParser.Parse(planXml);
            var statements = PlanStatements.EnumerateAll(parsed).ToList();
            var variant = statements.FirstOrDefault(s => s.QueryVariantID > 0);
            var basis = variant is not null ? $"QueryVariantID {variant.QueryVariantID}" : statements.Any(s => s.Dispatcher is not null) ? "Dispatcher element" : null;
            return new PlanFacts(temps.ToList(), basis is not null, basis, indexes.Distinct().ToList());
        }
        catch (Exception ex) when (ex is System.Xml.XmlException or InvalidOperationException)
        {
            return null;
        }
    }

    private static string Unbracket(string? name) => (name ?? "").Trim().TrimStart('[').TrimEnd(']');

    [McpServerTool(Name = "get_plan_force_precheck"), Description(
        "Reads one Query Store plan before anyone forces it. This read blocks only a force that cannot work: verdict blocked when the plan is not among the query's Query Store rows (plan_not_found) or is already the query's forced plan (plan_already_forced), else not_blocked, which never means the force is safe or will help. " +
        "Everything else that fired is a warning, never a block: the agent's own instructions decide what to do with it. Each warning carries a stable code, one sentence and the evidence. <<GUIDE>> " +
        "shown holds what a person reads first; unknown lists checks the store could not answer, which are neither blockers nor warnings. Store-only and read-only. " +
        "Warning codes: the force gate's own findings by name (parameter_sensitivity_cofired, secondary_replica_evidence, apc_owns_it, already_forced (another plan of the query is forced; forcing this one replaces it), forcing_failed_on_this_plan, apc_withdrew_it, apc_resolved_differently, " +
        "state_unavailable, apc_enabled_for_database, apc_names_this_plan_as_regressed, apc_enablement_unknown), recompile_hint (OPTION RECOMPILE), temp_table (the plan names a #temp table; a @table variable does not count), query_store_read_only (newest health row not READ_WRITE) and psp_variant (either plan is a Parameter Sensitive Plan variant). " +
        "Shown as facts with no threshold: each plan's executions, the minutes from the query's newest last_execution_time to as_of, and both plans' average rows per call with their ratio. " +
        "compare_plan_id defaults to the query's plan with the newest last_execution_time; when that is plan_id itself the plan-to-plan facts go to unknown. " +
        "Different compiled parameter values are shown (compiled_values, masked or withheld by mcp.dataDetail) with PlanInputComparison's judgement and are never a warning. " +
        "Indexes the plan names that are not in the table's newest index_object_stats capture are 'not in the latest capture': the capture may not list every index. Agent jobs whose job or step name mentions forcing are a name match only; the store holds no job step command.")]
    public static async Task<string> GetPlanForcePrecheck(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("The plan_id someone wants to force.")] long plan_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Plan to compare it with. Default: the query's plan with the newest last_execution_time.")] long? compare_plan_id = null,
        [Description("Hours of history ending at as_of (default 24).")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var table = await DarlingMcpQueryStoreHistoryTools.TryReadFromTableAsync(
                postgres, resolved.ServerId, database_name, query_id, start, windowEnd, TablePlanRowsSql, ReadPlanRowsAsync, cancellationToken);
            List<PlanRow> rows;
            DateTime effectiveStart;
            if (table is { } served)
            {
                rows = served.Rows;
                effectiveStart = served.Plan.ReadStart;
            }
            else
            {
                await using var command = postgres.CreateCommand(RawPlanRowsSql);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(resolved.ServerId);
                command.Parameters.AddWithValue(database_name);
                command.Parameters.AddWithValue(query_id);
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                rows = await ReadPlanRowsAsync(reader, cancellationToken);
                ReadScope.NoteSource(ReadScope.SourceRaw);
                effectiveStart = start;
            }

            /* #7010: where the window's coverage starts, as get_query_store_query_history says it: the interval table's own plan when it served the
               read (the raw floor describes a tier that did not answer), else the raw tier's floor. An unmet engine answer below stays bare, so a
               collector that never ran here is not read as a window past the data. A failed probe costs the notice, never the answer. */
            McpWindowNotice notice;
            if (table is { } tableServed)
            {
                var tableTruncated = RawWindowFloor.IsTruncated(effectiveStart, start);
                notice = new McpWindowNotice(
                    McpHelpers.FormatEffectiveStart(effectiveStart), tableTruncated,
                    tableTruncated ? DarlingMcpDataTools.QueryStoreTableNote(effectiveStart, tableServed.Plan.StartBound) : null);
            }
            else
            {
                notice = await DarlingMcpWindowNotice.ReadAsync(
                    () => DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, start, windowEnd, cancellationToken),
                    start, windowEnd, "query_store_stats", emptyAnswer: rows.Count == 0, logger: logger, cancellationToken: cancellationToken);
            }

            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_store", cancellationToken)
                    ?? PlanNotFound(resolved.DisplayName, database_name, query_id, plan_id, compare_plan_id, hours_back, windowEnd, notice, new List<PlanFold>());

            var plans = Fold(rows);
            var candidate = plans.FirstOrDefault(p => p.PlanId == plan_id);
            if (candidate is null)
                return PlanNotFound(resolved.DisplayName, database_name, query_id, plan_id, compare_plan_id, hours_back, windowEnd, notice, plans);

            PlanFold? compare;
            if (compare_plan_id is long wanted)
            {
                compare = plans.FirstOrDefault(p => p.PlanId == wanted);
                if (compare is null)
                    return DarlingMcpWindowNotice.FinishEmpty(
                        McpHelpers.Status("empty", $"compare_plan_id {wanted} has no Query Store history for query_id {query_id} in the window searched.", notice.AsHints()), notice);
            }
            else
            {
                var newest = plans.OrderByDescending(p => p.LastExecution ?? DateTime.MinValue).ThenByDescending(p => p.PlanId).First();
                compare = newest.PlanId == plan_id ? null : newest;
            }

            var queryLastExecution = plans.Max(p => p.LastExecution);
            var blockers = new List<Finding>();
            var warnings = new List<Finding>();
            var unknown = new List<Unknown>();
            var shown = new Dictionary<string, object?>();

            /* 1. The existing gate: its findings are warnings; the one that says this very plan is forced is the blocker below. */
            var selfForcedByGate = await AddGateWarningsAsync(postgres, resolved.ServerId, database_name, query_id, candidate, windowEnd, logger, warnings, unknown, cancellationToken);
            if (candidate.IsForced || selfForcedByGate)
                blockers.Add(new Finding(BlockerPlanAlreadyForced,
                    $"Plan {plan_id} is already the forced plan of query {query_id}, so a force would change nothing.",
                    new { plan_id, is_forced_plan = true, plan_forcing_type = candidate.ForcingType }));

            /* 2. recompile_hint. */
            string? text;
            await using (var textCommand = postgres.CreateCommand(QueryTextSql))
            {
                textCommand.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                textCommand.Parameters.AddWithValue(resolved.ServerId);
                textCommand.Parameters.AddWithValue(database_name);
                textCommand.Parameters.AddWithValue(query_id);
                textCommand.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
                textCommand.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
                text = await textCommand.ExecuteScalarAsync(cancellationToken) as string;
            }

            if (string.IsNullOrWhiteSpace(text) || WithheldStatementMarker.IsMarker(text))
                unknown.Add(new Unknown(CodeRecompileHint, "the statement text is not stored for this query, or is withheld by the statement filter"));
            else if (PlanInputComparison.HasRecompileHint(text))
                warnings.Add(new Finding(CodeRecompileHint, "The statement has OPTION (RECOMPILE), so every call gets its own plan and a forced plan is not what runs.", new { has_recompile_hint = true }));

            /* Executions, the time since the query last ran and rows per call are shown as facts, with no threshold. */
            var rowsPerCall = new Dictionary<string, object?>
            {
                ["candidate_plan_id"] = candidate.PlanId,
                ["candidate_avg_rows"] = candidate.AvgRows,
                ["compare_plan_id"] = compare?.PlanId,
                ["compare_avg_rows"] = compare?.AvgRows,
                ["larger_over_smaller"] = candidate.AvgRows is { } cr && compare?.AvgRows is { } kr ? RowsRatio(cr, kr) : null,
            };
            if (compare is null)
                unknown.Add(new Unknown(CheckRowsPerCall, compare_plan_id is null ? "plan_id is itself the query's newest plan and no compare_plan_id was given, so there is no second plan to compare" : "no second plan"));
            else if (candidate.AvgRows is null || compare.AvgRows is null)
                unknown.Add(new Unknown(CheckRowsPerCall, "a stored interval of one of the two plans has no avg_rowcount, so rows per call is not known"));
            else if (rowsPerCall["larger_over_smaller"] is null)
                unknown.Add(new Unknown(CheckRowsPerCall, "one plan averages 0 rows per call, so no ratio exists"));

            foreach (var plan in compare is null ? new[] { candidate } : new[] { candidate, compare })
            {
                if (plan.CpuPerExecMs is null)
                    unknown.Add(new Unknown(CheckCpuPerExecution, $"plan {plan.PlanId} has no avg_cpu_time_us in any stored interval, so its CPU per execution is not known"));
                if (plan.DurationPerExecMs is null)
                    unknown.Add(new Unknown(CheckDurationPerExecution, $"plan {plan.PlanId} has no avg_duration_us in any stored interval, so its duration per execution is not known"));
            }

            /* 3, 7, the compiled values and the indexes: the two stored plans. */
            var candidateXml = await ReadPlanXmlAsync(postgres, resolved.ServerId, database_name, query_id, plan_id, cancellationToken);
            var compareXml = compare is null ? null : await ReadPlanXmlAsync(postgres, resolved.ServerId, database_name, query_id, compare.PlanId, cancellationToken);
            var candidateFacts = ReadPlanFacts(candidateXml, database_name);
            var compareFacts = compare is null ? null : ReadPlanFacts(compareXml, database_name);

            if (candidateFacts is null)
                unknown.Add(new Unknown(CodeTempTable, "the candidate plan's XML is not stored, is withheld by the statement filter, or cannot be read"));
            else if (candidateFacts.TempTables.Count > 0)
                warnings.Add(new Finding(CodeTempTable,
                    "The plan reads or writes a temporary table; a force on such a statement failed when replayed.", new { tables = candidateFacts.TempTables }));

            var pspSides = new List<object>();
            if (candidateFacts?.IsPspVariant == true) pspSides.Add(new { plan_id, basis = candidateFacts.PspBasis });
            if (compareFacts?.IsPspVariant == true) pspSides.Add(new { plan_id = compare!.PlanId, basis = compareFacts.PspBasis });
            if (pspSides.Count > 0)
            {
                warnings.Add(new Finding(CodePspVariant,
                    "A plan is a Parameter Sensitive Plan variant; a force on a variant got 0 executions because the parent statement moved to another query and plan.", new { variants = pspSides }));
                unknown.Add(new Unknown(CodePspVariant + "_siblings", "the parent query and the sibling variants cannot be named: the store keeps no parent_query_id for a variant"));
            }
            else if (candidateFacts is null || (compare is not null && compareFacts is null))
                unknown.Add(new Unknown(CodePspVariant, "a plan's XML is not stored, is withheld or cannot be read, so whether it is a Parameter Sensitive Plan variant is not known"));

            /* 4. query_store_read_only. */
            await AddReadOnlyAsync(postgres, resolved.ServerId, database_name, windowEnd, warnings, unknown, cancellationToken);

            /* Shown. */
            shown["query_last_execution_time"] = FormatTime(queryLastExecution);
            shown["minutes_since_query_last_execution"] = queryLastExecution is { } lastRun ? Math.Round((windowEnd - lastRun).TotalMinutes, 1) : null;
            shown["rows_per_call"] = rowsPerCall;
            var shownPlans = new List<object> { ShownPlan("candidate", candidate, candidateXml) };
            if (compare is not null) shownPlans.Add(ShownPlan("compare", compare, compareXml));
            shown["plans"] = shownPlans;
            if (compare is not null)
                shown["compiled_value_judgement"] = candidateXml is null || compareXml is null ? null : PlanInputComparison.JudgePlans(compareXml, candidateXml).ToString();
            shown["indexes_not_in_latest_capture"] = await MissingIndexesAsync(postgres, resolved.ServerId, database_name, windowEnd, candidateFacts, unknown, cancellationToken);
            shown["job_name_matches"] = await JobNameMatchesAsync(postgres, resolved.ServerId, windowEnd, cancellationToken);

            /* A dictionary, not an anonymous object: "shown" is this read's own section, not a page cut (the cut-key census reads a `shown =` member as one). */
            return DarlingMcpWindowNotice.Finish(JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["server"] = resolved.DisplayName,
                ["database_name"] = database_name,
                ["query_id"] = query_id,
                ["plan_id"] = plan_id,
                ["compare_plan_id"] = compare?.PlanId,
                ["as_of"] = FormatTime(windowEnd),
                ["hours_back"] = hours_back,
                ["effective_start"] = notice.EffectiveStart,
                ["window_truncated"] = notice.WindowTruncated,
                ["truncation_note"] = notice.TruncationNote,
                ["verdict"] = blockers.Count > 0 ? VerdictBlocked : VerdictNotBlocked,
                ["blockers"] = blockers.Select(r => new { code = r.Code, sentence = r.Sentence, evidence = r.Evidence }),
                ["warnings"] = warnings.Select(r => new { code = r.Code, sentence = r.Sentence, evidence = r.Evidence }),
                ["shown"] = shown,
                ["unknown"] = unknown.Select(u => new { check = u.Check, reason = u.Reason }),
                ["note"] = "not_blocked means the force can be tried, not that it is safe or will help. Warnings are for the caller's own instructions to weigh."
            }, McpHelpers.JsonOptions), notice);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_plan_force_precheck", ex);
        }
    }

    /// <summary>The blocked answer for a plan that is not among the query's Query Store rows in the window: nothing else is read, because there is nothing to force.</summary>
    private static string PlanNotFound(
        string serverName, string database, long queryId, long planId, long? comparePlanId, int hoursBack, DateTime windowEnd, McpWindowNotice notice, List<PlanFold> plans) =>
        DarlingMcpWindowNotice.Finish(JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["database_name"] = database,
            ["query_id"] = queryId,
            ["plan_id"] = planId,
            ["compare_plan_id"] = comparePlanId,
            ["as_of"] = FormatTime(windowEnd),
            ["hours_back"] = hoursBack,
            ["effective_start"] = notice.EffectiveStart,
            ["window_truncated"] = notice.WindowTruncated,
            ["truncation_note"] = notice.TruncationNote,
            ["verdict"] = VerdictBlocked,
            ["blockers"] = new[]
            {
                new
                {
                    code = BlockerPlanNotFound,
                    sentence = $"plan_id {planId} is not among query_id {queryId}'s Query Store rows in database '{database}' in the {hoursBack}-hour window searched, so there is no plan to force.",
                    evidence = (object)new { plans_in_window = plans.Count, plan_ids_in_window = plans.Select(p => p.PlanId).Take(MaxListed).ToList() },
                },
            },
            ["warnings"] = Array.Empty<object>(),
            ["shown"] = new Dictionary<string, object?>(),
            ["unknown"] = Array.Empty<object>(),
            ["note"] = "blocked means the force cannot work; the other checks were not run. not_blocked would not mean the force is safe or will help.",
        }, McpHelpers.JsonOptions), notice);

    private static string? FormatTime(DateTime? t) => t?.ToString("o", CultureInfo.InvariantCulture);

    private static object ShownPlan(string role, PlanFold p, string? xml)
    {
        var values = xml is null ? null : PlanInputComparison.CompiledValues(xml);
        return new
        {
            role,
            plan_id = p.PlanId,
            executions = p.Executions,
            cpu_per_execution_ms = p.CpuPerExecMs,
            duration_per_execution_ms = p.DurationPerExecMs,
            avg_rows = p.AvgRows,
            last_execution_time = FormatTime(p.LastExecution),
            is_forced_plan = p.IsForced,
            force_failure_count = p.FailureCount,
            last_force_failure_reason = p.FailureReason,
            plan_forcing_type = p.ForcingType,
            compiled_values = values?.Select(kv => new { parameter = kv.Key, compiled_value = kv.Value }).ToList(),
        };
    }

    private static Task<string?> ReadPlanXmlAsync(NpgsqlDataSource postgres, int serverId, string database, long queryId, long planId, CancellationToken ct) =>
        DarlingStoredPlanReader.ReadQueryStorePlanByIdAsync(postgres, serverId, database, queryId, planId, cancellationToken: ct);

    /// <summary>A test seam for the finding read: the store's own read logs and swallows its failures, so a pin on the throw path needs a read that throws. Null in the service.</summary>
    internal static Func<int, DateTime, CancellationToken, Task<List<AnalysisFinding>>>? FindingReadForTests { get; set; }

    /// <summary>
    /// $1 server_id. The registry facts the force gate's <c>enablementIsCollected</c> asks the plan_correction collector about.
    /// <c>postgres_major_version</c> is selected but not read: every SELECT of <c>sql_major_version</c> carries the engine, the
    /// PostgreSQL major and the edition beside it (<c>EngineAwareVersionLabelTests</c>), and the engine kind decides first here.
    /// </summary>
    internal const string TargetFactsSql = "SELECT sql_engine_edition, sql_major_version, engine_kind, postgres_major_version FROM servers WHERE server_id = $1";

    /// <summary>
    /// Does the plan_correction collector run for this server (#4770)? The bot asks <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>
    /// with its runtime's target; this read builds the same target from the registry row the connect stamped (edition, SQL major, engine kind).
    /// Null when the row cannot be read, so the caller says so instead of guessing.
    /// </summary>
    private static async Task<bool?> ReadEnablementIsCollectedAsync(NpgsqlDataSource postgres, int serverId, CancellationToken ct)
    {
        try
        {
            await using var command = postgres.CreateCommand(TargetFactsSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(serverId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var edition = 0;
            var major = 0;
            string? kind = null;
            if (await reader.ReadAsync(ct))
            {
                edition = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                major = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                kind = reader.IsDBNull(2) ? null : reader.GetString(2);
            }

            var target = new CollectorTargetInfo
            {
                Engine = MonitoredEngineKind.EngineOf(kind) ?? CollectorTargetEngine.SqlServer,
                IsAzureSqlDb = edition == CollectorEngineCapability.AzureSqlDatabaseEngineEdition,
                IsAzureManagedInstance = edition == CollectorEngineCapability.AzureManagedInstanceEngineEdition,
                SqlMajorVersion = major,
            };
            return CollectorCatalog.AppliesTo(PlanCorrectionCollector.Instance, target);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// The gate. The target is the current PLAN_REGRESSION finding's own for this query when there is one (its replica role and
    /// parameter-sensitivity flag come from the finding, <see cref="FactRemediation.ExtractPlanRegressionTargets"/> builds it), with
    /// the plan swapped to plan_id. With no finding the target comes from the store rows: database, query and plan, the plan's last
    /// run as the best-plan age, and every finding-only field at its default (no replica role, no parameter-sensitivity flag, factor 0),
    /// and the two finding-only blockers go to <c>unknown</c> because nothing here can answer them.
    /// <para>Every gate blocker rides as a warning under its own name. The one exception is <c>already_forced</c> on the candidate
    /// itself: that is the pre-check's own <c>plan_already_forced</c> blocker, so it is left out of the warnings and the method
    /// returns true. <c>already_forced</c> for another plan of the query stays a warning (an agent may replace a force).</para>
    /// </summary>
    private static async Task<bool> AddGateWarningsAsync(
        NpgsqlDataSource postgres, int serverId, string database, long queryId, PlanFold candidate, DateTime windowEnd, ILogger? logger,
        List<Finding> warnings, List<Unknown> unknown, CancellationToken ct)
    {
        ForcePlanTarget? fromFinding = null;
        var findingReadFailed = false;
        try
        {
            var findings = await (FindingReadForTests?.Invoke(serverId, windowEnd, ct)
                ?? new PgFindingStore(postgres, logger).GetRecentFindingsAsync(serverId, 24, FindingOccurrences.WindowCoveringLimit, windowEnd, ct));
            fromFinding = findings
                .Where(f => string.Equals(f.RootFactKey, "PLAN_REGRESSION", StringComparison.Ordinal) && f.Remediation?.Targets is { Count: > 0 })
                .OrderByDescending(f => f.AnalysisTime)
                .SelectMany(f => f.Remediation!.Targets)
                .FirstOrDefault(t => t.QueryId == queryId && string.Equals(t.Database, database, StringComparison.OrdinalIgnoreCase));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "The PLAN_REGRESSION finding read for get_plan_force_precheck failed for server {ServerId}.", serverId);
            findingReadFailed = true;
        }

        ForcePlanTarget target;
        if (fromFinding is not null)
        {
            target = fromFinding with { PlanId = candidate.PlanId, BestPlanHash = null };
        }
        else
        {
            target = new ForcePlanTarget(database, queryId, candidate.PlanId, BestPlanLastSeenUtc: candidate.LastExecution);
            /* One entry per check, whichever way the finding read went: a failed read is the reason, a missing finding is the other. */
            unknown.Add(new Unknown(ForcePlanBlockerNames.ParameterSensitivityCoFired, findingReadFailed
                ? "the current PLAN_REGRESSION findings could not be read, so the parameter-sensitivity signature is not known"
                : "no current PLAN_REGRESSION finding for this query, so the parameter-sensitivity signature is not known"));
            unknown.Add(new Unknown(ForcePlanBlockerNames.SecondaryReplicaEvidence, findingReadFailed
                ? "the current PLAN_REGRESSION findings could not be read, so the replica the regression was measured on is not known"
                : "no current PLAN_REGRESSION finding for this query, so the replica the regression was measured on is not known"));
        }

        var enablementIsCollected = await ReadEnablementIsCollectedAsync(postgres, serverId, ct);
        if (enablementIsCollected is null)
            unknown.Add(new Unknown(ForcePlanBotPolicy.ReasonApcEnablementUnknown, "the server's registry row could not be read, so whether the plan_correction collector runs for it is not known"));

        var carrier = new AnalysisFinding
        {
            ServerId = serverId,
            RootFactKey = "PLAN_REGRESSION",
            Remediation = new RemediationAction("PLAN_REGRESSION", "force", new[] { target }),
        };
        var (states, reason) = await DarlingForcePlanTargetStateReader.TryReadAsync(postgres, serverId, new[] { carrier }, windowEnd, logger, ct);
        ForcePlanTargetState? state = null;
        states?.TryGetValue(ForcePlanTargetKey.Of(target), out state);
        var selfForced = state?.PlanIsForced == true;
        foreach (var blocker in ForcePlanBotPolicy.Blockers(target, state, states is null ? reason : null, enablementIsCollected == true))
        {
            /* This plan is the forced one: the pre-check's own blocker says so. A different plan forced stays this warning. */
            if (selfForced && blocker.Name == ForcePlanBlockerNames.AlreadyForced) continue;
            warnings.Add(new Finding(blocker.Name, blocker.Evidence, null));
        }

        return selfForced;
    }

    private static async Task AddReadOnlyAsync(
        NpgsqlDataSource postgres, int serverId, string database, DateTime windowEnd, List<Finding> warnings, List<Unknown> unknown, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(HealthSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct) || reader.IsDBNull(0) || string.IsNullOrWhiteSpace(reader.GetString(0)))
        {
            unknown.Add(new Unknown(CodeQueryStoreReadOnly, "no query_store_health row for this database at or before as_of, so Query Store's state is not known"));
            return;
        }

        var actual = reader.GetString(0);
        var desired = reader.IsDBNull(1) ? null : reader.GetString(1);
        var readonlyReason = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
        if (!string.Equals(actual, "READ_WRITE", StringComparison.OrdinalIgnoreCase))
            warnings.Add(new Finding(CodeQueryStoreReadOnly,
                $"Query Store is {actual} for this database, so a force is not recorded or kept.",
                new
                {
                    actual_state = actual,
                    desired_state = desired,
                    readonly_reason = readonlyReason,
                    readonly_reason_decoded = readonlyReason == 0 ? null : QueryStoreReadonlyReason.Decode(readonlyReason),
                    captured_at = FormatTime(reader.GetDateTime(3)),
                }));
    }

    private static async Task<object> MissingIndexesAsync(
        NpgsqlDataSource postgres, int serverId, string database, DateTime windowEnd, PlanFacts? facts, List<Unknown> unknown, CancellationToken ct)
    {
        if (facts is null || facts.Indexes.Count == 0) return Array.Empty<object>();
        var captured = new Dictionary<(string, string), HashSet<string>>();
        await using (var command = postgres.CreateCommand(IndexCaptureSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(database);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var key = (reader.IsDBNull(0) ? "" : reader.GetString(0), reader.IsDBNull(1) ? "" : reader.GetString(1));
                if (!captured.TryGetValue(key, out var set)) captured[key] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (!reader.IsDBNull(2)) set.Add(reader.GetString(2));
            }
        }

        if (captured.Count == 0)
        {
            unknown.Add(new Unknown("indexes_not_in_latest_capture", "no index_object_stats capture for this database at or before as_of"));
            return Array.Empty<object>();
        }

        return facts.Indexes
            .Where(i => captured.TryGetValue((i.Schema, i.Table), out var set) && !set.Contains(i.Index))
            .Select(i => new { schema = i.Schema, table = i.Table, index_name = i.Index, basis = "not in the latest index_object_stats capture; the capture may not list every index" })
            .Take(MaxListed).ToList();
    }

    private static async Task<object> JobNameMatchesAsync(NpgsqlDataSource postgres, int serverId, DateTime windowEnd, CancellationToken ct)
    {
        var matches = new List<object>();
        await using var command = postgres.CreateCommand(JobNamesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd.AddDays(-JobLookbackDays), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            matches.Add(new { job_name = reader.IsDBNull(0) ? null : reader.GetString(0), step_name = reader.IsDBNull(1) ? null : reader.GetString(1), basis = "name match only; the store holds no job step command" });
        return matches;
    }
}
