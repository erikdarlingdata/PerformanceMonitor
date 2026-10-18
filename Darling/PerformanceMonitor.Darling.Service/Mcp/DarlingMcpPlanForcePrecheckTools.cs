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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.PlanAnalysis;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #5632 step A2: <c>get_plan_force_precheck</c>, a store-only read that answers "may this Query Store plan be forced, and what
/// should a person see first". It writes nothing and never connects to the monitored server. Nothing calls it automatically yet.
/// <para><b>The verdict is one-sided.</b> <c>refuse</c> when any check fires, else <c>no_refusal_found</c>. The read never says a force
/// is safe or will help. A check the store cannot answer is listed in <c>unknown</c> and is not a refusal; the one exception is the
/// gate's own <c>state_unavailable</c>, which the gate treats as a blocker.</para>
/// <para><b>Reads.</b> The query's per-plan history from the grid's own tier (<see cref="DarlingMcpQueryStoreHistoryTools"/>'s helpers),
/// the force gate (<see cref="ForcePlanBotPolicy.Blockers"/> over <see cref="DarlingForcePlanTargetStateReader"/>), the two stored
/// plans, the statement text, the newest <c>query_store_health</c> row, the newest <c>index_object_stats</c> capture and the
/// <c>job_history</c> names. Compiled parameter values are customer values: they ride under <c>compiled_value</c>, which
/// <c>mcp.dataDetail</c> masks or withholds, and are never logged and never put in a refusal.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanForcePrecheckTools
{
    /// <summary>Stable refusal codes (a consumer API: add freely, never respell). The gate's own blockers keep their own names.</summary>
    internal const string CodeRecompileHint = "recompile_hint";
    internal const string CodeTempTable = "temp_table";
    internal const string CodeQueryStoreReadOnly = "query_store_read_only";
    internal const string CodeNotRunRecently = "not_run_recently";
    internal const string CodeTooFewExecutions = "too_few_executions";
    internal const string CodePspVariant = "psp_variant";
    internal const string CodeOutputDiffers = "output_differs";

    /// <summary>A query whose newest execution, over any plan, is older than this before <c>as_of</c> has stopped running: a force there has nothing to act on.</summary>
    internal static readonly TimeSpan NotRunRecentlyAfter = TimeSpan.FromHours(2);

    /// <summary>
    /// The per-plan execution floor: the same 25 PLAN_REGRESSION's per-plan group keeps (<c>HAVING SUM(execs) &gt;= 25</c> in
    /// <c>PgFactCollector.QueryPerf</c>) and the force bot's review floor, taken from the bot's setting so there is one number.
    /// </summary>
    internal static readonly int MinPlanExecutions = ForcePlanBotSettings.Default.MinReviewExecutions;

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

    internal sealed record Refusal(string Code, string Sentence, object? Evidence);

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
                execs > 0 ? members.Sum(r => r.Executions * (r.CpuUs ?? 0)) / execs / 1000.0 : null,
                execs > 0 ? members.Sum(r => r.Executions * (r.DurationUs ?? 0)) / execs / 1000.0 : null,
                execs > 0 && members.All(r => r.Rows is not null) ? members.Sum(r => r.Executions * r.Rows!.Value) / execs : null,
                members.Max(r => r.LastExecution), newest.IsForced, members.Max(r => r.FailureCount),
                members.OrderByDescending(r => r.CollectionTime).Select(r => r.FailureReason).FirstOrDefault(s => !string.IsNullOrEmpty(s)),
                newest.ForcingType);
        }).ToList();

    /// <summary>True when the plan's rows per call differ by <see cref="PlanInputComparison.OutputGrowthRatio"/> or more, either way, each floored at <see cref="PlanInputComparison.RowFloor"/>.</summary>
    internal static bool OutputDiffers(double candidateRows, double compareRows)
    {
        var a = Math.Max(candidateRows, PlanInputComparison.RowFloor);
        var b = Math.Max(compareRows, PlanInputComparison.RowFloor);
        return a >= PlanInputComparison.OutputGrowthRatio * b || b >= PlanInputComparison.OutputGrowthRatio * a;
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
        "Checks one Query Store plan before anyone forces it: verdict refuse (any refusal fired) or no_refusal_found, which never means the force is safe or will help. " +
        "refusals each carry a stable code, one sentence and evidence numbers; shown holds what a person reads first; unknown lists checks the store could not answer, which are not refusals. <<GUIDE>> " +
        "Store-only and read-only. Refusal codes: the force gate's own blockers by name (parameter_sensitivity_cofired, secondary_replica_evidence, apc_owns_it, already_forced, forcing_failed_on_this_plan, apc_withdrew_it, apc_resolved_differently, " +
        "state_unavailable, apc_enabled_for_database, apc_names_this_plan_as_regressed), recompile_hint (OPTION RECOMPILE), temp_table (the plan names a #temp table; a @table variable does not count), query_store_read_only (newest health row not READ_WRITE), " +
        "not_run_recently (no execution of any plan in the 2 hours before as_of), too_few_executions (the plan ran under 25 times in the window), psp_variant (either plan is a Parameter Sensitive Plan variant) and output_differs (rows per call differ 10x or more between the two plans). " +
        "compare_plan_id defaults to the query's plan with the newest last_execution_time; when that is plan_id itself the plan-to-plan checks go to unknown. " +
        "Different compiled parameter values are shown (compiled_values, masked or withheld by mcp.dataDetail) with PlanInputComparison's judgement and are never a refusal. " +
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

            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        $"No Query Store history for query_id {query_id} in database '{database_name}' in the {hours_back}-hour window searched, so there is nothing to check.");

            var plans = Fold(rows);
            var candidate = plans.FirstOrDefault(p => p.PlanId == plan_id);
            if (candidate is null)
                return McpHelpers.Status("empty",
                    $"plan_id {plan_id} has no Query Store history for query_id {query_id} in database '{database_name}' in the {hours_back}-hour window searched, so it cannot be checked.");

            PlanFold? compare;
            if (compare_plan_id is long wanted)
            {
                compare = plans.FirstOrDefault(p => p.PlanId == wanted);
                if (compare is null)
                    return McpHelpers.Status("empty", $"compare_plan_id {wanted} has no Query Store history for query_id {query_id} in the window searched.");
            }
            else
            {
                var newest = plans.OrderByDescending(p => p.LastExecution ?? DateTime.MinValue).ThenByDescending(p => p.PlanId).First();
                compare = newest.PlanId == plan_id ? null : newest;
            }

            var queryLastExecution = plans.Max(p => p.LastExecution);
            var refusals = new List<Refusal>();
            var unknown = new List<Unknown>();
            var shown = new Dictionary<string, object?>();

            /* 1. The existing gate. */
            await AddGateRefusalsAsync(postgres, resolved.ServerId, database_name, query_id, candidate, windowEnd, logger, refusals, unknown, cancellationToken);

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
                refusals.Add(new Refusal(CodeRecompileHint, "The statement has OPTION (RECOMPILE), so every call gets its own plan and a forced plan is not what runs.", new { has_recompile_hint = true }));

            /* 5. not_run_recently. */
            if (queryLastExecution is { } lastRun && windowEnd - lastRun > NotRunRecentlyAfter)
                refusals.Add(new Refusal(CodeNotRunRecently,
                    $"No plan of this query has run in the {NotRunRecentlyAfter.TotalHours:0} hours before {FormatTime(windowEnd)}.",
                    new { newest_last_execution_time = FormatTime(lastRun), as_of = FormatTime(windowEnd), threshold_hours = NotRunRecentlyAfter.TotalHours }));

            /* 6. too_few_executions. */
            if (candidate.Executions < MinPlanExecutions)
                refusals.Add(new Refusal(CodeTooFewExecutions,
                    $"Plan {plan_id} ran {candidate.Executions} times in the window, under the {MinPlanExecutions} PLAN_REGRESSION needs per plan.",
                    new { executions = candidate.Executions, minimum = MinPlanExecutions, hours_back }));

            /* 8. output_differs. */
            if (compare is null)
                unknown.Add(new Unknown(CodeOutputDiffers, compare_plan_id is null ? "plan_id is itself the query's newest plan and no compare_plan_id was given, so there is no second plan to compare" : "no second plan"));
            else if (candidate.AvgRows is not { } candRows || compare.AvgRows is not { } compRows)
                unknown.Add(new Unknown(CodeOutputDiffers, "a stored interval of one of the two plans has no avg_rowcount, so rows per call is not known"));
            else if (OutputDiffers(candRows, compRows))
                refusals.Add(new Refusal(CodeOutputDiffers,
                    "The two plans return very different row counts per call, so their cost is not comparable and the cheaper plan may be cheap only because it returns less.",
                    new { candidate_avg_rows = candRows, compare_avg_rows = compRows, ratio_threshold = PlanInputComparison.OutputGrowthRatio, row_floor = PlanInputComparison.RowFloor }));

            /* 3, 7, the compiled values and the indexes: the two stored plans. */
            var candidateXml = await ReadPlanXmlAsync(postgres, resolved.ServerId, database_name, query_id, plan_id, cancellationToken);
            var compareXml = compare is null ? null : await ReadPlanXmlAsync(postgres, resolved.ServerId, database_name, query_id, compare.PlanId, cancellationToken);
            var candidateFacts = ReadPlanFacts(candidateXml, database_name);
            var compareFacts = compare is null ? null : ReadPlanFacts(compareXml, database_name);

            if (candidateFacts is null)
                unknown.Add(new Unknown(CodeTempTable, "the candidate plan's XML is not stored, is withheld by the statement filter, or cannot be read"));
            else if (candidateFacts.TempTables.Count > 0)
                refusals.Add(new Refusal(CodeTempTable,
                    "The plan reads or writes a temporary table; a force on such a statement failed when replayed.", new { tables = candidateFacts.TempTables }));

            var pspSides = new List<object>();
            if (candidateFacts?.IsPspVariant == true) pspSides.Add(new { plan_id, basis = candidateFacts.PspBasis });
            if (compareFacts?.IsPspVariant == true) pspSides.Add(new { plan_id = compare!.PlanId, basis = compareFacts.PspBasis });
            if (pspSides.Count > 0)
            {
                refusals.Add(new Refusal(CodePspVariant,
                    "A plan is a Parameter Sensitive Plan variant; a force on a variant got 0 executions because the parent statement moved to another query and plan.", new { variants = pspSides }));
                unknown.Add(new Unknown(CodePspVariant + "_siblings", "the parent query and the sibling variants cannot be named: the store keeps no parent_query_id for a variant"));
            }
            else if (candidateFacts is null || (compare is not null && compareFacts is null))
                unknown.Add(new Unknown(CodePspVariant, "a plan's XML is not stored, is withheld or cannot be read, so whether it is a Parameter Sensitive Plan variant is not known"));

            /* 4. query_store_read_only. */
            await AddReadOnlyAsync(postgres, resolved.ServerId, database_name, windowEnd, refusals, unknown, cancellationToken);

            /* Shown. */
            shown["query_last_execution_time"] = FormatTime(queryLastExecution);
            var shownPlans = new List<object> { ShownPlan("candidate", candidate, candidateXml) };
            if (compare is not null) shownPlans.Add(ShownPlan("compare", compare, compareXml));
            shown["plans"] = shownPlans;
            if (compare is not null)
                shown["compiled_value_judgement"] = candidateXml is null || compareXml is null ? null : PlanInputComparison.JudgePlans(compareXml, candidateXml).ToString();
            shown["indexes_not_in_latest_capture"] = await MissingIndexesAsync(postgres, resolved.ServerId, database_name, windowEnd, candidateFacts, unknown, cancellationToken);
            shown["job_name_matches"] = await JobNameMatchesAsync(postgres, resolved.ServerId, windowEnd, cancellationToken);

            /* A dictionary, not an anonymous object: "shown" is this read's own section, not a page cut (the cut-key census reads a `shown =` member as one). */
            return JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["server"] = resolved.ServerName,
                ["database_name"] = database_name,
                ["query_id"] = query_id,
                ["plan_id"] = plan_id,
                ["compare_plan_id"] = compare?.PlanId,
                ["as_of"] = FormatTime(windowEnd),
                ["hours_back"] = hours_back,
                ["effective_start"] = McpHelpers.FormatEffectiveStart(effectiveStart),
                ["verdict"] = refusals.Count > 0 ? "refuse" : "no_refusal_found",
                ["refusals"] = refusals.Select(r => new { code = r.Code, sentence = r.Sentence, evidence = r.Evidence }),
                ["shown"] = shown,
                ["unknown"] = unknown.Select(u => new { check = u.Check, reason = u.Reason }),
                ["note"] = "no_refusal_found means no check fired; it does not mean the force is safe or will help."
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_plan_force_precheck", ex);
        }
    }

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

    /// <summary>
    /// The gate. The target is the current PLAN_REGRESSION finding's own for this query when there is one (its replica role and
    /// parameter-sensitivity flag come from the finding, <see cref="FactRemediation.ExtractPlanRegressionTargets"/> builds it), with
    /// the plan swapped to plan_id. With no finding the target comes from the store rows: database, query and plan, the plan's last
    /// run as the best-plan age, and every finding-only field at its default (no replica role, no parameter-sensitivity flag, factor 0),
    /// and the two finding-only blockers go to <c>unknown</c> because nothing here can answer them.
    /// </summary>
    private static async Task AddGateRefusalsAsync(
        NpgsqlDataSource postgres, int serverId, string database, long queryId, PlanFold candidate, DateTime windowEnd, ILogger? logger,
        List<Refusal> refusals, List<Unknown> unknown, CancellationToken ct)
    {
        ForcePlanTarget? fromFinding = null;
        try
        {
            var findings = await new PgFindingStore(postgres, logger).GetRecentFindingsAsync(serverId, 24, FindingOccurrences.WindowCoveringLimit, windowEnd, ct);
            fromFinding = findings
                .Where(f => string.Equals(f.RootFactKey, "PLAN_REGRESSION", StringComparison.Ordinal) && f.Remediation?.Targets is { Count: > 0 })
                .OrderByDescending(f => f.AnalysisTime)
                .SelectMany(f => f.Remediation!.Targets)
                .FirstOrDefault(t => t.QueryId == queryId && string.Equals(t.Database, database, StringComparison.OrdinalIgnoreCase));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "The PLAN_REGRESSION finding read for get_plan_force_precheck failed for server {ServerId}.", serverId);
            unknown.Add(new Unknown("parameter_sensitivity_cofired", "the current PLAN_REGRESSION findings could not be read"));
        }

        ForcePlanTarget target;
        if (fromFinding is not null)
        {
            target = fromFinding with { PlanId = candidate.PlanId, BestPlanHash = null };
        }
        else
        {
            target = new ForcePlanTarget(database, queryId, candidate.PlanId, BestPlanLastSeenUtc: candidate.LastExecution);
            unknown.Add(new Unknown(ForcePlanBlockerNames.ParameterSensitivityCoFired, "no current PLAN_REGRESSION finding for this query, so the parameter-sensitivity signature is not known"));
            unknown.Add(new Unknown(ForcePlanBlockerNames.SecondaryReplicaEvidence, "no current PLAN_REGRESSION finding for this query, so the replica the regression was measured on is not known"));
        }

        var carrier = new AnalysisFinding
        {
            ServerId = serverId,
            RootFactKey = "PLAN_REGRESSION",
            Remediation = new RemediationAction("PLAN_REGRESSION", "force", new[] { target }),
        };
        var (states, reason) = await DarlingForcePlanTargetStateReader.TryReadAsync(postgres, serverId, new[] { carrier }, windowEnd, logger, ct);
        ForcePlanTargetState? state = null;
        states?.TryGetValue(ForcePlanTargetKey.Of(target), out state);
        foreach (var blocker in ForcePlanBotPolicy.Blockers(target, state, states is null ? reason : null, enablementIsCollected: false))
            refusals.Add(new Refusal(blocker.Name, blocker.Evidence, null));
    }

    private static async Task AddReadOnlyAsync(
        NpgsqlDataSource postgres, int serverId, string database, DateTime windowEnd, List<Refusal> refusals, List<Unknown> unknown, CancellationToken ct)
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
            refusals.Add(new Refusal(CodeQueryStoreReadOnly,
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
