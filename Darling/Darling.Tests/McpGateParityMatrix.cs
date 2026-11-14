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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/// <summary>The app a row is asked of.</summary>
public enum GateApp
{
    Lite,
    Darling,
}

/// <summary>The token a configuration stores: none, one under the floor (15 characters), or one that clears it.</summary>
public enum GateToken
{
    None,
    UnderFloor,
    Usable,
}

/// <summary>What a request presents. The last three are Darling's named tokens, one per role.</summary>
public enum GateCaller
{
    NoToken,
    WrongToken,
    StoredToken,
    OperatorToken,
    AgentToken,
    ReaderToken,
}

/// <summary>How a <c>tools/call</c> is answered: 401 before the request is read, the SDK's unknown-tool error, or dispatch to the tool.</summary>
public enum GateOutcome
{
    Unauthorized,
    UnknownTool,
    Served,
    Unrecognized,
}

/// <summary>One configuration of the MCP gate. <paramref name="NamedTokens"/> adds Darling's three named tokens (an operator, an
/// agent and a reader); Lite has no named tokens, so its suite runs the configurations without them.</summary>
internal sealed record GateConfig(
    bool Writes, bool ServerAdmin, bool Investigations, GateToken Token, bool TokenlessLoopback, bool NamedTokens = false);

/// <summary>A started app: the client aimed at its listener.</summary>
internal interface IGateHost
{
    HttpClient Client { get; }
}

/// <summary>
/// The one matrix both apps' MCP gates are held to, and the one rule that says what each cell answers. Lite.Tests and
/// Darling.Tests compile this file; each suite starts its own app's host for a configuration and hands it to
/// <see cref="CheckAsync"/>, which sends the same requests to either app and compares each answer with <see cref="Expected"/>.
/// <para>
/// The rule, in the order a request meets it. A configuration with no token and the tokenless setting off does not start.
/// A token in force refuses every request that does not carry it with 401. Then the tool decides: a read is served; a write
/// is served under the writes arm (the writes setting and a usable token); an investigation state tool is served under the
/// investigations arm (its setting, a usable token and the writes setting off); anything else is an unknown tool.
/// </para>
/// <para>
/// The apps differ by intent in two places, each written once below: <see cref="TokenInForce"/> (Lite's tokenless setting
/// ignores a stored token, Darling's applies only when no token is stored) and <see cref="Registers"/> (the investigations
/// arm serves 2 tools in Lite and 19 in Darling).
/// </para>
/// </summary>
internal static class McpGateParityMatrix
{
    internal const string UsableToken = "gate-parity-stored-token-0123456789abcdef";
    internal const string UnderFloorToken = "gate-parity-low";
    internal const string WrongToken = "gate-parity-wrong-token-fedcba9876543210";
    internal const string OperatorToken = "gate-parity-operator-token-0123456789ab";
    internal const string AgentToken = "gate-parity-agent-token-0123456789abcdef";
    internal const string ReaderToken = "gate-parity-reader-token-0123456789abcde";

    /// <summary>The argument every call carries. No tool declares it, so a served tool is refused by the call-tool guard both
    /// hosts register before the tool body runs, and an unserved one gets the SDK's unknown-tool error: no call reads or writes a store.</summary>
    internal const string ProbeArgument = "gate_parity_probe";

    /// <summary>The reads both apps register: served to every admitted request.</summary>
    internal static readonly string[] SharedReads =
    {
        "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan", "analyze_server",
        "audit_config", "compare_analysis", "find_database", "get_active_queries", "get_alert_history", "get_alert_settings",
        "get_analysis_facts", "get_analysis_findings", "get_blocked_process_xml", "get_blocking_stats", "get_blocking_trend",
        "get_client_timeouts", "get_collection_health", "get_collection_log", "get_cpu_by_database", "get_cpu_scheduler_pressure",
        "get_cpu_utilization", "get_current_waits_trend", "get_daily_summary", "get_daily_summary_range", "get_database_config",
        "get_database_config_changes", "get_database_scoped_config", "get_database_sizes", "get_deadlock_detail", "get_deadlock_trend",
        "get_deadlocks", "get_default_trace_events", "get_file_io_stats", "get_file_io_trend", "get_health_parser_cpu_tasks",
        "get_health_parser_io_issues", "get_health_parser_memory_broker", "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues", "get_health_parser_severe_errors", "get_health_parser_significant_waits",
        "get_health_parser_system_health", "get_index_usage", "get_investigation_reports", "get_latch_stats", "get_load_by_caller",
        "get_lock_wait_trend", "get_long_query_completions", "get_memory_clerks", "get_memory_grants", "get_memory_pressure_events",
        "get_memory_stats", "get_memory_trend", "get_mute_rules", "get_notification_routes", "get_object_locking", "get_perfmon_stats",
        "get_perfmon_trend", "get_plan_cache_bloat", "get_plan_corrections", "get_plan_xml", "get_procedure_duration_trend",
        "get_pvs_stats", "get_query_duration_trend", "get_query_heatmap", "get_query_pileups", "get_query_store_duration_trend",
        "get_query_store_health", "get_query_store_regressions", "get_query_store_top", "get_query_trend", "get_resource_semaphore",
        "get_resource_use_by_tag", "get_running_jobs", "get_server_config", "get_server_config_changes", "get_server_properties",
        "get_server_summary", "get_session_stats", "get_spinlock_stats", "get_table_index_sizes", "get_tempdb_trend", "get_tool_guide",
        "get_top_procedures_by_cpu", "get_top_queries_by_cpu", "get_trace_flag_changes", "get_trace_flags", "get_wait_stats",
        "get_wait_trend", "get_wait_types", "get_waiting_tasks", "list_investigation_requests", "list_query_tag_rules",
        "list_query_tags", "list_servers", "test_query_tag_rule",
    };

    /// <summary>The writes both apps register: served only under the writes arm.</summary>
    internal static readonly string[] SharedWrites =
    {
        "mute_analysis_finding", "unmute_analysis_finding", "set_notification_route_enabled", "delete_notification_route",
        "create_query_tag", "update_query_tag", "delete_query_tag", "assign_query_tag", "unassign_query_tag",
        "create_query_tag_rule", "update_query_tag_rule", "delete_query_tag_rule",
    };

    /// <summary>The investigation state tools both apps register: served only under the investigations arm.</summary>
    internal static readonly string[] SharedInvestigationTools =
    {
        "claim_investigation_request", "submit_investigation_report",
    };

    /// <summary>The message an agent posts to a request's thread: Darling's third investigation state tool.</summary>
    internal const string DarlingInvestigationMessageTool = "post_investigation_message";

    /// <summary>Darling's agent action tools: writes that Darling also serves under the investigations arm, where each call
    /// is decided inside the tool.</summary>
    internal static readonly string[] DarlingAgentActionTools =
    {
        "create_mute_rule", "update_mute_rule", "delete_mute_rule", "set_mute_rule_enabled",
        "create_custom_alert_rule", "update_custom_alert_rule", "delete_custom_alert_rule",
        "set_alert_threshold_override", "merge_deadlock_signatures",
        "force_query_store_plan", "unforce_query_store_plan", "evict_cached_plan", "kill_session",
        "recompile_module", "create_plan_guide", "drop_plan_guide",
    };

    /// <summary>The 17 tools Darling's investigations arm serves beyond the 2 shared ones. Lite has no tool of these names
    /// (<see cref="Registers"/>).</summary>
    internal static readonly string[] DarlingInvestigationArmTools =
        DarlingAgentActionTools.Append(DarlingInvestigationMessageTool).ToArray();

    /// <summary>Lite's tools with no Darling tool of the same name: a read each. Darling's twin of get_blocked_process_reports is <c>get_blocking</c>; get_store_health (#8788) has none, Darling's store page being #8787.</summary>
    internal static readonly string[] LiteOnlyReads = { "get_blocked_process_reports", "get_store_health" };

    /// <summary>Every tool name a row calls, in both apps: the 112 shared names and the names one app alone answers.</summary>
    internal static readonly string[] Tools = SharedReads
        .Concat(SharedWrites)
        .Concat(SharedInvestigationTools)
        .Concat(DarlingInvestigationArmTools)
        .Concat(LiteOnlyReads)
        .ToArray();

    private static readonly bool[] OffOn = { false, true };

    /// <summary>The shared configurations: every combination of the three switches, the stored token and the tokenless setting.</summary>
    internal static IEnumerable<GateConfig> Configurations() =>
        from writes in OffOn
        from serverAdmin in OffOn
        from investigations in OffOn
        from token in new[] { GateToken.None, GateToken.UnderFloor, GateToken.Usable }
        from tokenless in OffOn
        select new GateConfig(writes, serverAdmin, investigations, token, tokenless);

    /// <summary>Darling's named-token configurations: the three roles with every switch off and with every switch on, with the
    /// tokenless setting on, and beside a stored token under each arm.</summary>
    internal static IEnumerable<GateConfig> NamedTokenConfigurations()
    {
        yield return new GateConfig(false, false, false, GateToken.None, false, NamedTokens: true);
        yield return new GateConfig(true, true, true, GateToken.None, false, NamedTokens: true);
        yield return new GateConfig(false, false, false, GateToken.None, true, NamedTokens: true);
        yield return new GateConfig(false, false, false, GateToken.Usable, false, NamedTokens: true);
        yield return new GateConfig(true, true, false, GateToken.Usable, false, NamedTokens: true);
        yield return new GateConfig(false, false, true, GateToken.Usable, false, NamedTokens: true);
    }

    /// <summary>The callers a configuration is asked by. One that does not start has a single row, for the refused start.</summary>
    internal static IEnumerable<GateCaller> Callers(GateConfig config)
    {
        yield return GateCaller.NoToken;
        if (!Starts(config))
        {
            yield break;
        }

        yield return GateCaller.WrongToken;
        if (config.Token != GateToken.None)
        {
            yield return GateCaller.StoredToken;
        }

        if (config.NamedTokens)
        {
            yield return GateCaller.OperatorToken;
            yield return GateCaller.AgentToken;
            yield return GateCaller.ReaderToken;
        }
    }

    /// <summary>The shared rows: each shared configuration with each of its callers. Every row calls every name in <see cref="Tools"/>.</summary>
    public static TheoryData<bool, bool, bool, GateToken, bool, GateCaller> SharedRows() => RowsOf(Configurations());

    /// <summary>Darling's named-token rows. The fifth and sixth columns are as in <see cref="SharedRows"/>.</summary>
    public static TheoryData<bool, bool, bool, GateToken, bool, GateCaller> NamedTokenRows() => RowsOf(NamedTokenConfigurations());

    private static TheoryData<bool, bool, bool, GateToken, bool, GateCaller> RowsOf(IEnumerable<GateConfig> configurations)
    {
        var rows = new TheoryData<bool, bool, bool, GateToken, bool, GateCaller>();
        foreach (var config in configurations)
        {
            foreach (var caller in Callers(config))
            {
                rows.Add(config.Writes, config.ServerAdmin, config.Investigations, config.Token, config.TokenlessLoopback, caller);
            }
        }

        return rows;
    }

    /// <summary>The stored token's value, or null for none.</summary>
    internal static string? StoredTokenValue(GateConfig config) => config.Token switch
    {
        GateToken.Usable => UsableToken,
        GateToken.UnderFloor => UnderFloorToken,
        _ => null,
    };

    /// <summary>The bearer token a caller presents, or null for no <c>Authorization</c> header.</summary>
    internal static string? Bearer(GateConfig config, GateCaller caller) => caller switch
    {
        GateCaller.WrongToken => WrongToken,
        GateCaller.StoredToken => StoredTokenValue(config),
        GateCaller.OperatorToken => OperatorToken,
        GateCaller.AgentToken => AgentToken,
        GateCaller.ReaderToken => ReaderToken,
        _ => null,
    };

    /// <summary>Whether the app starts at all: both refuse a start with no token unless the tokenless setting is on.</summary>
    internal static bool Starts(GateConfig config) =>
        config.NamedTokens || config.Token != GateToken.None || config.TokenlessLoopback;

    /// <summary>INTENDED DIFFERENCE: whether a token gates the listener. Lite's tokenless setting ignores a stored token, so
    /// nothing gates and only the reads are served; Darling's applies only when no token is stored, so a stored token keeps
    /// gating and the switches keep their meaning. Named tokens always gate.</summary>
    internal static bool TokenInForce(GateApp app, GateConfig config)
    {
        if (config.NamedTokens)
        {
            return true;
        }

        if (config.Token == GateToken.None)
        {
            return false;
        }

        return !(app == GateApp.Lite && config.TokenlessLoopback);
    }

    /// <summary>INTENDED DIFFERENCE: whether the app has a tool of this name at all. Lite has none of
    /// <see cref="DarlingInvestigationArmTools"/>, so its investigations arm serves the 2 shared investigation tools where
    /// Darling's serves 19. Darling has no tool named as Lite's <see cref="LiteOnlyReads"/>.</summary>
    internal static bool Registers(GateApp app, string tool) => app == GateApp.Lite
        ? !DarlingInvestigationArmTools.Contains(tool)
        : !LiteOnlyReads.Contains(tool);

    /// <summary>Whether the request passes the token gate.</summary>
    internal static bool Admitted(GateApp app, GateConfig config, GateCaller caller)
    {
        if (!TokenInForce(app, config))
        {
            return true;
        }

        return caller switch
        {
            GateCaller.StoredToken => config.Token != GateToken.None,
            GateCaller.OperatorToken or GateCaller.AgentToken or GateCaller.ReaderToken => config.NamedTokens,
            _ => false,
        };
    }

    /// <summary>The two arms an admitted caller is served under. The stored token follows the switches and needs to clear the
    /// floor; a named token follows its role whatever the switches say: an operator has the writes, an agent the
    /// investigations arm, a reader neither.</summary>
    internal static (bool Writes, bool Investigations) Arms(GateApp app, GateConfig config, GateCaller caller)
    {
        switch (caller)
        {
            case GateCaller.OperatorToken:
                return (true, false);
            case GateCaller.AgentToken:
                return (false, true);
            case GateCaller.ReaderToken:
                return (false, false);
        }

        var usable = TokenInForce(app, config) && config.Token == GateToken.Usable && caller == GateCaller.StoredToken;
        return (config.Writes && usable, config.Investigations && usable && !config.Writes);
    }

    /// <summary>THE rule: what one call answers. The server-admin switch moves no name here, because it serves only tools
    /// Lite does not have.</summary>
    internal static GateOutcome Expected(GateApp app, GateConfig config, GateCaller caller, string tool)
    {
        if (!Admitted(app, config, caller))
        {
            return GateOutcome.Unauthorized;
        }

        if (!Registers(app, tool))
        {
            return GateOutcome.UnknownTool;
        }

        var (writes, investigations) = Arms(app, config, caller);
        bool served;
        if (SharedReads.Contains(tool) || LiteOnlyReads.Contains(tool))
        {
            served = true;
        }
        else if (SharedWrites.Contains(tool))
        {
            served = writes;
        }
        else if (SharedInvestigationTools.Contains(tool) || tool == DarlingInvestigationMessageTool)
        {
            served = investigations;
        }
        else if (DarlingAgentActionTools.Contains(tool))
        {
            served = investigations || writes;
        }
        else
        {
            throw new ArgumentException($"'{tool}' is not a tool in the matrix.", nameof(tool));
        }

        return served ? GateOutcome.Served : GateOutcome.UnknownTool;
    }

    /// <summary>The matrix names <see cref="Expected"/> serves this caller, or null when it is refused at the gate.</summary>
    internal static SortedSet<string>? ExpectedListed(GateApp app, GateConfig config, GateCaller caller) =>
        Admitted(app, config, caller)
            ? new SortedSet<string>(Tools.Where(tool => Expected(app, config, caller, tool) == GateOutcome.Served), StringComparer.Ordinal)
            : null;

    /// <summary>
    /// Runs one row against a started app and returns every cell that differs from <see cref="Expected"/>, empty when the
    /// row holds. An admitted caller's <c>tools/list</c> must hold exactly the matrix names the rule serves, and a call to each
    /// name in <see cref="Tools"/> must answer what the rule says, so nothing is listed but refused and nothing is hidden but
    /// callable. For Lite the comparison is the whole list: every tool Lite serves is a name in the matrix.
    /// A caller the gate refuses is answered before the request is read, whatever it asks for. With no token presented every
    /// call is sent; a wrong token is presented once, on <c>tools/list</c>, because Darling holds a source that keeps presenting wrong tokens.
    /// </summary>
    internal static async Task<List<string>> CheckAsync(GateApp app, GateConfig config, GateCaller caller, IGateHost host)
    {
        var differences = new List<string>();
        var bearer = Bearer(config, caller);
        var expectedListed = ExpectedListed(app, config, caller);

        var listed = await ListToolsAsync(host.Client, bearer);
        if (expectedListed is null)
        {
            if (listed.Status != HttpStatusCode.Unauthorized)
            {
                differences.Add($"tools/list: expected 401, got {(int)listed.Status}");
            }

            if (caller == GateCaller.WrongToken)
            {
                return differences;
            }
        }
        else if (listed.Names is null)
        {
            differences.Add($"tools/list: expected the tools, got {(int)listed.Status} {listed.Detail}");
        }
        else
        {
            var inMatrix = app == GateApp.Lite
                ? listed.Names
                : new SortedSet<string>(listed.Names.Intersect(Tools), StringComparer.Ordinal);
            foreach (var name in expectedListed.Except(inMatrix))
            {
                differences.Add($"tools/list: '{name}' should be listed and is not");
            }

            foreach (var name in inMatrix.Except(expectedListed))
            {
                differences.Add($"tools/list: '{name}' is listed and should not be");
            }
        }

        foreach (var tool in Tools)
        {
            var expected = Expected(app, config, caller, tool);
            var (outcome, detail) = await CallAsync(host.Client, bearer, tool);
            if (outcome != expected)
            {
                differences.Add($"tools/call {tool}: expected {expected}, got {outcome}{detail}");
            }
        }

        return differences;
    }

    /// <summary>A usable stored token with every switch off, and the same with the investigations switch on: the pair that
    /// shows what the investigations arm adds.</summary>
    internal static readonly GateConfig ReadsOnly = new(false, false, false, GateToken.Usable, false);

    internal static readonly GateConfig InvestigationsArm = ReadsOnly with { Investigations = true };

    /// <summary>
    /// The investigations arm, by the whole of <c>tools/list</c>: turning the switch on adds exactly the names the rule
    /// serves under the arm, 2 in Lite and 19 in Darling, and takes none away. The two full lists are compared, so a tool
    /// outside the matrix that the arm serves fails here.
    /// </summary>
    internal static async Task AssertInvestigationsArmAddsWhatTheRuleNamesAsync(GateApp app, IGateHost readsOnly, IGateHost investigationsArm)
    {
        var armOff = await ListToolsAsync(readsOnly.Client, UsableToken);
        var armOn = await ListToolsAsync(investigationsArm.Client, UsableToken);
        Assert.True(armOff.Names is not null, $"tools/list with every switch off: {(int)armOff.Status} {armOff.Detail}");
        Assert.True(armOn.Names is not null, $"tools/list under the investigations arm: {(int)armOn.Status} {armOn.Detail}");

        var expected = new SortedSet<string>(
            Tools.Where(tool => Expected(app, InvestigationsArm, GateCaller.StoredToken, tool) == GateOutcome.Served
                && Expected(app, ReadsOnly, GateCaller.StoredToken, tool) != GateOutcome.Served),
            StringComparer.Ordinal);
        Assert.Equal(app == GateApp.Lite ? 2 : 19, expected.Count);
        Assert.Equal(expected, new SortedSet<string>(armOn.Names!.Except(armOff.Names!), StringComparer.Ordinal));
        Assert.Empty(armOff.Names!.Except(armOn.Names!));
    }

    /// <summary>Fails with every differing cell of a row, so one broken gate shows each tool it moved.</summary>
    internal static void AssertNoDifferences(GateApp app, GateConfig config, GateCaller caller, List<string> differences) =>
        Assert.True(
            differences.Count == 0,
            $"{app}, {config}, caller {caller}: {differences.Count} of {Tools.Length + 1} answers differ from the shared rule:"
            + Environment.NewLine + string.Join(Environment.NewLine, differences));

    private static async Task<(HttpStatusCode Status, string Body)> PostAsync(HttpClient client, string? bearer, string json)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/")
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/event-stream");
        if (bearer is not null)
        {
            request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + bearer);
        }

        using var response = await client.SendAsync(request);
        return (response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    /// <summary>The transport answers as an event stream (one <c>data:</c> line of JSON) or as bare JSON.</summary>
    private static string JsonPayload(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(line => line.StartsWith("data:", StringComparison.Ordinal));
        return dataLine is null ? body : dataLine["data:".Length..].Trim();
    }

    private static string Shown(string body) => body.Length <= 300 ? body : body[..300] + "...";

    /// <summary>The names <c>tools/list</c> returns, following its cursor; null names with the status when the request is refused.</summary>
    internal static async Task<(HttpStatusCode Status, SortedSet<string>? Names, string Detail)> ListToolsAsync(HttpClient client, string? bearer)
    {
        var names = new SortedSet<string>(StringComparer.Ordinal);
        string? cursor = null;
        do
        {
            var parameters = cursor is null ? "{}" : "{\"cursor\":" + JsonSerializer.Serialize(cursor) + "}";
            var (status, body) = await PostAsync(
                client, bearer, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":" + parameters + "}");
            if (status != HttpStatusCode.OK)
            {
                return (status, null, Shown(body));
            }

            using var document = JsonDocument.Parse(JsonPayload(body));
            if (!document.RootElement.TryGetProperty("result", out var result) || !result.TryGetProperty("tools", out var tools))
            {
                return (status, null, Shown(body));
            }

            foreach (var tool in tools.EnumerateArray())
            {
                names.Add(tool.GetProperty("name").GetString()!);
            }

            cursor = result.TryGetProperty("nextCursor", out var next) && next.ValueKind == JsonValueKind.String ? next.GetString() : null;
        }
        while (cursor is not null);

        return (HttpStatusCode.OK, names, string.Empty);
    }

    /// <summary>Calls <paramref name="tool"/> with <see cref="ProbeArgument"/> and says which of the three answers came back.</summary>
    internal static async Task<(GateOutcome Outcome, string Detail)> CallAsync(HttpClient client, string? bearer, string tool)
    {
        var (status, body) = await PostAsync(
            client, bearer,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"" + tool + "\",\"arguments\":{\"" + ProbeArgument + "\":true}}}");
        if (status == HttpStatusCode.Unauthorized)
        {
            return (GateOutcome.Unauthorized, string.Empty);
        }

        if (status != HttpStatusCode.OK)
        {
            return (GateOutcome.Unrecognized, $" (HTTP {(int)status} {Shown(body)})");
        }

        using var document = JsonDocument.Parse(JsonPayload(body));
        var root = document.RootElement;
        if (root.TryGetProperty("error", out var error)
            && error.TryGetProperty("message", out var message)
            && (message.GetString() ?? string.Empty).Contains("Unknown tool", StringComparison.OrdinalIgnoreCase))
        {
            return (GateOutcome.UnknownTool, string.Empty);
        }

        if (root.TryGetProperty("result", out var result)
            && result.TryGetProperty("isError", out var isError) && isError.ValueKind == JsonValueKind.True
            && result.TryGetProperty("content", out var content) && content.ValueKind == JsonValueKind.Array
            && content.EnumerateArray().Any(block => block.TryGetProperty("text", out var text)
                && (text.GetString() ?? string.Empty).Replace("\\u0027", "'", StringComparison.Ordinal)
                    .Contains($"Unknown argument '{ProbeArgument}' for tool '{tool}'", StringComparison.Ordinal)))
        {
            return (GateOutcome.Served, string.Empty);
        }

        return (GateOutcome.Unrecognized, $" ({Shown(body)})");
    }
}
