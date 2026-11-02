/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 Phase A, the investigation MCP tools without a store: the <c>mcp.allowInvestigations</c> gate (ruling R2) and
/// its one Critical line, the annotation hints, the classification of the four tools, the report validation
/// (every bad field at once, action proposals refused at Level 1, the note and summary lengths), the formats, and the
/// agent-route refusal on the two route writes (ruling 1: agent_level 1 or more only). The store paths are
/// <see cref="InvestigationToolsLiveTests"/>.
/// </summary>
public sealed class DarlingMcpInvestigationToolsTests
{
    private const string Token = "investigation-gate-token-value-0123456789";

    private const string Evidence = "[{\"tool\":\"get_wait_stats\",\"arguments\":{\"server_name\":\"s1\"},\"key_numbers\":\"LCK_M_X 82%\"}]";

    [Fact]
    public void ResolveForStart_WritesOff_InvestigationsOn_WithAToken_ServesThem()
    {
        var effective = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowInvestigations = true }, Token, out var problem);

        Assert.Null(problem);
        Assert.True(effective.Investigations);
        Assert.False(effective.Writes);
        Assert.True(effective.Offers("claim_investigation_request"));
        Assert.True(effective.Offers("submit_investigation_report"));
        /* #5915: the Level 2 action tools are offered to an agent (each through the gate); every other write is not. */
        Assert.True(effective.Offers("create_mute_rule"));
        Assert.True(effective.Offers("delete_custom_alert_rule"));
        Assert.False(effective.Offers("update_alert_settings"));
        Assert.False(effective.Offers("mute_analysis_finding"));
        Assert.False(effective.Offers("set_notification_route_enabled"));
        Assert.False(effective.Offers("edit_server"));
    }

    [Fact]
    public void ResolveForStart_BothOn_KeepsTheInvestigationToolsOff_AndServesTheWrites()
    {
        var mcp = new McpConfig { AllowInvestigations = true, AllowWrites = true };
        var effective = DarlingMcpWritePolicy.ResolveForStart(mcp, Token, out _);

        Assert.True(DarlingMcpWritePolicy.InvestigationsConflict(mcp));
        Assert.False(effective.Investigations);
        Assert.True(effective.Writes);
        Assert.False(effective.Offers("claim_investigation_request"));
        Assert.False(effective.Offers("submit_investigation_report"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("short")]
    public void ResolveForStart_NoUsableToken_KeepsThemOff(string token)
    {
        var effective = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowInvestigations = true }, token, out var problem);

        Assert.False(effective.Investigations);
        Assert.Equal(token.Length > 0, problem is not null);
    }

    [Fact]
    public void ResolveForStart_Default_ServesNeitherInvestigationTool_ButBothReads()
    {
        var effective = DarlingMcpWritePolicy.ResolveForStart(new McpConfig(), Token, out _);

        Assert.Equal(DarlingMcpWritePolicy.Effective.Off, effective);
        Assert.False(effective.Offers("claim_investigation_request"));
        Assert.True(effective.Offers("list_investigation_requests"));
        Assert.True(effective.Offers("get_investigation_reports"));
    }

    [Fact]
    public void Start_BothOn_LogsTheConflictLine_ExactlyOnce_AsTheOnlyCritical()
    {
        var mcp = new McpConfig { AllowInvestigations = true, AllowWrites = true };
        var logger = new CapturingTestLogger();

        DarlingMcpHostService.LogInvestigationsGate(logger, mcp, DarlingMcpWritePolicy.ResolveForStart(mcp, Token, out var problem), problem);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        Assert.Single(logger.Lines);
        Assert.Contains(DarlingMcpWritePolicy.InvestigationConflictLine, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_Served_WarnsOnce_NoToken_IsCritical_Off_LogsNothing()
    {
        var on = new McpConfig { AllowInvestigations = true };

        var served = new CapturingTestLogger();
        DarlingMcpHostService.LogInvestigationsGate(served, on, DarlingMcpWritePolicy.ResolveForStart(on, Token, out var p1), p1);
        Assert.Equal(1, served.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, served.CountAtLevel(LogLevel.Critical));

        var noToken = new CapturingTestLogger();
        DarlingMcpHostService.LogInvestigationsGate(noToken, on, DarlingMcpWritePolicy.ResolveForStart(on, "", out var p2), p2);
        Assert.Equal(1, noToken.CountAtLevel(LogLevel.Critical));
        Assert.Contains("investigation tools stay off", noToken.Joined, StringComparison.Ordinal);

        var off = new CapturingTestLogger();
        DarlingMcpHostService.LogInvestigationsGate(off, new McpConfig(), DarlingMcpWritePolicy.Effective.Off, null);
        Assert.Empty(off.Lines);
    }

    [Fact]
    public void Annotations_ClaimAndSubmit_AreNeitherReadOnlyNorDestructive_TheReadsAreReadOnly()
    {
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
            all.Add(tool);

        DarlingMcpWritePolicy.Apply(all, new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true));

        var byName = all.ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool.Annotations!, StringComparer.Ordinal);
        foreach (var name in DarlingMcpWritePolicy.InvestigationTools)
        {
            Assert.False(byName[name].ReadOnlyHint, $"{name} writes investigation state");
            Assert.False(byName[name].DestructiveHint, $"{name} changes no configuration and no collected data");
        }

        foreach (var name in new[] { "list_investigation_requests", "get_investigation_reports" })
        {
            Assert.True(byName[name].ReadOnlyHint);
            Assert.False(byName[name].DestructiveHint);
        }

        /* #5915, #5916: the investigations arm serves exactly the nine Level 2 action tools of the writes, each through the gate. The
           literal is the behaviour pin: a write added to or dropped from the served set changes this list on purpose. */
        var servedWrites = byName.Keys.Where(DarlingMcpWritePolicy.IsWrite).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(
            new[]
            {
                "create_custom_alert_rule", "create_mute_rule", "create_plan_guide", "delete_custom_alert_rule", "delete_mute_rule",
                "drop_plan_guide", "evict_cached_plan", "force_query_store_plan", "kill_session", "merge_deadlock_signatures", "recompile_module", "set_alert_threshold_override", "set_mute_rule_enabled", "unforce_query_store_plan", "update_custom_alert_rule", "update_mute_rule",
            },
            servedWrites);
        /* Every other write stays unserved: alert settings, the finding mute, routes, Custom Views, server tags, server admin. */
        foreach (var never in new[]
        {
            "update_alert_settings", "mute_analysis_finding", "unmute_analysis_finding", "undo_plan_change", "set_notification_route_enabled", "set_notification_route_fallback", "delete_notification_route",
            "create_custom_view", "update_custom_view", "delete_custom_view", "create_server_tag", "update_server_tag",
            "delete_server_tag", "assign_server_tag", "unassign_server_tag", "add_servers", "edit_server", "remove_server",
        })
            Assert.DoesNotContain(never, byName.Keys);
        /* Served to an agent, they are still changes to configuration: not read-only, and flagged destructive. */
        foreach (var name in servedWrites)
        {
            Assert.False(byName[name].ReadOnlyHint, $"{name} changes configuration");
            Assert.True(byName[name].DestructiveHint, $"{name} is a destructive change");
        }
    }

    [Fact]
    public void Classification_TheReadsAreReadOnly_ClaimAndSubmitAreTheThirdSet_NeverWrites()
    {
        Assert.Contains("list_investigation_requests", DarlingMcpToolClassification.ReadOnlyTools);
        Assert.Contains("get_investigation_reports", DarlingMcpToolClassification.ReadOnlyTools);
        Assert.Equal(
            new[] { "claim_investigation_request", "submit_investigation_report" },
            DarlingMcpWritePolicy.InvestigationTools.OrderBy(n => n, StringComparer.Ordinal));
        Assert.Empty(DarlingMcpWritePolicy.InvestigationTools.Intersect(DarlingMcpWritePolicy.WriteTools));
        Assert.Empty(DarlingMcpWritePolicy.InvestigationTools.Intersect(DarlingMcpToolClassification.ReadOnlyTools));
    }

    [Fact]
    public void Instructions_NameTheInvestigationSwitch_AndTheWritesOffCondition()
    {
        Assert.Contains(
            "Investigation tools (claim_investigation_request, submit_investigation_report) write only investigation state and are " +
            "served when darling.json sets mcp.allowInvestigations with a token and mcp.allowWrites is off.",
            DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void Formats_ReportFormat0_IsRefused_1_IsAccepted_TheBriefIs1()
    {
        Assert.False(InvestigationFormats.IsAcceptedReportFormat(0));
        Assert.True(InvestigationFormats.IsAcceptedReportFormat(1));
        Assert.Equal(1, InvestigationFormats.BriefFormat);
    }

    [Fact]
    public void ValidateReport_AnEmptyReport_ListsEveryMissingField_AtOnce()
    {
        var problems = InvestigationToolCore.ValidateReport(
            null, null, null, null, null, null, null, null, out _, out _, out _);

        Assert.Equal(
            new[] { "request_id", "claim_id", "verdict", "root_cause", "summary", "evidence" },
            problems.Select(p => p.Split(':')[0]));

        var message = InvestigationToolCore.InvalidReportMessage(problems);
        foreach (var problem in problems)
            Assert.Contains(problem, message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateReport_AValidReport_HasNoProblem_AndNormalizesTheEvidence()
    {
        var problems = InvestigationToolCore.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "a blocking chain", Evidence, "summary",
            new string('n', InvestigationToolCore.MaxNoteChars), "[]", out _, out _, out var evidenceJson);

        Assert.Empty(problems);
        Assert.NotNull(evidenceJson);
    }

    [Theory]
    [InlineData("[{\"kind\":\"kill_session\"}]")]
    [InlineData("anything at all")]
    public void ValidateReport_AnyActionProposal_IsRefusedAtLevel1(string proposals)
    {
        var problems = InvestigationToolCore.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "cause", Evidence, "summary", null, proposals,
            out _, out _, out _);

        var only = Assert.Single(problems);
        Assert.StartsWith("action_proposals:", only, StringComparison.Ordinal);
    }

    /// <summary>#7109: Darling's submit passes <c>serverActionTools</c>. An <c>action_proposals</c> report stays refused, and the message names the seven
    /// action tools to call with <c>dry_run</c> false, where the proposal is stored.</summary>
    [Theory]
    [InlineData("[{\"kind\":\"kill_session\"}]")]
    [InlineData("anything at all")]
    public void ValidateReport_OnDarling_RefusesActionProposals_AndNamesTheActionTools(string proposals)
    {
        var problems = InvestigationToolCore.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "cause",
            "[{\"tool\":\"get_wait_stats\",\"arguments\":{},\"key_numbers\":\"LCK_M_X 82%\",\"basis\":\"verified\"}]", "summary", null, proposals,
            out _, out _, out _, requireBasis: true, serverActionTools: true);

        var only = Assert.Single(problems);
        Assert.StartsWith("action_proposals: refused", only, StringComparison.Ordinal);
        foreach (var tool in new[]
        {
            "force_query_store_plan", "unforce_query_store_plan", "evict_cached_plan", "kill_session", "recompile_module", "create_plan_guide", "drop_plan_guide",
        })
            Assert.Contains(tool, only, StringComparison.Ordinal);
        Assert.Contains("dry_run false", only, StringComparison.Ordinal);
        Assert.Contains("action_proposals", InvestigationToolCore.InvalidReportMessage(problems), StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateReport_ANoteOverItsCap_AndAWrongVerdict_AndEvidenceWithoutNumbers_AreEachNamed()
    {
        var problems = InvestigationToolCore.ValidateReport(
            Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "maybe", "cause",
            "[{\"tool\":\"get_wait_stats\",\"arguments\":{}}]", "summary",
            new string('n', InvestigationToolCore.MaxNoteChars + 1), null, out _, out _, out _);

        Assert.Equal(new[] { "verdict", "proposed_note", "evidence" }, problems.Select(p => p.Split(':')[0]));
        Assert.Contains("key_numbers", problems[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task Submit_AnInvalidReport_IsTheSharedRefusal_NamingTheFirstField_BeforeAnyStoreCall()
    {
        /* Neither id parses, so the tool never reaches the store: the null store is never touched. */
        var answer = await DarlingMcpInvestigationTools.SubmitAsync(
            null!, "not-a-guid", "", 1, "real", "", Evidence, "", null, null, default);

        using var document = JsonDocument.Parse(answer);
        Assert.Equal("invalid", document.RootElement.GetProperty("status").GetString());
        Assert.Equal("request_id", document.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        var message = document.RootElement.GetProperty("message").GetString()!;
        foreach (var field in new[] { "request_id:", "claim_id:", "root_cause:", "summary:" })
            Assert.Contains(field, message, StringComparison.Ordinal);
    }

    /* ---- review round 2 (M1): get_investigation_reports masks its own fields at mcp.dataDetail ---- */

    private const string Literal = "zq-literal-7731";
    private const string Login = @"CORP\zq_login";
    private const string Host = "ZQHOST09";

    /// <summary>Text with a literal, a login and a host, as an agent or a person could write it into any field.</summary>
    private static string Canaries(string field) =>
        $"{field}: WHERE name = '{Literal}' AND login_name = N'{Login}' on host {Host}";

    private static InvestigationReportsRead StoredRead()
    {
        var request = Guid.NewGuid();
        var at = new DateTime(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
        var evidence = new JsonArray(new JsonObject
        {
            ["tool"] = "get_blocking",
            ["arguments"] = new JsonObject { ["server_name"] = "SQLPROD01", ["hours_back"] = 4 },
            ["key_numbers"] = Canaries("key_numbers"),
        }).ToJsonString();
        var brief = new JsonObject
        {
            ["brief_format"] = 1,
            ["data_detail"] = "full",
            ["findings"] = new JsonArray(new JsonObject { ["category"] = "blocking", ["story"] = Canaries("story") }),
            ["earlier_reports"] = new JsonArray(new JsonObject
            {
                ["verdict"] = "real", ["root_cause"] = Canaries("earlier root_cause"), ["summary"] = Canaries("earlier summary"),
            }),
        }.ToJsonString();
        return new InvestigationReportsRead(
            [],
            [new InvestigationReportRow(request, 1, "agent-a", at, "real", Canaries("root_cause"), true, Canaries("summary"), evidence)],
            [
                new InvestigationJournalRow(at, request, 7, "closed", "person:" + Login, Canaries("reason"), null),
                new InvestigationJournalRow(at, request, 7, "claimed", "agent:agent-a", null, null),
            ],
            brief);
    }

    /// <summary>A Darling read (it keeps feedback, so the evidence is served by basis, #6004): one verified, one inferred and
    /// one unlabelled item, each carrying the canaries in its free text.</summary>
    private static InvestigationReportsRead DarlingRead()
    {
        JsonObject Item(string tool, string? basis) => new()
        {
            ["tool"] = tool,
            ["arguments"] = new JsonObject { ["server_name"] = "SQLPROD01", ["hours_back"] = 4 },
            ["key_numbers"] = Canaries(tool),
            ["basis"] = basis,
        };

        var stored = StoredRead();
        var report = stored.Reports[0] with
        {
            EvidenceJson = new JsonArray(Item("get_blocking", "verified"), Item("get_waits", "inferred"), Item("get_deadlocks", null))
                .ToJsonString(),
        };
        return stored with { Reports = [report], Feedback = [] };
    }

    [Fact]
    public void ReportsAnswer_MaskLiterals_MasksEveryEvidenceListByBasis()
    {
        var answer = InvestigationToolCore.ReportsAnswer(DarlingRead(), 24, McpDataDetail.MaskLiterals);

        Assert.DoesNotContain(Literal, answer, StringComparison.Ordinal);
        var report = JsonNode.Parse(answer)!["reports"]![0]!;
        Assert.Null(report["evidence"]);
        foreach (var (list, tool) in new[] { ("evidence_verified", "get_blocking"), ("evidence_inferred", "get_waits"), ("evidence_unlabelled", "get_deadlocks") })
        {
            var item = Assert.Single(report[list]!.AsArray())!;
            Assert.Equal(tool, (string?)item["tool"]);
            var text = (string?)item["key_numbers"];
            Assert.NotNull(text);
            Assert.DoesNotContain(Literal, text, StringComparison.Ordinal);
            Assert.Contains("WHERE name = ", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ReportsAnswer_MetadataOnly_WithholdsEveryEvidenceListByBasis()
    {
        var answer = InvestigationToolCore.ReportsAnswer(DarlingRead(), 24, McpDataDetail.MetadataOnly);

        foreach (var canary in new[] { Literal, Login, Host })
            Assert.DoesNotContain(canary, answer, StringComparison.Ordinal);
        var report = JsonNode.Parse(answer)!["reports"]![0]!;
        foreach (var (list, tool) in new[] { ("evidence_verified", "get_blocking"), ("evidence_inferred", "get_waits"), ("evidence_unlabelled", "get_deadlocks") })
        {
            var item = Assert.Single(report[list]!.AsArray())!;
            Assert.Equal(tool, (string?)item["tool"]);
            Assert.Equal(McpDataDetailMasker.WithheldText, (string?)item["key_numbers"]);
        }
    }

    /* #5834: the answer names its page count requests_returned and carries the read's two truncation flags. */
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void ReportsAnswer_SaysWhenTheWindowOrTheJournalHeldMore(bool requestsTruncated, bool journalTruncated)
    {
        var read = StoredRead() with { RequestsTruncated = requestsTruncated, JournalTruncated = journalTruncated };
        var root = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(read, 24, McpDataDetail.Full))!;

        Assert.Equal(read.Requests.Count, (int)root["requests_returned"]!);
        Assert.Equal(requestsTruncated, (bool)root["truncated"]!);
        Assert.Equal(journalTruncated, (bool)root["journal_truncated"]!);
        Assert.Null(root["request_count"]);
    }

    [Fact]
    public void ReportsAnswer_MaskLiterals_ReplacesEveryFieldsLiteral_KeepsTheStructuredFields()
    {
        var answer = InvestigationToolCore.ReportsAnswer(StoredRead(), 24, McpDataDetail.MaskLiterals);

        Assert.DoesNotContain(Literal, answer, StringComparison.Ordinal);
        var root = JsonNode.Parse(answer)!;
        var report = root["reports"]![0]!;
        foreach (var text in new[]
        {
            (string?)report["root_cause"], (string?)report["summary"], (string?)report["evidence"]![0]!["key_numbers"],
            (string?)root["journal"]![0]!["reason"], (string?)root["brief"]!["findings"]![0]!["story"],
            (string?)root["brief"]!["earlier_reports"]![0]!["root_cause"],
        })
        {
            Assert.NotNull(text);
            Assert.DoesNotContain(Literal, text, StringComparison.Ordinal);
            Assert.Contains("WHERE name = ", text, StringComparison.Ordinal);
        }

        Assert.Equal("person:" + Login, (string?)root["journal"]![0]!["actor"]);
        Assert.Equal("get_blocking", (string?)report["evidence"]![0]!["tool"]);
        Assert.Equal("real", (string?)report["verdict"]);
        Assert.Equal("agent-a", (string?)report["agent_id"]);
        Assert.Equal("maskLiterals", (string?)root["brief"]!["data_detail"]);
    }

    [Fact]
    public void ReportsAnswer_MetadataOnly_WithholdsTheFreeText_AndMasksAPersonsLogin()
    {
        var answer = InvestigationToolCore.ReportsAnswer(StoredRead(), 24, McpDataDetail.MetadataOnly);

        foreach (var canary in new[] { Literal, Login, Host, "zq_login" })
            Assert.DoesNotContain(canary, answer, StringComparison.Ordinal);
        var root = JsonNode.Parse(answer)!;
        var report = root["reports"]![0]!;
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)report["root_cause"]);
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)report["summary"]);
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)report["evidence"]![0]!["key_numbers"]);
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)root["journal"]![0]!["reason"]);
        Assert.StartsWith("person:login#", (string?)root["journal"]![0]!["actor"], StringComparison.Ordinal);
        Assert.Equal("agent:agent-a", (string?)root["journal"]![1]!["actor"]);
        Assert.Null(root["journal"]![1]!["reason"]);
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)root["brief"]!["findings"]![0]!["story"]);
        Assert.Equal("blocking", (string?)root["brief"]!["findings"]![0]!["category"]);
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)root["brief"]!["earlier_reports"]![0]!["summary"]);
        Assert.Equal("metadataOnly", (string?)root["brief"]!["data_detail"]);
        Assert.Equal(4, (int?)report["evidence"]![0]!["arguments"]!["hours_back"]);
        Assert.True((bool?)report["root_cause_found"]);
    }

    [Fact]
    public void ReportsAnswer_Full_ServesTheReportAsStored()
    {
        var answer = InvestigationToolCore.ReportsAnswer(StoredRead(), 24, McpDataDetail.Full);
        var root = JsonNode.Parse(answer)!;
        Assert.Equal(Canaries("root_cause"), (string?)root["reports"]![0]!["root_cause"]);
        Assert.Equal("person:" + Login, (string?)root["journal"]![0]!["actor"]);
        Assert.Equal(Canaries("story"), (string?)root["brief"]!["findings"]![0]!["story"]);
        Assert.Equal("full", (string?)root["brief"]!["data_detail"]);
        Assert.Equal("ok", (string?)root["status"]);
    }

    /* ---- review round 2 (M2): report caps, each at the cap and one over ---- */

    private static string EvidenceOf(int items, int valueChars) =>
        "[" + string.Join(",", Enumerable.Repeat(
            "{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"" + new string('k', valueChars) + "\"}", items)) + "]";

    [Fact]
    public void CapProblems_RootCause_8000Passes_8001IsRefused()
    {
        Assert.Empty(InvestigationToolCore.CapProblems(new string('r', 8000), Evidence));
        var problem = Assert.Single(InvestigationToolCore.CapProblems(new string('r', 8001), Evidence));
        Assert.Equal(("root_cause", 8000), (problem.Field, problem.Cap));
    }

    [Fact]
    public void CapProblems_Evidence_50ItemsPass_51AreRefused()
    {
        Assert.Empty(InvestigationToolCore.CapProblems("r", EvidenceOf(50, 10)));
        var problem = Assert.Single(InvestigationToolCore.CapProblems("r", EvidenceOf(51, 10)));
        Assert.Equal(("evidence", 50), (problem.Field, problem.Cap));
    }

    [Fact]
    public void CapProblems_AStringValue_1000Passes_1001IsRefused_NestedToo()
    {
        Assert.Empty(InvestigationToolCore.CapProblems("r", EvidenceOf(1, 1000)));
        var problem = Assert.Single(InvestigationToolCore.CapProblems("r", EvidenceOf(1, 1001)));
        Assert.Equal(("evidence", 1000), (problem.Field, problem.Cap));
        var nested = "[{\"tool\":\"t\",\"arguments\":{\"a\":[{\"b\":\"" + new string('x', 1001) + "\"}]},\"key_numbers\":\"1\"}]";
        Assert.Equal(1000, Assert.Single(InvestigationToolCore.CapProblems("r", nested)).Cap);
    }

    [Fact]
    public void CapProblems_TheArray_32768BytesPass_32769AreRefused()
    {
        /* 32 items of 1,000 characters, then a last item padded to land the printed size exactly on the cap. */
        var items = Enumerable.Range(0, 32).Select(_ => "{\"key_numbers\":\"" + new string('k', 1000) + "\"}").ToList();
        string Build(int pad) => "[" + string.Join(",", items.Append("{\"key_numbers\":\"" + new string('p', pad) + "\"}")) + "]";
        var basis = InvestigationToolCore.PgJsonbTextBytes(JsonNode.Parse(Build(0)));
        var atCap = (int)(InvestigationToolCore.MaxEvidenceBytes - basis);
        Assert.InRange(atCap, 1, 1000);

        Assert.Equal(32768, InvestigationToolCore.PgJsonbTextBytes(JsonNode.Parse(Build(atCap))));
        Assert.Empty(InvestigationToolCore.CapProblems("r", Build(atCap)));
        var problem = Assert.Single(InvestigationToolCore.CapProblems("r", Build(atCap + 1)));
        Assert.Equal(("evidence", 32768), (problem.Field, problem.Cap));
    }

    [Theory]
    [InlineData("[1e3]", 6)]                      // [1000]
    [InlineData("[1.0E-2]", 7)]                   // [0.010]
    [InlineData("{\"a\": 1, \"b\": [true, null]}", 27)]
    [InlineData("[\"<\\u00e9\\u0001\\\"\"]", 15)]  // ["<é\u0001\""]: é is two UTF-8 bytes
    public void PgJsonbTextBytes_IsTheWidthPostgresPrints(string json, long bytes) =>
        Assert.Equal(bytes, InvestigationToolCore.PgJsonbTextBytes(JsonNode.Parse(json)));

    [Fact]
    public void PgJsonbTextBytes_AHugeExponent_CannotHideUnderTheCap()
    {
        var problem = Assert.Single(InvestigationToolCore.CapProblems("r", "[{\"tool\":\"t\",\"arguments\":{\"n\":1e40000},\"key_numbers\":\"1\"}]"));
        Assert.Equal(32768, problem.Cap);
    }

    [Fact]
    public async Task Submit_OverACap_IsRefused_NamingTheFieldAndTheCap_BeforeAnyStoreCall()
    {
        /* request_id does not parse, so the tool journals nothing and never reaches the null store. */
        var answer = await DarlingMcpInvestigationTools.SubmitAsync(
            null!, "not-a-guid", "", 1, "real", new string('r', 8001), Evidence, "s", null, null, default);

        using var document = JsonDocument.Parse(answer);
        Assert.Equal("refused", document.RootElement.GetProperty("status").GetString());
        Assert.Equal("root_cause", document.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal(8000, document.RootElement.GetProperty("hints").GetProperty("cap").GetInt32());
        Assert.Contains("8001 characters, over 8000", document.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    /* ---- review round 2 (L4): an agent id is a name ---- */

    [Theory]
    [InlineData("agent-a", true)]
    [InlineData("claude.code_2:lane-C", true)]
    [InlineData("", false)]
    [InlineData("agent a", false)]
    [InlineData("x · closed · person:admin", false)]
    [InlineData("<b>agent</b>", false)]
    public void IsValidAgentId_AllowsOnlyTheNameCharset(string agentId, bool valid)
    {
        Assert.Equal(valid, InvestigationRules.IsValidAgentId(agentId));
        Assert.True(InvestigationRules.IsValidAgentId(new string('a', 64)));
        Assert.False(InvestigationRules.IsValidAgentId(new string('a', 65)));
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData(0, false)]
    [InlineData(1, true)]
    public void RouteWrites_RefuseOnlyAnAgentLevelOfOneOrMore(int? agentLevel, bool refused)
    {
        var route = new PgNotificationRouteStore.RouteSummary(
            7, "*", null, ["email"], "", true, DateTime.UtcNow) { AgentLevel = agentLevel };

        var answer = DarlingMcpAlertTools.AgentRouteRefusal(route, "delete_notification_route");

        Assert.Equal(refused, answer is not null);
        if (answer is not null)
        {
            using var document = JsonDocument.Parse(answer);
            Assert.Equal("refused", document.RootElement.GetProperty("status").GetString());
            Assert.Contains("admin notification routes page", answer, StringComparison.Ordinal);
        }
    }
}
