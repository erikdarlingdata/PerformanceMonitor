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
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Level 1 investigation brief (#5632 Phase A, plan sections 5 and 7, ruling R3): one test per source, the budget
/// trim, masking under all three <c>mcp.dataDetail</c> levels with a literal, a login and a host canary, the earlier-report
/// labels, the next-calls name check, and the format numbers.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class InvestigationBriefComposerTests
{
    private const string LiteralCanary = "987654321";
    private const string LoginCanary = "canary_login_5632";
    private const string HostCanary = "canary-host-5632";
    private static readonly DateTime Now = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    private static InvestigationBriefRequest Request(string family = InvestigationFamily.Blocking, bool isPostgres = false) =>
        new(Guid.Parse("11111111-2222-3333-4444-555555555555"), 7, "SQLPROD01", isPostgres, family, "claimed",
            Now.AddHours(4), Now.AddMinutes(15), 3, "Blocking", "abc123");

    private static InvestigationBriefAlert Alert(DateTime at, string? query = null, string metric = "Blocking") =>
        new(at, metric, 12.5, 5, false, "Blocking detected: 12 sessions", null,
            new[]
            {
                new InvestigationBriefContextItem("Top blocker", new[]
                {
                    new KeyValuePair<string, string>("Database", "SalesDb"),
                    new KeyValuePair<string, string>("Query", query ?? "SELECT * FROM dbo.Orders WHERE id = " + LiteralCanary),
                    new KeyValuePair<string, string>("Login", LoginCanary),
                    new KeyValuePair<string, string>("Host Name", HostCanary),
                }),
            });

    private static InvestigationBriefFinding Finding(params InvestigationNextCall[] nextTools) =>
        new("abc123", "blocking", "Lock waits from a long transaction", "LCK_M_X waits rose on SalesDb", "LCK_M_X", 1234.567,
            0.81234, 0.6, Now.AddMinutes(-5), 4,
            new InvestigationBriefRemediation("force_plan", "Review the blockers before forcing.", new[]
            {
                new InvestigationBriefRemediationTarget("SalesDb", 42, 7, false, new[] { "parameter_sensitivity_cofired" }, null),
            }),
            nextTools);

    private static InvestigationEarlierReport Report(int minutesAgo, string rootCause = "A scan WHERE id = " + LiteralCanary + " held the lock") =>
        new(Guid.NewGuid(), Now.AddMinutes(-minutesAgo), "agent-a", "known_cause", rootCause, true, "Same blocker as last week");

    private static InvestigationBriefSources Sources(
        IReadOnlyList<InvestigationBriefAlert>? alerts = null,
        IReadOnlyList<InvestigationBriefFinding>? findings = null,
        IReadOnlyList<InvestigationEarlierReport>? reports = null,
        IReadOnlyList<InvestigationRecentChange>? changes = null,
        IReadOnlyList<InvestigationBriefNote>? notes = null,
        IReadOnlyList<InvestigationProfileText>? profiles = null,
        InvestigationBriefRequest? request = null) =>
        new(request ?? Request(),
            alerts ?? new[] { Alert(Now.AddMinutes(-20)), Alert(Now.AddMinutes(-1)) },
            findings ?? new[] { Finding() },
            reports ?? new[] { Report(600) },
            changes ?? new[] { new InvestigationRecentChange(Now.AddDays(-1), "mute_rule", "enabled mute rule for Blocking", "%WHERE id = " + LiteralCanary + "%") },
            notes ?? new[] { new InvestigationBriefNote(LoginCanary, "dba-lead", Now.AddDays(30), "Nightly ETL holds locks from 01:00 to 02:00.") },
            profiles ?? new[] { new InvestigationProfileText("server", "Production OLTP. Never restart during business hours.") });

    private static JsonObject Brief(InvestigationBriefSources sources, McpDataDetail mode = McpDataDetail.Full) =>
        JsonNode.Parse(InvestigationBriefComposer.Compose(sources, mode, Now))!.AsObject();

    /* ---- format numbers (plan section 7) ---- */

    [Fact]
    public void Formats_RefuseZeroAndAcceptOne()
    {
        Assert.False(InvestigationFormats.IsAcceptedReportFormat(0));
        Assert.True(InvestigationFormats.IsAcceptedReportFormat(1));
        Assert.Equal(1, InvestigationFormats.BriefFormat);
        Assert.Equal(
            "report_format 0 is not supported. Darling accepts report_format 1. The brief you claimed is brief_format 1.",
            InvestigationFormats.UnsupportedReportFormatMessage(0));
    }

    [Fact]
    public void Brief_CarriesTheFormatTheLevelAndTheContract()
    {
        var brief = Brief(Sources());
        Assert.Equal(1, (int)brief["brief_format"]!);
        Assert.Equal("full", (string?)brief["data_detail"]);
        Assert.Equal("2026-10-14T12:00:00.000000Z", (string?)brief["composed_at"]);
        Assert.Equal(1, (int)brief["autonomy_level"]!);
        Assert.Equal(InvestigationBriefComposer.ReportTool, (string?)brief["report_contract"]!["tool"]);
        Assert.Equal(1, (int)brief["report_contract"]!["report_format"]!);
        Assert.Equal("SQLPROD01", (string?)brief["request"]!["server"]);
        Assert.Equal("sqlserver", (string?)brief["request"]!["engine"]);
        Assert.Empty(brief["truncated"]!["sections"]!.AsArray());
    }

    /* ---- one test per source ---- */

    [Fact]
    public void Alerts_CarryContextByLabel_FirstThenNewest()
    {
        var first = Alert(Now.AddMinutes(-30));
        var newest = Alert(Now.AddMinutes(-1));
        var alerts = Brief(Sources(alerts: new[] { first, newest }))["alerts"]!.AsArray();
        Assert.Equal(2, alerts.Count);
        Assert.Equal("2026-10-14T11:30:00.000000Z", (string?)alerts[0]!["alert_time"]);
        Assert.Equal(12.5, (double)alerts[0]!["current_value"]!);
        Assert.Equal("Blocking detected: 12 sessions", (string?)alerts[0]!["detail_text"]);
        var fields = alerts[0]!["context"]![0]!["fields"]!.AsObject();
        Assert.Equal("SalesDb", (string?)fields["Database"]);
        Assert.Contains(LiteralCanary, (string?)fields["Query"]);
    }

    [Fact]
    public void Findings_RemediationIsVerdictAndBlockers_NeverAScript()
    {
        var json = InvestigationBriefComposer.Compose(Sources(), McpDataDetail.Full, Now);
        var finding = JsonNode.Parse(json)!["findings"]![0]!;
        Assert.Equal("abc123", (string?)finding["story_path_hash"]);
        Assert.Equal(0.81, (double)finding["severity"]!);
        Assert.Equal(1234.57, (double)finding["root_fact"]!["value"]!);
        var target = finding["remediation"]!["targets"]![0]!;
        Assert.False((bool)target["eligible"]!);
        Assert.Equal("parameter_sensitivity_cofired", (string?)target["blockers"]![0]);
        Assert.DoesNotContain("force_sql", json, StringComparison.Ordinal);
        Assert.DoesNotContain("remediation_command", json, StringComparison.Ordinal);
    }

    [Fact]
    public void EarlierReports_AreLabeledUnreviewedAndSelfAttributed()
    {
        var reports = Brief(Sources(reports: new[] { Report(60), Report(600) }))["earlier_reports"]!.AsArray();
        Assert.Equal(2, reports.Count);
        foreach (var report in reports)
        {
            var labels = report!["labels"]!.AsArray().Select(l => (string?)l).ToList();
            Assert.Equal(new[] { "unreviewed", "self_attributed" }, labels);
        }
    }

    [Fact]
    public void Notes_Profile_AndChanges_AreStated()
    {
        var brief = Brief(Sources());
        Assert.Equal("Nightly ETL holds locks from 01:00 to 02:00.", (string?)brief["notes"]![0]!["text"]);
        Assert.Equal("dba-lead", (string?)brief["notes"]![0]!["decided_by"]);
        Assert.Equal("Production OLTP. Never restart during business hours.", (string?)brief["environment_profile"]![0]!["text"]);
        Assert.Equal("mute_rule", (string?)brief["recent_changes"]![0]!["kind"]);
    }

    /* ---- next calls ---- */

    [Fact]
    public void FamilyCallTables_NameOnlyRecommendationTableTools()
    {
        foreach (var family in InvestigationFamily.All)
        {
            Assert.True(InvestigationBriefComposer.SqlServerFamilyCalls.ContainsKey(family), family);
            Assert.True(InvestigationBriefComposer.PostgresFamilyCalls.ContainsKey(family), family);
        }

        foreach (var tool in InvestigationBriefComposer.SqlServerFamilyCalls.Values.SelectMany(t => t))
            Assert.Contains(tool, ToolRecommendations.AllToolNames);
        foreach (var tool in InvestigationBriefComposer.PostgresFamilyCalls.Values.SelectMany(t => t))
            Assert.Contains(tool, PgTargetToolRecommendations.AllToolNames);
    }

    [Fact]
    public void NextCalls_NameOnlyRegisteredTools_EachOnce_WithTheServer()
    {
        var registered = RegisteredToolNames();
        var findings = new[]
        {
            Finding(
                new InvestigationNextCall("get_blocking", new Dictionary<string, string>(), "dup of the family call"),
                new InvestigationNextCall("drop_everything", new Dictionary<string, string>(), "not a tool"),
                new InvestigationNextCall("get_wait_trend", new Dictionary<string, string> { ["wait_type"] = "LCK_M_X" }, "trend")),
        };
        var calls = Brief(Sources(findings: findings))["next_calls"]!.AsArray();
        var names = calls.Select(c => (string?)c!["tool"]).ToList();
        Assert.Equal(names.Distinct().Count(), names.Count);
        Assert.DoesNotContain("drop_everything", names);
        Assert.Contains("get_wait_trend", names);
        foreach (var call in calls)
        {
            Assert.Contains((string)call!["tool"]!, registered);
            Assert.Equal("SQLPROD01", (string?)call["arguments"]!["server_name"]);
        }

        Assert.Equal("LCK_M_X", (string?)calls.Single(c => (string?)c!["tool"] == "get_wait_trend")!["arguments"]!["wait_type"]);
    }

    [Fact]
    public void FetchTools_AndEveryFamilyCall_AreRegisteredTools()
    {
        var registered = RegisteredToolNames();
        foreach (var (_, _, fetch) in InvestigationBriefComposer.Sections)
        {
            if (fetch is not null) Assert.Contains(fetch, registered);
        }

        foreach (var tool in InvestigationBriefComposer.SqlServerFamilyCalls.Values.Concat(InvestigationBriefComposer.PostgresFamilyCalls.Values).SelectMany(t => t))
            Assert.Contains(tool, registered);
    }

    /* ---- budget ---- */

    [Fact]
    public void Budget_CutsEachSectionFromItsTail_AndNamesTheCut()
    {
        var bigQuery = "SELECT " + new string('x', 5_000);
        var alerts = Enumerable.Range(0, 40).Select(i => Alert(Now.AddMinutes(-i), bigQuery)).ToList();
        var reports = Enumerable.Range(0, 60).Select(i => Report(i * 60, new string('r', 900))).ToList();
        var json = InvestigationBriefComposer.Compose(Sources(alerts: alerts, reports: reports), McpDataDetail.Full, Now);

        Assert.True(json.Length <= InvestigationBriefComposer.TotalCharCap, json.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var brief = JsonNode.Parse(json)!.AsObject();
        var kept = brief["alerts"]!.AsArray();
        Assert.InRange(kept.Count, 1, 39);
        Assert.Equal("2026-10-14T12:00:00.000000Z", (string?)kept[0]!["alert_time"]); // priority order kept: the head survives
        var query = (string)kept[0]!["context"]![0]!["fields"]!["Query"]!;
        Assert.Equal(InvestigationBriefComposer.FieldCharCap, query.Length);
        Assert.EndsWith(InvestigationBriefComposer.CutMarker, query, StringComparison.Ordinal);

        var cuts = brief["truncated"]!["sections"]!.AsArray();
        var alertCut = cuts.Single(c => (string?)c!["section"] == "alerts")!;
        Assert.Equal(40 - kept.Count, (int)alertCut["dropped"]!);
        Assert.Equal("get_alert_history", (string?)alertCut["fetch"]);
        Assert.Contains(cuts, c => (string?)c!["section"] == "earlier_reports");
        Assert.True((int)brief["truncated"]!["fields_cut"]! > 0);
        Assert.True((int)brief["truncated"]!["chars"]! > 0);
    }

    /* ---- masking under all three levels ---- */

    [Fact]
    public void Full_ChangesNothing()
    {
        var json = InvestigationBriefComposer.Compose(Sources(), McpDataDetail.Full, Now);
        Assert.Contains(LiteralCanary, json, StringComparison.Ordinal);
        Assert.Contains(LoginCanary, json, StringComparison.Ordinal);
        Assert.Contains(HostCanary, json, StringComparison.Ordinal);
        Assert.DoesNotContain(McpDataDetailMasker.WithheldText, json, StringComparison.Ordinal);
    }

    [Fact]
    public void MaskLiterals_ReplacesLiterals_KeepsLoginsAndHosts()
    {
        var json = InvestigationBriefComposer.Compose(Sources(), McpDataDetail.MaskLiterals, Now);
        var brief = JsonNode.Parse(json)!.AsObject();
        Assert.Equal("maskLiterals", (string?)brief["data_detail"]);
        Assert.DoesNotContain(LiteralCanary, json, StringComparison.Ordinal);
        Assert.Contains(LoginCanary, json, StringComparison.Ordinal);
        Assert.Contains(HostCanary, json, StringComparison.Ordinal);
        Assert.Contains("SELECT * FROM dbo.Orders WHERE id = ", (string?)brief["alerts"]![0]!["context"]![0]!["fields"]!["Query"]);
        Assert.Equal("known_cause", (string?)brief["earlier_reports"]![0]!["verdict"]);
    }

    [Fact]
    public void MetadataOnly_WithholdsStatements_MasksLoginsAndHosts_KeepsNames()
    {
        var json = InvestigationBriefComposer.Compose(Sources(), McpDataDetail.MetadataOnly, Now);
        var brief = JsonNode.Parse(json)!.AsObject();
        Assert.Equal("metadataOnly", (string?)brief["data_detail"]);
        Assert.DoesNotContain(LiteralCanary, json, StringComparison.Ordinal);
        Assert.DoesNotContain(LoginCanary, json, StringComparison.Ordinal);
        Assert.DoesNotContain(HostCanary, json, StringComparison.Ordinal);
        var fields = brief["alerts"]![0]!["context"]![0]!["fields"]!;
        Assert.Equal(McpDataDetailMasker.WithheldText, (string?)fields["Query"]);
        Assert.Equal("SalesDb", (string?)fields["Database"]);
        Assert.Equal("SQLPROD01", (string?)brief["request"]!["server"]);
    }

    /* ---- review round 1 (M1): structured fields are kept, only free text is masked ---- */

    [Theory]
    [InlineData(McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.MetadataOnly)]
    public void StructuredFields_AreKept_OnlyTheNarrativeIsMasked(McpDataDetail mode)
    {
        // A hash and a time that start with digits: a literal pass over them would garble both.
        var finding = new InvestigationBriefFinding("9f8e7d6c5b4a", "blocking", "Lock waits from a long transaction",
            "A scan WHERE id = " + LiteralCanary + " held the lock", "LCK_M_X", 1234.567, 0.81234, 0.6, Now.AddMinutes(-5), 4,
            new InvestigationBriefRemediation("force_plan", "Review the blockers before forcing.", new[]
            {
                new InvestigationBriefRemediationTarget("SalesDb", 42, 7, false, new[] { "parameter_sensitivity_cofired" }, "2 plans in 24 hours"),
            }),
            Array.Empty<InvestigationNextCall>());
        var json = InvestigationBriefComposer.Compose(Sources(findings: new[] { finding }), mode, Now);
        var brief = JsonNode.Parse(json)!.AsObject();

        var f = brief["findings"]![0]!;
        Assert.Equal("blocking", (string?)f["category"]);
        Assert.Equal("2026-10-14T11:55:00.000000Z", (string?)f["analysis_time"]);
        Assert.Equal("9f8e7d6c5b4a", (string?)f["story_path_hash"]);
        Assert.Equal("Lock waits from a long transaction", (string?)f["headline"]);
        Assert.Equal("force_plan", (string?)f["remediation"]!["verb"]);
        Assert.Equal("Review the blockers before forcing.", (string?)f["remediation"]!["guidance"]);
        Assert.Equal("SalesDb", (string?)f["remediation"]!["targets"]![0]!["database"]);
        Assert.Equal("parameter_sensitivity_cofired", (string?)f["remediation"]!["targets"]![0]!["blockers"]![0]);
        Assert.Equal("2 plans in 24 hours", (string?)f["remediation"]!["targets"]![0]!["state_note"]);
        Assert.Equal(4, (int?)f["occurrences"]);

        var story = (string?)f["story"];
        Assert.DoesNotContain(LiteralCanary, story, StringComparison.Ordinal);
        if (mode == McpDataDetail.MetadataOnly)
            Assert.Equal(McpDataDetailMasker.WithheldText, story);
        else
            Assert.Equal("A scan WHERE id = ? held the lock", story);

        // The earlier report's verdict and time, the note's expiry, the profile's scope and the change's kind stay.
        Assert.Equal("known_cause", (string?)brief["earlier_reports"]![0]!["verdict"]);
        Assert.Equal("agent-a", (string?)brief["earlier_reports"]![0]!["agent_id"]);
        Assert.Equal("2026-10-14T02:00:00.000000Z", (string?)brief["earlier_reports"]![0]!["submitted_at"]);
        Assert.Equal("2026-11-13T12:00:00.000000Z", (string?)brief["notes"]![0]!["expires_at"]);
        Assert.Equal("server", (string?)brief["environment_profile"]![0]!["scope"]);
        Assert.Equal("mute_rule", (string?)brief["recent_changes"]![0]!["kind"]);
        Assert.DoesNotContain(LiteralCanary, json, StringComparison.Ordinal);
    }

    /* ---- review round 1 (L1): one login and host numbering per brief ---- */

    [Fact]
    public void MetadataOnly_NumbersEachLoginOnceAcrossTheWholeBrief()
    {
        var other = new InvestigationBriefAlert(Now.AddMinutes(-10), "Blocking", 12.5, 5, false, "Blocking detected", null,
            new[]
            {
                new InvestigationBriefContextItem("Blocked", new[]
                {
                    new KeyValuePair<string, string>("Login", "other_login_5632"),
                    new KeyValuePair<string, string>("Excluded Logins", "other_login_5632, " + LoginCanary),
                }),
            });
        var sources = Sources(alerts: new[] { Alert(Now.AddMinutes(-20)), other });
        var json = InvestigationBriefComposer.Compose(sources, McpDataDetail.MetadataOnly, Now);
        var brief = JsonNode.Parse(json)!.AsObject();

        Assert.DoesNotContain(LoginCanary, json, StringComparison.Ordinal);
        Assert.DoesNotContain("other_login_5632", json, StringComparison.Ordinal);
        var canary = (string?)brief["alerts"]![0]!["context"]![0]!["fields"]!["Login"];
        var otherFields = brief["alerts"]![1]!["context"]![0]!["fields"]!;
        var otherLogin = (string?)otherFields["Login"];
        Assert.StartsWith("login#", canary, StringComparison.Ordinal);
        Assert.StartsWith("login#", otherLogin, StringComparison.Ordinal);
        Assert.NotEqual(canary, otherLogin);
        Assert.Equal(otherLogin + ", " + canary, (string?)otherFields["Excluded Logins"]);
        Assert.Equal(canary, (string?)brief["notes"]![0]!["author"]);
    }

    /* ---- the async entry ---- */

    [Fact]
    public async Task ComposeAsync_UsesTheConfiguredLevel_AndAnswersNullForAnUnknownRequest()
    {
        var setting = new McpDataDetailSetting(McpDataDetail.MetadataOnly);
        var composer = new InvestigationBriefComposer(new FakeReader(Sources()), setting, () => Now);
        var json = await composer.ComposeAsync(Guid.NewGuid());
        Assert.NotNull(json);
        Assert.Equal("metadataOnly", (string?)JsonNode.Parse(json!)!["data_detail"]);

        var none = new InvestigationBriefComposer(new FakeReader(null), setting, () => Now);
        Assert.Null(await none.ComposeAsync(Guid.NewGuid()));
    }

    /* ---- the reader's pure helpers ---- */

    [Fact]
    public void Reader_ContextParsesStoredJson_AndSkipsBadJson()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Top request", Fields = { ("Query", "SELECT 1"), ("Database", "SalesDb") } });
        var items = PgInvestigationBriefSourceReader.Context(AlertContextSerializer.Serialize(context));
        Assert.Single(items);
        Assert.Equal("Top request", items[0].Heading);
        Assert.Equal("Query", items[0].Fields[0].Key);
        Assert.Empty(PgInvestigationBriefSourceReader.Context("not json"));
        Assert.Empty(PgInvestigationBriefSourceReader.Context(null));
    }

    private static HashSet<string> RegisteredToolNames() =>
        typeof(ToolRecommendations).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .Select(n => n!)
            .ToHashSet(StringComparer.Ordinal);

    private sealed class FakeReader(InvestigationBriefSources? sources) : IInvestigationBriefSourceReader
    {
        public Task<InvestigationBriefSources?> ReadAsync(Guid requestId, CancellationToken cancellationToken = default) =>
            Task.FromResult(sources);
    }
}
