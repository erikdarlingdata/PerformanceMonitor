/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The report fields #6004 adds (D17 of #5632 Phase C), one behaviour per fact and none of them needing a store: a commit or
/// release link, a required evidence basis, the verified and inferred lists, and <c>queries_run</c> (its caps and its
/// masking). The live facts (<c>family_of</c> and the links stored and read back) are in <see cref="InvestigationToolsLiveTests"/>.
/// </summary>
public sealed class InvestigationReportFieldsTests
{
    private const string Verified = "{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"412 s\",\"basis\":\"verified\"}";
    private const string Inferred = "{\"tool\":\"get_wait_stats\",\"arguments\":{},\"key_numbers\":\"LCK_M_X\",\"basis\":\"inferred\"}";
    private const string Old = "{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"written before the basis\"}";
    private const string Literal = "zq-literal-7731";

    private static string One(string kind) =>
        "[{\"kind\":\"" + kind + "\",\"url\":\"https://git.example.invalid/pm/" + kind + "/9\",\"key\":\"abc123\"}]";

    [Theory]
    [InlineData("commit")]
    [InlineData("release")]
    public void Links_ACommitOrAReleaseKind_IsAccepted(string kind)
    {
        Assert.Null(InvestigationReportLinks.Validate(One(kind), out var links));
        Assert.Equal(kind, Assert.Single(links).Kind);
        InvestigationReportLinks.ThrowIfInvalid(links, "report");
    }

    [Fact]
    public void Links_AnUnknownKind_IsRefused_NamingAllFive()
    {
        var problem = InvestigationReportLinks.Validate(One("ticket"), out _);
        Assert.Contains("kind 'ticket' is not jira, pull_request, bug_report, commit or release", problem, StringComparison.Ordinal);
        var thrown = Assert.Throws<ArgumentException>(() =>
            InvestigationReportLinks.ThrowIfInvalid([new InvestigationReportLink("ticket", "https://x.example.invalid/1", null)], "report"));
        Assert.Contains("commit or release", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FamilyOf_AFullRequestIdParses_AShortFormIsAProblem_AndBlankIsNone()
    {
        var id = Guid.NewGuid();
        Assert.Null(InvestigationReportFields.ValidateFamilyOf(id.ToString(), out var full));
        Assert.Equal(id, full);

        Assert.NotNull(InvestigationReportFields.ValidateFamilyOf(id.ToString()[..8], out var shortForm));
        Assert.Null(shortForm);

        Assert.Null(InvestigationReportFields.ValidateFamilyOf("  ", out var blank));
        Assert.Null(blank);
    }

    private static System.Collections.Generic.List<string> Problems(string evidence, bool requireBasis) =>
        InvestigationToolCore.ValidateReport(Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "cause", evidence, "summary",
            null, null, out _, out _, out _, requireBasis);

    [Fact]
    public void Evidence_AnItemWithoutABasis_IsRefused_NamingTheItem_ButOnlyWhenTheBasisIsRequired()
    {
        var evidence = "[" + Verified + "," + Old + "]";

        var problem = Assert.Single(Problems(evidence, requireBasis: true));
        Assert.StartsWith("evidence:", problem, StringComparison.Ordinal);
        Assert.Contains("item 1 has no basis", problem, StringComparison.Ordinal);
        Assert.DoesNotContain("item 0", problem, StringComparison.Ordinal);

        /* Lite's advise-only submit does not ask for it: its older evidence shape is still accepted. */
        Assert.Empty(Problems(evidence, requireBasis: false));
    }

    [Fact]
    public void Evidence_ABasisThatIsNeitherVerifiedNorInferred_IsRefused_NamingTheItem()
    {
        var evidence = "[" + Verified + ",{\"tool\":\"t\",\"arguments\":{},\"key_numbers\":\"1\",\"basis\":\"guessed\"}]";

        var problem = Assert.Single(Problems(evidence, requireBasis: true));
        Assert.Contains("item 1 basis 'guessed' is not verified or inferred", problem, StringComparison.Ordinal);
    }

    private static InvestigationReportsRead Read(string evidence, string? queriesRun = null, Guid? familyOf = null) =>
        new([],
            [new InvestigationReportRow(Guid.NewGuid(), 1, "agent-a", new DateTime(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc), "real",
                "cause", true, "summary", evidence, FamilyOf: familyOf, QueriesRunJson: queriesRun)],
            [],
            null,
            Feedback: []);

    [Fact]
    public void Reports_AreServedAsTwoLists_AndEvidenceWrittenBeforeTheBasisIsUnlabelled()
    {
        var answer = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(
            Read("[" + Inferred + "," + Verified + "," + Old + "]"), 24, McpDataDetail.Full))!;
        var report = answer["reports"]![0]!;

        Assert.Equal("get_blocking", (string?)Assert.Single(report["evidence_verified"]!.AsArray())!["tool"]);
        Assert.Equal("get_wait_stats", (string?)Assert.Single(report["evidence_inferred"]!.AsArray())!["tool"]);
        Assert.Equal("written before the basis", (string?)Assert.Single(report["evidence_unlabelled"]!.AsArray())!["key_numbers"]);
        Assert.Null(report["evidence"]);

        /* A report with every item labelled has no unlabelled list. */
        var labelled = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(Read("[" + Verified + "]"), 24, McpDataDetail.Full))!;
        Assert.Null(labelled["reports"]![0]!["evidence_unlabelled"]);
        Assert.Empty(labelled["reports"]![0]!["evidence_inferred"]!.AsArray());
    }

    private static string Statements(int count, int length) =>
        new JsonArray(Enumerable.Range(0, count).Select(i => (JsonNode?)new string('q', length)).ToArray()).ToJsonString();

    private const string ValidEvidence = "[" + Verified + "]";

    private static async Task<JsonElement> SubmitWith(string queriesRun)
    {
        /* request_id does not parse, so the tool journals nothing and never reaches the null store. */
        var answer = await DarlingMcpInvestigationTools.SubmitAsync(
            null!, "not-a-guid", "", 1, "real", "cause", ValidEvidence, "s", null, null, default, queries_run: queriesRun);
        return JsonDocument.Parse(answer).RootElement.Clone();
    }

    [Fact]
    public async Task QueriesRun_TwentyItemsOfTwoThousandCharacters_AreNotOverACap()
    {
        Assert.Empty(InvestigationToolCore.CapProblems("r", ValidEvidence, Statements(InvestigationReportFields.MaxQueriesRun, InvestigationReportFields.MaxQueryChars)));
        /* Not refused as a cap: this one is only invalid for the ids the test leaves unparsed. */
        Assert.Equal("invalid", (await SubmitWith(Statements(InvestigationReportFields.MaxQueriesRun, InvestigationReportFields.MaxQueryChars)))
            .GetProperty("status").GetString());
    }

    [Fact]
    public async Task QueriesRun_ATwentyFirstItem_IsRefused_NamingTheFieldAndTheCap()
    {
        var answer = await SubmitWith(Statements(InvestigationReportFields.MaxQueriesRun + 1, 10));

        Assert.Equal("refused", answer.GetProperty("status").GetString());
        Assert.Equal("queries_run", answer.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal(InvestigationReportFields.MaxQueriesRun, answer.GetProperty("hints").GetProperty("cap").GetInt32());
        Assert.Contains("21 items, over 20", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueriesRun_AQueryOfTwoThousandAndOneCharacters_IsRefused_NeverCut()
    {
        var answer = await SubmitWith(Statements(1, InvestigationReportFields.MaxQueryChars + 1));

        Assert.Equal("refused", answer.GetProperty("status").GetString());
        Assert.Equal("queries_run", answer.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal(InvestigationReportFields.MaxQueryChars, answer.GetProperty("hints").GetProperty("cap").GetInt32());
        Assert.Contains("item 0 is 2001 characters, over 2000", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void QueriesRun_TheStoreChecksTheCapsItselfToo_BeforeAnyWrite()
    {
        Assert.Throws<ArgumentException>(() => InvestigationReportFields.ThrowIfInvalid(Statements(21, 1), "report"));
        Assert.Throws<ArgumentException>(() => InvestigationReportFields.ThrowIfInvalid(Statements(1, 2001), "report"));
        InvestigationReportFields.ThrowIfInvalid(Statements(20, 2000), "report");
    }

    [Fact]
    public void QueriesRun_IsStatementText_MaskedAtTheLevelTheHostServes()
    {
        var queries = new JsonArray($"SELECT * FROM t WHERE name = '{Literal}'").ToJsonString();

        var full = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(Read(ValidEvidence, queries), 24, McpDataDetail.Full))!;
        Assert.Contains(Literal, (string?)full["reports"]![0]!["queries_run"]![0], StringComparison.Ordinal);

        var literals = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(Read(ValidEvidence, queries), 24, McpDataDetail.MaskLiterals))!;
        var masked = (string?)literals["reports"]![0]!["queries_run"]![0];
        Assert.DoesNotContain(Literal, masked, StringComparison.Ordinal);
        Assert.Contains("WHERE name = ", masked, StringComparison.Ordinal);

        var metadata = InvestigationToolCore.ReportsAnswer(Read(ValidEvidence, queries), 24, McpDataDetail.MetadataOnly);
        Assert.DoesNotContain(Literal, metadata, StringComparison.Ordinal);
        Assert.Equal(McpDataDetailMasker.WithheldText,
            (string?)JsonNode.Parse(metadata)!["reports"]![0]!["queries_run"]![0]);
    }

    [Fact]
    public void QueriesRunAndFamilyOf_AreAbsentFromTheAnswer_WhenTheReportGaveNone()
    {
        var family = Guid.NewGuid();
        var bare = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(Read(ValidEvidence), 24, McpDataDetail.Full))!["reports"]![0]!.AsObject();
        Assert.False(bare.ContainsKey("queries_run"));
        Assert.False(bare.ContainsKey("family_of"));

        var withFamily = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(Read(ValidEvidence, familyOf: family), 24, McpDataDetail.Full))!;
        Assert.Equal(family.ToString(), (string?)withFamily["reports"]![0]!["family_of"]);
    }

    [Fact]
    public void TheSubmitToolDescription_StatesTheCapsAndKindsFromTheConstants()
    {
        var method = typeof(DarlingMcpInvestigationTools).GetMethod(nameof(DarlingMcpInvestigationTools.SubmitInvestigationReport))!;
        var description = method.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;

        Assert.Contains(InvestigationReportFields.QueriesRunRule, description, StringComparison.Ordinal);
        Assert.Contains(InvestigationReportLinks.KindsAlternation, description, StringComparison.Ordinal);
        /* The rule's numbers are the constants written out (an attribute takes only a const string). */
        Assert.Contains($"at most {InvestigationReportFields.MaxQueriesRun} ", InvestigationReportFields.QueriesRunRule, StringComparison.Ordinal);
        Assert.Contains($"at most {InvestigationReportFields.MaxQueryChars.ToString("N0", CultureInfo.InvariantCulture)} ", InvestigationReportFields.QueriesRunRule, StringComparison.Ordinal);
    }
}
