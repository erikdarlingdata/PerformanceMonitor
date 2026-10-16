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
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5918: the pure part of practice requests. A practice brief holds no earlier report and no prior link, says it is practice,
/// and a brief that is not practice is byte for byte what it was; the score follows the feedback verdicts (a corrected verdict
/// is the answer, right is the report's verdict at full credit, partly at half, wrong with no correction has no answer).
/// </summary>
public sealed class InvestigationPracticeTests
{
    private static readonly DateTime Now = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);

    private static InvestigationBriefSources Sources(bool practice, bool defaultFlag = false)
    {
        var request = new InvestigationBriefRequest(new Guid("11111111-1111-1111-1111-111111111111"), 7, "srv", false, "blocking", "open", Now.AddHours(4), null, 1, "Blocking", null);
        var alerts = new[] { new InvestigationBriefAlert(Now, "Blocking", 5, 1, false, "chain of 5", null, Array.Empty<InvestigationBriefContextItem>()) };
        var earlier = new[] { new InvestigationEarlierReport(new Guid("22222222-2222-2222-2222-222222222222"), Now.AddDays(-1), "agent-z", "real", "ZEBRA-ROOT", true, "ZEBRA-SUMMARY") };
        var links = new[] { new InvestigationPriorLink(new Guid("33333333-3333-3333-3333-333333333333"), Now.AddDays(-1), "ticket", "https://example.invalid/ZEBRA-LINK", null) };
        return defaultFlag
            ? new InvestigationBriefSources(request, alerts, Array.Empty<InvestigationBriefFinding>(), earlier, Array.Empty<InvestigationRecentChange>(),
                Array.Empty<InvestigationBriefNote>(), Array.Empty<InvestigationProfileText>(), links)
            : new InvestigationBriefSources(request, alerts, Array.Empty<InvestigationBriefFinding>(), earlier, Array.Empty<InvestigationRecentChange>(),
                Array.Empty<InvestigationBriefNote>(), Array.Empty<InvestigationProfileText>(), links, null, practice);
    }

    [Fact]
    public void APracticeBrief_HoldsNoEarlierReportAndNoPriorLink_AndSaysItIsPractice()
    {
        var brief = InvestigationBriefComposer.Compose(Sources(practice: true), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now);
        Assert.DoesNotContain("ZEBRA", brief, StringComparison.Ordinal);
        var node = JsonNode.Parse(brief)!;
        Assert.True(node["request"]!["practice"]!.GetValue<bool>());
        Assert.Empty(node["earlier_reports"]!.AsArray());
        Assert.Null(node["prior_links"]);
        Assert.Contains("practice request", node["read_rules"]!.ToJsonString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ABriefThatIsNotPractice_IsUnchanged_ByteForByte_AndStillCarriesEarlierReports()
    {
        var withFlag = InvestigationBriefComposer.Compose(Sources(practice: false), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now);
        var withoutFlag = InvestigationBriefComposer.Compose(Sources(practice: false, defaultFlag: true), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now);
        Assert.Equal(withoutFlag, withFlag);
        Assert.Contains("ZEBRA-ROOT", withFlag, StringComparison.Ordinal);
        Assert.DoesNotContain("practice", withFlag, StringComparison.OrdinalIgnoreCase);
    }

    private static InvestigationFeedbackRow Row(string verdict, string? corrected = null, string? missed = null, int minutes = 0) =>
        new(Guid.Empty, verdict, corrected, missed, null, "lee", Now.AddMinutes(minutes));

    [Theory]
    [InlineData("right", null, "false_positive", 1.0)]
    [InlineData("partly", null, "false_positive", 0.5)]
    [InlineData("wrong", "real", "real", 1.0)]
    [InlineData("partly", "real", "real", 1.0)]
    [InlineData("wrong", null, null, 0.0)]
    public void TheAnswer_FollowsTheFeedbackVerdicts(string verdict, string? corrected, string? expected, double credit)
    {
        var (answer, got) = InvestigationPracticeScoring.Settle("false_positive", new[] { Row(verdict, corrected) });
        Assert.Equal(expected, answer);
        Assert.Equal(credit, got);
    }

    [Fact]
    public void TheNewestCorrectedVerdict_Wins_AndALaterRecordWithNoCorrection_DoesNotErase_It()
    {
        var feedback = new[] { Row("wrong", "needs_person", minutes: 1), Row("wrong", "real", minutes: 5), Row("right", minutes: 9) };
        Assert.Equal(("real", 1.0), InvestigationPracticeScoring.Settle("false_positive", feedback));
    }

    [Fact]
    public void ACaseWithNoFeedback_HasNoAnswer()
    {
        Assert.Equal((null, 0.0), InvestigationPracticeScoring.Settle("real", Array.Empty<InvestigationFeedbackRow>()));
    }

    [Fact]
    public void TheScorePerAgent_CountsTheNewestRunPerCase_AndSeparatesCasesWithNoAnswer()
    {
        var caseA = Guid.NewGuid();
        var caseB = Guid.NewGuid();
        var caseC = Guid.NewGuid();
        InvestigationPracticeRun RunOf(string agent, Guid source, string verdict, int minutes, string? original, params InvestigationFeedbackRow[] feedback) =>
            InvestigationPracticeScoring.Run(Guid.NewGuid(), source, "srv", "blocking", agent, verdict, Now.AddMinutes(minutes), original, feedback);

        var runs = new[]
        {
            RunOf("agent-a", caseA, "real", 1, "real", Row("right")),
            RunOf("agent-a", caseB, "real", 2, "false_positive", Row("wrong", "real", "looked at the head blocker")),
            RunOf("agent-a", caseC, "real", 3, "real", Row("wrong")),
            RunOf("agent-b", caseA, "false_positive", 1, "real", Row("right")),
            RunOf("agent-b", caseA, "real", 9, "real", Row("right")),
            RunOf("agent-b", caseB, "false_positive", 2, "false_positive", Row("wrong", "real")),
        };

        var scores = InvestigationPracticeScoring.ByAgent(runs);
        var a = Assert.Single(scores, s => s.AgentId == "agent-a");
        Assert.Equal((3, 2, 1, 2), (a.Cases, a.Scored, a.Unscored, a.Matched));
        Assert.Equal(1.0, a.Percent);
        Assert.Equal(1, a.MissedStepCases);
        var b = Assert.Single(scores, s => s.AgentId == "agent-b");
        Assert.Equal((2, 2, 0, 1), (b.Cases, b.Scored, b.Unscored, b.Matched));
        Assert.Equal(0.5, b.Percent);
        Assert.Equal(new[] { "agent-a", "agent-b" }, scores.Select(s => s.AgentId));
    }

    [Fact]
    public void AnAgentWithNothingScored_HasNoPercent()
    {
        var run = InvestigationPracticeScoring.Run(Guid.NewGuid(), Guid.NewGuid(), "srv", "blocking", "agent-a", "real", Now, "real", new[] { Row("wrong") });
        var score = Assert.Single(InvestigationPracticeScoring.ByAgent(new[] { run }));
        Assert.Null(score.Percent);
        Assert.Equal(1, score.Unscored);
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5918 on a live store. A practice request copies the judged case without its answers: the brief composed for it holds none
/// of the report, the proposed note, or any feedback field (a case with every feedback field filled in), while a live request
/// of the same story does carry the earlier report (the control). A case with no report, no feedback, or that is already a
/// practice request cannot be replayed and writes nothing. The gate refuses every action on it with <c>practice</c>, a live
/// alert never joins it or counts it against the route, and an agent's practice report is scored against the feedback.
/// </summary>
public sealed class InvestigationPracticeLiveTests
{
    private const int ServerId = 7_591_801;
    private const string ServerName = "LcPracticeSrv";
    private static readonly DateTime Now = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert(DateTime at) =>
        new(1, ServerId, ServerName, "blocking", "Blocking", at, new[] { ServerId + ":blocking" }, null, 50, 200, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static async Task<Guid> ReportedAsync(InvestigationRequestStore store, DateTime at, string verdict, string? note, CancellationToken ct)
    {
        var requestId = (await store.OpenOrJoinAsync(Alert(at), ct)).RequestId!.Value;
        var claim = await store.ClaimAsync(requestId, "agent-orig", ct);
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-orig", 1, verdict, "ZEBRA-ROOT", true, "ZEBRA-SUMMARY",
            "[{\"tool\":\"get_blocking\"}]", note), ct));
        return requestId;
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, InvestigationRequestStore, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the practice request live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(dataSource, new InvestigationRequestStore(dataSource, 30, () => Now), ct);
    }

    private static async Task<string> BriefAsync(NpgsqlDataSource dataSource, Guid request, CancellationToken ct)
    {
        var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => Now).ReadAsync(request, ct);
        return InvestigationBriefComposer.Compose(sources!, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, Now);
    }

    [Fact]
    public async Task APracticeBrief_HoldsNoAnswerFromTheCase_EvenWithEveryFeedbackFieldFilledIn()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var original = await ReportedAsync(store, Now.AddHours(-3), "real", "ZEBRA-NOTE the head blocker is the nightly batch", ct);
            await ExecAsync(dataSource, "UPDATE config.investigation_notes SET state = 'approved', decided_by = 'lee', decided_at = now(), expires_at = now() + interval '30 days'", ct);
            Assert.Equal(InvestigationFeedbackOutcome.Recorded, (await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(
                new InvestigationFeedbackSubmission(original, "wrong", "false_positive", "ZEBRA-MISSED the batch window", "ZEBRA-COMMENT it was the batch", "zebra-author"), ct)).Outcome);

            var practice = await new InvestigationPracticeStore(dataSource, 30, () => Now).OpenAsync(original, "person:lee", ct);
            Assert.Equal(InvestigationPracticeOutcome.Opened, practice.Outcome);

            var brief = await BriefAsync(dataSource, practice.RequestId!.Value, ct);
            Assert.DoesNotContain("ZEBRA", brief, StringComparison.Ordinal);
            Assert.DoesNotContain("zebra-author", brief, StringComparison.Ordinal);
            Assert.DoesNotContain("agent-orig", brief, StringComparison.Ordinal);
            var node = JsonNode.Parse(brief)!;
            Assert.True(node["request"]!["practice"]!.GetValue<bool>());
            Assert.NotEmpty(node["alerts"]!.AsArray());
            Assert.Empty(node["earlier_reports"]!.AsArray());
            Assert.Empty(node["notes"]!.AsArray());

            /* The control: the same story's live brief does carry the earlier report and the approved note, so the leak path was real. */
            var live = (await store.OpenOrJoinAsync(Alert(Now.AddHours(-1)), ct)).RequestId!.Value;
            var liveBrief = await BriefAsync(dataSource, live, ct);
            Assert.Contains("ZEBRA-ROOT", liveBrief, StringComparison.Ordinal);
            Assert.Contains("ZEBRA-NOTE", liveBrief, StringComparison.Ordinal);

            Assert.Equal(original.ToString(), await ScalarAsync(dataSource, $"SELECT practice_of FROM config.investigation_requests WHERE request_id = '{practice.RequestId}'", ct));
            Assert.Equal("1", await ScalarAsync(dataSource, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{practice.RequestId}'", ct));
            Assert.Equal("opened", await ScalarAsync(dataSource, $"SELECT event FROM config.investigation_journal WHERE request_id = '{practice.RequestId}'", ct));
        });
    }

    [Fact]
    public async Task ACaseThatWasNeverJudged_CannotBeReplayed_AndNothingIsWritten()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var practiceStore = new InvestigationPracticeStore(dataSource, 30, () => Now);
            var open = (await store.OpenOrJoinAsync(Alert(Now.AddHours(-5)), ct)).RequestId!.Value;
            var noFeedback = await ReportedAsync(store, Now.AddHours(-3), "real", null, ct);
            var judged = noFeedback;

            Assert.Equal(InvestigationPracticeOutcome.NotFound, (await practiceStore.OpenAsync(Guid.NewGuid(), "person:lee", ct)).Outcome);
            Assert.Equal(InvestigationPracticeOutcome.NoReport, (await practiceStore.OpenAsync(open, "person:lee", ct)).Outcome);
            Assert.Equal(InvestigationPracticeOutcome.NoFeedback, (await practiceStore.OpenAsync(noFeedback, "person:lee", ct)).Outcome);
            Assert.Equal("0", await ScalarAsync(dataSource, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));

            await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);
            var first = await practiceStore.OpenAsync(judged, "person:lee", ct);
            Assert.Equal(InvestigationPracticeOutcome.Opened, first.Outcome);
            var again = await practiceStore.OpenAsync(judged, "person:lee", ct);
            Assert.Equal(InvestigationPracticeOutcome.AlreadyLive, again.Outcome);
            Assert.Equal(first.RequestId, again.RequestId);
            Assert.Equal(InvestigationPracticeOutcome.IsPractice, (await practiceStore.OpenAsync(first.RequestId!.Value, "person:lee", ct)).Outcome);
            Assert.Equal("1", await ScalarAsync(dataSource, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task APracticeRequest_RefusesEveryAction_WithPractice_WhateverItsLevel()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var original = await ReportedAsync(store, Now.AddHours(-3), "real", null, ct);
            await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(new InvestigationFeedbackSubmission(original, "right", null, null, null, "lee"), ct);
            var practice = (await new InvestigationPracticeStore(dataSource, 30, () => Now).OpenAsync(original, "person:lee", ct)).RequestId!.Value;
            await ExecAsync(dataSource, $"UPDATE config.investigation_requests SET autonomy_level = 2 WHERE request_id = '{practice}'", ct);
            var claim = await store.ClaimAsync(practice, "agent-b", ct);

            var writes = 0;
            var hooks = new AgentActionHooks
            {
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, int?>>((_, _) => 2),
                ReadBefore = (_, _, _) => Task.FromResult<string?>("{}"),
                Write = (_, _, _) => { writes++; return Task.FromResult<string?>("{}"); },
            };
            var call = new AgentActionCall("create_mute_rule", "mute", new AgentAction(practice, claim.ClaimId!.Value, "agent-b", "the chain is a known batch",
                "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12\"}]"), new AgentActionTarget(ServerId, ServerName, null, null));

            var result = await store.RunAgentActionAsync(call, hooks, ct);
            Assert.Equal(AgentActionCodes.Practice, result.Refusal?.Reason);
            Assert.Equal(0, writes);
        });
    }

    [Fact]
    public async Task ALiveAlert_NeverJoinsAPracticeRequest_AndItNeverCountsAgainstTheRoute()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var original = await ReportedAsync(store, Now.AddHours(-3), "real", null, ct);
            await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(new InvestigationFeedbackSubmission(original, "right", null, null, null, "lee"), ct);
            var practice = (await new InvestigationPracticeStore(dataSource, 30, () => Now).OpenAsync(original, "person:lee", ct)).RequestId!.Value;

            var live = await store.OpenOrJoinAsync(Alert(Now), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, live.Outcome);
            Assert.NotEqual(practice, live.RequestId);
            Assert.Equal("1", await ScalarAsync(dataSource, $"SELECT alert_count FROM config.investigation_requests WHERE request_id = '{practice}'", ct));
            var use = await store.RouteUseAsync(ct);
            Assert.Equal(2, use[1].LastDay);
        });
    }

    [Fact]
    public async Task APracticeReport_IsScoredAgainstTheFeedback_PerAgent()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var original = await ReportedAsync(store, Now.AddHours(-3), "real", null, ct);
            await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(
                new InvestigationFeedbackSubmission(original, "wrong", "false_positive", "the batch window", null, "lee"), ct);
            var practiceStore = new InvestigationPracticeStore(dataSource, 30, () => Now);
            var practice = (await practiceStore.OpenAsync(original, "person:lee", ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(practice, "agent-b", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                practice, claim.ClaimId!.Value, "agent-b", 1, "false_positive", "batch", true, "it was the batch", "[{\"tool\":\"get_blocking\"}]"), ct));

            var score = Assert.Single(await practiceStore.ReadScoresAsync(Now.AddDays(-1), ct));
            Assert.Equal("agent-b", score.AgentId);
            Assert.Equal((1, 1, 1), (score.Cases, score.Scored, score.Matched));
            Assert.Equal(1.0, score.Percent);
            Assert.Equal(1, score.MissedStepCases);
        });
    }
}
