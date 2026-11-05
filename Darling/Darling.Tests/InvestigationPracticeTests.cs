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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
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

    [Theory]
    [InlineData(true, false, true)]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    public void TheReportReadHidesTheReplayedCase_OnlyUnderTheInvestigationsArm(bool investigations, bool writes, bool expected)
    {
        var services = new ServiceCollection();
        DarlingMcpHostService.ConfigureMcpServices(services, DarlingPeerDirectory.Snapshot.Empty,
            writes: new DarlingMcpWritePolicy.Effective(writes, false, investigations));
        using var provider = services.BuildServiceProvider();
        Assert.Equal(expected, provider.GetRequiredService<InvestigationReadPolicy>().HideLivePracticeAnswers);
    }

    [Fact]
    public void TheRequestRow_NamesWhatAPracticeReplays_AndOtherRowsKeepTheirShapeWithNoKey()
    {
        var plain = new InvestigationRequestRow(Guid.NewGuid(), 1, 7, "srv", "blocking", "Blocking", Now, Now, 1, "open", Now, Now.AddHours(4), null, null, 0, null);
        var replayed = Guid.NewGuid();
        var practice = plain with { RequestId = Guid.NewGuid(), PracticeOf = replayed };

        var plainNode = JsonSerializer.SerializeToNode(InvestigationToolCore.RequestPayload(plain))!.AsObject();
        var practiceNode = JsonSerializer.SerializeToNode(InvestigationToolCore.RequestPayload(practice))!.AsObject();

        Assert.False(plainNode.ContainsKey("practice_of"), "absent, not null");
        Assert.Equal(replayed.ToString(), practiceNode["practice_of"]!.GetValue<string>());
        Assert.Equal(plainNode.Select(p => p.Key), practiceNode.Where(p => p.Key != "practice_of").Select(p => p.Key));
        Assert.Equal("reported_at", plainNode.Last().Key);
    }

    [Fact]
    public void WithoutReplayedAnswers_DropsTheReportFeedbackAndJournalOfTheReplayedCaseOnly()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var request = new InvestigationRequestRow(
            a, 1, 7, "srv", "blocking", "Blocking", Now, Now, 1, "reported", Now, Now.AddHours(4), null, null, 1, Now);
        InvestigationReportRow Report(Guid id) =>
            new(id, 1, "agent", Now, "real", "root", true, null, "[]");
        var read = new InvestigationReportsRead(
            [request, request with { RequestId = b }], [Report(a), Report(b)],
            [new(Now, a, 7, "reported", "agent", null, null), new(Now, b, 7, "reported", "agent", null, null)], null,
            Feedback: [Row("right") with { RequestId = a }, Row("wrong") with { RequestId = b }]);

        var kept = DarlingMcpInvestigationTools.WithoutReplayedAnswers(read, new HashSet<Guid> { a });

        Assert.Equal(2, kept.Requests.Count);
        Assert.Equal(b, Assert.Single(kept.Reports).RequestId);
        Assert.Equal(b, Assert.Single(kept.Feedback!).RequestId);
        Assert.Equal(b, Assert.Single(kept.Journal).RequestId);
        Assert.Same(read, DarlingMcpInvestigationTools.WithoutReplayedAnswers(read, new HashSet<Guid>()));
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

            /* The practice brief's notes are empty and its note is not the case's; the live brief of the same story (read below, once
               the practice is no longer live) does carry the earlier report and the approved note, so the leak path was real. */
            Assert.Equal(original.ToString(), await ScalarAsync(dataSource, $"SELECT practice_of FROM config.investigation_requests WHERE request_id = '{practice.RequestId}'", ct));
            Assert.Equal("1", await ScalarAsync(dataSource, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{practice.RequestId}'", ct));
            Assert.Equal("opened", await ScalarAsync(dataSource, $"SELECT event FROM config.investigation_journal WHERE request_id = '{practice.RequestId}'", ct));

            /* The control, once the practice is done (a live practice also hides the case from other briefs, see the next fact). */
            await ExecAsync(dataSource, $"UPDATE config.investigation_requests SET state = 'closed' WHERE request_id = '{practice.RequestId}'", ct);
            var live = (await store.OpenOrJoinAsync(Alert(Now.AddHours(-1)), ct)).RequestId!.Value;
            var liveBrief = await BriefAsync(dataSource, live, ct);
            Assert.Contains("ZEBRA-ROOT", liveBrief, StringComparison.Ordinal);
            Assert.Contains("ZEBRA-NOTE", liveBrief, StringComparison.Ordinal);
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
                LoadRoutes = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, PerformanceMonitor.Notifications.NotificationRoute?>>((_, _, _) => TestRouteHooks.AtLevel(2)),
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

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static string[] Ids(JsonElement root, string array) =>
        root.GetProperty(array).EnumerateArray().Select(e => e.GetProperty("request_id").GetString()!).ToArray();

    [Fact]
    public async Task WhileAPracticeIsLive_TheInvestigationsArm_ReadsNoReportOrFeedbackOfTheReplayedCase_AndEveryOtherReport()
    {
        await RunAsync(async (dataSource, fixedClockStore, ct) =>
        {
            /* The tool reads on the real clock, so this fact writes on it too (a fixed clock would age the practice out). */
            var store = new InvestigationRequestStore(dataSource, 30);
            var now = DateTime.UtcNow;
            var replayed = await ReportedAsync(store, now.AddHours(-9), "real", null, ct);
            var other = await ReportedAsync(store, now.AddHours(-6), "false_positive", null, ct);
            var feedback = new InvestigationFeedbackStore(dataSource, 30);
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(replayed, "wrong", "false_positive", "ZEBRA-MISSED", "ZEBRA-COMMENT", "lee"), ct);
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(other, "right", null, null, "OTHER-COMMENT", "lee"), ct);
            var practiceStore = new InvestigationPracticeStore(dataSource, 30);
            var practice = (await practiceStore.OpenAsync(replayed, "person:lee", ct)).RequestId!.Value;

            /* One request by id and the window, both: the answer key is absent from each. */
            foreach (var requestId in new string?[] { replayed.ToString(), null })
            {
                var arm = await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, requestId, 24, ct, hideLivePracticeAnswers: true);
                Assert.DoesNotContain("ZEBRA-COMMENT", arm, StringComparison.Ordinal);
                Assert.DoesNotContain("ZEBRA-MISSED", arm, StringComparison.Ordinal);
                var root = Root(arm);
                Assert.DoesNotContain(replayed.ToString(), Ids(root, "reports"));
                Assert.DoesNotContain(replayed.ToString(), root.GetProperty("journal").EnumerateArray().Select(j => j.GetProperty("request_id").GetString() ?? "").ToArray());
                Assert.Contains(replayed.ToString(), Ids(root, "requests"));
                if (requestId is null)
                {
                    Assert.Equal(other.ToString(), Assert.Single(Ids(root, "reports")));
                    var feedbackOfOther = root.GetProperty("reports").EnumerateArray().Single().GetProperty("feedback");
                    Assert.Equal("OTHER-COMMENT", Assert.Single(feedbackOfOther.EnumerateArray()).GetProperty("comment").GetString());
                    var row = root.GetProperty("requests").EnumerateArray().Single(r => r.GetProperty("request_id").GetString() == practice.ToString());
                    Assert.Equal(replayed.ToString(), row.GetProperty("practice_of").GetString());
                    Assert.All(root.GetProperty("requests").EnumerateArray().Where(r => r.GetProperty("request_id").GetString() != practice.ToString()),
                        r => Assert.False(r.TryGetProperty("practice_of", out _), "a request that is not practice has no practice_of key"));
                }
            }

            /* The writes arm (and a host serving neither arm): nothing is hidden. */
            var operators = Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, null, 24, ct, hideLivePracticeAnswers: false));
            Assert.Contains(replayed.ToString(), Ids(operators, "reports"));
            Assert.Contains("ZEBRA-COMMENT", operators.GetRawText(), StringComparison.Ordinal);

            /* A claimed practice still hides it; once its report is submitted the case shows again. */
            var claim = await store.ClaimAsync(practice, "agent-b", ct);
            Assert.DoesNotContain(replayed.ToString(), Ids(Root(await DarlingMcpInvestigationTools.ReportsAsync(
                dataSource, McpDataDetail.Full, null, null, 24, ct, hideLivePracticeAnswers: true)), "reports"));
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                practice, claim.ClaimId!.Value, "agent-b", 1, "false_positive", "batch", true, "done", "[{\"tool\":\"get_blocking\"}]"), ct));
            var after = Root(await DarlingMcpInvestigationTools.ReportsAsync(dataSource, McpDataDetail.Full, null, null, 24, ct, hideLivePracticeAnswers: true));
            Assert.Contains(replayed.ToString(), Ids(after, "reports"));
            Assert.Contains("ZEBRA-COMMENT", after.GetRawText(), StringComparison.Ordinal);
        });
    }

    private static string[] EarlierIds(string brief) =>
        JsonNode.Parse(brief)!["earlier_reports"]!.AsArray().Select(n => n!["request_id"]!.GetValue<string>()).ToArray();

    private static string[] PriorLinkIds(string brief) =>
        JsonNode.Parse(brief)!["prior_links"] is JsonArray links
            ? links.Select(n => n!["request_id"]!.GetValue<string>()).ToArray()
            : [];

    [Fact]
    public async Task AnotherBrief_OnTheSameServerAndFamily_ReadsNoPracticeReport_AndNoReportOfACaseALivePracticeReplays()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var link = new[] { new InvestigationReportLink("jira", "https://jira.example.invalid/browse/OPS-9", "OPS-9") };

            async Task<Guid> ReportWithLinkAsync(Guid request, string agent, string root)
            {
                var claim = await store.ClaimAsync(request, agent, ct);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                    request, claim.ClaimId!.Value, agent, 1, "false_positive", root, true, "summary", "[{\"tool\":\"get_blocking\"}]", null, link), ct));
                return request;
            }

            var original = await ReportWithLinkAsync((await store.OpenOrJoinAsync(Alert(Now.AddHours(-5)), ct)).RequestId!.Value, "agent-orig", "CASE-ROOT");
            var bystander = await ReportWithLinkAsync((await store.OpenOrJoinAsync(Alert(Now.AddHours(-4)), ct)).RequestId!.Value, "agent-other", "OTHER-ROOT");
            await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(
                new InvestigationFeedbackSubmission(original, "wrong", "false_positive", "missed", "comment", "lee"), ct);

            var practice = (await new InvestigationPracticeStore(dataSource, 30, () => Now).OpenAsync(original, "person:lee", ct)).RequestId!.Value;
            var live = (await store.OpenOrJoinAsync(Alert(Now), ct)).RequestId!.Value;
            Assert.NotEqual(practice, live);

            /* While the practice is live: a live brief on the same server and family reads the bystander only, and no link of the case. */
            var whileLive = await BriefAsync(dataSource, live, ct);
            Assert.Equal(new[] { bystander.ToString() }, EarlierIds(whileLive));
            Assert.Equal(new[] { bystander.ToString() }, PriorLinkIds(whileLive));
            Assert.DoesNotContain("CASE-ROOT", whileLive, StringComparison.Ordinal);

            /* The practising agent reports. The practice report is never history for a live brief (neither report nor link), and the
               case is no longer replayed by a live practice, so its report reads again like any judged report's. */
            await ReportWithLinkAsync(practice, "agent-practice", "PRACTICE-ROOT");
            var afterReport = await BriefAsync(dataSource, live, ct);
            Assert.DoesNotContain(practice.ToString(), EarlierIds(afterReport));
            Assert.DoesNotContain(practice.ToString(), PriorLinkIds(afterReport));
            Assert.DoesNotContain("PRACTICE-ROOT", afterReport, StringComparison.Ordinal);
            Assert.Equal(new[] { bystander.ToString(), original.ToString() }.OrderBy(x => x), EarlierIds(afterReport).OrderBy(x => x));

            /* A practice that is open but dead (claimed three times) hides nothing: the case's report reads again. */
            var replay = (await new InvestigationPracticeStore(dataSource, 30, () => Now).OpenAsync(original, "person:lee", ct)).RequestId!.Value;
            Assert.DoesNotContain(original.ToString(), EarlierIds(await BriefAsync(dataSource, live, ct)));
            await ExecAsync(dataSource, $"UPDATE config.investigation_requests SET state = 'open', claim_count = 3 WHERE request_id = '{replay}'", ct);
            Assert.Contains(original.ToString(), EarlierIds(await BriefAsync(dataSource, live, ct)));
        });
    }

    [Fact]
    public async Task APracticeRequest_IsNotRealTraffic_ForTheFeedbackList_AndTheFamilyCounts_AndItCopiesTheAlertCount()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            /* A case that took three alerts: the practice reads the same count. */
            var request = (await store.OpenOrJoinAsync(Alert(Now.AddHours(-3)), ct)).RequestId!.Value;
            Assert.Equal(request, (await store.OpenOrJoinAsync(Alert(Now.AddHours(-3).AddMinutes(1)), ct)).RequestId);
            Assert.Equal(request, (await store.OpenOrJoinAsync(Alert(Now.AddHours(-3).AddMinutes(2)), ct)).RequestId);
            Assert.Equal("3", await ScalarAsync(dataSource, $"SELECT alert_count FROM config.investigation_requests WHERE request_id = '{request}'", ct));
            var claim = await store.ClaimAsync(request, "agent-orig", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                request, claim.ClaimId!.Value, "agent-orig", 1, "false_positive", "root", true, "summary", "[{\"tool\":\"get_blocking\"}]"), ct));
            var feedback = new InvestigationFeedbackStore(dataSource, 30);
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(request, "right", null, null, null, "lee"), ct);

            var practiceStore = new InvestigationPracticeStore(dataSource, 30, () => Now);
            var practice = (await practiceStore.OpenAsync(request, "person:lee", ct)).RequestId!.Value;
            Assert.Equal("3", await ScalarAsync(dataSource, $"SELECT alert_count FROM config.investigation_requests WHERE request_id = '{practice}'", ct));
            Assert.Equal(3, JsonNode.Parse(await BriefAsync(dataSource, practice, ct))!["request"]!["alert_count"]!.GetValue<int>());

            var practiceClaim = await store.ClaimAsync(practice, "agent-practice", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                practice, practiceClaim.ClaimId!.Value, "agent-practice", 1, "false_positive", "root", true, "summary", "[{\"tool\":\"get_blocking\"}]"), ct));
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(practice, "right", null, null, null, "lee"), ct);

            /* The practice report and its feedback are in no real-traffic read, but they are in the Practice scores. */
            var (rows, _) = await feedback.ListReportedAsync(Now.AddDays(-1), null, 50, ct);
            Assert.Equal(request, Assert.Single(rows).RequestId);
            var family = Assert.Single(await feedback.FamilyCountsAsync(Now.AddDays(-1), null, ct));
            Assert.Equal((1, 1, 1), (family.Reported, family.WithFeedback, family.FalsePositives));
            Assert.Single(await practiceStore.ReadScoresAsync(Now.AddDays(-1), ct));
        });
    }

    [Fact]
    public async Task ADeadPractice_DoesNotBlockAFreshReplayOfTheCase()
    {
        await RunAsync(async (dataSource, store, ct) =>
        {
            var original = await ReportedAsync(store, Now.AddHours(-3), "real", null, ct);
            await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(new InvestigationFeedbackSubmission(original, "right", null, null, null, "lee"), ct);
            var practiceStore = new InvestigationPracticeStore(dataSource, 30, () => Now);
            var first = await practiceStore.OpenAsync(original, "person:lee", ct);
            Assert.Equal(InvestigationPracticeOutcome.AlreadyLive, (await practiceStore.OpenAsync(original, "person:lee", ct)).Outcome);

            /* Claimed three times and released: the computed state reads expired, though the stored state still says open. */
            await ExecAsync(dataSource, $"UPDATE config.investigation_requests SET state = 'open', claim_count = 3 WHERE request_id = '{first.RequestId}'", ct);
            var second = await practiceStore.OpenAsync(original, "person:lee", ct);
            Assert.Equal(InvestigationPracticeOutcome.Opened, second.Outcome);
            Assert.NotEqual(first.RequestId, second.RequestId);
        });
    }
}

/// <summary>
/// #5918 on the real grants: the Viewer connects as the <c>admin</c> role, so opening a practice request must work through
/// the replayed INSERT grants (managed section 10e, provision-roles step 3j), as the real login each source made, and the
/// <c>viewer</c> role must be refused with no row written. The fixed-name roles are cluster-wide, so the class is in the
/// <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class InvestigationPracticeGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/mcp roles
       are why the class sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];
    private const int ServerId = 7_591_802;
    private const string ServerName = "LcPracticeGrantSrv";

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A reported, judged case written by the store owner, and the practice request opened as each login.</summary>
    private static async Task RunAsync(Func<NpgsqlConnection, ScratchPostgres, CancellationToken, Task<(string Admin, string Viewer)>> provision)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the practice grant live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await DarlingMcpTestData.RegisterServerAsync(owner, ServerId, ServerName, ct);
            Assert.SkipWhen(
                await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the grants would adopt it.");
            var (adminPassword, viewerPassword) = await provision(owner, scratch, ct);

            await using var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var store = new InvestigationRequestStore(ownerSource, 30);
            var at = DateTime.UtcNow.AddHours(-3);
            var original = (await store.OpenOrJoinAsync(new InvestigationAlert(
                1, ServerId, ServerName, "blocking", "Blocking", at, new[] { ServerId + ":blocking" }, null, 50, 200,
                LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(original, "agent-orig", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                original, claim.ClaimId!.Value, "agent-orig", 1, "real", "root", true, "summary", "[{\"tool\":\"get_blocking\"}]"), ct));
            await new InvestigationFeedbackStore(ownerSource, 30).RecordAsync(
                new InvestigationFeedbackSubmission(original, "right", null, null, null, "lee"), ct);
            var alertRows = await ScalarAsync(owner, $"SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{original}'", ct);
            Assert.NotEqual("0", alertRows);

            /* The viewer role is refused, and nothing is written. */
            await using (var viewerSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = "viewer", Password = viewerPassword }.ConnectionString))
            {
                var denied = await Assert.ThrowsAsync<PostgresException>(() => new InvestigationPracticeStore(viewerSource, 30).OpenAsync(original, "person:viewer", ct));
                Assert.Equal("42501", denied.SqlState);
            }

            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_requests WHERE practice_of IS NOT NULL", ct));

            /* The admin role opens it: the row names the case, and its alerts are the case's. */
            await using (var adminSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = "admin", Password = adminPassword }.ConnectionString))
            {
                var opened = await new InvestigationPracticeStore(adminSource, 30).OpenAsync(original, "person:admin", ct);
                Assert.Equal(InvestigationPracticeOutcome.Opened, opened.Outcome);
                var practice = opened.RequestId!.Value;
                Assert.Equal(original.ToString(), await ScalarAsync(owner,
                    $"SELECT practice_of FROM config.investigation_requests WHERE request_id = '{practice}'", ct));
                Assert.Equal(alertRows, await ScalarAsync(owner,
                    $"SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{practice}'", ct));

                /* #6003 review round 2, on the real grants: the opened row's once key is the trigger's; admin cannot name the
                   journal's identity or event id, nor link an alert to a live request. */
                Assert.Equal("practice_opened:" + practice.ToString("D"), await ScalarAsync(owner,
                    $"SELECT once_key FROM config.investigation_journal WHERE request_id = '{practice}' AND event = 'opened'", ct));
                await using var admin = await adminSource.OpenConnectionAsync(ct);
                foreach (var sql in new[]
                {
                    "INSERT INTO config.investigation_journal (journal_id, request_id, event, actor) OVERRIDING SYSTEM VALUE VALUES (9000000000, NULL, 'claimed', 'person:admin')",
                    "INSERT INTO config.investigation_journal (event_id, request_id, event, actor) VALUES (gen_random_uuid(), NULL, 'claimed', 'person:admin')",
                    $"INSERT INTO config.investigation_request_alerts (request_id, alert_time, server_id, metric_name) VALUES ('{original}', now(), {ServerId}, 'Blocking')",
                })
                {
                    Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(admin, sql, ct))).SqlState);
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "pr5918none", cleanupCt));
        }
    }

    [Fact]
    public async Task TheManagedBatch_LetsAdminOpenAPracticeRequest_AndRefusesViewer() =>
        await RunAsync(async (owner, scratch, ct) =>
        {
            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            await ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)), ct);
            return (ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword);
        });

    [Fact]
    public async Task TheBringYourOwnScript_LetsAdminOpenAPracticeRequest_AndRefusesViewer() =>
        await RunAsync(async (owner, scratch, ct) =>
        {
            Assert.SkipUnless(
                await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
                "The script names the store owner 'darling', and this rig has no such role.");
            const string AdminPassword = "ScriptAdmin5918", ViewerPassword = "ScriptViewer5918", McpPassword = "ScriptMcp5918";
            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", AdminPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", ViewerPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", McpPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", "ScriptWeb6991", StringComparison.Ordinal);
            await ExecAsync(owner, script, ct);
            return (AdminPassword, ViewerPassword);
        });
}

/// <summary>
/// The Practice tab's panel on the shared STA thread (#5918), reached through the Investigations window that seats it: Start
/// with nothing selected says so without reaching the store (it listens on nothing here), a read-only seat gets no Start
/// control, and the score text reads in points or says nothing was scored.
/// </summary>
public sealed class InvestigationPracticePanelStaTests
{
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static void OnPanel(bool readOnly, Func<InvestigationPracticePanel, Task> body) =>
        StaTestThread.Run(async () =>
        {
            await using var viewer = new ViewerDataService(UnreachableStore);
            var window = new InvestigationsWindow(viewer, readOnly, "tester");
            try
            {
                await body(window.PracticePanel);
            }
            finally
            {
                window.Close();
            }
        });

    [Fact]
    public void Start_WithNoCaseSelected_SaysSo_BeforeAnyWrite() =>
        OnPanel(false, async panel =>
        {
            await panel.StartCommandAsync();
            Assert.Equal("Select a judged case to replay.", panel.PracticeStatusText.Text);
        });

    [Fact]
    public void AReadOnlySeat_HasNoStartControl_AndAnAdminSeatHasOne()
    {
        OnPanel(true, panel =>
        {
            Assert.Equal(System.Windows.Visibility.Collapsed, panel.StartPracticePanel.Visibility);
            return Task.CompletedTask;
        });
        OnPanel(false, panel =>
        {
            Assert.Equal(System.Windows.Visibility.Visible, panel.StartPracticePanel.Visibility);
            return Task.CompletedTask;
        });
    }

    [Fact]
    public void AScoreRendersInPoints_AndAnUnscoredAgentSaysSo()
    {
        var scored = new InvestigationPracticeScore("agent-b", 3, 2, 1, 2, 2.0, 2.0, 1.0, 0);
        var item = new InvestigationPracticePanel.PracticeScoreItem(scored);
        Assert.Equal("agent-b", item.AgentId);
        Assert.StartsWith("2 of 2 (", item.ScoreDisplay, StringComparison.Ordinal);
        Assert.Equal("not scored yet", InvestigationPracticePanel.ScoreText(scored with { Percent = null }));
    }
}

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The Practice tab's panel on a live store, on the shared STA thread (#5918): of two reported cases, only the one with
/// feedback is listed; Start on it opens a practice request; once an agent's practice report lands the scores grid shows the
/// agent with its points.
/// </summary>
public sealed class InvestigationPracticePanelLiveStaTests
{
    private const int ServerId = 7_591_803;
    private const string ServerName = "LcPracticePanelSrv";

    [Fact]
    public async Task ThePanel_ListsOnlyTheJudgedCase_StartsItsPractice_AndShowsTheScore()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the practice panel live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30);
        async Task<Guid> ReportedAsync(DateTime at)
        {
            var id = (await store.OpenOrJoinAsync(new InvestigationAlert(
                1, ServerId, ServerName, "blocking", "Blocking", at, new[] { ServerId + ":blocking" }, null, 50, 200,
                LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
            var claim = await store.ClaimAsync(id, "agent-orig", ct);
            Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                id, claim.ClaimId!.Value, "agent-orig", 1, "real", "root", true, "summary", "[{\"tool\":\"get_blocking\"}]"), ct));
            return id;
        }

        var judged = await ReportedAsync(DateTime.UtcNow.AddHours(-9));
        var unjudged = await ReportedAsync(DateTime.UtcNow.AddHours(-6));
        await new InvestigationFeedbackStore(dataSource, 30).RecordAsync(
            new InvestigationFeedbackSubmission(judged, "right", null, null, null, "lee"), ct);

        var connectionString = scratch.ConnectionString;
        var practice = Guid.Empty;
        StaTestThread.Run(async () =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var window = new InvestigationsWindow(viewer, false, "lee");
            try
            {
                var panel = window.PracticePanel;
                await panel.LoadAsync();
                var items = panel.PracticeCasesGrid.Items.Cast<InvestigationPracticePanel.PracticeCaseItem>().ToList();
                var listed = Assert.Single(items);
                Assert.Equal(judged, listed.Row.RequestId);
                Assert.DoesNotContain(items, i => i.Row.RequestId == unjudged);

                panel.PracticeCasesGrid.SelectedItem = listed;
                await panel.StartCommandAsync();
                Assert.StartsWith("Practice request opened (", panel.PracticeStatusText.Text, StringComparison.Ordinal);
                practice = Guid.Parse(panel.PracticeStatusText.Text.Split('(', ')')[1]);

                /* The Requests tab's read marks the practice request and no other. */
                var requests = await viewer.GetInvestigationRequestsAsync(null);
                var marked = Assert.Single(requests, r => r.PracticeOf is not null);
                Assert.Equal((practice, judged, "Practice"), (marked.RequestId, marked.PracticeOf!.Value, marked.PracticeDisplay));
                Assert.All(requests.Where(r => r.PracticeOf is null), r => Assert.Equal("", r.PracticeDisplay));

                var claim = await store.ClaimAsync(practice, "agent-b", ct);
                Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
                    practice, claim.ClaimId!.Value, "agent-b", 1, "real", "root", true, "same", "[{\"tool\":\"get_blocking\"}]"), ct));
                await panel.LoadAsync();
                var scores = ((IEnumerable<InvestigationPracticePanel.PracticeScoreItem>)panel.PracticeScoresGrid.ItemsSource).ToList();
                var score = Assert.Single(scores);
                Assert.Equal("agent-b", score.AgentId);
                Assert.StartsWith("1 of 1 (", score.ScoreDisplay, StringComparison.Ordinal);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotEqual(Guid.Empty, practice);
        await using var check = dataSource.CreateCommand($"SELECT practice_of::text FROM config.investigation_requests WHERE request_id = '{practice}'");
        Assert.Equal(judged.ToString(), Convert.ToString(await check.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
    }
}

/// <summary>
/// The Admin page's Practice Scores rows and score words (#5918), run under Node through the Admin page's pure-function
/// harness (<c>admin-server-edit-harness.mjs</c>, scenario <c>pure</c>): points of possible and the percentage, "not scored yet"
/// when nothing was scored (null or absent percent, a zero is still a score), one row per agent with every column, and no rows
/// for an answer that is not a list. Skipped when Node is not installed.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminPracticeScoresBehaviourTests
{
    private static JsonElement[] Calls(params (string Fn, object?[] Args)[] calls) =>
        AdminServerEditBehaviourTests.Run("pure", JsonSerializer.Serialize(new { calls = calls.Select(c => new { fn = c.Fn, args = c.Args }) }))
            .GetProperty("results").EnumerateArray().ToArray();

    [Fact]
    public void ThePracticeScoreText_ReadsInPoints_AndSaysWhenNothingWasScored()
    {
        var text = Calls(
            ("practiceScoreText", [new { points = 3.5, possible = 4, percent = 88 }]),
            ("practiceScoreText", [new { points = 0, possible = 2, percent = 0 }]),
            ("practiceScoreText", [new { points = 0, possible = 0, percent = (int?)null }]),
            ("practiceScoreText", [new { points = 0, possible = 0 }]))
            .Select(r => r.GetString()).ToArray();
        Assert.Equal(["3.5 of 4 (88%)", "0 of 2 (0%)", "not scored yet", "not scored yet"], text);
    }

    [Fact]
    public void ThePracticeRows_CarryEveryColumnPerAgent_AndNothingForANonList()
    {
        var results = Calls(
            ("practiceRows", [new object[]
            {
                new { agent_id = "agent-a", cases = 3, scored = 2, matched = 1, unscored = 1, missed_step_cases = 2, points = 1.5, possible = 2, percent = 75 },
                new { agent_id = "agent-b", cases = 1, scored = 0, matched = 0, unscored = 1, missed_step_cases = 0, points = 0, possible = 0, percent = (int?)null },
            }]),
            ("practiceRows", [null]),
            ("practiceRows", [new { not = "a list" }]));
        var rows = results[0].EnumerateArray().ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("agent-a", rows[0].GetProperty("agent_id").GetString());
        Assert.Equal((3, 2, 1, 1, 2), (rows[0].GetProperty("cases").GetInt32(), rows[0].GetProperty("scored").GetInt32(),
            rows[0].GetProperty("matched").GetInt32(), rows[0].GetProperty("unscored").GetInt32(), rows[0].GetProperty("missed_step_cases").GetInt32()));
        Assert.Equal("1.5 of 2 (75%)", rows[0].GetProperty("score").GetString());
        Assert.Equal("not scored yet", rows[1].GetProperty("score").GetString());
        Assert.Empty(results[1].EnumerateArray());
        Assert.Empty(results[2].EnumerateArray());
    }
}
