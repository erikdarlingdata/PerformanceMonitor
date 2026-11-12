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
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7658: the cards' buttons, built from service data. A pending proposal carries Approve and Refuse; a done change carries Undo only when its
/// kind has an undo path, never a kill; a report carries the three verdict buttons; a pending note carries Approve and Reject. Every card
/// with buttons carries the press-again line and names the record its buttons act on, so the poster saves it; a card without buttons names
/// none. The Confirm value round-trips the card it names, and the two client bodies carry what Slack needs and nothing else.
/// </summary>
public sealed class SlackCardsTests
{
    private static readonly Guid Request = Guid.Parse("76580000-0000-0000-0000-0000000000e1");
    private static readonly Guid Proposal = Guid.Parse("76580000-0000-0000-0000-0000000000a1");

    private static JsonArray Blocks(SlackThreadPostContent content) => JsonNode.Parse(JsonSerializer.Serialize(content.Blocks))!.AsArray();

    private static JsonObject? ActionsBlock(SlackThreadPostContent content) =>
        Blocks(content).OfType<JsonObject>().FirstOrDefault(b => b["type"]?.GetValue<string>() == "actions");

    private static List<(string ActionId, string Value, string Text)> Buttons(SlackThreadPostContent content) =>
        ActionsBlock(content)?["elements"]?.AsArray().OfType<JsonObject>()
            .Select(e => (e["action_id"]!.GetValue<string>(), e["value"]!.GetValue<string>(), e["text"]!["text"]!.GetValue<string>()))
            .ToList() ?? [];

    private static string AllText(SlackThreadPostContent content) => JsonSerializer.Serialize(content.Blocks);

    [Fact]
    public void TheProposalCard_CarriesApproveAndRefuse_ThePressAgainLine_AndNamesItsRecord()
    {
        var card = SlackThreadPosts.ProposalPost("force_query_store_plan", "srv-7", "pending", "EXEC x", "https://pm.example/#/p", McpDataDetail.Full,
            new DateTime(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc), Proposal);

        var buttons = Buttons(card);
        Assert.Equal([SlackCardActions.ProposalApproveFirst, SlackCardActions.ProposalRefuse], buttons.Select(b => b.ActionId));
        Assert.All(buttons, b => Assert.Equal(Proposal.ToString("D"), b.Value));
        Assert.Contains(SlackCards.PressAgain, AllText(card));
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Proposal, Proposal.ToString("D")), card.Card);
    }

    [Fact]
    public void AProposalDecisionPost_CarriesNoButtons_AndNamesNoRecord()
    {
        var post = SlackThreadPosts.ProposalPost("force_query_store_plan", "srv-7", "run by Erik", null, null, McpDataDetail.Full);
        Assert.Null(ActionsBlock(post));
        Assert.Null(post.Card);
        Assert.DoesNotContain(SlackCards.PressAgain, AllText(post));
    }

    [Theory]
    [InlineData(AgentChangeKinds.Mute)]
    [InlineData(AgentChangeKinds.CustomRule)]
    [InlineData(AgentChangeKinds.ThresholdOverride)]
    [InlineData(AgentChangeKinds.SignatureMerge)]
    [InlineData(AgentChangeKinds.PlanForce)]
    [InlineData(AgentChangeKinds.PlanUnforce)]
    [InlineData(AgentChangeKinds.PlanGuideCreate)]
    [InlineData(AgentChangeKinds.PlanGuideDrop)]
    public void ADoneChangeWithAnUndoPath_CarriesUndo_AndNamesTheCalledRow(string kind)
    {
        Assert.True(SlackCards.HasUndo(kind));
        var card = SlackThreadPosts.ChangePost("mute_alert", "cpu on srv-7", "done", McpDataDetail.Full, 4242, kind);
        var button = Assert.Single(Buttons(card));
        Assert.Equal((SlackCardActions.UndoFirst, "4242", "Undo"), button);
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Change, "4242"), card.Card);
        Assert.Contains(SlackCards.PressAgain, AllText(card));
    }

    [Theory]
    [InlineData(AgentChangeKinds.SessionKill)]
    [InlineData(AgentChangeKinds.PlanEvict)]
    [InlineData(AgentChangeKinds.ModuleRecompile)]
    [InlineData(null)]
    [InlineData("")]
    public void AKill_AnEviction_ARecompile_AndAnUnknownKind_CarryNoUndo(string? kind)
    {
        Assert.False(SlackCards.HasUndo(kind));
        var card = SlackThreadPosts.ChangePost("kill_session", "87", "done", McpDataDetail.Full, 4242, kind);
        Assert.Null(ActionsBlock(card));
        Assert.Null(card.Card);
    }

    [Fact]
    public void AChangeWithoutACalledRow_CarriesNoUndo_EvenForAnUndoableKind()
    {
        var card = SlackThreadPosts.ChangePost("mute_alert", "cpu", "done", McpDataDetail.Full, null, AgentChangeKinds.Mute);
        Assert.Null(ActionsBlock(card));
        Assert.Null(card.Card);
    }

    [Fact]
    public void TheReportCard_CarriesTheThreeVerdictButtons_WithTheRequestId_AndPointsAtTheWebFormForCorrections()
    {
        var content = new ReportNoticeContent(Request, "plan regression", "srv-7", "cpu", "the plan changed", [], "https://pm.example");
        var card = SlackThreadPosts.ReportPost(content, McpDataDetail.Full, verdictButtons: true);

        var buttons = Buttons(card);
        Assert.Equal([SlackCardActions.VerdictRight, SlackCardActions.VerdictPartly, SlackCardActions.VerdictWrong], buttons.Select(b => b.ActionId));
        Assert.All(buttons, b => Assert.Equal(Request.ToString("D"), b.Value));
        Assert.Contains("web feedback form", AllText(card));
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Report, Request.ToString("D")), card.Card);

        var plain = SlackThreadPosts.ReportPost(content, McpDataDetail.Full);
        Assert.Null(ActionsBlock(plain));
        Assert.Null(plain.Card);
    }

    [Fact]
    public void TheNoteCard_CarriesApproveAndReject_TheNoteAtTheMode_AndNamesTheNote()
    {
        var full = SlackThreadPosts.NotePost(31, "agent:a", "Index 'ix_a' on dbo.T is unused since 2026-01-01", "srv-7", "cpu", McpDataDetail.Full);
        Assert.Equal([(SlackCardActions.NoteApprove, "31", "Approve note"), (SlackCardActions.NoteReject, "31", "Reject note")], Buttons(full));
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Note, "31"), full.Card);
        Assert.Contains("ix_a", AllText(full));

        var masked = SlackThreadPosts.NotePost(31, "agent:a", "Index 'ix_a' on dbo.T is unused since 2026-01-01", "srv-7", "cpu", McpDataDetail.MaskLiterals);
        Assert.DoesNotContain("ix_a", AllText(masked));
        Assert.Equal(2, Buttons(masked).Count);

        var withheld = SlackThreadPosts.NotePost(31, "agent:a", "Index 'ix_a' on dbo.T is unused since 2026-01-01", "srv-7", "cpu", McpDataDetail.MetadataOnly);
        Assert.DoesNotContain("ix_a", AllText(withheld));
        Assert.DoesNotContain("unused", AllText(withheld));
        Assert.Contains("withheld", AllText(withheld));
        Assert.Equal(2, Buttons(withheld).Count);
    }

    [Fact]
    public void TheConfirmAnswer_CarriesTheConfirmButton_TheLines_AndTheSamePersonLine()
    {
        var value = new SlackConfirmValue(Proposal.ToString("D"), "C1", "1700000000.000200", null).Format();
        var answer = SlackThreadPosts.ConfirmAnswer("These are the facts now.", ["*plan_id:* 5"], SlackCardActions.ProposalApproveConfirm, "Confirm", value, "primary");
        var button = Assert.Single(Buttons(answer));
        Assert.Equal((SlackCardActions.ProposalApproveConfirm, value, "Confirm"), button);
        Assert.Contains("plan_id", AllText(answer));
        Assert.Contains("same person makes both presses", AllText(answer));
        Assert.Null(answer.Card);
    }

    [Fact]
    public void TheDecidedCard_SaysTheOutcomeAndWhoDecided_AndCarriesNoButtons()
    {
        var card = SlackThreadPosts.DecidedCard("Proposed action: force_query_store_plan", "run", "Erik <Darling>", "https://pm.example/#/p?a=1|2");
        Assert.Null(ActionsBlock(card));
        Assert.Contains("*Outcome:* run", AllText(card));
        Assert.Contains("Erik", AllText(card));
        Assert.Contains("%7C2", AllText(card));
        Assert.DoesNotContain("<Darling>", card.Text);
        Assert.Equal("Proposed action: force_query_store_plan: run by Erik &lt;Darling&gt;", card.Text);
    }

    [Fact]
    public void TheConfirmValue_RoundTrips_AndRefusesAValueThatIsNotOne()
    {
        var value = new SlackConfirmValue("4242", "C1", "1700000000.000200", SlackConfirmValue.HashOf("ALTER x"));
        var parsed = SlackConfirmValue.Parse(value.Format());
        Assert.Equal(value, parsed);
        Assert.Equal(16, value.StatementHash!.Length);
        Assert.Null(SlackConfirmValue.Parse(new SlackConfirmValue("1", "C1", "2", null).Format())!.StatementHash);
        Assert.Null(SlackConfirmValue.Parse(null));
        Assert.Null(SlackConfirmValue.Parse("4242"));
        Assert.Null(SlackConfirmValue.Parse("|C1|1.2|"));
        Assert.Null(SlackConfirmValue.Parse("a|b|c|d|e"));
        Assert.NotEqual(SlackConfirmValue.HashOf("ALTER x"), SlackConfirmValue.HashOf("ALTER y"));
    }

    [Fact]
    public void TheActionTable_NamesEachButtonsKind_Step_AndVerdict()
    {
        Assert.Equal(10, SlackCardActions.All.Count);
        Assert.Equal(SlackCardActions.All.Count, SlackCardActions.All.Distinct().Count());
        Assert.All(SlackCardActions.All, a => Assert.NotNull(SlackCardActions.KindOf(a)));
        Assert.Null(SlackCardActions.KindOf("something_else"));
        Assert.True(SlackCardActions.IsConfirm(SlackCardActions.ProposalApproveConfirm));
        Assert.True(SlackCardActions.IsConfirm(SlackCardActions.UndoConfirm));
        Assert.False(SlackCardActions.IsConfirm(SlackCardActions.ProposalApproveFirst));
        Assert.Equal("partly", SlackCardActions.VerdictOf(SlackCardActions.VerdictPartly));
        Assert.Null(SlackCardActions.VerdictOf(SlackCardActions.NoteApprove));
        Assert.Equal(SlackCardKinds.Change, SlackCardActions.KindOf(SlackCardActions.UndoConfirm));
    }

    [Fact]
    public void ThePressParse_ReadsTheFirstAction_ItsValue_AndTheResponseUrl()
    {
        var payload = new JsonObject
        {
            ["type"] = "block_actions",
            ["response_url"] = "https://hooks.slack.com/actions/T1/1/abc",
            ["actions"] = new JsonArray(new JsonObject { ["action_id"] = "undo_1", ["value"] = "4242" }, new JsonObject { ["action_id"] = "other" }),
        };
        var press = SlackPress.Parse(payload)!;
        Assert.Equal("undo_1", press.ActionId);
        Assert.Equal("4242", press.Value);
        Assert.Equal("hooks.slack.com", press.ResponseUrl!.Host);

        Assert.Null(SlackPress.Parse(new JsonObject { ["type"] = "block_actions" }));
        Assert.Null(SlackPress.Parse(new JsonObject { ["actions"] = new JsonArray() }));
        Assert.Null(SlackPress.Parse(new JsonObject { ["actions"] = new JsonArray(new JsonObject { ["value"] = "1" }) })!);
        Assert.Null(SlackPress.Parse(new JsonObject { ["actions"] = new JsonArray(new JsonObject { ["action_id"] = "x" }), ["response_url"] = "not a url" })!.ResponseUrl);
    }

    [Fact]
    public void TheUpdateBody_NamesTheMessage_AndTheResponseBody_IsEphemeralAndKeepsTheCard()
    {
        var update = JsonNode.Parse(SlackBotClient.BuildUpdateBody("C1", "1700000000.000200", "Decided", [new { type = "section" }]))!.AsObject();
        Assert.Equal("C1", update["channel"]!.GetValue<string>());
        Assert.Equal("1700000000.000200", update["ts"]!.GetValue<string>());
        Assert.Equal("Decided", update["text"]!.GetValue<string>());
        Assert.Single(update["blocks"]!.AsArray());
        Assert.Empty(JsonNode.Parse(SlackBotClient.BuildUpdateBody("C1", "1.2", "x", null))!["blocks"]!.AsArray());

        var response = JsonNode.Parse(SlackBotClient.BuildResponseBody("Only you see this", null))!.AsObject();
        Assert.Equal("ephemeral", response["response_type"]!.GetValue<string>());
        Assert.False(response["replace_original"]!.GetValue<bool>());
        Assert.Equal("Only you see this", response["text"]!.GetValue<string>());
        Assert.False(response.ContainsKey("blocks"));
        Assert.DoesNotContain("token", SlackBotClient.BuildResponseBody("x", null), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("https://hooks.slack.com/actions/T1/1/abc", "hooks.slack.com", true)]
    [InlineData("http://hooks.slack.com/actions/T1/1/abc", "hooks.slack.com", false)]
    [InlineData("https://evil.example/actions", "hooks.slack.com", false)]
    [InlineData("https://hooks.slack.com.evil.example/x", "hooks.slack.com", false)]
    [InlineData("http://127.0.0.1:9/respond", "127.0.0.1", true)]
    [InlineData("http://127.0.0.1:9/respond", "hooks.slack.com", false)]
    public void AResponseUrl_IsAnsweredOnlyOnTheAllowedHost(string url, string host, bool allowed)
    {
        Assert.Equal(allowed, SlackBotClient.IsAllowedResponseUrl(new Uri(url), host));
    }

    [Fact]
    public void TheThreadPass_BuildsTheProposalCardWithButtons_ForTheCreatedRowOnly()
    {
        var created = Row("proposal_created", $$"""{"proposal_id":"{{Proposal}}","tool":"force_query_store_plan","state":"pending"}""");
        var card = SlackThreadPass.Content(created, [], McpDataDetail.Full, "https://pm.example");
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Proposal, Proposal.ToString("D")), card.Card);
        Assert.Equal(2, Buttons(card).Count);

        var run = SlackThreadPass.Content(created with { Event = "proposal_run", DetailJson = $$"""{"proposal_id":"{{Proposal}}","tool":"force_query_store_plan","state":"run","by":"person:7"}""" }, [], McpDataDetail.Full, "");
        Assert.Null(run.Card);
        Assert.Null(ActionsBlock(run));
    }

    [Fact]
    public void TheThreadPass_BuildsUndo_OnADoneChangeWithAnUndoPath_AndNotOnAKill()
    {
        var mute = Row("action_done", """{"called_id":4242,"after":{"status":"muted"}}""",
            calledDetail: """{"tool":"mute_alert","kind":"mute","target":{"id":"cpu","database":null}}""");
        var card = SlackThreadPass.Content(mute, [], McpDataDetail.Full, "");
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Change, "4242"), card.Card);
        Assert.Equal(SlackCardActions.UndoFirst, Assert.Single(Buttons(card)).ActionId);

        var kill = Row("action_done", """{"called_id":4243}""", calledDetail: """{"tool":"kill_session","kind":"session_kill","target":{"id":"87"}}""");
        var killCard = SlackThreadPass.Content(kill, [], McpDataDetail.Full, "");
        Assert.Null(killCard.Card);
        Assert.Null(ActionsBlock(killCard));

        var undone = SlackThreadPass.Content(mute with { Event = "action_undone", Actor = "person:7" }, [], McpDataDetail.Full, "");
        Assert.Null(undone.Card);
    }

    [Fact]
    public void TheThreadPass_BuildsTheNoteCard_FromAPendingNoteRow_AndTheReportCardWithVerdicts()
    {
        var note = Row(InvestigationRequestStore.NotePending, """{"note_id":31,"metric":"cpu"}""") with { JournalId = 31, Actor = "agent:a", Reason = "Index ix_a is unused", PostKey = "note:31" };
        var card = SlackThreadPass.Content(note, [], McpDataDetail.Full, "");
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Note, "31"), card.Card);
        Assert.Equal(2, Buttons(card).Count);
        Assert.Contains("ix_a", AllText(card));

        var reported = Row("reported", null) with { Verdict = "plan regression", RootCause = "the plan changed" };
        var report = SlackThreadPass.Content(reported, [], McpDataDetail.Full, "https://pm.example");
        Assert.Equal(new SlackCardFacts(SlackCardKinds.Report, Request.ToString("D")), report.Card);
        Assert.Equal(3, Buttons(report).Count);
    }

    /// <summary>A store with no app-level token is not listening, so a press on its card would reach another store's listener and be dropped:
    /// its cards carry no buttons, name no record, and point at the web page instead. With the token the buttons are there.</summary>
    [Fact]
    public void WithoutAnAppToken_TheCardsCarryNoButtons_AndPointAtTheWebPage()
    {
        var proposal = Row("proposal_created", $$"""{"proposal_id":"{{Proposal}}","tool":"force_query_store_plan","state":"pending"}""");
        var change = Row("action_done", """{"called_id":4242,"after":{"status":"muted"}}""",
            calledDetail: """{"tool":"mute_alert","kind":"mute","target":{"id":"cpu","database":null}}""");
        var report = Row("reported", null) with { Verdict = "plan regression", RootCause = "the plan changed" };
        var note = Row(InvestigationRequestStore.NotePending, """{"note_id":31,"metric":"cpu"}""") with { JournalId = 31, Actor = "agent:a", Reason = "Index ix_a is unused", PostKey = "note:31" };

        foreach (var row in new[] { proposal, change, report, note })
        {
            var silent = SlackThreadPass.Content(row, [], McpDataDetail.Full, "https://pm.example", buttons: false);
            Assert.Null(ActionsBlock(silent));
            Assert.Null(silent.Card);
            Assert.DoesNotContain(SlackCards.PressAgain, AllText(silent));
            Assert.Contains("web", AllText(silent), StringComparison.OrdinalIgnoreCase);

            var listening = SlackThreadPass.Content(row, [], McpDataDetail.Full, "https://pm.example", buttons: true);
            Assert.NotNull(ActionsBlock(listening));
            Assert.NotNull(listening.Card);
        }

        /* The note's text still posts without buttons, so the thread shows what was proposed. */
        Assert.Contains("ix_a", AllText(SlackThreadPass.Content(note, [], McpDataDetail.Full, "", buttons: false)));
    }

    private static SlackThreadJournalRow Row(string eventKind, string? detail, string? calledDetail = null) =>
        new(100, eventKind, new DateTime(2026, 11, 6, 11, 0, 0, DateTimeKind.Utc), "service", null, detail, eventKind + ":100",
            Request, 1, 7, "srv-7", "cpu", "high", "alert", null, calledDetail, 0, null, null, new DateTime(2026, 11, 6, 10, 0, 0, DateTimeKind.Utc));
}
