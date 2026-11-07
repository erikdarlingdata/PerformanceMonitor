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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7658: the press handler over fake payloads, a fake card store, recorded commands and a recording card API, with a fixed clock. Every
/// answer goes to the payload's <c>response_url</c> and nothing is posted in the thread by the handler; a decision edits the card and marks
/// it; a press on a stored card journals the person (or the unmapped identity) and the identity used, while a press refused before its card
/// is found leaves no journal row; a press from another workspace, on a message that is not a stored card, by a person nobody mapped, on a
/// card already decided, or on a record that is no longer pending changes nothing and says so.
/// </summary>
public sealed class SlackInteractionHandlerTests
{
    private const string Team = "T0TEAM";
    private const string User = "U0PERSON";
    private const string Channel = "C0BOT";
    private const string CardTs = "1700000000.000200";
    private static readonly Guid Request = Guid.Parse("76580000-0000-0000-0000-0000000000e1");
    private static readonly Guid Proposal = Guid.Parse("76580000-0000-0000-0000-0000000000a1");
    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);
    private static readonly Uri ResponseUrl = new("https://hooks.slack.com/actions/T0TEAM/1/abc");
    private static readonly SlackPerson Erik = new(7, 70, "Erik", true);

    private sealed class FakeCards : ISlackCardStore
    {
        public Dictionary<string, SlackCard> Cards { get; } = new(StringComparer.Ordinal);
        public List<(string Channel, string Ts, string By)> Decided { get; } = [];

        public FakeCards With(SlackCard card)
        {
            Cards[card.ChannelId + "|" + card.MessageTs] = card;
            return this;
        }

        public Task SaveCardAsync(SlackCard card, CancellationToken cancellationToken) { With(card); return Task.CompletedTask; }

        public Task<SlackCard?> FindCardAsync(string channelId, string messageTs, CancellationToken cancellationToken) =>
            Task.FromResult(Cards.TryGetValue(channelId + "|" + messageTs, out var card) ? card : null);

        public Task<bool> MarkCardDecidedAsync(string channelId, string messageTs, string decidedBy, CancellationToken cancellationToken)
        {
            Decided.Add((channelId, messageTs, decidedBy));
            return Task.FromResult(true);
        }
    }

    private sealed class RecordingApi : ISlackCardApi
    {
        public List<(Uri Url, string Text, string BlocksJson)> Responses { get; } = [];
        public List<(string Channel, string Ts, string Text, string BlocksJson)> Updates { get; } = [];

        public Task<SlackPostResult> UpdateAsync(string channel, string ts, string text, IReadOnlyList<object>? blocks, CancellationToken cancellationToken)
        {
            Updates.Add((channel, ts, text, JsonSerializer.Serialize(blocks ?? [])));
            return Task.FromResult(new SlackPostResult(true, ts, channel, null));
        }

        public Task<SlackPostResult> RespondAsync(Uri responseUrl, string text, IReadOnlyList<object>? blocks, CancellationToken cancellationToken)
        {
            Responses.Add((responseUrl, text, JsonSerializer.Serialize(blocks ?? [])));
            return Task.FromResult(new SlackPostResult(true, null, null, null));
        }

        public string LastResponse => Responses.Count == 0 ? "" : Responses[^1].Text + " " + Responses[^1].BlocksJson;
    }

    /// <summary>The commands recorded: who was asked for, what ran with which approver, and every journal row. Answers are scripted per command.</summary>
    private sealed class FakeCommands : ISlackPressCommands
    {
        public Dictionary<string, SlackPerson> People { get; } = new(StringComparer.Ordinal);
        public HashSet<(long Person, string Capability, int? Server)> Grants { get; } = [];
        public List<(Guid? Request, int? Server, string Actor, string Action, JsonObject Detail)> Journal { get; } = [];
        public List<(string Command, Guid Id, ProposalApprover Approver)> ProposalCalls { get; } = [];
        public List<(long CalledId, string Login, bool Confirm, string? Hash)> UndoCalls { get; } = [];
        public List<(Guid Request, string Verdict, string Author)> Verdicts { get; } = [];
        public List<(long Note, bool Approve, string Actor)> Notes { get; } = [];
        public int GrantChecks { get; private set; }
        public Func<ProposalApprover, ProposalPressResult> FirstPress { get; set; } = _ => new ProposalPressResult("first_pressed", null, "These are the facts now.", new JsonObject { ["plan_id"] = 5, ["statement"] = "EXEC x" });
        public Func<ProposalApprover, ProposalPressResult> Confirm { get; set; } = _ => new ProposalPressResult(ProposalStates.Run, null, "Done. The plan is forced.");
        public Func<ProposalApprover, AgentActionRefusal?> Refuse { get; set; } = _ => null;
        public SlackChangeFacts? Change { get; set; } = new(4242, AgentChangeKinds.Mute, 7, true);
        public string UndoStatement { get; set; } = "Undo the agent's mute: the alert fires again.";
        public bool NoteDecides { get; set; } = true;

        /// <summary>When set, the verdict command throws it, as a store or network failure inside a command would.</summary>
        public Exception? VerdictFails { get; set; }

        public Task<SlackPerson?> FindPersonAsync(string teamId, string userId, CancellationToken ct) =>
            Task.FromResult(People.TryGetValue(teamId + ":" + userId, out var p) ? p : null);

        public Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken ct)
        {
            GrantChecks++;
            return Task.FromResult(Grants.Contains((personId, capability, serverId)) || Grants.Contains((personId, capability, null)));
        }

        public Task JournalPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken ct)
        {
            Journal.Add((requestId, serverId, actor, action, detail));
            return Task.CompletedTask;
        }

        public Task<SlackProposalFacts?> ProposalAsync(Guid proposalId, CancellationToken ct) =>
            Task.FromResult<SlackProposalFacts?>(new SlackProposalFacts(proposalId, Request, "force_query_store_plan", 7, ProposalStates.Pending));

        public Task<ProposalPressResult> ProposalFirstPressAsync(Guid proposalId, ProposalApprover approver, CancellationToken ct)
        {
            ProposalCalls.Add(("first", proposalId, approver));
            return Task.FromResult(FirstPress(approver));
        }

        public Task<ProposalPressResult> ProposalConfirmAsync(Guid proposalId, ProposalApprover approver, CancellationToken ct)
        {
            ProposalCalls.Add(("confirm", proposalId, approver));
            return Task.FromResult(Confirm(approver));
        }

        public Task<AgentActionRefusal?> ProposalRefuseAsync(Guid proposalId, ProposalApprover approver, CancellationToken ct)
        {
            ProposalCalls.Add(("refuse", proposalId, approver));
            return Task.FromResult(Refuse(approver));
        }

        public Task<SlackChangeFacts?> ChangeAsync(long calledId, CancellationToken ct) => Task.FromResult(Change);

        public Task<SlackUndoStep> UndoAsync(long calledId, string login, bool confirm, string? shownHash, CancellationToken ct)
        {
            UndoCalls.Add((calledId, login, confirm, shownHash));
            if (!confirm)
                return Task.FromResult(new SlackUndoStep(SlackUndoStep.Confirm, null, "Confirm to undo this change.", UndoStatement));
            return Task.FromResult(shownHash == SlackConfirmValue.HashOf(UndoStatement)
                ? new SlackUndoStep(SlackUndoStep.Undone, null, "Undone: the mute is back to what it was before the agent's change.", UndoStatement)
                : new SlackUndoStep(SlackUndoStep.Refused, "statement_changed", "The undo statement is not the one you were shown.", UndoStatement));
        }

        public Task<InvestigationFeedbackResult> RecordVerdictAsync(Guid requestId, string verdict, string author, CancellationToken ct)
        {
            if (VerdictFails is not null)
                throw VerdictFails;
            Verdicts.Add((requestId, verdict, author));
            return Task.FromResult(new InvestigationFeedbackResult(InvestigationFeedbackOutcome.Recorded, null));
        }

        public Task<bool> DecideNoteAsync(long noteId, bool approve, string actor, CancellationToken ct)
        {
            Notes.Add((noteId, approve, actor));
            return Task.FromResult(NoteDecides);
        }
    }

    private static SlackInboundInteraction Press(string actionId, string value, string channel = Channel, string ts = CardTs, string user = User,
        string userTeam = Team, string team = Team, Uri? responseUrl = null)
    {
        var payload = new JsonObject
        {
            ["type"] = "block_actions",
            ["api_app_id"] = "A0APP",
            ["team"] = new JsonObject { ["id"] = team, ["domain"] = "example" },
            ["user"] = new JsonObject { ["id"] = user, ["team_id"] = userTeam, ["username"] = "person" },
            ["container"] = new JsonObject { ["type"] = "message", ["channel_id"] = channel, ["message_ts"] = ts },
            ["response_url"] = (responseUrl ?? ResponseUrl).ToString(),
            ["actions"] = new JsonArray(new JsonObject { ["action_id"] = actionId, ["value"] = value }),
        };
        return new SlackInboundInteraction(team, "A0APP", user, userTeam, channel, ts, payload);
    }

    private static SlackCard ProposalCard() => new(Request, "proposal_created:100", Channel, CardTs, SlackCardKinds.Proposal, Proposal.ToString("D"), 7);
    private static SlackCard ChangeCard() => new(Request, "action_done:101", Channel, CardTs, SlackCardKinds.Change, "4242", 7);
    private static SlackCard ReportCard() => new(Request, "reported:102", Channel, CardTs, SlackCardKinds.Report, Request.ToString("D"), 7);
    private static SlackCard NoteCard() => new(Request, "note:31", Channel, CardTs, SlackCardKinds.Note, "31", 7);

    private static (SlackInteractionHandler Handler, FakeCards Cards, FakeCommands Commands, RecordingApi Api) Build(SlackCard? card, bool mapped = true,
        McpDataDetail detail = McpDataDetail.Full, ILogger? logger = null)
    {
        var cards = new FakeCards();
        if (card is not null)
            cards.With(card);
        var commands = new FakeCommands();
        if (mapped)
            commands.People[Team + ":" + User] = Erik;
        var api = new RecordingApi();
        var handler = new SlackInteractionHandler(cards, commands, api, logger ?? NullLogger.Instance, detail, () => "https://pm.example", () => Now);
        return (handler, cards, commands, api);
    }

    private static string ConfirmValueIn(RecordingApi api, string actionId)
    {
        var blocks = JsonNode.Parse(api.Responses[^1].BlocksJson)!.AsArray();
        var button = blocks.OfType<JsonObject>().Where(b => b["type"]?.GetValue<string>() == "actions")
            .SelectMany(b => b["elements"]!.AsArray().OfType<JsonObject>())
            .Single(e => e["action_id"]!.GetValue<string>() == actionId);
        return button["value"]!.GetValue<string>();
    }

    [Fact]
    public async Task Approve_TakesTwoPressesByTheSamePerson_AnswersEachEphemerally_AndEditsTheCardOnTheDecision()
    {
        var (handler, cards, commands, api) = Build(ProposalCard());
        var ct = TestContext.Current.CancellationToken;

        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D")), ct);

        var first = Assert.Single(commands.ProposalCalls);
        Assert.Equal(("first", Proposal), (first.Command, first.Id));
        Assert.True(first.Approver.IsPerson);
        Assert.Equal((7L, 70L, ServerActionSurfaces.Slack), (first.Approver.PersonId!.Value, first.Approver.IdentityId!.Value, first.Approver.Surface));
        var answer = Assert.Single(api.Responses);
        Assert.Equal(ResponseUrl, answer.Url);
        Assert.Contains("These are the facts now", answer.Text);
        Assert.Contains("plan_id", answer.BlocksJson);
        Assert.DoesNotContain("EXEC x", answer.BlocksJson);
        Assert.Empty(api.Updates);
        var confirmValue = ConfirmValueIn(api, SlackCardActions.ProposalApproveConfirm);
        Assert.Equal(new SlackConfirmValue(Proposal.ToString("D"), Channel, CardTs, null), SlackConfirmValue.Parse(confirmValue));

        /* The Confirm sits on the ephemeral answer: its container is not a stored card, and the value names the card. */
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveConfirm, confirmValue, ts: "1700000000.000999"), ct);

        Assert.Equal(2, commands.ProposalCalls.Count);
        Assert.Equal(("confirm", Proposal, "person:7"), (commands.ProposalCalls[1].Command, commands.ProposalCalls[1].Id, commands.ProposalCalls[1].Approver.Key));
        Assert.Equal(2, api.Responses.Count);
        Assert.Contains("Done", api.Responses[1].Text);
        var update = Assert.Single(api.Updates);
        Assert.Equal((Channel, CardTs), (update.Channel, update.Ts));
        Assert.DoesNotContain("\"actions\"", update.BlocksJson);
        Assert.Contains("run", update.Text);
        Assert.Contains("Erik", update.Text);
        Assert.Contains("force_query_store_plan", update.BlocksJson);
        Assert.Equal((Channel, CardTs, "person:7"), Assert.Single(cards.Decided));

        Assert.Equal(2, commands.Journal.Count);
        Assert.All(commands.Journal, row =>
        {
            Assert.Equal(Request, row.Request);
            Assert.Equal(7, row.Server);
            Assert.Equal("person:7", row.Actor);
            Assert.Equal("slack:" + Team + ":" + User, row.Detail["identity"]!.GetValue<string>());
        });
        Assert.Equal([SlackCardActions.ProposalApproveFirst, SlackCardActions.ProposalApproveConfirm], commands.Journal.Select(r => r.Action));
        Assert.Equal(["first_pressed", ProposalStates.Run], commands.Journal.Select(r => r.Detail["outcome"]!.GetValue<string>()));
    }

    [Fact]
    public async Task Refuse_TakesOnePress_EditsTheCard_AndJournalsThePerson()
    {
        var (handler, cards, commands, api) = Build(ProposalCard());
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalRefuse, Proposal.ToString("D")), TestContext.Current.CancellationToken);

        var call = Assert.Single(commands.ProposalCalls);
        Assert.Equal(("refuse", "person:7"), (call.Command, call.Approver.Key));
        Assert.Contains("Refused", Assert.Single(api.Responses).Text);
        var update = Assert.Single(api.Updates);
        Assert.Contains("refused", update.Text);
        Assert.DoesNotContain("\"actions\"", update.BlocksJson);
        Assert.Single(cards.Decided);
        Assert.Equal(ProposalStates.Refused, Assert.Single(commands.Journal).Detail["outcome"]!.GetValue<string>());
    }

    [Fact]
    public async Task AConfirmByAnotherPerson_IsRefusedByTheStore_AndTheCardStays()
    {
        var (handler, cards, commands, api) = Build(ProposalCard());
        commands.People[Team + ":U0OTHER"] = new SlackPerson(8, 80, "Other", true);
        commands.Confirm = approver => approver.Key == "person:7"
            ? new ProposalPressResult(ProposalStates.Run, null, "Done.")
            : new ProposalPressResult("refused", InvestigationRequestStore.ProposalPressCodes.OtherApprover, "Another person made the first press.");
        var value = new SlackConfirmValue(Proposal.ToString("D"), Channel, CardTs, null).Format();

        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveConfirm, value, ts: "1700000000.000999", user: "U0OTHER"), TestContext.Current.CancellationToken);

        Assert.Equal("person:8", Assert.Single(commands.ProposalCalls).Approver.Key);
        Assert.Contains("Another person", Assert.Single(api.Responses).Text);
        Assert.Empty(api.Updates);
        Assert.Empty(cards.Decided);
        var row = Assert.Single(commands.Journal);
        Assert.Equal(("person:8", "refused", InvestigationRequestStore.ProposalPressCodes.OtherApprover),
            (row.Actor, row.Detail["outcome"]!.GetValue<string>(), row.Detail["code"]!.GetValue<string>()));
    }

    [Fact]
    public async Task APressOnARecordThatIsNoLongerPending_DoesNothing_AndSaysSo()
    {
        var (handler, cards, commands, api) = Build(ProposalCard());
        commands.FirstPress = _ => new ProposalPressResult("refused", InvestigationRequestStore.ProposalPressCodes.NotPending, "This proposal is run, not waiting for a person. Nothing was run.");
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D")), TestContext.Current.CancellationToken);

        Assert.Contains("Nothing was run", Assert.Single(api.Responses).Text);
        Assert.DoesNotContain("\"actions\"", api.Responses[0].BlocksJson);
        Assert.Empty(api.Updates);
        Assert.Empty(cards.Decided);
        Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotPending, Assert.Single(commands.Journal).Detail["code"]!.GetValue<string>());

        var (noteHandler, noteCards, noteCommands, noteApi) = Build(NoteCard());
        noteCommands.Grants.Add((7, PersonCapabilities.DecideNote, null));
        noteCommands.NoteDecides = false;
        await noteHandler.HandleInteractiveAsync(Press(SlackCardActions.NoteApprove, "31"), TestContext.Current.CancellationToken);
        Assert.Contains("no longer pending", Assert.Single(noteApi.Responses).Text);
        Assert.Empty(noteApi.Updates);
        Assert.Empty(noteCards.Decided);
    }

    [Fact]
    public async Task APressFromAnotherWorkspace_IsRefused_BeforeAnyCommandOrLookup()
    {
        var (handler, cards, commands, api) = Build(ProposalCard());
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D"), userTeam: "T0OTHER"), TestContext.Current.CancellationToken);

        Assert.Equal(SlackInteractionHandler.OtherWorkspace, Assert.Single(api.Responses).Text);
        Assert.Empty(commands.ProposalCalls);
        Assert.Empty(commands.Journal);
        Assert.Empty(api.Updates);
        Assert.Empty(cards.Decided);
    }

    [Fact]
    public async Task APressOnAMessageThisStoreDidNotPost_OrOnTheWrongKindOfCard_IsRefused_AndNothingRuns()
    {
        var (handler, _, commands, api) = Build(ProposalCard());
        var ct = TestContext.Current.CancellationToken;

        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D"), ts: "1700000000.000300"), ct);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D"), channel: "C0OTHER"), ct);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.VerdictRight, Request.ToString("D")), ct);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Guid.NewGuid().ToString("D")), ct);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveConfirm, new SlackConfirmValue(Proposal.ToString("D"), Channel, "1700000000.000300", null).Format(), ts: "9.9"), ct);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveConfirm, "not a confirm value", ts: "9.9"), ct);

        Assert.Equal(6, api.Responses.Count);
        Assert.All(api.Responses.Take(5), r => Assert.Equal(SlackInteractionHandler.NotOurCard, r.Text));
        Assert.Contains("could not be read", api.Responses[5].Text);
        Assert.Empty(commands.ProposalCalls);
        Assert.Empty(commands.Verdicts);
        Assert.Empty(commands.Journal);
        Assert.Empty(api.Updates);
    }

    [Fact]
    public async Task APressByAPersonNobodyMapped_OrADisabledPerson_IsRefused_AndJournaledWithTheIdentity()
    {
        var (handler, _, commands, api) = Build(ProposalCard(), mapped: false);
        var ct = TestContext.Current.CancellationToken;
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D")), ct);

        Assert.Equal(SlackInteractionHandler.NotMapped, Assert.Single(api.Responses).Text);
        Assert.Empty(commands.ProposalCalls);
        var row = Assert.Single(commands.Journal);
        Assert.Equal(("slack:" + Team + ":" + User, "unmapped"), (row.Actor, row.Detail["outcome"]!.GetValue<string>()));
        Assert.Equal(Request, row.Request);

        commands.People[Team + ":" + User] = Erik with { Enabled = false };
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D")), ct);
        Assert.Equal(SlackInteractionHandler.Disabled, api.Responses[1].Text);
        Assert.Empty(commands.ProposalCalls);
        Assert.Equal(("person:7", "disabled"), (commands.Journal[1].Actor, commands.Journal[1].Detail["outcome"]!.GetValue<string>()));
    }

    [Fact]
    public async Task APressOnACardAlreadyDecided_IsRefusedWithWhoDecidedAndWhen_RunsNothing_AndIsJournaled()
    {
        var decidedAt = new DateTime(2026, 11, 6, 11, 30, 0, DateTimeKind.Utc);
        var (handler, cards, commands, api) = Build(ReportCard() with { State = SlackCardStates.Decided, DecidedBy = "person:9", DecidedAtUtc = decidedAt });
        var ct = TestContext.Current.CancellationToken;
        commands.Grants.Add((7, PersonCapabilities.Feedback, null));
        commands.Grants.Add((7, PersonCapabilities.Undo, null));

        /* A decided report card keeps its buttons only when its edit failed; a verdict pressed there would record again. */
        await handler.HandleInteractiveAsync(Press(SlackCardActions.VerdictRight, Request.ToString("D")), ct);

        var answer = Assert.Single(api.Responses);
        Assert.Contains("already decided", answer.Text);
        Assert.Contains("person:9", answer.Text);
        Assert.Contains("2026-11-06 11:30", answer.Text);
        Assert.Empty(commands.Verdicts);
        Assert.Equal(0, commands.GrantChecks);
        Assert.Empty(api.Updates);
        Assert.Empty(cards.Decided);
        var row = Assert.Single(commands.Journal);
        Assert.Equal((Request, 7, "person:7", SlackCardActions.VerdictRight, "decided"), (row.Request, row.Server, row.Actor, row.Action, row.Detail["outcome"]!.GetValue<string>()));
        Assert.Equal("slack:" + Team + ":" + User, row.Detail["identity"]!.GetValue<string>());

        /* The confirming press of an undo on a decided change card runs nothing either, and the person who decided it reads as "you". */
        const string changeTs = "1700000000.000201";
        cards.With(ChangeCard() with { MessageTs = changeTs, State = SlackCardStates.Decided, DecidedBy = "person:7", DecidedAtUtc = decidedAt });
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoConfirm, new SlackConfirmValue("4242", Channel, changeTs, "abc").Format(), ts: "9.9"), ct);

        Assert.Contains("already decided by you", api.Responses[1].Text);
        Assert.Empty(commands.UndoCalls);
        Assert.Equal(0, commands.GrantChecks);
        Assert.Empty(api.Updates);
        Assert.Equal(2, commands.Journal.Count);
        Assert.Equal((SlackCardActions.UndoConfirm, "decided"), (commands.Journal[1].Action, commands.Journal[1].Detail["outcome"]!.GetValue<string>()));
    }

    [Fact]
    public async Task AFailedPress_IsAnsweredInPlainWordsWithoutTheExceptionText_AndTheLogCarriesTheExceptionWithTheRequestAndTheCard()
    {
        var log = new CapturingTestLogger();
        var (handler, cards, commands, api) = Build(ReportCard(), logger: log);
        var ct = TestContext.Current.CancellationToken;
        commands.Grants.Add((7, PersonCapabilities.Feedback, null));
        commands.VerdictFails = new InvalidOperationException("relation \"config.investigation_feedback\" does not exist; constraint fk_feedback_request");

        await handler.HandleInteractiveAsync(Press(SlackCardActions.VerdictRight, Request.ToString("D")), ct);

        var answer = Assert.Single(api.Responses);
        Assert.Contains("could not be completed", answer.Text);
        Assert.Contains("service log", answer.Text);
        Assert.Contains(SlackCards.PressAgain, answer.Text);
        Assert.DoesNotContain("investigation_feedback", answer.Text + answer.BlocksJson);
        Assert.DoesNotContain("fk_feedback_request", answer.Text + answer.BlocksJson);
        Assert.Empty(commands.Verdicts);
        Assert.Empty(api.Updates);
        Assert.Empty(cards.Decided);

        Assert.Same(commands.VerdictFails, Assert.Single(log.ExceptionsAtLevel(LogLevel.Warning)));
        var line = Assert.Single(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains(SlackCardActions.VerdictRight, line);
        Assert.Contains(Request.ToString("D"), line);
        Assert.Contains(Channel, line);
        Assert.Contains(CardTs, line);
        Assert.Contains("person:7", line);
    }

    [Fact]
    public async Task AVerdict_NeedsTheFeedbackGrant_RecordsThePersonsVerdict_AndEditsTheCard()
    {
        var (handler, cards, commands, api) = Build(ReportCard());
        var ct = TestContext.Current.CancellationToken;

        await handler.HandleInteractiveAsync(Press(SlackCardActions.VerdictPartly, Request.ToString("D")), ct);
        Assert.Contains("feedback grant", Assert.Single(api.Responses).Text);
        Assert.Empty(commands.Verdicts);
        Assert.Equal(("no_grant", PersonCapabilities.Feedback), (commands.Journal[0].Detail["outcome"]!.GetValue<string>(), commands.Journal[0].Detail["code"]!.GetValue<string>()));

        commands.Grants.Add((7, PersonCapabilities.Feedback, 7));
        await handler.HandleInteractiveAsync(Press(SlackCardActions.VerdictPartly, Request.ToString("D")), ct);
        Assert.Equal((Request, "partly", "person:7"), Assert.Single(commands.Verdicts));
        Assert.Contains("Verdict recorded: partly", api.Responses[1].Text);
        var update = Assert.Single(api.Updates);
        Assert.Contains("verdict partly", update.Text);
        Assert.DoesNotContain("\"actions\"", update.BlocksJson);
        Assert.Single(cards.Decided);
    }

    [Fact]
    public async Task Undo_ShowsTheStatementFirst_ChecksTheGrantAtTheConfirm_RunsOnlyTheShownStatement_AndEditsTheCard()
    {
        var (handler, cards, commands, api) = Build(ChangeCard());
        var ct = TestContext.Current.CancellationToken;

        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoFirst, "4242"), ct);
        Assert.Equal((4242L, "7", false, (string?)null), Assert.Single(commands.UndoCalls));
        Assert.Equal(0, commands.GrantChecks);
        Assert.Contains("mute", api.Responses[0].BlocksJson);
        var confirmValue = ConfirmValueIn(api, SlackCardActions.UndoConfirm);
        Assert.Equal(SlackConfirmValue.HashOf(commands.UndoStatement), SlackConfirmValue.Parse(confirmValue)!.StatementHash);
        Assert.Empty(api.Updates);

        /* Without the grant the confirming press runs nothing. */
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoConfirm, confirmValue, ts: "9.9"), ct);
        Assert.Equal(1, commands.GrantChecks);
        Assert.Single(commands.UndoCalls);
        Assert.Contains("undo grant", api.Responses[1].Text);
        Assert.Empty(api.Updates);

        commands.Grants.Add((7, PersonCapabilities.Undo, 7));
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoConfirm, confirmValue, ts: "9.9"), ct);
        Assert.Equal(2, commands.UndoCalls.Count);
        Assert.Equal((4242L, "7", true, SlackConfirmValue.HashOf(commands.UndoStatement)), commands.UndoCalls[1]);
        Assert.Contains("Undone", api.Responses[2].Text);
        var update = Assert.Single(api.Updates);
        Assert.Contains("undone", update.Text);
        Assert.DoesNotContain("\"actions\"", update.BlocksJson);
        Assert.Equal((Channel, CardTs, "person:7"), Assert.Single(cards.Decided));
        Assert.Equal(["confirm", "no_grant", "undone"], commands.Journal.Select(r => r.Detail["outcome"]!.GetValue<string>()));
    }

    [Fact]
    public async Task Undo_OnAKill_OrOnAChangeAlreadyUndone_RunsNothing()
    {
        var (handler, _, commands, api) = Build(ChangeCard());
        var ct = TestContext.Current.CancellationToken;
        commands.Grants.Add((7, PersonCapabilities.Undo, null));

        commands.Change = new SlackChangeFacts(4242, AgentChangeKinds.SessionKill, 7, true);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoFirst, "4242"), ct);
        Assert.Contains("no undo", api.Responses[0].Text);

        commands.Change = new SlackChangeFacts(4242, AgentChangeKinds.Mute, 7, false);
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoFirst, "4242"), ct);
        Assert.Contains("already undone", api.Responses[1].Text);

        commands.Change = null;
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoConfirm, new SlackConfirmValue("4242", Channel, CardTs, "abc").Format(), ts: "9.9"), ct);
        Assert.Contains("no undo", api.Responses[2].Text);

        Assert.Empty(commands.UndoCalls);
        Assert.Empty(api.Updates);
        Assert.Equal(["not_undoable", "not_pending", "not_undoable"], commands.Journal.Select(r => r.Detail["outcome"]!.GetValue<string>()));
    }

    [Fact]
    public async Task ANoteDecision_NeedsTheDecideNoteGrant_DecidesAsThePerson_AndEditsTheCard()
    {
        var (handler, cards, commands, api) = Build(NoteCard());
        var ct = TestContext.Current.CancellationToken;

        await handler.HandleInteractiveAsync(Press(SlackCardActions.NoteReject, "31"), ct);
        Assert.Contains("decide_note grant", api.Responses[0].Text);
        Assert.Empty(commands.Notes);

        commands.Grants.Add((7, PersonCapabilities.DecideNote, 7));
        await handler.HandleInteractiveAsync(Press(SlackCardActions.NoteReject, "31"), ct);
        Assert.Equal((31L, false, "person:7"), Assert.Single(commands.Notes));
        Assert.Contains("rejected", api.Responses[1].Text);
        Assert.Contains("rejected", Assert.Single(api.Updates).Text);

        await handler.HandleInteractiveAsync(Press(SlackCardActions.NoteApprove, "31"), ct);
        Assert.Equal((31L, true, "person:7"), commands.Notes[1]);
        Assert.Contains("future briefs", api.Responses[2].Text);
        Assert.Equal(2, cards.Decided.Count);
    }

    [Fact]
    public async Task EveryAnswerIsEphemeral_ThroughTheResponseUrl_AndAPressWithoutOneIsStillActedOn()
    {
        var (handler, _, commands, api) = Build(ProposalCard());
        var ct = TestContext.Current.CancellationToken;
        await handler.HandleInteractiveAsync(Press(SlackCardActions.ProposalApproveFirst, Proposal.ToString("D"), responseUrl: new Uri("https://hooks.slack.com/actions/T0TEAM/2/def")), ct);
        Assert.Equal("https://hooks.slack.com/actions/T0TEAM/2/def", Assert.Single(api.Responses).Url.ToString());

        var press = Press(SlackCardActions.ProposalRefuse, Proposal.ToString("D"));
        press.Payload.Remove("response_url");
        await handler.HandleInteractiveAsync(press, ct);
        Assert.Single(api.Responses);
        Assert.Equal(2, commands.ProposalCalls.Count);
        Assert.Equal("refuse", commands.ProposalCalls[1].Command);
        Assert.Single(api.Updates);
    }

    [Fact]
    public async Task AnActionTheHandlerDoesNotKnow_AndAnEvent_AreIgnored()
    {
        var (handler, _, commands, api) = Build(ProposalCard());
        var ct = TestContext.Current.CancellationToken;
        await handler.HandleInteractiveAsync(Press("prototype_button", "1"), ct);
        await handler.HandleEventAsync(new SlackInboundEvent(Team, "A0APP", "message", Channel, "1.1", null, User, new JsonObject(), new JsonObject()), ct);
        Assert.Empty(api.Responses);
        Assert.Empty(api.Updates);
        Assert.Empty(commands.Journal);
        Assert.Empty(commands.ProposalCalls);
    }

    [Fact]
    public async Task AtMetadataOnly_TheUndoAnswerWithholdsTheStatement_AndTheFactsLeaveOutTheStatement()
    {
        var (handler, _, commands, api) = Build(ChangeCard(), detail: McpDataDetail.MetadataOnly);
        commands.Change = new SlackChangeFacts(4242, AgentChangeKinds.PlanForce, 7, true);
        commands.UndoStatement = "EXEC sp_query_store_unforce_plan @query_id = 9, @plan_id = 5;";
        await handler.HandleInteractiveAsync(Press(SlackCardActions.UndoFirst, "4242"), TestContext.Current.CancellationToken);
        Assert.DoesNotContain("sp_query_store_unforce_plan", api.Responses[0].BlocksJson);
        Assert.Contains("withheld", api.Responses[0].BlocksJson);
        Assert.Equal(SlackConfirmValue.HashOf(commands.UndoStatement), SlackConfirmValue.Parse(ConfirmValueIn(api, SlackCardActions.UndoConfirm))!.StatementHash);
    }
}
