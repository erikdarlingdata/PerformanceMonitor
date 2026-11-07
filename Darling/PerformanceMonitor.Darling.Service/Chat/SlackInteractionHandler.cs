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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7658: one step of a person's undo: <c>confirm</c> (the statement, nothing written), <c>undone</c>, <c>refused</c> or <c>failed</c>.</summary>
public sealed record SlackUndoStep(string Status, string? Code, string Message, string? Statement = null)
{
    public const string Confirm = "confirm";
    public const string Undone = "undone";
    public const string Refused = "refused";
    public const string Failed = "failed";
}

/// <summary>#7658: what a proposal card says about its record when the card is edited: the tool and the request.</summary>
public sealed record SlackProposalFacts(Guid ProposalId, Guid RequestId, string Tool, int ServerId, string State);

/// <summary>#7658: what a change card says about its record: the kind, the server, whether an undo can still be tried now.</summary>
public sealed record SlackChangeFacts(long CalledId, string? Kind, int? ServerId, bool CanTryUndo);

/// <summary>
/// #7658: the commands a press runs, behind one seam. The product binds them to the store, the proposal press, the agent change undo and the
/// feedback store on the owner connection; a test stands a recorder in. Every grant is checked by the command that writes, in its own
/// transaction: the proposal press checks the approver, the undo and the note and verdict commands take the person's grant from
/// <see cref="HasGrantAsync"/> at the confirming press.
/// </summary>
public interface ISlackPressCommands
{
    /// <summary>The person mapped to a Slack identity, or null.</summary>
    Task<SlackPerson?> FindPersonAsync(string teamId, string userId, CancellationToken cancellationToken);

    /// <summary>Whether the person holds <paramref name="capability"/> for the server (or the fleet, with no server).</summary>
    Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken);

    /// <summary>The journal row of one press.</summary>
    Task JournalPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken);

    /// <summary>The proposal, or null.</summary>
    Task<SlackProposalFacts?> ProposalAsync(Guid proposalId, CancellationToken cancellationToken);

    /// <summary>The first press of a proposal.</summary>
    Task<ProposalPressResult> ProposalFirstPressAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken);

    /// <summary>The confirming press of a proposal.</summary>
    Task<ProposalPressResult> ProposalConfirmAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken);

    /// <summary>A person's refusal of a pending proposal: null when refused, else why the press was not taken.</summary>
    Task<AgentActionRefusal?> ProposalRefuseAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken);

    /// <summary>The change an <c>action_called</c> row made, or null.</summary>
    Task<SlackChangeFacts?> ChangeAsync(long calledId, CancellationToken cancellationToken);

    /// <summary>A person's undo of a change: without <paramref name="confirm"/> the statement alone; with it, and with <paramref name="shownHash"/> the hash
    /// of the statement the person was shown, the undo itself.</summary>
    Task<SlackUndoStep> UndoAsync(long calledId, string login, bool confirm, string? shownHash, CancellationToken cancellationToken);

    /// <summary>Records a person's verdict on a report.</summary>
    Task<InvestigationFeedbackResult> RecordVerdictAsync(Guid requestId, string verdict, string author, CancellationToken cancellationToken);

    /// <summary>Approves or rejects a pending note. False when the note is gone or already decided.</summary>
    Task<bool> DecideNoteAsync(long noteId, bool approve, string actor, CancellationToken cancellationToken);
}

/// <summary>
/// #7658: the buttons. One press reaches here after the listener's filters (the workspace, the app, the user's workspace, the channel). The
/// handler then checks the card: the press names a message by channel and ts, and the service acts only on a card it posted and stored, whose
/// kind and record match the button. The presser is <c>user.id</c> in <c>user.team_id</c>, mapped to a person through the people directory; a
/// person nobody mapped, or a disabled one, is refused. Every press is answered where only the presser sees it, through the payload's
/// <c>response_url</c>: the facts and a Confirm button on a first press, the outcome on a confirming press, and every refusal. A decision
/// edits the card in place: the buttons go, and the card says how it ended and who decided; the card row is marked decided whether or not
/// the edit succeeded, and a later press on a decided card is refused with who decided it and when. A press on a stored card writes a
/// journal row with the person (or the unmapped identity) and the identity used, a press on a decided card included; a press refused before
/// its card is found (another workspace, a Confirm value that cannot be read, a message this store did not post) is answered and leaves no
/// journal row. A press whose record is no longer pending changes nothing and says so. A command that throws is answered in fixed words,
/// never with the exception's text, and the log carries the exception with the request and the card.
/// </summary>
public sealed class SlackInteractionHandler : ISlackEventHandlers
{
    private readonly ISlackCardStore _cards;
    private readonly ISlackPressCommands _commands;
    private readonly ISlackCardApi _api;
    private readonly ILogger _logger;
    private readonly McpDataDetail _dataDetail;
    private readonly Func<string> _baseUrl;
    private readonly Func<DateTime> _utcNow;

    public SlackInteractionHandler(ISlackCardStore cards, ISlackPressCommands commands, ISlackCardApi api, ILogger logger, McpDataDetail dataDetail,
        Func<string>? baseUrl = null, Func<DateTime>? utcNow = null)
    {
        _cards = cards ?? throw new ArgumentNullException(nameof(cards));
        _commands = commands ?? throw new ArgumentNullException(nameof(commands));
        _api = api ?? throw new ArgumentNullException(nameof(api));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dataDetail = dataDetail;
        _baseUrl = baseUrl ?? (static () => "");
        _utcNow = utcNow ?? (static () => DateTime.UtcNow);
    }

    /// <summary>The refusal a press from another workspace gets. The listener drops such a press first; this is the handler's own check.</summary>
    public const string OtherWorkspace = "This store acts only on presses from its own workspace.";

    /// <summary>The refusal a press on a message this store did not post gets.</summary>
    public const string NotOurCard = "This button is not on a card this store posted, so nothing was done.";

    /// <summary>The refusal a presser nobody mapped gets.</summary>
    public const string NotMapped = "Your Slack account is not mapped to a person in this store, so nothing was done. An administrator can map it on the People page.";

    /// <summary>The refusal a disabled person gets.</summary>
    public const string Disabled = "Your person is disabled in this store, so nothing was done.";

    /// <summary>The answer when a command throws: fixed words, never the exception's text, which goes to the service log with the request and the card.</summary>
    public const string PressFailed = "The press could not be completed. The service log has the detail.";

    /// <summary>The refusal a press on a decided card gets: who decided it and when. A decided card keeps its buttons only when its edit failed.</summary>
    public static string AlreadyDecided(SlackCard card, SlackPerson presser)
    {
        var who = string.IsNullOrEmpty(card.DecidedBy) ? "someone" : string.Equals(card.DecidedBy, PersonActor(presser), StringComparison.Ordinal) ? "you" : card.DecidedBy;
        var when = card.DecidedAtUtc is { } at ? at.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture) : "an earlier press";
        return $"This card was already decided by {who} at {when}, so nothing was done.";
    }

    /// <summary>Mentions are not this handler's: the chat requests (#7659) take events.</summary>
    public Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public async Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(inbound);
        var press = SlackPress.Parse(inbound.Payload);
        if (press is null || SlackCardActions.KindOf(press.ActionId) is not { } kind)
        {
            _logger.LogDebug("Slack press ignored: no action the handler knows");
            return;
        }

        if (!string.Equals(inbound.UserTeamId, inbound.TeamId, StringComparison.Ordinal))
        {
            await RespondAsync(press, OtherWorkspace, cancellationToken);
            return;
        }

        /* The card: a first press names it by its container; a confirming press by its value, since the Confirm sits on the ephemeral answer. */
        string channel = inbound.Channel, ts = inbound.MessageTs, recordId = press.Value ?? "";
        string? shownHash = null;
        if (SlackCardActions.IsConfirm(press.ActionId))
        {
            if (SlackConfirmValue.Parse(press.Value) is not { } confirm)
            {
                _logger.LogInformation("Slack press on {Action} refused: its confirmation value could not be read", press.ActionId);
                await RespondAsync(press, "This confirmation could not be read. Press the card's button again.", cancellationToken);
                return;
            }

            (recordId, channel, ts, shownHash) = (confirm.RecordId, confirm.Channel, confirm.MessageTs, confirm.StatementHash);
        }

        var card = await _cards.FindCardAsync(channel, ts, cancellationToken);
        if (card is null || !string.Equals(card.Kind, kind, StringComparison.Ordinal) || !string.Equals(card.RecordId, recordId, StringComparison.Ordinal))
        {
            _logger.LogInformation("Slack press on {Action} refused: the message is not a stored card of that record", press.ActionId);
            await RespondAsync(press, NotOurCard, cancellationToken);
            return;
        }

        var identity = "slack:" + inbound.TeamId + ":" + inbound.UserId;
        var person = await _commands.FindPersonAsync(inbound.TeamId, inbound.UserId, cancellationToken);
        if (person is null || !person.Enabled)
        {
            await JournalAsync(card, person is null ? identity : PersonActor(person), identity, press, person is null ? "unmapped" : "disabled", null, cancellationToken);
            await RespondAsync(press, person is null ? NotMapped : Disabled, cancellationToken);
            return;
        }

        /* A decided card: a press ended it, and its buttons stayed only because the card's edit failed. Nothing runs, and the answer says who decided it and when. */
        if (string.Equals(card.State, SlackCardStates.Decided, StringComparison.Ordinal))
        {
            await JournalAsync(card, PersonActor(person), identity, press, SlackCardStates.Decided, null, cancellationToken);
            await RespondAsync(press, AlreadyDecided(card, person), cancellationToken);
            return;
        }

        var context = new PressContext(inbound, press, card, person, identity);
        try
        {
            switch (kind)
            {
                case SlackCardKinds.Proposal:
                    await ProposalAsync(context, cancellationToken);
                    break;
                case SlackCardKinds.Change:
                    await UndoAsync(context, cancellationToken);
                    break;
                case SlackCardKinds.Report:
                    await VerdictAsync(context, cancellationToken);
                    break;
                case SlackCardKinds.Note:
                    await NoteAsync(context, cancellationToken);
                    break;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Slack press {Action} by {Actor} on {Kind} {Record} of request {RequestId} (server {ServerId}, card {Channel}/{Ts}) failed",
                press.ActionId, context.Actor, card.Kind, card.RecordId, card.RequestId, card.ServerId, card.ChannelId, card.MessageTs);
            await RespondAsync(press, PressFailed + " " + SlackCards.PressAgain, cancellationToken);
        }
    }

    private sealed record PressContext(SlackInboundInteraction Inbound, SlackPress Press, SlackCard Card, SlackPerson Person, string Identity)
    {
        public string Actor => PersonActor(Person);
        public string Login => Person.PersonId.ToString(CultureInfo.InvariantCulture);
        public ProposalApprover Approver => ProposalApprover.Person(Person.PersonId, Person.IdentityId, ServerActionSurfaces.Slack);

        /// <summary>The statement hash a Confirm carried, or null on a first press.</summary>
        public string? ShownHash => SlackConfirmValue.Parse(Press.Value)?.StatementHash;
    }

    /// <summary>The journal actor of a mapped person: <c>person:&lt;id&gt;</c>, the id and not the name, as the proposal press records it.</summary>
    public static string PersonActor(SlackPerson person) => "person:" + person.PersonId.ToString(CultureInfo.InvariantCulture);

    private async Task ProposalAsync(PressContext c, CancellationToken ct)
    {
        if (!Guid.TryParse(c.Card.RecordId, out var proposalId))
        {
            await RespondAsync(c.Press, NotOurCard, ct);
            return;
        }

        switch (c.Press.ActionId)
        {
            case SlackCardActions.ProposalApproveFirst:
                {
                    var result = await _commands.ProposalFirstPressAsync(proposalId, c.Approver, ct);
                    await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, result.Status, result.Code, ct);
                    if (result.Status == "first_pressed")
                    {
                        var confirm = new SlackConfirmValue(c.Card.RecordId, c.Card.ChannelId, c.Card.MessageTs, null).Format();
                        var answer = SlackThreadPosts.ConfirmAnswer(result.Message, FactsLines(result.Facts), SlackCardActions.ProposalApproveConfirm, "Confirm", confirm, "primary");
                        await RespondAsync(c.Press, answer, ct);
                    }
                    else
                    {
                        await RespondAsync(c.Press, result.Message, ct);
                        if (result.Status == "superseded")
                        {
                            await EditProposalCardAsync(c, proposalId, "superseded", ct);
                        }
                    }

                    break;
                }

            case SlackCardActions.ProposalApproveConfirm:
                {
                    var result = await _commands.ProposalConfirmAsync(proposalId, c.Approver, ct);
                    await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, result.Status, result.Code, ct);
                    await RespondAsync(c.Press, result.Message, ct);
                    if (result.Status is ProposalStates.Run or ProposalStates.Failed or ProposalStates.Unknown or ProposalStates.Superseded
                        || (result.Status == ProposalStates.Refused && result.Code is not null && IsFinalRefusal(result.Code)))
                    {
                        await EditProposalCardAsync(c, proposalId, result.Status, ct);
                    }

                    break;
                }

            case SlackCardActions.ProposalRefuse:
                {
                    var refusal = await _commands.ProposalRefuseAsync(proposalId, c.Approver, ct);
                    await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, refusal is null ? ProposalStates.Refused : "not_taken", refusal?.Reason, ct);
                    if (refusal is null)
                    {
                        await RespondAsync(c.Press, "Refused. The agent's proposed action will not run.", ct);
                        await EditProposalCardAsync(c, proposalId, ProposalStates.Refused, ct);
                    }
                    else
                    {
                        await RespondAsync(c.Press, refusal.Message, ct);
                    }

                    break;
                }
        }
    }

    /* The write's own re-check ends the proposal refused; a press refusal (the grant, the person, the state) leaves it pending. */
    private static bool IsFinalRefusal(string code) => code is not (
        InvestigationRequestStore.ProposalPressCodes.NotApprover or InvestigationRequestStore.ProposalPressCodes.AskerCannotApprove
        or InvestigationRequestStore.ProposalPressCodes.AdministratorNeedsSinglePerson or InvestigationRequestStore.ProposalPressCodes.NoFirstPress
        or InvestigationRequestStore.ProposalPressCodes.OtherApprover or InvestigationRequestStore.ProposalPressCodes.NotPending
        or InvestigationRequestStore.ProposalPressCodes.Expired or InvestigationRequestStore.ProposalPressCodes.NotFound
        or InvestigationRequestStore.ProposalPressCodes.AlreadyRun or ActionProposalPress.NotSupported);

    private async Task EditProposalCardAsync(PressContext c, Guid proposalId, string state, CancellationToken ct)
    {
        var facts = await _commands.ProposalAsync(proposalId, ct);
        var headline = "Proposed action: " + (facts?.Tool ?? "action");
        var link = ProposalNotice.LinkFor(_baseUrl(), proposalId);
        await EditCardAsync(c, headline, state, link, ct);
    }

    private async Task UndoAsync(PressContext c, CancellationToken ct)
    {
        if (!long.TryParse(c.Card.RecordId, NumberStyles.None, CultureInfo.InvariantCulture, out var calledId))
        {
            await RespondAsync(c.Press, NotOurCard, ct);
            return;
        }

        var change = await _commands.ChangeAsync(calledId, ct);
        if (change is null || !SlackCards.HasUndo(change.Kind))
        {
            await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, "not_undoable", null, ct);
            await RespondAsync(c.Press, "This change has no undo, so nothing was done.", ct);
            return;
        }

        if (!change.CanTryUndo)
        {
            await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, "not_pending", null, ct);
            await RespondAsync(c.Press, "This change was already undone or is too old to undo, so nothing was done.", ct);
            return;
        }

        if (c.Press.ActionId == SlackCardActions.UndoFirst)
        {
            var step = await _commands.UndoAsync(calledId, c.Login, confirm: false, null, ct);
            await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, step.Status, step.Code, ct);
            if (step.Status == SlackUndoStep.Confirm && step.Statement is not null)
            {
                var hash = SlackConfirmValue.HashOf(step.Statement);
                var confirm = new SlackConfirmValue(c.Card.RecordId, c.Card.ChannelId, c.Card.MessageTs, hash).Format();
                var statementLines = _dataDetail == McpDataDetail.MetadataOnly
                    ? new List<string> { "_The statement is withheld at this store's data detail setting._" }
                    : new List<string> { "```" + SlackThreadPosts.Bounded(_dataDetail == McpDataDetail.MaskLiterals ? McpDataDetailMasker.MaskLiterals(step.Statement) : step.Statement, 1500) + "```" };
                await RespondAsync(c.Press, SlackThreadPosts.ConfirmAnswer(step.Message, statementLines, SlackCardActions.UndoConfirm, "Confirm undo", confirm, "danger"), ct);
            }
            else
            {
                await RespondAsync(c.Press, step.Message, ct);
            }

            return;
        }

        /* The confirming press: the grant is checked here, once, before the undo runs. */
        if (!await _commands.HasGrantAsync(c.Person.PersonId, PersonCapabilities.Undo, change.ServerId, ct))
        {
            await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, "no_grant", PersonCapabilities.Undo, ct);
            await RespondAsync(c.Press, "You do not hold the undo grant for this server, so nothing was done.", ct);
            return;
        }

        var undone = await _commands.UndoAsync(calledId, c.Login, confirm: true, c.ShownHash, ct);
        await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, undone.Status, undone.Code, ct);
        await RespondAsync(c.Press, undone.Message, ct);
        if (undone.Status == SlackUndoStep.Undone)
        {
            await EditCardAsync(c, "Agent change", "undone", null, ct);
        }
    }

    private async Task VerdictAsync(PressContext c, CancellationToken ct)
    {
        var verdict = SlackCardActions.VerdictOf(c.Press.ActionId)!;
        if (!Guid.TryParse(c.Card.RecordId, out var requestId))
        {
            await RespondAsync(c.Press, NotOurCard, ct);
            return;
        }

        if (!await _commands.HasGrantAsync(c.Person.PersonId, PersonCapabilities.Feedback, c.Card.ServerId, ct))
        {
            await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, "no_grant", PersonCapabilities.Feedback, ct);
            await RespondAsync(c.Press, "You do not hold the feedback grant for this server, so nothing was recorded.", ct);
            return;
        }

        var result = await _commands.RecordVerdictAsync(requestId, verdict, c.Actor, ct);
        var taken = result.Outcome is InvestigationFeedbackOutcome.Recorded or InvestigationFeedbackOutcome.Replaced;
        await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, taken ? "recorded" : result.Outcome.ToString().ToLowerInvariant(), null, ct);
        if (!taken)
        {
            await RespondAsync(c.Press, result.Message ?? "The verdict was not recorded.", ct);
            return;
        }

        await RespondAsync(c.Press, $"Verdict recorded: {verdict}. Corrections go on the web feedback form.", ct);
        await EditCardAsync(c, "Agent report", "verdict " + verdict, SlackThreadPosts.RequestLink(_baseUrl(), requestId), ct);
    }

    private async Task NoteAsync(PressContext c, CancellationToken ct)
    {
        if (!long.TryParse(c.Card.RecordId, NumberStyles.None, CultureInfo.InvariantCulture, out var noteId))
        {
            await RespondAsync(c.Press, NotOurCard, ct);
            return;
        }

        if (!await _commands.HasGrantAsync(c.Person.PersonId, PersonCapabilities.DecideNote, c.Card.ServerId, ct))
        {
            await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, "no_grant", PersonCapabilities.DecideNote, ct);
            await RespondAsync(c.Press, "You do not hold the decide_note grant for this server, so nothing was done.", ct);
            return;
        }

        var approve = c.Press.ActionId == SlackCardActions.NoteApprove;
        var decided = await _commands.DecideNoteAsync(noteId, approve, c.Actor, ct);
        await JournalAsync(c.Card, c.Actor, c.Identity, c.Press, decided ? (approve ? "approved" : "rejected") : "not_pending", null, ct);
        if (!decided)
        {
            await RespondAsync(c.Press, "This note is no longer pending, so nothing was done.", ct);
            return;
        }

        await RespondAsync(c.Press, approve ? "Note approved. It goes into future briefs." : "Note rejected.", ct);
        await EditCardAsync(c, "Note", approve ? "approved" : "rejected", null, ct);
    }

    /* The decided card: the headline, how it ended and who decided, with the record's link; the buttons are gone. The card row is marked decided. */
    private async Task EditCardAsync(PressContext c, string headline, string outcome, string? link, CancellationToken ct)
    {
        var content = SlackThreadPosts.DecidedCard(headline, outcome, c.Person.DisplayName, link);
        var edited = await _api.UpdateAsync(c.Card.ChannelId, c.Card.MessageTs, content.Text, content.Blocks, ct);
        if (!edited.Ok)
        {
            _logger.LogWarning("Slack card {Channel}/{Ts} could not be edited after its decision: {Error}", c.Card.ChannelId, c.Card.MessageTs, edited.Error);
        }

        await _cards.MarkCardDecidedAsync(c.Card.ChannelId, c.Card.MessageTs, c.Actor, ct);
    }

    private Task JournalAsync(SlackCard card, string actor, string identity, SlackPress press, string outcome, string? code, CancellationToken ct) =>
        _commands.JournalPressAsync(card.RequestId, card.ServerId, actor, press.ActionId, new JsonObject
        {
            ["identity"] = identity,
            ["kind"] = card.Kind,
            ["record"] = card.RecordId,
            ["channel"] = card.ChannelId,
            ["message_ts"] = card.MessageTs,
            ["outcome"] = outcome,
            ["code"] = code,
        }, ct);

    private static List<string> FactsLines(JsonObject? facts)
    {
        var lines = new List<string>();
        if (facts is null)
        {
            return lines;
        }

        foreach (var (key, value) in facts)
        {
            if (value is null || key.Contains("statement", StringComparison.OrdinalIgnoreCase) || key.Contains("definition", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            lines.Add("*" + SlackThreadPosts.Bounded(key, 50) + ":* " + SlackThreadPosts.Bounded(value is JsonValue v ? v.ToString() : value.ToJsonString(), 200));
        }

        return lines;
    }

    private async Task RespondAsync(SlackPress press, string text, CancellationToken ct) =>
        await RespondAsync(press, new SlackThreadPostContent(text, [new { type = "section", text = new { type = "mrkdwn", text } }]), ct);

    private async Task RespondAsync(SlackPress press, SlackThreadPostContent content, CancellationToken ct)
    {
        if (press.ResponseUrl is null)
        {
            _logger.LogInformation("Slack press {Action} had no response_url; the answer is not shown", press.ActionId);
            return;
        }

        var result = await _api.RespondAsync(press.ResponseUrl, content.Text, content.Blocks, ct);
        if (!result.Ok)
        {
            _logger.LogWarning("Slack press {Action} could not be answered: {Error}", press.ActionId, result.Error);
        }
    }
}

/// <summary>#7658: the parts of a <c>block_actions</c> payload a press needs: the first action's id and value, and the <c>response_url</c>.</summary>
public sealed record SlackPress(string ActionId, string? Value, Uri? ResponseUrl)
{
    /// <summary>The payload parsed, or null when it holds no action.</summary>
    public static SlackPress? Parse(JsonObject? payload)
    {
        if (payload?["actions"] is not JsonArray actions || actions.Count == 0 || actions[0] is not JsonObject action)
        {
            return null;
        }

        var actionId = action["action_id"] is JsonValue id ? id.ToString() : null;
        if (string.IsNullOrWhiteSpace(actionId))
        {
            return null;
        }

        var value = action["value"] is JsonValue v ? v.ToString() : null;
        Uri? responseUrl = null;
        if (payload["response_url"] is JsonValue url && Uri.TryCreate(url.ToString(), UriKind.Absolute, out var parsed))
        {
            responseUrl = parsed;
        }

        return new SlackPress(actionId, value, responseUrl);
    }
}
