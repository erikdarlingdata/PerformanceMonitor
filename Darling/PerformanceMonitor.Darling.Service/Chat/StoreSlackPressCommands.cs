/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>
/// #7658: one step of a plan change's undo: the show (<paramref name="confirm"/> false: the statement the undo runs now, nothing written) or the
/// write (<paramref name="confirm"/> true, with <paramref name="shownStatement"/> the statement the person was shown). The product's step is
/// <see cref="DarlingMcpPlanForceTools.UndoPlanChangeAsync"/>, which reads the monitored server; a test stands its own server in.
/// </summary>
public delegate Task<PlanUndoAnswer> SlackPlanUndoStep(long calledId, string login, bool confirm, string? shownStatement, CancellationToken cancellationToken);

/// <summary>
/// #7658: the press commands on the store owner's connection. The proposal presses are <see cref="ActionProposalPress"/>, the same two presses the
/// web and the Viewer run; a refusal is the store's <see cref="InvestigationRequestStore.RefuseProposalAsync"/>; an undo is the person's undo
/// (<see cref="AgentChangeUndo"/>, or the plan undo for a plan change), a verdict the feedback store's record, and a note decision the store's
/// one decide statement. The person is recorded as <c>person:&lt;id&gt;</c> everywhere, as the proposal press records it.
/// </summary>
public sealed class StoreSlackPressCommands : ISlackPressCommands
{
    private readonly NpgsqlDataSource _owner;
    private readonly InvestigationRequestStore _store;
    private readonly ActionProposalPress _press;
    private readonly AgentChangeUndo _changes;
    private readonly InvestigationFeedbackStore _feedback;
    private readonly ApprovedActionHooks _hooks;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly SlackPlanUndoStep _planUndo;

    public StoreSlackPressCommands(NpgsqlDataSource owner, InvestigationRequestStore store, ILogger logger, Func<DateTime>? utcNow = null, SlackPlanUndoStep? planUndo = null)
    {
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (static () => DateTime.UtcNow);
        _press = new ActionProposalPress(owner, logger, utcNow);
        _changes = new AgentChangeUndo(owner, ServiceCommandDeadlines.SerialLoopSeconds, utcNow);
        _planUndo = planUndo ?? ((calledId, login, confirm, shownStatement, token) =>
            DarlingMcpPlanForceTools.UndoPlanChangeAsync(_changes, _owner, _logger, calledId, login, confirm, shownStatement, token));
        _feedback = new InvestigationFeedbackStore(owner, ServiceCommandDeadlines.SerialLoopSeconds);
        _hooks = new ApprovedActionHooks
        {
            LoadRoutes = (connection, _, token) => DarlingAgentActionGate.LoadRoutesAsync(connection, logger, token),
            ReadCredential = (serverId, token) => _store.ReadServerCredentialStateAsync(owner, serverId, token),
        };
    }

    /// <inheritdoc />
    public Task<SlackPerson?> FindPersonAsync(string teamId, string userId, CancellationToken cancellationToken) =>
        _store.FindSlackPersonAsync(teamId, userId, cancellationToken);

    /// <inheritdoc />
    public Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken) =>
        _store.HasGrantAsync(personId, capability, serverId, cancellationToken);

    /// <inheritdoc />
    public Task JournalPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken) =>
        _store.JournalSlackPressAsync(requestId, serverId, actor, action, detail, cancellationToken);

    /// <inheritdoc />
    public async Task<SlackProposalFacts?> ProposalAsync(Guid proposalId, CancellationToken cancellationToken)
    {
        var row = await _store.GetProposalAsync(proposalId, cancellationToken);
        return row is null ? null : new SlackProposalFacts(row.ProposalId, row.RequestId, row.Tool, row.ServerId, row.State);
    }

    /// <inheritdoc />
    public Task<ProposalPressResult> ProposalFirstPressAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken) =>
        _press.FirstPressAsync(proposalId, approver, cancellationToken);

    /// <inheritdoc />
    public Task<ProposalPressResult> ProposalConfirmAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken) =>
        _press.ConfirmAsync(proposalId, approver, cancellationToken);

    /// <inheritdoc />
    public Task<AgentActionRefusal?> ProposalRefuseAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken) =>
        _store.RefuseProposalAsync(proposalId, approver, _hooks, "refused from Slack", cancellationToken);

    /// <inheritdoc />
    public async Task<SlackChangeFacts?> ChangeAsync(long calledId, CancellationToken cancellationToken)
    {
        var row = await _changes.GetAsync(calledId, cancellationToken);
        return row is null ? null : new SlackChangeFacts(row.CalledId, row.Kind, row.ServerId, row.CanTryUndo(_utcNow()));
    }

    /// <inheritdoc />
    public async Task<SlackUndoStep> UndoAsync(long calledId, string login, bool confirm, string? shownHash, CancellationToken cancellationToken)
    {
        var row = await _changes.GetAsync(calledId, cancellationToken);
        if (row is null)
        {
            return new SlackUndoStep(SlackUndoStep.Refused, "not_found", "That change is not in the journal.");
        }

        if (row.IsPlanChange)
        {
            /* The plan undo shows its statement first and writes only the statement it showed: the confirming press reads it again and runs it
               when its hash is the one the person's Confirm carried. */
            var shown = await _planUndo(calledId, login, confirm: false, null, cancellationToken);
            if (shown.Status != PlanUndoAnswer.Confirm || shown.Statement is null)
            {
                return new SlackUndoStep(StatusWord(shown.Status), shown.Code, shown.Message, shown.Statement);
            }

            if (!confirm)
            {
                return new SlackUndoStep(SlackUndoStep.Confirm, null, "This is the statement the undo runs now. Confirm to run it. The confirming press reads the server again.", shown.Statement);
            }

            if (shownHash is null || !string.Equals(SlackConfirmValue.HashOf(shown.Statement), shownHash, StringComparison.Ordinal))
            {
                return new SlackUndoStep(SlackUndoStep.Refused, "statement_changed",
                    "The undo statement is not the one you were shown, so nothing was run. Press Undo on the card again to see the current one.", shown.Statement);
            }

            var done = await _planUndo(calledId, login, confirm: true, shown.Statement, cancellationToken);
            return new SlackUndoStep(StatusWord(done.Status), done.Code, done.Message, done.Statement ?? shown.Statement);
        }

        var what = $"Undo the agent's {row.Kind ?? "change"}: it goes back to what it was before the agent changed it.";
        if (!confirm)
        {
            return new SlackUndoStep(SlackUndoStep.Confirm, null, "Confirm to undo this change.", what);
        }

        var result = await _changes.UndoAsync(calledId, login, cancellationToken);
        return new SlackUndoStep(result.Undone ? SlackUndoStep.Undone : SlackUndoStep.Refused, result.Code, result.Words, what);
    }

    private static string StatusWord(string planStatus) => planStatus switch
    {
        PlanUndoAnswer.Confirm => SlackUndoStep.Confirm,
        PlanUndoAnswer.Undone => SlackUndoStep.Undone,
        PlanUndoAnswer.Failed => SlackUndoStep.Failed,
        _ => SlackUndoStep.Refused,
    };

    /// <inheritdoc />
    public Task<InvestigationFeedbackResult> RecordVerdictAsync(Guid requestId, string verdict, string author, CancellationToken cancellationToken) =>
        _feedback.RecordAsync(new InvestigationFeedbackSubmission(requestId, verdict, null, null, null, author), cancellationToken);

    /// <inheritdoc />
    public Task<bool> DecideNoteAsync(long noteId, bool approve, string actor, CancellationToken cancellationToken) =>
        _store.DecideNoteAsync(noteId, approve, actor, cancellationToken);
}
