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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7658: one step of a person's undo.</summary>
public sealed record SlackUndoStep(string Status, string? Code, string Message, string? Statement = null)
{
    public const string Confirm = "confirm";
    public const string Undone = "undone";
    public const string Refused = "refused";
    public const string Failed = "failed";
}

/// <summary>#7658: what a proposal card says about its record.</summary>
public sealed record SlackProposalFacts(Guid ProposalId, Guid RequestId, string Tool, int ServerId, string State);

/// <summary>#7658: what a change card says about its record.</summary>
public sealed record SlackChangeFacts(long CalledId, string? Kind, int? ServerId, bool CanTryUndo);

/// <summary>#7658: the commands a press runs, behind one seam.</summary>
public interface ISlackPressCommands
{
    Task<SlackPerson?> FindPersonAsync(string teamId, string userId, CancellationToken cancellationToken);

    Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken);

    Task JournalPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken);

    Task<SlackProposalFacts?> ProposalAsync(Guid proposalId, CancellationToken cancellationToken);

    Task<ProposalPressResult> ProposalFirstPressAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken);

    Task<ProposalPressResult> ProposalConfirmAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken);

    Task<AgentActionRefusal?> ProposalRefuseAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken);

    Task<SlackChangeFacts?> ChangeAsync(long calledId, CancellationToken cancellationToken);

    Task<SlackUndoStep> UndoAsync(long calledId, string login, bool confirm, string? shownHash, CancellationToken cancellationToken);

    Task<InvestigationFeedbackResult> RecordVerdictAsync(Guid requestId, string verdict, string author, CancellationToken cancellationToken);

    Task<bool> DecideNoteAsync(long noteId, bool approve, string actor, CancellationToken cancellationToken);
}

/// <summary>#7658: the buttons.</summary>
public sealed class SlackInteractionHandler : ISlackEventHandlers
{
    public SlackInteractionHandler(ISlackCardStore cards, ISlackPressCommands commands, ISlackCardApi api, ILogger logger, McpDataDetail dataDetail,
        Func<string>? baseUrl = null, Func<DateTime>? utcNow = null)
    {
    }

    public const string OtherWorkspace = "This store acts only on presses from its own workspace.";

    public const string NotOurCard = "This button is not on a card this store posted, so nothing was done.";

    public const string NotMapped = "Your Slack account is not mapped to a person in this store, so nothing was done. An administrator can map it on the People page.";

    public const string Disabled = "Your person is disabled in this store, so nothing was done.";

    public Task HandleEventAsync(SlackInboundEvent inbound, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task HandleInteractiveAsync(SlackInboundInteraction inbound, CancellationToken cancellationToken) => Task.CompletedTask;

    public static string PersonActor(SlackPerson person) => "";
}

/// <summary>#7658: the parts of a <c>block_actions</c> payload a press needs.</summary>
public sealed record SlackPress(string ActionId, string? Value, Uri? ResponseUrl)
{
    public static SlackPress? Parse(JsonObject? payload) => null;
}
