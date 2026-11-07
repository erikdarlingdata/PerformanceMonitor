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
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The states of a Slack card (#7658).</summary>
public static class SlackCardStates
{
    public const string Live = "live";
    public const string Decided = "decided";
}

/// <summary>#7658: one card the bot posted with buttons.</summary>
public sealed record SlackCard(Guid RequestId, string PostKey, string ChannelId, string MessageTs, string Kind, string RecordId, int ServerId,
    string State = SlackCardStates.Live, string? DecidedBy = null, DateTime? DecidedAtUtc = null);

/// <summary>#7658: where the bot's cards live.</summary>
public interface ISlackCardStore
{
    Task SaveCardAsync(SlackCard card, CancellationToken cancellationToken);

    Task<SlackCard?> FindCardAsync(string channelId, string messageTs, CancellationToken cancellationToken);

    Task<bool> MarkCardDecidedAsync(string channelId, string messageTs, string decidedBy, CancellationToken cancellationToken);
}

/// <summary>#7658: the person behind a Slack identity.</summary>
public sealed record SlackPerson(long PersonId, long IdentityId, string DisplayName, bool Enabled);

/// <summary>#7658: the owner reads and writes a Slack press needs.</summary>
public interface ISlackPressStore
{
    Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken cancellationToken);

    Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken);

    Task JournalSlackPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken);
}

public sealed partial class InvestigationRequestStore : ISlackCardStore, ISlackPressStore
{
    public const string SlackPressed = "slack_press";

    public const string NotePostKeyPrefix = "note:";

    public const string NotePending = "note_pending";

    public static string SlackExternalId(string teamId, string userId) => (teamId ?? "").Trim() + ":" + (userId ?? "").Trim();

    public Task SaveCardAsync(SlackCard card, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<SlackCard?> FindCardAsync(string channelId, string messageTs, CancellationToken cancellationToken) => Task.FromResult<SlackCard?>(null);

    public Task<bool> MarkCardDecidedAsync(string channelId, string messageTs, string decidedBy, CancellationToken cancellationToken) => Task.FromResult(false);

    public Task<SlackPerson?> FindSlackPersonAsync(string teamId, string userId, CancellationToken cancellationToken) => Task.FromResult<SlackPerson?>(null);

    public Task<bool> HasGrantAsync(long personId, string capability, int? serverId, CancellationToken cancellationToken) => Task.FromResult(false);

    public Task JournalSlackPressAsync(Guid? requestId, int? serverId, string actor, string action, JsonObject detail, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<AgentActionRefusal?> RefuseProposalAsync(Guid proposalId, ProposalApprover approver, ApprovedActionHooks hooks, string? reason,
        CancellationToken cancellationToken = default) =>
        Task.FromResult<AgentActionRefusal?>(new AgentActionRefusal(ProposalPressCodes.NotFound, "There is no proposal with that id."));

    public Task<bool> RefuseProposalRowAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, ProposalApprover approver, string? reason,
        CancellationToken cancellationToken = default) => Task.FromResult(false);
}
