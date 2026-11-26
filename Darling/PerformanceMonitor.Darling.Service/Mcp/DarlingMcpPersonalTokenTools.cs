/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// What the personal MCP token tools need from the host (#9643): whether personal tokens are served here (<c>mcp.personalTokens</c> on, not a
/// front door, with a snapshot), the snapshot the MCP gate reads, and the store owner's connection. The tools take it as one service
/// parameter, so a host with personal tokens off still resolves it and the tools answer <c>not_served</c>.
/// </summary>
public sealed class DarlingPersonalTokenSeat
{
    internal DarlingPersonalTokenSeat(PersonalTokenSettings? settings, DarlingPersonalTokenSnapshot? snapshot, StoreOwnerSource owner)
    {
        Settings = settings;
        Snapshot = snapshot;
        Owner = owner;
    }

    internal PersonalTokenSettings? Settings { get; }

    internal DarlingPersonalTokenSnapshot? Snapshot { get; }

    internal StoreOwnerSource Owner { get; }
}

/// <summary>
/// The personal MCP token tools (#9643): <c>mint_personal_token</c> makes a token for a named person, <c>revoke_personal_token</c> revokes one and
/// <c>get_personal_tokens</c> lists them. The two writes and the list are all served only under <c>mcp.allowServerAdmin</c> (with <c>mcp.allowWrites</c>
/// and a token), like <c>add_servers</c>, and sit in <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>. They run the same core as the web page
/// (<see cref="DarlingPersonalTokenAdmin"/>) on the store owner's connection. The mint answer is the one place a token value ever appears: it is in
/// no log line, no journal row and no exception text.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPersonalTokenTools
{
    internal const string MintToolName = "mint_personal_token";
    internal const string RevokeToolName = "revoke_personal_token";
    internal const string ListToolName = "get_personal_tokens";

    /// <summary>The journal actor of an MCP token change: <c>mcp:</c> plus the caller the request filter worked out.</summary>
    internal static string ActorOf() => "mcp:" + ReadCaller.StoreIdentity();

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "mint_personal_token"), Description(
        "Makes a personal MCP token for one named person, the way the web dashboard's token page does for the person who is signed in. THE ANSWER HOLDS THE TOKEN: its `token` field is the bearer secret, shown this once and never again, so hand it to the person and keep it out of notes and logs. The person is named as get_people names them (display name, matched exactly with case ignored). They must be enabled and have exactly one web identity, which is the sign-in the token acts as. The token's role is reader (the reads): this tool makes reader tokens only, and any other role is refused as role_not_allowed, because an operator token is made on the web dashboard's token page by the person who signs in. It is refused as data_detail_wider when your own data detail is stricter than the new token would be served. A token lasts 1 to mcp.personalTokens.maxLifetimeDays days. A person holds at most mcp.personalTokens.maxTokensPerPerson live tokens, with a live token's name unique per person, and making too many in 24 hours is refused. A refusal answers status refused with a code. Personal tokens must be on (mcp.personalTokens.enabled) or the call is refused as not_served. Served only when mcp.allowWrites and mcp.allowServerAdmin are both on. Undo with revoke_personal_token.")]
    public static Task<string> MintPersonalToken(
        [Description("The person's display name, as get_people shows it.")] string person,
        [Description("A name for the token: 1 to 64 characters of letters, digits, '.', '_', ':' and '-'. It cannot be web, mcp, cli or viewer.")] string name,
        [Description("How many days the token lasts.")] int lifetime_days,
        NpgsqlDataSource postgres,
        DarlingPersonalTokenSeat seat,
        ILogger? logger = null,
        [Description("reader (the default and the only role this tool makes).")] string role = McpTokenEntry.ReaderRole,
        McpDataDetailSetting? maskSetting = null,
        CancellationToken cancellationToken = default) =>
        MintAsync(postgres, seat, logger, person, name, lifetime_days, role, ActorOf(), TimeProvider.System, maskSetting?.Mode ?? McpDataDetail.Full, cancellationToken);

    /// <summary>The mint tool body, so a test drives it with its own seat, caller and clock.</summary>
    internal static async Task<string> MintAsync(NpgsqlDataSource postgres, DarlingPersonalTokenSeat seat, ILogger? logger, string person, string name,
        int lifetimeDays, string role, string actor, TimeProvider clock, McpDataDetail callerDetail, CancellationToken ct)
    {
        try
        {
            var settings = seat.Settings;
            var snapshot = seat.Snapshot;
            if (settings is null || snapshot is null)
            {
                return Refuse("not_served", "Personal MCP tokens are not served here (mcp.personalTokens.enabled is off, or this is a front door). Nothing was changed.");
            }

            if (string.IsNullOrWhiteSpace(person))
            {
                return Refuse("invalid", "person is required. Nothing was changed.");
            }

            if (!DarlingPersonalTokenAdmin.IsAcceptableName(name))
            {
                return Refuse("bad_name", "A token name is 1 to 64 characters of letters, digits, '.', '_', ':' and '-', and cannot be web, mcp, cli or viewer. Nothing was changed.");
            }

            if (lifetimeDays < 1 || lifetimeDays > settings.MaxLifetimeDays)
            {
                return Refuse("bad_lifetime", string.Create(CultureInfo.InvariantCulture, $"A token lasts 1 to {settings.MaxLifetimeDays} days. Nothing was changed."));
            }

            /* #9643: the service does not store a person's role, and the web page makes an operator token only for a signed-in admin, so this
               tool makes readers only. An operator token is made on the page, where the person signs in. */
            if (!string.Equals(role, McpTokenEntry.ReaderRole, StringComparison.Ordinal))
            {
                return Refuse("role_not_allowed", "This tool makes reader tokens only. An operator token is made on the web dashboard's token page, where the person signs in. Nothing was changed.");
            }

            /* A token a caller makes is served at the stricter of the service's level and the personal tokens' own level. The caller's own level is
               never wider than the service's, so a caller narrowed below that cannot make a token that reads more than it does. */
            if (callerDetail > settings.ServedDetail)
            {
                return Refuse("data_detail_wider", "Your data detail is stricter than a new personal token would be served, so this call cannot make one. Nothing was changed.");
            }

            var owner = seat.Owner.Read();
            if (owner is null)
            {
                return Refuse("no_owner_source", "The service has not published its store owner connection yet, so a token cannot be written. Nothing was changed.");
            }

            var people = await PersonDirectory.ListAsync(postgres, ct);
            var matches = people.Where(p => string.Equals(p.DisplayName, person.Trim(), StringComparison.OrdinalIgnoreCase)).ToList();
            if (matches.Count == 0)
            {
                return Refuse("person_not_found", "No person has that display name. get_people lists them. Nothing was changed.");
            }

            if (matches.Count > 1)
            {
                return Refuse("person_ambiguous", string.Create(CultureInfo.InvariantCulture, $"{matches.Count} people have that display name. Nothing was changed."));
            }

            var found = matches[0];
            if (!found.Enabled)
            {
                return Refuse("person_disabled", "That person is disabled, and a disabled person's sign-in does not count. Nothing was changed.");
            }

            var webIdentities = found.Identities.Where(i => string.Equals(i.Kind, PersonIdentityKinds.Web, StringComparison.Ordinal)).ToList();
            if (webIdentities.Count != 1)
            {
                return Refuse("person_identity", string.Create(CultureInfo.InvariantCulture,
                    $"That person has {webIdentities.Count} web identities, and a token needs exactly one (the sign-in it acts as). Nothing was changed."));
            }

            var made = await DarlingPersonalTokenAdmin.MintAsync(owner, seat.Owner, snapshot, settings, webIdentities[0].ExternalId, found.DisplayName, name, role,
                lifetimeDays, actor, clock, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct, CancellationToken.None);
            switch (made.Outcome)
            {
                case PersonalTokenCreateOutcome.NameTaken:
                    return Refuse("name_taken", "That person already has a live token with that name. Nothing was changed.");
                case PersonalTokenCreateOutcome.AtCap:
                    return Refuse("at_cap", string.Create(CultureInfo.InvariantCulture,
                        $"That person already has {settings.MaxTokensPerPerson} live tokens. Revoke one first. Nothing was changed."));
                case PersonalTokenCreateOutcome.SubjectTaken:
                    return Refuse("subject_clash", "That display name is already the subject of a token that belongs to a different sign-in. Nothing was changed.");
                case PersonalTokenCreateOutcome.TooManyRecent:
                    return Refuse("too_many_recent", "That person's tokens were made too many times in the last 24 hours. Try again later. Nothing was changed.");
            }

            return new JsonObject
            {
                ["status"] = "minted",
                ["person"] = found.DisplayName,
                ["token"] = made.Token,
                ["token_shown_once"] = "This is the only time the token appears. It cannot be read back.",
                ["record"] = DarlingPersonalTokenAdmin.RowJson(made.Record!, clock.GetUtcNow(), withSubject: true),
                ["undo"] = new JsonObject
                {
                    ["tool"] = RevokeToolName,
                    ["arguments"] = new JsonObject { ["token_id"] = made.Record!.TokenId },
                },
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(MintToolName, ex);
        }
    }

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "revoke_personal_token"), Description(
        "Revokes one live personal MCP token by its id (get_personal_tokens shows the ids). The gate refuses the token from then on. A token that is absent, already revoked or expired is one not_found answer. Served only when mcp.allowWrites and mcp.allowServerAdmin are both on.")]
    public static Task<string> RevokePersonalToken(
        [Description("The token's id, as get_personal_tokens shows it.")] string token_id,
        DarlingPersonalTokenSeat seat,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RevokeAsync(seat, logger, token_id, ActorOf(), TimeProvider.System, cancellationToken);

    /// <summary>The revoke tool body, so a test drives it with its own seat, caller and clock.</summary>
    internal static async Task<string> RevokeAsync(DarlingPersonalTokenSeat seat, ILogger? logger, string tokenId, string actor, TimeProvider clock, CancellationToken ct)
    {
        try
        {
            var snapshot = seat.Snapshot;
            if (seat.Settings is null || snapshot is null)
            {
                return Refuse("not_served", "Personal MCP tokens are not served here (mcp.personalTokens.enabled is off, or this is a front door). Nothing was changed.");
            }

            /* An id that is not a token id is the same not_found as an absent one. */
            if (!DarlingWebSeat.IsPersonalTokenId(tokenId))
            {
                return Refuse("not_found", "No live token has that id. Nothing was changed.");
            }

            var owner = seat.Owner.Read();
            if (owner is null)
            {
                return Refuse("no_owner_source", "The service has not published its store owner connection yet, so a token cannot be revoked. Nothing was changed.");
            }

            var revoked = await DarlingPersonalTokenAdmin.RevokeAsync(owner, seat.Owner, snapshot, tokenId, null, null, actor, actor, actor, actor, clock,
                logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct, CancellationToken.None);
            return revoked
                ? new JsonObject { ["status"] = "revoked", ["token_id"] = tokenId }.ToJsonString()
                : Refuse("not_found", "No live token has that id. Nothing was changed.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(RevokeToolName, ex);
        }
    }

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_personal_tokens"), Description(
        "Lists personal MCP tokens, every person's (served only with the tools that make and revoke them), with each token's id, name, role, the person it acts as (subject), when it was made, expires and was last used, its state (live, expired or revoked) and who revoked it and why. It never shows a token value or a hash: a value is shown once, when mint_personal_token makes it. Live tokens come first, and truncated is true when older dead rows were left out. Answers not_served when personal tokens are off (mcp.personalTokens.enabled).")]
    public static Task<string> GetPersonalTokens(DarlingPersonalTokenSeat seat, CancellationToken cancellationToken = default) =>
        ListAsync(seat, TimeProvider.System, cancellationToken);

    /// <summary>The list tool body: <c>{tokens_returned, truncated, tokens: [...]}</c>.</summary>
    internal static async Task<string> ListAsync(DarlingPersonalTokenSeat seat, TimeProvider clock, CancellationToken ct)
    {
        try
        {
            if (seat.Settings is null || seat.Snapshot is null)
            {
                return Refuse("not_served", "Personal MCP tokens are not served here (mcp.personalTokens.enabled is off, or this is a front door).");
            }

            var owner = seat.Owner.Read();
            if (owner is null)
            {
                return Refuse("no_owner_source", "The service has not published its store owner connection yet, so the tokens cannot be read.");
            }

            var now = clock.GetUtcNow();
            var page = await PersonalTokenStore.ListAsync(owner, null, now, ct);
            return new JsonObject
            {
                ["tokens_returned"] = page.Rows.Count,
                ["truncated"] = page.Truncated,
                ["tokens"] = new JsonArray(page.Rows.Select(r => (JsonNode)DarlingPersonalTokenAdmin.RowJson(r, now, withSubject: true)).ToArray()),
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ListToolName, ex);
        }
    }

    /* Every refusal answers status refused with a code, so the write log reads it as refused and not completed (#9643). */
    private static string Refuse(string code, string message) =>
        new JsonObject { ["status"] = "refused", ["code"] = code, ["message"] = message }.ToJsonString();
}
