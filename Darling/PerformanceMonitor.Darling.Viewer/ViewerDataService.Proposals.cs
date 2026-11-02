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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Action proposals in the Viewer (#7109). The Viewer is an administrator path: it names no person, so it approves only a proposal on a route
/// that allows single-person approval, and the service refuses it on any other. It reads and presses through the command plane, because the
/// press runs the prepare step against the monitored server and only the store owner writes the proposal tables.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The command types the service runs for proposals; they match <c>ProposalCommands</c> in the service.</summary>
    public const string CommandReadProposals = "read_action_proposals";
    public const string CommandProposalFirstPress = "proposal_first_press";
    public const string CommandProposalConfirm = "proposal_confirm";

    /// <summary>The wait for a press: the service reads the monitored server, and on the confirm writes to it.</summary>
    public static readonly TimeSpan ProposalPressTimeout = TimeSpan.FromSeconds(90);

    /// <summary>The wait for the list.</summary>
    public static readonly TimeSpan ReadProposalsTimeout = TimeSpan.FromSeconds(30);

    /// <summary>The proposals, newest first, of every request or of one server. Null when the service has not answered.</summary>
    public async Task<IReadOnlyList<ProposalViewRow>?> ReadProposalsAsync(int? serverId, string login, CancellationToken cancellationToken = default)
    {
        var commandId = await EnqueueCommandAsync(CommandReadProposals, serverId, "{}", login, cancellationToken);
        var result = await PollCommandResultAsync(commandId, ReadProposalsTimeout, cancellationToken);
        if (result is null)
            return null;
        await TryDeleteCommandAsync(commandId, cancellationToken);
        return ProposalViewRow.ReadList(result.ResultJson);
    }

    /// <summary>The first press (<paramref name="confirm"/> false) or the confirming press of a proposal as the administrator path.</summary>
    public async Task<ProposalPressAnswer> PressProposalAsync(Guid proposalId, bool confirm, string login, CancellationToken cancellationToken = default)
    {
        _ = AgentChangeUndo.PersonActor(login);
        var args = System.Text.Json.JsonSerializer.Serialize(new { proposal_id = proposalId.ToString("D") });
        var commandId = await EnqueueCommandAsync(confirm ? CommandProposalConfirm : CommandProposalFirstPress, null, args, login, cancellationToken);
        var result = await PollCommandResultAsync(commandId, ProposalPressTimeout, cancellationToken);
        if (result is null)
        {
            return new ProposalPressAnswer("timed_out", null, "The service has not answered yet. Read the list again before pressing twice.", null, []);
        }

        await TryDeleteCommandAsync(commandId, cancellationToken);
        return ProposalPressAnswer.Read(result.ResultJson, result.ResultStatus);
    }

    /// <summary>True when no enabled mapped person holds any approve grant (#7109): the route editor then says nobody can approve. The Viewer's
    /// role reads the people tables, as the People window does. A store below V237 has no people tables, so nothing is queried and the answer is
    /// false (no sentence), as it is for a read that fails; a probe that cannot tell reads the tables.</summary>
    public async Task<bool> NobodyCanApproveAnywhereAsync(CancellationToken cancellationToken = default)
    {
        var version = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        if (!PeopleText.StoreHasPeople(version))
            return false;
        return await new InvestigationRequestStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds).NobodyCanApproveAnywhereAsync(cancellationToken);
    }

    private async Task TryDeleteCommandAsync(long commandId, CancellationToken cancellationToken)
    {
        try
        {
            await DeleteCommandAsync(commandId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The row holds no secret; the service-side reaper is the backstop. */
        }
    }
}

/// <summary>What the service answered to one press: the status word (<c>first_pressed</c>, <c>superseded</c>, <c>refused</c>, or the final state
/// <c>run</c>, <c>failed</c>, <c>unknown</c>), its code and sentence, the facts the fresh dry run read, and the facts that changed.</summary>
public sealed record ProposalPressAnswer(string Status, string? Code, string Message, JsonObject? Facts, IReadOnlyList<string> Changed)
{
    public bool FirstPressed => Status == "first_pressed";

    /// <summary>The facts in lines for the confirm question, one per member; empty when there are none.</summary>
    public string FactsText => Facts is null ? "" : string.Join(Environment.NewLine, Facts.Select(p => p.Key + ": " + (p.Value?.ToJsonString().Trim('"') ?? "none")));

    /// <summary>The sentence to show for the answer: the message, and for a superseded proposal what changed.</summary>
    public string Words => Status == "superseded" && Changed.Count > 0 && !Message.Contains(string.Join(", ", Changed), StringComparison.Ordinal)
        ? Message + " Changed: " + string.Join(", ", Changed) + "."
        : Message;

    public static ProposalPressAnswer Read(string? json, string? resultStatus)
    {
        try
        {
            if (json is not null && JsonNode.Parse(json) is JsonObject o)
            {
                var status = o["status"] is JsonValue s && s.TryGetValue(out string? sv) ? sv : null;
                var message = o["message"] is JsonValue m && m.TryGetValue(out string? mv) ? mv : null;
                if (status is not null && message is not null)
                {
                    var code = o["code"] is JsonValue c && c.TryGetValue(out string? cv) ? cv : null;
                    var changed = o["changed"] is JsonArray a ? a.Select(x => x?.GetValue<string>() ?? "").Where(x => x.Length > 0).ToList() : [];
                    return new ProposalPressAnswer(status, code, message, o["facts"] as JsonObject, changed);
                }

                if (o["error"] is JsonValue e && e.TryGetValue(out string? ev) && !string.IsNullOrWhiteSpace(ev))
                    return new ProposalPressAnswer(resultStatus ?? "failed", null, ev, null, []);
            }
        }
        catch (System.Text.Json.JsonException)
        {
            /* fall through to the plain sentence */
        }

        return new ProposalPressAnswer(resultStatus ?? "failed", null, "The service could not press the proposal.", null, []);
    }
}

/// <summary>One row of the Proposals grid (#7109).</summary>
public sealed class ProposalViewRow
{
    public required Guid ProposalId { get; init; }

    public required string Kind { get; init; }

    public required string Server { get; init; }

    /// <summary>The short target line the service words for the tool (the session, its database and login time, the plan and query ids). It is
    /// built from the target after <c>mcp.dataDetail</c> has been applied, so a statement-class value never reaches it.</summary>
    public required string Target { get; init; }

    public required string State { get; init; }

    public string Agent { get; init; } = "";

    /// <summary>The agent's own words, labelled as the agent's wherever they are shown.</summary>
    public string Reason { get; init; } = "";

    public DateTime ExpiresAtUtc { get; init; }

    public bool SinglePerson { get; init; }

    public bool CanApproveHere { get; init; }

    /// <summary>Who made the first press, in words (a person's display name, or the administrator path), or null before any press.</summary>
    public string? FirstPressName { get; init; }

    public bool NobodyCanApprove { get; init; }

    public string? Link { get; init; }

    public string ReasonDisplay => Reason.Length == 0 ? "" : "Agent says: " + Reason;

    public string ExpiresDisplay => ExpiresAtUtc.ToLocalTime().ToString("g", CultureInfo.CurrentCulture);

    public bool IsPending => State == ProposalStates.Pending;

    /// <summary>What the grid says for this proposal, short enough for its column: approve here, or that a mapped person approves it in the web.</summary>
    public string ApprovalDisplay => ApprovalText(withLink: false);

    /// <summary>The same, with the first press and the web link in full, for the status line.</summary>
    public string ApprovalDetail => ApprovalText(withLink: true);

    private string ApprovalText(bool withLink)
    {
        if (!IsPending)
            return "";
        var text = CanApproveHere
            ? "Approve here (single-person route)"
            : "A mapped person approves this in the web" + (withLink && Link is not null ? ": " + Link : ".");
        if (withLink && FirstPressName is not null)
            text = "First press by " + FirstPressName + ". " + text;
        return NobodyCanApprove ? text + (withLink && Link is not null && !CanApproveHere ? Environment.NewLine : " ") + ActionApprovalRules.NobodyCanApproveMessage : text;
    }

    /// <summary>The check before Approve: what is wrong with it, or null.</summary>
    internal static string? ApproveError(ProposalViewRow? selected) =>
        selected is null ? "Select a proposal first."
        : !selected.IsPending ? "This proposal is " + selected.State + ", so there is nothing to approve."
        : !selected.CanApproveHere ? selected.ApprovalDetail
        : null;

    /// <summary>Reads the service's <c>{proposals: [...]}</c>; an unreadable answer is an empty list.</summary>
    public static IReadOnlyList<ProposalViewRow> ReadList(string? json)
    {
        var rows = new List<ProposalViewRow>();
        try
        {
            if (json is not null && JsonNode.Parse(json) is JsonObject o && o["proposals"] is JsonArray list)
            {
                foreach (var item in list.OfType<JsonObject>())
                {
                    if (!Guid.TryParse(Text(item, "proposal_id"), out var id))
                        continue;
                    _ = DateTime.TryParse(Text(item, "expires_at"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var expires);
                    rows.Add(new ProposalViewRow
                    {
                        ProposalId = id,
                        Kind = Capitalized(Text(item, "kind") ?? Text(item, "tool") ?? ""),
                        Server = Text(item, "server_name") ?? "",
                        Target = Text(item, "target_text")
                            ?? (item["target"] is JsonObject t ? string.Join(", ", t.Select(p => p.Key + "=" + (p.Value?.ToJsonString().Trim('"') ?? ""))) : ""),
                        State = Text(item, "state") ?? "",
                        Agent = Text(item, "agent") ?? "",
                        Reason = Text(item, "reason") ?? "",
                        ExpiresAtUtc = DateTime.SpecifyKind(expires, DateTimeKind.Utc),
                        SinglePerson = Flag(item, "single_person"),
                        CanApproveHere = Flag(item, "can_approve_here"),
                        FirstPressName = Text(item, "first_press_name"),
                        NobodyCanApprove = Flag(item, "nobody_can_approve"),
                        Link = Text(item, "link"),
                    });
                }
            }
        }
        catch (System.Text.Json.JsonException)
        {
            /* an unreadable answer reads as no proposals */
        }

        return rows;
    }

    private static string Capitalized(string text) => text.Length == 0 ? text : char.ToUpper(text[0], CultureInfo.CurrentCulture) + text[1..];

    private static string? Text(JsonObject o, string name) => o[name] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

    private static bool Flag(JsonObject o, string name) => o[name] is JsonValue v && v.TryGetValue(out bool b) && b;
}
