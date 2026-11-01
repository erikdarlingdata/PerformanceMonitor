/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>The answer to a press: <see cref="Status"/> is <c>first_pressed</c>, <c>superseded</c>, <c>refused</c>, <c>failed</c> (the press could not
/// read the server: the proposal stays pending), or the final state of a confirmed proposal (<c>run</c>, <c>failed</c>, <c>unknown</c>,
/// <c>refused</c>). <see cref="Facts"/> is what the fresh dry run read. <see cref="Changed"/> names the facts that differ from the binding.</summary>
public sealed record ProposalPressResult(string Status, string? Code, string Message, JsonObject? Facts = null, IReadOnlyList<string>? Changed = null);

/// <summary>
/// The two presses of a proposal (#7109), the API the web proposal page and the Viewer call (slice 5). No MCP tool reaches it, and no agent
/// token maps to a person, so the agent never decides a proposal.
/// <para>The first press runs the tool's prepare step and a dry run again against the stored server. Facts that differ from the binding end
/// the proposal <c>superseded</c>, and the answer says what changed. Otherwise it records the first press and returns the facts. The confirming
/// press, by the same approver, prepares and dry runs again and compares with the binding and the first press's facts. Then
/// <see cref="InvestigationRequestStore.ApproveProposalForRunAsync"/> (transaction 1) and
/// <see cref="InvestigationRequestStore.RunApprovedActionAsync"/> (transaction 2, the tool's write step).</para>
/// <para>Built for the PostgreSQL kill. The other Level 3 tools answer <c>not_supported</c> until their prepare and write steps are wired here.</para>
/// </summary>
public sealed class ActionProposalPress
{
    public const string NotSupported = "not_supported";

    private readonly NpgsqlDataSource _owner;
    private readonly ILogger _logger;
    private readonly InvestigationRequestStore _store;
    private readonly ApprovedActionHooks _hooks;

    /// <summary><paramref name="owner"/> is the store owner's data source: only the owner writes proposals and the journal, and only it reads
    /// the remediation login and the people tables.</summary>
    public ActionProposalPress(NpgsqlDataSource owner, ILogger logger, Func<DateTime>? utcNow = null)
    {
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _store = new InvestigationRequestStore(owner, ServiceCommandDeadlines.SerialLoopSeconds, utcNow);
        _hooks = new ApprovedActionHooks
        {
            LoadRoutes = (connection, _, token) => DarlingAgentActionGate.LoadRoutesAsync(connection, logger, token),
            ReadCredential = (serverId, token) => _store.ReadServerCredentialStateAsync(owner, serverId, token),
        };
    }

    /// <summary>The prepared form of one proposal's tool: the binding facts the fresh dry run read, and the write step to run with the permit.</summary>
    private sealed record Prepared(JsonObject? Binding, Func<ServerActionPermit, CancellationToken, Task<ApprovedWriteOutcome>>? Write,
        ProposalPressResult? Stop, bool Supersede = false);

    /// <summary>The first press: checks, a fresh prepare and dry run, then the first press or <c>superseded</c>.</summary>
    public async Task<ProposalPressResult> FirstPressAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(approver);
        var check = await _store.CheckProposalPressAsync(proposalId, approver, _hooks, cancellationToken);
        if (check.Refusal is { } refusal)
            return new ProposalPressResult("refused", refusal.Reason, refusal.Message);

        var proposal = check.Proposal!;
        var prepared = await PrepareAsync(proposal, check.Permit!, cancellationToken);
        if (await StopAsync(proposal, approver, prepared, cancellationToken) is { } stop)
            return stop;

        var changed = Differences(JsonNode.Parse(proposal.BindingJson)!.AsObject(), prepared.Binding!);
        if (changed.Count > 0)
            return await SupersedeAsync(proposal, approver, prepared.Binding!, changed, cancellationToken);

        var detail = new JsonObject { ["approver"] = approver.ToJson() };
        if (!await _store.RecordFirstPressAsync(proposalId, approver.Key, prepared.Binding!, detail, cancellationToken))
            return new ProposalPressResult("refused", InvestigationRequestStore.ProposalPressCodes.NotPending,
                "This proposal is no longer waiting for a person. Nothing was run.");
        return new ProposalPressResult("first_pressed", null,
            "These are the facts now. Confirm to run the action. The confirming press reads them again.", prepared.Binding);
    }

    /// <summary>The confirming press: the same approver as the first press, a fresh prepare and dry run compared with the binding and the first
    /// press's facts, then the two transactions.</summary>
    public async Task<ProposalPressResult> ConfirmAsync(Guid proposalId, ProposalApprover approver, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(approver);
        var check = await _store.CheckProposalPressAsync(proposalId, approver, _hooks, cancellationToken);
        if (check.Refusal is { } refusal)
            return new ProposalPressResult("refused", refusal.Reason, refusal.Message);

        var proposal = check.Proposal!;
        if (proposal.FirstPressBy is null)
            return new ProposalPressResult("refused", InvestigationRequestStore.ProposalPressCodes.NoFirstPress,
                "Nobody has pressed this proposal yet. The first press runs a fresh dry run and shows its facts. Confirm after that.");
        if (!string.Equals(proposal.FirstPressBy, approver.Key, StringComparison.Ordinal))
            return new ProposalPressResult("refused", InvestigationRequestStore.ProposalPressCodes.OtherApprover,
                "Another person made the first press. The same person makes both presses: press first, read the facts, then confirm.");

        var prepared = await PrepareAsync(proposal, check.Permit!, cancellationToken);
        if (await StopAsync(proposal, approver, prepared, cancellationToken) is { } stop)
            return stop;

        var changed = Differences(JsonNode.Parse(proposal.BindingJson)!.AsObject(), prepared.Binding!);
        if (proposal.FirstPressFactsJson is { } firstFacts)
            foreach (var key in Differences(JsonNode.Parse(firstFacts)!.AsObject(), prepared.Binding!))
                if (!changed.Contains(key))
                    changed.Add(key);
        if (changed.Count > 0)
            return await SupersedeAsync(proposal, approver, prepared.Binding!, changed, cancellationToken);

        var approval = await _store.ApproveProposalForRunAsync(proposalId, approver, _hooks, cancellationToken);
        if (approval.Refusal is { } approvalRefusal)
            return new ProposalPressResult("refused", approvalRefusal.Reason, approvalRefusal.Message);

        var permit = approval.Permit!;
        var run = await _store.RunApprovedActionAsync(permit, _hooks, token => prepared.Write!(permit.Permit, token), cancellationToken);
        return new ProposalPressResult(run.State, run.Code, run.Message, prepared.Binding);
    }

    /// <summary>A prepare step that could not read the facts: a server that refused the dry run (the session is gone or changed) ends the
    /// proposal <c>superseded</c>, and a failure to read leaves it pending.</summary>
    private async Task<ProposalPressResult?> StopAsync(ActionProposalRow proposal, ProposalApprover approver, Prepared prepared, CancellationToken ct)
    {
        if (prepared.Stop is not { } stop)
            return null;
        if (!prepared.Supersede)
            return stop;
        await _store.SupersedeProposalAsync(proposal.ProposalId, approver.Key,
            new JsonObject { ["reason"] = stop.Code, ["message"] = stop.Message }, ct);
        return stop with { Status = "superseded" };
    }

    private async Task<ProposalPressResult> SupersedeAsync(ActionProposalRow proposal, ProposalApprover approver, JsonObject fresh, List<string> changed,
        CancellationToken ct)
    {
        var list = new JsonArray();
        foreach (var key in changed)
            list.Add(key);
        await _store.SupersedeProposalAsync(proposal.ProposalId, approver.Key,
            new JsonObject { ["reason"] = "facts_changed", ["changed"] = list, ["fresh"] = fresh.DeepClone() }, ct);
        return new ProposalPressResult("superseded", "facts_changed",
            $"The facts changed since the agent proposed this ({string.Join(", ", changed)}), so nothing was run and the proposal has ended.", fresh, changed);
    }

    /// <summary>The keys whose values differ between the stored facts and the fresh ones, in either direction.</summary>
    internal static List<string> Differences(JsonObject stored, JsonObject fresh)
    {
        var changed = new List<string>();
        foreach (var (key, value) in fresh)
            if (!stored.TryGetPropertyValue(key, out var old) || !JsonNode.DeepEquals(old, value))
                changed.Add(key);
        foreach (var (key, _) in stored)
            if (!fresh.ContainsKey(key) && !changed.Contains(key))
                changed.Add(key);
        changed.Sort(StringComparer.Ordinal);
        return changed;
    }

    private Task<Prepared> PrepareAsync(ActionProposalRow proposal, ServerActionPermit permit, CancellationToken ct)
    {
        var target = JsonNode.Parse(proposal.TargetJson)!.AsObject();
        if (proposal.Tool == DarlingMcpPlanForceTools.KillToolName && target.ContainsKey("pid"))
            return PreparePgKillAsync(proposal, permit, target, ct);
        return Task.FromResult(new Prepared(null, null, new ProposalPressResult("refused", NotSupported,
            $"Approving {proposal.Tool} is not built yet. Nothing was run, and the proposal stays pending until it expires.")));
    }

    /// <summary>The PostgreSQL kill: the stored pid and start second, the prepare step's dry read, and the binding in the shape the proposal
    /// stored it.</summary>
    private async Task<Prepared> PreparePgKillAsync(ActionProposalRow proposal, ServerActionPermit permit, JsonObject target, CancellationToken ct)
    {
        var pid = target["pid"]!.GetValue<int>();
        var startEpoch = target["backend_start_epoch"]!.GetValue<long>();
        var prepared = await DarlingMcpPlanForceTools.PrepareKillBackendAsync(_owner, _logger, permit, proposal.ServerId, permit.ServerName, pid,
            startEpoch, ct);
        if (prepared.FixedAnswer is not null)
            return new Prepared(null, null, new ProposalPressResult("failed", prepared.RecordReason ?? "prepare_failed",
                $"The facts could not be read from {permit.ServerName}. Nothing was run, and the proposal is still waiting."));
        if (prepared.NotDone is { } notDone)
            return new Prepared(null, null, new ProposalPressResult(notDone.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                notDone.Reason ?? "dry_run_failed", notDone.Message + " Nothing was run."), Supersede: notDone.Outcome == RemediationOutcome.Refused);

        var binding = prepared.Binding!;
        var executor = prepared.Executor!;
        var facts = JsonSerializer.SerializeToNode(DarlingMcpPlanForceTools.PgKillBindingImage(binding), McpHelpers.JsonOptions)!.AsObject();
        return new Prepared(facts, async (runPermit, token) =>
        {
            var written = await DarlingMcpPlanForceTools.WriteKillBackendAsync(executor, runPermit, binding, token);
            if (written.Outcome == RemediationOutcome.Unknown)
                return new ApprovedWriteOutcome("unknown", written.Message + " There is no undo.");
            if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                return new ApprovedWriteOutcome("failed", written.Message);
            return new ApprovedWriteOutcome("run", written.Message + " There is no undo.",
                JsonSerializer.Serialize(new { message = written.Message, still_there = written.PgStillThere }, McpHelpers.JsonOptions));
        }, null);
    }
}
