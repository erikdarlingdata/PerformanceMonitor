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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The Server actions tab's data and writes for the Viewer (#6269). The list is read from the store as the Viewer's role (every
/// role reads <c>config.config_server_actions</c>); a change goes through the service, because no role but the store owner writes the table.</summary>
public sealed partial class ViewerDataService
{
    /// <summary>The command type for a person's switch change (#6269); matches the service executor's <c>set_server_action</c> case.</summary>
    public const string CommandSetServerAction = "set_server_action";

    /// <summary>The wait for a switch change's answer: the service itself waits up to 60 s for an agent change in progress on that server, then answers (#6269).</summary>
    public static readonly TimeSpan SetServerActionTimeout = TimeSpan.FromSeconds(75);

    /// <summary>Every monitored server with a state for each action (a server with no row reads off).</summary>
    public Task<IReadOnlyList<ServerActionSwitchRow>> ReadServerActionsAsync(CancellationToken cancellationToken = default) =>
        ServerActionSwitches.ListAsync(_dataSource, cancellationToken);

    /// <summary>
    /// Turns one server's action switch on or off as <paramref name="login"/> (sent as <c>requested_by</c>; the service journals it as that
    /// person, surface viewer). A refusal comes back as the answer; a read-only role throws <see cref="ViewerReadOnlyException"/>.
    /// </summary>
    public async Task<ServerActionAnswer> SetServerActionAsync(int serverId, string action, bool enabled, string login, CancellationToken cancellationToken = default)
    {
        _ = AgentChangeUndo.PersonActor(login);
        var args = System.Text.Json.JsonSerializer.Serialize(new { action, enabled });
        var commandId = await EnqueueCommandAsync(CommandSetServerAction, serverId, args, login, cancellationToken);
        var result = await PollCommandResultAsync(commandId, SetServerActionTimeout, cancellationToken);
        if (result is null)
        {
            return new ServerActionAnswer(false, "timed_out", "The service has not answered yet. Read the list again before trying twice.");
        }

        try
        {
            await DeleteCommandAsync(commandId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The row holds no secret; the service-side reaper is the backstop. */
        }

        return ServerActionAnswer.Read(result.ResultJson, result.ResultStatus);
    }
}

/// <summary>What the service answered to one switch change: whether it took, the status word, and the sentence to show.</summary>
public sealed record ServerActionAnswer(bool Succeeded, string Status, string Message)
{
    /// <summary>Reads the service's result_json: <c>{status: set|unchanged|refused, code, message}</c>, or <c>{error}</c> when the command itself failed.</summary>
    public static ServerActionAnswer Read(string? json, string? resultStatus)
    {
        try
        {
            if (json is not null && JsonNode.Parse(json) is JsonObject o)
            {
                var status = o["status"] is JsonValue s && s.TryGetValue(out string? sv) ? sv : null;
                var message = o["message"] is JsonValue m && m.TryGetValue(out string? mv) ? mv : null;
                if (status is not null && message is not null)
                    return new ServerActionAnswer(status is "set" or "unchanged", status, message);
                if (o["error"] is JsonValue e && e.TryGetValue(out string? ev) && !string.IsNullOrWhiteSpace(ev))
                    return new ServerActionAnswer(false, resultStatus ?? "failed", ev);
            }
        }
        catch (System.Text.Json.JsonException)
        {
            /* fall through to the plain sentence */
        }

        return new ServerActionAnswer(false, resultStatus ?? "failed", "The service could not change the switch.");
    }
}

/// <summary>One row of the Server actions grid (#6269): one server and one action its engine can run.</summary>
public sealed class ServerActionViewRow
{
    public ServerActionViewRow(ServerActionSwitchRow server, ServerActionSwitchState state)
    {
        ArgumentNullException.ThrowIfNull(server);
        ArgumentNullException.ThrowIfNull(state);
        Server = server;
        State = state;
    }

    public ServerActionSwitchRow Server { get; }

    public ServerActionSwitchState State { get; }

    public int ServerId => Server.ServerId;

    public string ServerName => Server.ServerName;

    /// <summary>The engine label from the shared parser, so pg, aurora and mixed-case spellings read PostgreSQL like the store treats them.</summary>
    public string EngineDisplay => Server.EngineKind == PerformanceMonitor.Collectors.CollectorTargetEngine.PostgreSql ? "PostgreSQL" : "SQL Server";

    /// <summary>False for a switch that is on but that the server's engine cannot run (an engine edit left it behind). It can only be turned off.</summary>
    public bool CanRun => State.Valid;

    public const string EngineCannotRunNote = "This server's engine can't run this action. Turn it off to clear it.";

    /// <summary>The sentence shown beside a switch that is on but that the engine cannot run; empty otherwise.</summary>
    public string Note => State.Valid ? "" : EngineCannotRunNote;

    public string Action => State.Action;

    public string ActionDisplay => ActionLabel(State.Action);

    public bool Enabled => State.Enabled;

    public string StateDisplay => State.Enabled ? "ON" : "off";

    /// <summary>Who changed the switch, without the journal's <c>person:</c> prefix (an MCP caller keeps its own spelling).</summary>
    public string ChangedBy => WithoutPersonPrefix(State.SetBy);

    internal static string WithoutPersonPrefix(string? setBy)
    {
        const string prefix = InvestigationPersonWrites.PersonActorPrefix;
        return setBy is null ? "" : setBy.StartsWith(prefix, StringComparison.Ordinal) && setBy.Length > prefix.Length ? setBy[prefix.Length..] : setBy;
    }

    /// <summary>The change time, the sort key of the Changed column.</summary>
    public DateTime? ChangedAtUtc => State.UpdatedAtUtc;

    public string ChangedDisplay => State.UpdatedAtUtc is DateTime at ? at.ToLocalTime().ToString("g", CultureInfo.CurrentCulture) : "";

    /// <summary>One row per server and per action its engine can run, plus any switch that is on but that the engine cannot run (listed with a note so it can be turned off).</summary>
    public static IReadOnlyList<ServerActionViewRow> From(IEnumerable<ServerActionSwitchRow> servers) =>
        servers.SelectMany(s => s.Actions.Where(a => a.Valid || a.Enabled).Select(a => new ServerActionViewRow(s, a))).ToList();

    /// <summary>An action in plain words.</summary>
    public static string ActionLabel(string action) => ServerActionNames.Label(action);

    private static string Lets(string action) => action switch
    {
        ServerActionNames.ForcePlan => "force or unforce a Query Store plan",
        ServerActionNames.EvictPlan => "evict a plan from the plan cache",
        ServerActionNames.KillSession => "kill a session",
        ServerActionNames.RecompileModule => "recompile a procedure, function or trigger",
        ServerActionNames.CreatePlanGuide => "create a plan guide",
        ServerActionNames.DropPlanGuide => "drop a plan guide",
        var other => other,
    };

    /// <summary>What the person is told before a switch is turned ON: what an agent may then do, what it needs, and that it is journaled.</summary>
    public static string WarningText(string serverName, string action) =>
        "Turning this on lets agents change " + serverName + ": they will be able to " + Lets(action) + " on it." + Environment.NewLine + Environment.NewLine
        + "It also needs the remediation login for this server, with the grants the Darling README describes. "
        + "Every agent change is journaled, and can be undone where that is possible." + Environment.NewLine + Environment.NewLine
        + "Turn it on?";

    /// <summary>The check before a change: what is wrong with it, or null.</summary>
    internal static string? ChangeError(ServerActionViewRow? selected, bool turnOn) =>
        selected is null ? "Select a server and an action first."
        : turnOn && !selected.CanRun ? EngineCannotRunNote
        : selected.Enabled == turnOn ? selected.ActionDisplay + " is already " + (turnOn ? "on" : "off") + " for " + selected.ServerName + "."
        : null;
}
