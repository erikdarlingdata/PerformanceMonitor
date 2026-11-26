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
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9641: the four command line verbs for external alert sources (<c>--add-alert-source</c>, <c>--rotate-alert-source</c>,
/// <c>--remove-alert-source</c>, <c>--list-alert-sources</c>, #5654) as MCP tools. They run the verbs' own code:
/// <see cref="DarlingCliCommands.ChangeAlertSourceAsync"/> makes the change and <see cref="DarlingCliCommands.PrintAlertSourceResult"/> words the
/// outcome, so the text an operator reads at the command line and the <c>report</c> a tool answers with are one text.
/// <para>
/// <b>The three writes create, change and drop a database login</b>, which the <c>mcp</c> store role cannot do. They run on the store owner's
/// data source the worker publishes (<see cref="StoreOwnerSource"/>, the seat the web host's token routes use), not on the MCP data source, and they
/// are served only under <c>mcp.allowWrites</c> and <c>mcp.allowServerAdmin</c> together, as <c>add_servers</c> is
/// (<see cref="DarlingMcpWritePolicy.ServerAdminTools"/>). The list reads the registry on the MCP data source (the role holds SELECT on it) and is
/// served like any read.
/// </para>
/// <para>
/// <b>The password.</b> The answer of an add or a rotate holds the new login's password once, inside the connection strings of its
/// <c>report</c>. Nothing here logs it, puts it in an exception message or writes it to the store: only its SCRAM verifier reaches the server
/// (<see cref="ExternalAlertSourceAdmin"/>), and the one log line a call writes names the source and the outcome. The host's write-call log
/// (<see cref="McpToolLatencyFilter"/>) names the tool, the caller and an outcome word, never the answer.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpAlertSourceTools
{
    internal const string AddToolName = "add_alert_source";
    internal const string RotateToolName = "rotate_alert_source";
    internal const string RemoveToolName = "remove_alert_source";
    internal const string ListToolName = "list_alert_sources";

    internal const string NoOwnerMessage =
        "The service has not published its store owner connection yet, so the login cannot be created, changed or dropped. Try again shortly. Nothing was changed.";

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "add_alert_source"), Description(
        "Creates an EXTERNAL ALERT SOURCE: a database login of its own that can do one thing, call config.submit_external_alert, plus its registry row, the same as the command line's --add-alert-source. Changes shared monitoring configuration and creates a database login. " +
        "<<GUIDE>>" +
        "THE ANSWER HOLDS THE NEW LOGIN'S PASSWORD, in the connection strings inside report, shown ONCE: Darling keeps no copy, nothing logs it, and a lost one is replaced with rotate_alert_source. Do not repeat it in notes, tickets or chat. The password enters this client's transcript, and when the mcp block has no TLS block the network listener is plain HTTP, so it crosses the network unencrypted, as the bearer token does. " +
        "name is 2 to 31 characters, a lower-case letter first, then lower-case letters, digits or underscores. The login is darling_src_<name>. A name already registered, or a login that already exists and is not a registered source, is refused and nothing is changed. " +
        "Answer: {status:\"added\", source, login, report} when the login was created, or {status:\"needs_dba_sql\", source, login, report} when the store's owner may not create logins: the source is registered, the password is in report, and report holds the statements a database administrator runs before it works. " +
        "notes, when present, names a step after the commit that did not finish, and the change stands. A refusal is {status:\"refused\", reason, message}. Undo: remove_alert_source. " +
        "Needs mcp.allowWrites AND mcp.allowServerAdmin.")]
    public static Task<string> AddAlertSource(
        StoreOwnerSource owner,
        PostgresConfig postgres,
        [Description("The source's name: 2 to 31 characters, a lower-case letter first, then lower-case letters, digits or underscores (for example 'netmon').")] string name,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ChangeAsync(AddToolName, "--add-alert-source", owner.Read(), postgres, name, logger, cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "rotate_alert_source"), Description(
        "Gives a registered EXTERNAL ALERT SOURCE a new password, the same as the command line's --rotate-alert-source. The old password stops working for a new connection at once, and the login's open sessions are ended when the store owner may end them. Changes a database login. " +
        "<<GUIDE>>" +
        "THE ANSWER HOLDS THE NEW PASSWORD, in the connection strings inside report, shown ONCE: Darling keeps no copy and nothing logs it. Do not repeat it in notes, tickets or chat. The password enters this client's transcript, and when the mcp block has no TLS block the network listener is plain HTTP, so it crosses the network unencrypted, as the bearer token does. " +
        "name is a source list_alert_sources shows. A name no source has, or a source whose login is missing or was not made by add_alert_source, is refused and nothing is changed. " +
        "Answer: {status:\"rotated\", source, login, report}, or {status:\"needs_dba_sql\", source, login, report} when the store's owner may not change logins: nothing is changed yet, and report holds the statements a database administrator runs. " +
        "Without confirm=true nothing is changed: the answer is {status:\"confirm_required\", source, login, open_sessions, message}, which says which login gets a new password and how many open sessions end. Call again with confirm=true to rotate. " +
        "A refusal is {status:\"refused\", reason, message}. " +
        "Needs mcp.allowWrites AND mcp.allowServerAdmin.")]
    public static Task<string> RotateAlertSource(
        StoreOwnerSource owner,
        PostgresConfig postgres,
        [Description("The source's name, as list_alert_sources shows it.")] string name,
        [Description("false (the default) changes nothing and answers {status:\"confirm_required\"} with what rotate would do. Pass true to rotate.")] bool confirm = false,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ChangeAsync(RotateToolName, "--rotate-alert-source", owner.Read(), postgres, name, logger, cancellationToken, confirm);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "remove_alert_source"), Description(
        "Removes an EXTERNAL ALERT SOURCE, the same as the command line's --remove-alert-source: drops its database login and its registry row and ends the login's sessions, so it can no longer send alerts. Its queued alert rows go with it. Alerts it already sent stay in the alert history. Changes shared monitoring configuration and drops a database login. " +
        "<<GUIDE>>" +
        "name is a source list_alert_sources shows. A name no source has is refused and nothing is changed. " +
        "Answer: {status:\"removed\", source, login, report}, where report says whether the login was dropped, how many queued alert rows went with the source and how many of them were not yet delivered, or {status:\"needs_dba_sql\", source, login, report} when the store's owner may not drop logins: the registry row is gone and report holds the statements a database administrator runs. " +
        "Without confirm=true nothing is changed: the answer is {status:\"confirm_required\", source, login, undelivered_alerts, open_sessions, message}, which says which login and row go, how many undelivered queued alerts are deleted with them and how many open sessions end. Call again with confirm=true to remove. " +
        "A refusal is {status:\"refused\", reason, message}. There is no undo: add_alert_source makes a new login with a new password. " +
        "Needs mcp.allowWrites AND mcp.allowServerAdmin.")]
    public static Task<string> RemoveAlertSource(
        StoreOwnerSource owner,
        PostgresConfig postgres,
        [Description("The source's name, as list_alert_sources shows it.")] string name,
        [Description("false (the default) changes nothing and answers {status:\"confirm_required\"} with what remove would do. Pass true to remove.")] bool confirm = false,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ChangeAsync(RemoveToolName, "--remove-alert-source", owner.Read(), postgres, name, logger, cancellationToken, confirm);

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "list_alert_sources"), Description(
        "Lists the EXTERNAL ALERT SOURCES: each source's name, its database login, whether it is enabled, when it was created, when it last sent an alert and how many of its alerts are not yet delivered. The same as the command line's --list-alert-sources. No password: the store holds none. " +
        "<<GUIDE>>" +
        "Answer: {status:\"ok\", sources:[{source, login, enabled, created_at_utc, last_alert_sent_utc, undelivered}]}, status empty when there is none, or unavailable when the store has not been upgraded to the version that holds the external alert tables. " +
        "A source's alerts appear in get_alert_history with external_source set to its name. add_alert_source, rotate_alert_source and remove_alert_source manage the sources.")]
    public static async Task<string> ListAlertSources(
        NpgsqlDataSource postgres,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            var rows = await ExternalAlertSourceAdmin.ListAsync(connection, cancellationToken);
            if (rows is null)
            {
                return McpHelpers.Status("unavailable",
                    "This store has no external alert tables yet. Start the Darling service once so it upgrades the store.");
            }

            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", "No alert sources. add_alert_source makes one.");
            }

            var sources = new JsonArray();
            foreach (var row in rows)
            {
                sources.Add(new JsonObject
                {
                    ["source"] = row.Name,
                    ["login"] = row.RoleName,
                    ["enabled"] = row.Enabled,
                    ["created_at_utc"] = row.CreatedAt.UtcDateTime.ToString("o", CultureInfo.InvariantCulture),
                    ["last_alert_sent_utc"] = row.LastSubmit is { } last ? last.UtcDateTime.ToString("o", CultureInfo.InvariantCulture) : null,
                    ["undelivered"] = row.Undelivered,
                });
            }

            return new JsonObject { ["status"] = "ok", ["sources"] = sources }.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ListToolName, ex);
        }
    }

    /// <summary>
    /// One add, rotate or remove on the owner's data source. Pure of the host: a test passes a data source and a logger. The CLI's own change and
    /// printing code runs, so a refusal's text and a success's report are the command line's. <paramref name="verb"/> is the command line verb the
    /// code branches on.
    /// </summary>
    internal static async Task<string> ChangeAsync(
        string toolName, string verb, NpgsqlDataSource? ownerSource, PostgresConfig? postgres, string? name, ILogger? logger, CancellationToken cancellationToken,
        bool confirm = true)
    {
        if (!ExternalAlertSourceAdmin.IsValidName(name))
        {
            return Refused("invalid_name",
                "The name must be 2 to 31 characters, a lower-case letter first, then lower-case letters, digits or underscores. Nothing was changed.");
        }

        if (ownerSource is null)
        {
            return Refused("no_owner_connection", NoOwnerMessage);
        }

        try
        {
            await using var connection = await ownerSource.OpenConnectionAsync(cancellationToken);
            if (!confirm && verb != "--add-alert-source")
            {
                return await ConfirmRequiredAsync(verb, connection, name!, cancellationToken);
            }

            /* The commit is the point of no return, and the new password exists only in the answer. Every step after it runs to its end
               whatever the caller does, so a cancel can never swallow a password the login already has (ExternalAlertSourceAdmin, DarlingCliCommands). */
            var change = await DarlingCliCommands.ChangeAlertSourceAsync(verb, connection, name!, cancellationToken, postgres);
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);
            var exitCode = DarlingCliCommands.PrintAlertSourceResult(
                verb, change.Result, name!, ownerSource.ConnectionString, output, error, postgres, change.GroupExtras, change.LineDropFailure);
            logger?.LogInformation("Alert source {Source}: {Tool} finished as {Outcome}", name, toolName, change.Result.Outcome);

            if (exitCode != DarlingCliCommands.CollectorToggleExitCode.Success)
            {
                return Refused(ReasonOf(change.Result.Outcome), error.ToString().Trim());
            }

            var status = change.Result.Outcome == ExternalAlertSourceAdmin.Outcome.NeedsDbaSql
                ? "needs_dba_sql"
                : verb == "--add-alert-source" ? "added" : verb == "--rotate-alert-source" ? "rotated" : "removed";
            var notes = error.ToString().Trim();
            return new JsonObject
            {
                ["status"] = status,
                ["source"] = name,
                ["login"] = change.Result.RoleName,
                ["report"] = output.ToString().Trim(),
                ["notes"] = notes.Length == 0 ? null : notes,
            }.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("Alert source {Source}: {Tool} failed ({ErrorType})", name, toolName, ex.GetType().Name);
            return McpHelpers.FormatError(toolName, ex);
        }
    }

    /// <summary>
    /// What a rotate or a remove would do, as the <c>confirm_required</c> answer, or the <c>not_registered</c> refusal when the source is not
    /// registered. An unconfirmed call ends here either way and never reaches the change, so a source added under that name a moment later
    /// is not rotated or removed without a confirm. Reads only.
    /// </summary>
    private static async Task<string> ConfirmRequiredAsync(string verb, NpgsqlConnection connection, string name, CancellationToken cancellationToken)
    {
        var sources = await ExternalAlertSourceAdmin.ListAsync(connection, cancellationToken);
        var source = sources?.FirstOrDefault(row => string.Equals(row.Name, name, StringComparison.Ordinal));
        if (source is null)
        {
            return Refused("not_registered", $"No source named '{name}'. list_alert_sources shows the sources there are. Nothing was changed.");
        }

        var sessions = await ExternalAlertSourceAdmin.CountOpenSessionsAsync(connection, source.RoleName, cancellationToken);
        var rotate = verb == "--rotate-alert-source";
        var tool = rotate ? RotateToolName : RemoveToolName;
        var message = rotate
            ? FormattableString.Invariant(
                $"Rotating source '{name}' gives its login {source.RoleName} a new password. The old password stops working for new connections at once, and the {sessions} open session(s) of the login are ended. ")
                + "The new password is shown once, in the answer. Nothing was changed. Call again with confirm=true to rotate."
            : FormattableString.Invariant(
                $"Removing source '{name}' drops its login {source.RoleName} and its registry row, deletes its {source.Undelivered} undelivered queued alert(s), and ends the login's {sessions} open session(s). ")
                + "There is no undo. Nothing was changed. Call again with confirm=true to remove.";
        return new JsonObject
        {
            ["status"] = "confirm_required",
            ["tool"] = tool,
            ["source"] = name,
            ["login"] = source.RoleName,
            ["undelivered_alerts"] = rotate ? null : source.Undelivered,
            ["open_sessions"] = sessions,
            ["message"] = message,
        }.ToJsonString(McpHelpers.JsonOptions);
    }

    private static string ReasonOf(ExternalAlertSourceAdmin.Outcome outcome) => outcome switch
    {
        ExternalAlertSourceAdmin.Outcome.InvalidName => "invalid_name",
        ExternalAlertSourceAdmin.Outcome.NoTables => "no_tables",
        ExternalAlertSourceAdmin.Outcome.AlreadyRegistered => "already_registered",
        ExternalAlertSourceAdmin.Outcome.LoginExists => "login_exists",
        ExternalAlertSourceAdmin.Outcome.NotRegistered => "not_registered",
        ExternalAlertSourceAdmin.Outcome.LoginNotManaged => "login_not_managed",
        _ => "refused",
    };

    private static string Refused(string reason, string message) =>
        new JsonObject { ["status"] = "refused", ["reason"] = reason, ["message"] = message }.ToJsonString(McpHelpers.JsonOptions);
}
