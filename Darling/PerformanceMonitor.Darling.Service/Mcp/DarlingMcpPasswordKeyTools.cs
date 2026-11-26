/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The password key verbs as MCP tools (#9644): <c>new_password_key</c> (<c>--new-password-key</c>), <c>reset_password_key</c>
/// (<c>--reset-password-key</c>) and <c>self_check_password_key</c> (<c>--self-check-password-key</c>). Each calls the code the verb
/// calls: <see cref="DarlingCliCommands.NewPasswordKey"/>, <see cref="DarlingCliCommands.ResetPasswordKeyInStoreAsync"/> and
/// <see cref="DarlingPasswordKeySelfCheck.Run"/>, with in-memory writers where the verb prints to the console, so the words are the verb's.
/// The first two are in <see cref="DarlingMcpWritePolicy.ServerAdminTools"/>: served only with <c>mcp.allowWrites</c> and
/// <c>mcp.allowServerAdmin</c>, as <c>add_servers</c> is. No answer holds a key: a new key goes to a file on the service's machine and
/// only its fingerprint comes back. The password key has no "pins" verb, so no tool is made for one (the service runs the one-time pin
/// step itself, at start and again on every sweep while it waits).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPasswordKeyTools
{
    internal const string NewKeyToolName = "new_password_key";
    internal const string ResetKeyToolName = "reset_password_key";
    internal const string SelfCheckToolName = "self_check_password_key";

    /// <summary>The refusal of a reset the caller did not confirm. The CLI verb takes no flag, so the tool asks for one: a reset
    /// makes every saved password be entered again.</summary>
    internal const string ResetNeedsConfirmText =
        "This marks the service's password key replaced, and every saved password sealed with it must then be entered again. Nothing was changed. Call again with confirm: true to do it.";

    internal const string StoreNotReadyText = "The service has not opened its store connection yet. Try again in a moment.";

    internal const string RelativePathText =
        "Give an absolute path. A relative one would resolve against the service's own working directory, which is not where anyone looks for a key file.";

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = NewKeyToolName), Description(
        "Writes a new password key to a file on the service's machine, for a passwordKey secret (the same as the --new-password-key verb), and returns only its fingerprint, never the key. " +
        "Needs mcp.allowWrites and mcp.allowServerAdmin. " +
        "It never writes over anything: a path that already holds a file, folder or link is refused and nothing is written. The file is readable only by the account the service runs as. " +
        "It changes nothing else: put the key in your secret store, point passwordKey in darling.json at it, then delete the file. Making a key does not seal or unseal any saved password: " +
        "a saved password is sealed with the key the store publishes, and only reset_password_key replaces that key, after which every saved password must be entered again. " +
        "Returns {status:\"written\"|\"refused\"|\"failed\", message}.")]
    public static string NewPasswordKey(
        [Description("An absolute path on the service's machine where nothing exists yet.")] string path,
        ILogger? logger = null)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(path) || path.StartsWith('-'))
            {
                return Outcome("refused", "Give the absolute path of a file that does not exist yet.");
            }

            if (!Path.IsPathRooted(path))
            {
                return Outcome("refused", RelativePathText);
            }

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = DarlingCliCommands.NewPasswordKey(new[] { path }, output, error);
            return exit == 0
                ? Outcome("written", output.ToString().Trim())
                : Outcome("failed", error.ToString().Trim());
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("{Tool} failed: {Message}", NewKeyToolName, ex.GetType().Name);
            return McpHelpers.FormatError(NewKeyToolName, ex);
        }
    }

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = ResetKeyToolName), Description(
        "Marks the service's current password key as replaced (the same as the --reset-password-key verb). Needs mcp.allowWrites and mcp.allowServerAdmin. " +
        "What it does to the secrets sealed with the old key: they can no longer be opened, so every saved password sealed to it (the SQL logins of the registered servers, the remediation logins and the mail server password) must be entered again. " +
        "The answer says how many. Without passwordKey in darling.json the next service start retires the key file and makes a new key: restart the service. With passwordKey set the service never makes a key, so put a new one in that source (new_password_key makes one) and then restart. " +
        "Key files are not touched. Until the restart the service refuses to seal or open a saved password. " +
        "Pass confirm: true to do it; without it nothing is changed. " +
        "Returns {status:\"reset\"|\"nothing_to_reset\"|\"refused\"|\"failed\", saved_passwords_to_re_enter, message}.")]
    public static Task<string> ResetPasswordKey(
        [Description("Must be true. A reset makes every saved password be entered again.")] bool confirm = false,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ResetPasswordKeyAsync(confirm, ownerSource?.Read(), confirm && KeyComesFromSetting(), logger, cancellationToken);

    /// <summary>The reset behind the tool, with the owner connection and the <c>passwordKey</c> fact handed in.</summary>
    internal static async Task<string> ResetPasswordKeyAsync(
        bool confirm, Npgsql.NpgsqlDataSource? owner, bool fromSetting, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            if (!confirm)
            {
                return Outcome("refused", ResetNeedsConfirmText);
            }

            if (owner is null)
            {
                return Outcome("refused", StoreNotReadyText);
            }

            var outcome = await DarlingCliCommands.ResetPasswordKeyInStoreAsync(owner, fromSetting, cancellationToken).ConfigureAwait(false);
            logger?.LogWarning("Password key reset through MCP: {Sealed} saved password(s) now need to be entered again.", outcome.SealedPasswords);
            return JsonSerializer.Serialize(new
            {
                status = outcome.Replaced ? "reset" : "nothing_to_reset",
                saved_passwords_to_re_enter = outcome.SealedPasswords,
                message = outcome.Text,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("{Tool} failed: {Message}", ResetKeyToolName, ex.GetType().Name);
            return Outcome("failed", "Could not reset the password key in the store. The service log has the details.");
        }
    }

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = SelfCheckToolName), Description(
        "Runs the password key self-check (the same as the hidden --self-check-password-key verb): it makes and reloads keys in a temporary folder on the service's machine, checks the Unix file permission, owner and link checks, and opens a sealed test vector. " +
        "It reads and changes no key and no stored password, and removes its temporary folder. It checks the Unix file calls, so it runs on Linux or macOS only: on Windows it is refused. " +
        "The vector is a test file kept in the repository (Darling.Tests/Fixtures/PasswordSeal/linux-self-check-vector.json) that a normal install does not carry, so this is a build check, not a health check of the live key. " +
        "Returns {status:\"passed\"|\"failed\"|\"refused\", message, lines}.")]
    public static string SelfCheckPasswordKey(
        [Description("The path of the test vector file on the service's machine.")] string vector_path,
        ILogger? logger = null)
    {
        try
        {
            if (!DarlingPasswordKeySelfCheck.CanRunHere)
            {
                return Outcome("refused", DarlingPasswordKeySelfCheck.UnsupportedPlatformLine);
            }

            if (string.IsNullOrWhiteSpace(vector_path))
            {
                return Outcome("refused", "Usage: " + DarlingPasswordKeySelfCheck.UsageLine);
            }

            using var output = new StringWriter();
            using var error = new StringWriter();
            var exit = DarlingPasswordKeySelfCheck.Run(new[] { vector_path }, output, error);
            var lines = (output.ToString() + error.ToString()).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            return JsonSerializer.Serialize(new
            {
                status = exit == 0 ? "passed" : "failed",
                message = lines.Length == 0 ? string.Empty : lines[^1],
                lines,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("{Tool} failed: {Message}", SelfCheckToolName, ex.GetType().Name);
            return McpHelpers.FormatError(SelfCheckToolName, ex);
        }
    }

    /// <summary>Whether darling.json sets <c>passwordKey</c>, which changes the closing sentence of a reset: the same read the verb
    /// makes. A config that cannot be read costs only that wording.</summary>
    private static bool KeyComesFromSetting()
    {
        try
        {
            return !string.IsNullOrWhiteSpace(DarlingConfig.Load().PasswordKey);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
