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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The password key verbs as MCP tools (#9644): <c>new_password_key</c> (<c>--new-password-key</c>) and <c>reset_password_key</c>
/// (<c>--reset-password-key</c>). Each calls the code the verb calls: <see cref="DarlingCliCommands.WriteNewPasswordKey"/> and
/// <see cref="DarlingCliCommands.ResetPasswordKeyInStoreAsync"/>, so the words are the verb's. Both are in
/// <see cref="DarlingMcpWritePolicy.ServerAdminTools"/>: served only with <c>mcp.allowWrites</c> and <c>mcp.allowServerAdmin</c>, as
/// <c>add_servers</c> is. No answer holds a key: a new key goes to the service's own <c>pending-password-keys</c> folder
/// (<see cref="DarlingPendingPasswordKeys"/>) under a name the service makes, and only its path and fingerprint come back. The tool
/// takes no path. The password key has no "pins" verb, so no tool is made for one (the service runs the one-time pin step itself,
/// at start and again on every sweep while it waits). The self-check (<c>--self-check-password-key</c>) stays a command-line build check.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPasswordKeyTools
{
    internal const string NewKeyToolName = "new_password_key";
    internal const string ResetKeyToolName = "reset_password_key";

    /// <summary>Why a reset is refused when it is asked without the fingerprint of the key it would replace.</summary>
    internal const string ResetNeedsFingerprintText =
        "confirm_fingerprint is needed. Call without confirm first: the answer gives the key's fingerprint and how many saved passwords are lost. Nothing was changed.";

    internal const string StartupNotReadyText = "The service has not finished reading its configuration yet. Try again in a moment.";

    internal const string StoreNotReadyText = "The service has not opened its store connection yet. Try again in a moment.";

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "new_password_key"), Description(
        "Writes a new password key to the service's own pending-password-keys folder on its machine, for a passwordKey secret (the same as the --new-password-key verb), and returns the file's path and the key's fingerprint, never the key. " +
        "Takes no path: the folder is inside the folder that holds the service's active password key file, and the file is named new-password-key-<fingerprint>.key. " +
        "Needs mcp.allowWrites and mcp.allowServerAdmin. " +
        "The folder is created readable only by the account the service runs as, and an existing folder that is a link, is owned by another account or that anyone else can read or change is refused and left as it is. A key folder on a network location is refused before anything is made. " +
        "It is refused when the active password key comes from the passwordKey setting or the service has no key file yet, and when 5 key files are already waiting in the folder: install or delete them first. " +
        "It never writes over anything. It changes nothing else: put the key in your secret store, point passwordKey in darling.json at it, then delete the file. Making a key does not seal or unseal any saved password: " +
        "a saved password is sealed with the key the store publishes, and only reset_password_key replaces that key, after which every saved password must be entered again. " +
        "Returns {status:\"written\"|\"refused\"|\"failed\", message, path, fingerprint}.")]
    public static string NewPasswordKey(PasswordKeyStartupFacts? startup = null, ILogger? logger = null)
    {
        try
        {
            var facts = startup?.Read();
            if (facts is null)
            {
                return Outcome("refused", StartupNotReadyText);
            }

            if (facts.CredentialsDirectory is null)
            {
                logger?.LogWarning("{Tool} refused: the service could not work out the folder that holds its password key file when it started.", NewKeyToolName);
            }

            return NewPasswordKeyIn(facts.CredentialsDirectory, facts.KeyFromSetting, logger);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("{Tool} failed: {Message}", NewKeyToolName, ex.GetType().Name);
            return McpHelpers.FormatError(NewKeyToolName, ex);
        }
    }

    /// <summary>The write behind the tool, with the service's credentials folder (null when it cannot be found) and the
    /// <c>passwordKey</c> fact handed in.</summary>
    internal static string NewPasswordKeyIn(string? credentialsDirectory, bool fromSetting, ILogger? logger)
    {
        var outcome = DarlingPendingPasswordKeys.Create(credentialsDirectory, fromSetting, logger);
        if (!outcome.Written)
        {
            return Outcome(outcome.Failed ? "failed" : "refused", outcome.Message);
        }

        return JsonSerializer.Serialize(new
        {
            status = "written",
            message = outcome.Message,
            path = outcome.FilePath,
            fingerprint = outcome.Fingerprint,
        }, McpHelpers.JsonOptions);
    }

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "reset_password_key"), Description(
        "Marks the service's current password key as replaced (the same as the --reset-password-key verb). Needs mcp.allowWrites and mcp.allowServerAdmin. " +
        "A reset has no undo: the secrets sealed with the old key can no longer be opened, so every saved password sealed to it (the SQL logins of the registered servers, the remediation logins and the mail server password) must be entered again. " +
        "Call it first without confirm: nothing is changed, and the answer gives how many saved passwords are sealed and the current key's fingerprint. " +
        "To do it, call again with confirm: true and confirm_fingerprint set to that fingerprint. A missing or wrong fingerprint is refused and nothing is changed. " +
        "After a reset, without passwordKey in darling.json the next service start retires the key file and makes a new key: restart the service. With passwordKey set the service never makes a key, so put a new one in that source (new_password_key makes one) and then restart. " +
        "Key files are not touched. Until the restart the service refuses to seal or open a saved password. " +
        "Returns {status:\"reset\"|\"needs_confirmation\"|\"nothing_to_reset\"|\"refused\", saved_passwords_to_re_enter, key_fingerprint, message}.")]
    public static Task<string> ResetPasswordKey(
        [Description("Must be true to reset. A reset has no undo and makes every saved password be entered again.")] bool confirm = false,
        [Description("The fingerprint the call without confirm answered. Required with confirm: true.")] string? confirm_fingerprint = null,
        StoreOwnerSource? ownerSource = null,
        PasswordKeyStartupFacts? startup = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ResetPasswordKeyAsync(confirm, confirm_fingerprint, ownerSource?.Read(), confirm && startup?.Read()?.KeyFromSetting == true, logger, cancellationToken);

    /// <summary>The reset behind the tool, with the owner connection and the <c>passwordKey</c> fact handed in.</summary>
    internal static async Task<string> ResetPasswordKeyAsync(
        bool confirm, string? confirmFingerprint, Npgsql.NpgsqlDataSource? owner, bool fromSetting, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            if (owner is null)
            {
                return Outcome("refused", StoreNotReadyText);
            }

            if (!confirm)
            {
                var preview = await DarlingCliCommands.PreviewPasswordKeyResetAsync(owner, cancellationToken).ConfigureAwait(false);
                if (preview.Fingerprint is null)
                {
                    return Outcome("nothing_to_reset", DarlingCliCommands.NothingToResetText);
                }

                return JsonSerializer.Serialize(new
                {
                    status = "needs_confirmation",
                    saved_passwords_to_re_enter = preview.SealedPasswords,
                    key_fingerprint = preview.Fingerprint,
                    message = $"A reset marks the service's password key {preview.Fingerprint} replaced. It has no undo: the {preview.SealedPasswords.ToString(CultureInfo.InvariantCulture)} saved password(s) sealed with it can no longer be opened, and every one must be entered again. "
                        + $"Nothing was changed. To do it, call again with confirm: true and confirm_fingerprint: {preview.Fingerprint}.",
                }, McpHelpers.JsonOptions);
            }

            if (string.IsNullOrWhiteSpace(confirmFingerprint))
            {
                return Outcome("refused", ResetNeedsFingerprintText);
            }

            var outcome = await DarlingCliCommands.ResetPasswordKeyInStoreAsync(owner, fromSetting, cancellationToken, confirmFingerprint).ConfigureAwait(false);
            if (outcome.FingerprintMismatch)
            {
                return Outcome("refused", outcome.Text + " Call without confirm to get the current key's fingerprint.");
            }

            if (outcome.Replaced)
            {
                logger?.LogWarning("Password key reset through MCP: {Sealed} saved password(s) now need to be entered again.", outcome.SealedPasswords);
            }
            else
            {
                logger?.LogInformation("{Tool} found no published password key to reset.", ResetKeyToolName);
            }

            return JsonSerializer.Serialize(new
            {
                status = outcome.Replaced ? "reset" : "nothing_to_reset",
                saved_passwords_to_re_enter = outcome.SealedPasswords,
                message = outcome.Text,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The log gets the store's own error text, which names no key and no password.
            logger?.LogWarning("{Tool} failed: {Type}: {Message}", ResetKeyToolName, ex.GetType().Name, ex.Message);
            return McpHelpers.FormatError(ResetKeyToolName, ex);
        }
    }

    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
