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
using System.IO;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Cross-platform secret indirection (#1804): every plaintext secret slot in darling.json — a monitored
/// server's <c>password</c>, <c>smtp.password</c>, the mcp/web <c>network.token</c> — also accepts a
/// REFERENCE instead of a literal: <c>env:NAME</c> reads the named environment variable, and
/// <c>file:/path</c> reads the file's contents (trimmed — compose <c>secrets:</c> mounts and 0600
/// credential files both end with a newline). A reference is not a secret ON DISK IN THE CONFIG, which is
/// the whole point: it does not trip the plaintext warnings, and it is the supported shape on Linux where
/// DPAPI (<see cref="DarlingSecrets"/>) does not exist. On Windows the DPAPI fields stay preferred and
/// byte-for-byte unchanged — this is an addition beside them, useful to BYO-store shops on any OS.
///
/// <para>A literal password that genuinely starts with <c>env:</c> or <c>file:</c> cannot be expressed
/// inline — put it in a file and reference it. The prefixes are matched case-sensitively and only at the
/// start, so nothing else changes meaning.</para>
/// </summary>
public static class DarlingSecretSource
{
    private const string EnvPrefix = "env:";
    private const string FilePrefix = "file:";

    /* Where a "readable by ordinary users" warning about a file: target goes. The resolvers are static and run from
       config properties with no logger of their own; the service points this at its logger when it starts. A secret
       is resolved on every use, so each path is reported once per process. */
    private static Action<string>? _fileAccessWarning;
    private static readonly HashSet<string> s_warnedPaths = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Routes the warning a world-readable <c>file:</c> secret raises to <paramref name="sink"/>; null
    /// drops it (the CLI, and every test that does not ask for it).</summary>
    public static void ReportFileAccessTo(Action<string>? sink)
    {
        _fileAccessWarning = sink;
        lock (s_warnedPaths)
        {
            s_warnedPaths.Clear();
        }
    }

    /// <summary>
    /// Warns once per path when a <c>file:</c> target can be read by ordinary local users: the file holds the secret
    /// in the clear, so read access is the secret. Windows only: the ACL check is, and in a container the container
    /// boundary is the control.
    /// </summary>
    private static void WarnWhenReadableByOrdinaryUsers(string path, string settingName)
    {
        if (_fileAccessWarning is not { } warn
            || !OperatingSystem.IsWindows()
            || !DarlingFileSecurity.IsReadableByOrdinaryUsers(path))
        {
            return;
        }

        lock (s_warnedPaths)
        {
            if (!s_warnedPaths.Add(path))
            {
                return;
            }
        }

        warn(
            $"{settingName}: secret file '{path}' is readable by Users, Authenticated Users, Everyone or INTERACTIVE, so " +
            "any local account can read this secret. Restrict it to SYSTEM, Administrators and the service account.");
    }

    /// <summary>True when <paramref name="value"/> is a reference rather than a literal — the callers'
    /// plaintext warnings key on this (a reference is not plaintext-in-config).</summary>
    public static bool IsReference(string? value)
        => value is not null
           && (value.StartsWith(EnvPrefix, StringComparison.Ordinal)
               || value.StartsWith(FilePrefix, StringComparison.Ordinal));

    /// <summary>The sentence every request path answers with when a password field holds a reference. It names no
    /// value: the text the caller typed is never repeated.</summary>
    internal const string RequestReferenceRefusalText = ServerConnectionRule.ReferenceRefusedText;

    /// <summary>
    /// The one question a request path asks of a password it was handed (web add, web edit, MCP add_servers, MCP
    /// edit_server, the test_connect command): null when the text is a password to use as typed, otherwise
    /// <see cref="RequestReferenceRefusalText"/>. It is <see cref="IsReference"/> and nothing else, so what is refused is
    /// exactly what <see cref="Resolve"/> would have dereferenced: same prefixes, same case rule, same start-of-text
    /// rule, no more and no less. A password that only contains <c>env:</c> after its first character is a password.
    /// </summary>
    internal static string? RequestReferenceRefusal(string? value)
        => IsReference(value) ? RequestReferenceRefusalText : null;

    /// <summary>
    /// Resolves a secret slot's value: a reference is dereferenced (a missing/empty target is a
    /// configuration error naming BOTH the setting and the target, never a silent empty secret); a
    /// literal passes through untouched.
    /// </summary>
    public static string Resolve(string value, string settingName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        if (value.StartsWith(EnvPrefix, StringComparison.Ordinal))
        {
            var name = value.Substring(EnvPrefix.Length).Trim();
            if (name.Length == 0)
            {
                throw new InvalidOperationException($"{settingName}: 'env:' reference names no environment variable.");
            }

            /* Whitespace-only is as empty as empty (#3914): the file branch below trims its contents, and a caller
               that treats a blank result as "not set" must never receive one from a variable that IS set. */
            var resolved = Environment.GetEnvironmentVariable(name);
            if (string.IsNullOrWhiteSpace(resolved))
            {
                throw new InvalidOperationException(
                    $"{settingName}: environment variable '{name}' is not set (or empty or blank) — the referenced secret cannot resolve.");
            }

            return resolved;
        }

        if (value.StartsWith(FilePrefix, StringComparison.Ordinal))
        {
            var path = value.Substring(FilePrefix.Length).Trim();
            if (path.Length == 0)
            {
                throw new InvalidOperationException($"{settingName}: 'file:' reference names no path.");
            }

            string contents;
            try
            {
                contents = File.ReadAllText(path);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(CouldNotRead(settingName, path, ex), ex);
            }

            WarnWhenReadableByOrdinaryUsers(path, settingName);

            /* Trimmed: compose secrets and hand-written credential files end with a newline, and a
               trailing newline inside a password is never what the operator meant. */
            var trimmed = contents.Trim();
            if (trimmed.Length == 0)
            {
                throw new InvalidOperationException(
                    $"{settingName}: secret file '{path}' is empty — the referenced secret cannot resolve.");
            }

            return trimmed;
        }

        return value;
    }

    /// <summary>
    /// The refusal for a <c>file:</c> secret that could not be read (#1804, #9793). On Windows it is the setting, the path
    /// and the exception's own text. On Unix (the platform comes from <see cref="ComposeCredentialDirectoryGuard.UnixModes"/>,
    /// which a test stands in) a missing file says it is not there and, for a compose secret, that <c>docker compose up -d</c>
    /// mounts it after the compose file is edited and <c>docker compose restart</c> does not. A file that is there but
    /// denied names the account the service runs as (id and name) and the file's owner and mode where a stat can read them,
    /// never the file's content or length. When the service is not root (#8141: the image runs as 1654, and a 3.12 compose
    /// install wrote <c>secrets/*</c> as 0600 files of the host user) it also adds what to do: for a compose secret, the two
    /// upgrade lines from the Container user section of docs/darling/security.md and the restart; elsewhere, the path and
    /// the user that needs to read it. This read happens while the config loads, before the credentials-directory refusal
    /// can run, so it has to say both lines.
    /// </summary>
    internal static string CouldNotRead(string settingName, string path, Exception ex)
    {
        var refusal = $"{settingName}: secret file '{path}' could not be read: {ex.Message}";
        if (ComposeCredentialDirectoryGuard.Current.UnixModes is not { } modes)
        {
            return refusal;
        }

        var lead = refusal.TrimEnd('.') + ". ";
        if (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return lead + MissingFileAdvice(settingName, path);
        }

        if (ex is not UnauthorizedAccessException)
        {
            return refusal;
        }

        var euid = modes.EffectiveUserId;
        var facts = $"The service runs as {DescribeAccount(euid, modes.EffectiveUserName)}" + DescribeFile(modes, path) + ".";
        if (euid == 0)
        {
            return lead + facts;
        }

        facts = facts.TrimEnd('.') + ", which cannot read it. ";
        return path.StartsWith(ComposeSecretsMount, StringComparison.Ordinal)
            ? lead + facts + "If this install ran as root before, run these once on the host, in the compose folder: "
                + $"{ComposeSecretsModeFix}, then {DarlingManagedRoles.ComposeVolumeOwnershipCommand(euid)}; "
                + "then restart the service (docker compose restart darling). See Container user in docs/darling/security.md."
            : lead + facts + $"Give that user read access to '{path}', then restart the service.";
    }

    /// <summary>What to do about a secret file that is not there (#9793). A compose secret exists in the container only
    /// when <c>docker compose up -d</c> has recreated it with the secret listed, which a restart does not do. Shared with the
    /// password key's own refusal, which reads a <c>file:</c> reference through <see cref="DarlingPasswordKeyFileReader"/>.</summary>
    internal static string MissingFileAdvice(string settingName, string path)
    {
        if (path.StartsWith(ComposeSecretsMount, StringComparison.Ordinal) && path.Length > ComposeSecretsMount.Length)
        {
            var secret = path[ComposeSecretsMount.Length..];
            return $"The file is not there: '{path}' is where compose mounts the secret '{secret}'. "
                + "Compose mounts a secret only when 'docker compose up -d' runs after the secret is listed under the service and under secrets: in docker-compose.yml, "
                + "and 'docker compose restart' does not mount it.";
        }

        return $"The file is not there: '{path}'. Create it, or correct the path in {settingName}, then restart the service. "
            + "In a container, a file mounted from the host appears only after 'docker compose up -d' recreates the container with the mount listed in docker-compose.yml.";
    }

    private static string DescribeAccount(uint userId, string? userName)
    {
        var id = userId.ToString(CultureInfo.InvariantCulture);
        return string.IsNullOrWhiteSpace(userName) ? $"user {id}" : $"user {id} ({userName})";
    }

    /// <summary>The file's owner and mode as a stat gives them, which needs no read of the file. Empty when neither can be
    /// told, so the message says only what it knows.</summary>
    private static string DescribeFile(IUnixDirectoryModes modes, string path)
    {
        string? owner = null;
        string? mode = null;
        try
        {
            if (modes.OwnerOf(path) is { } found)
            {
                owner = $"user {found.UserId.ToString(CultureInfo.InvariantCulture)}";
            }
        }
        catch (Exception lookup) when (lookup is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            // A stat that fails leaves out the part it would have answered.
        }

        try
        {
            mode = Convert.ToString((int)modes.Get(path) & 0xFFF, 8).PadLeft(4, '0');
        }
        catch (Exception lookup) when (lookup is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            // The owner is still worth saying when the mode cannot be read.
        }

        return (owner, mode) switch
        {
            (not null, not null) => $". The file is owned by {owner} and its mode is {mode}",
            (not null, null) => $". The file is owned by {owner}",
            (null, not null) => $". The file's mode is {mode}",
            _ => "",
        };
    }

    /// <summary>
    /// The log line for a network token that could not be resolved (#9793). <c>ResolveToken</c> reads <c>encryptedToken</c>
    /// whenever it is set, on any OS, and the token reference only when it is not, so which setting was read picks the line.
    /// An <c>encryptedToken</c> is a DPAPI blob that may have been made on another machine or account, so its line says to make
    /// a new one here, or to use a token reference on another OS. A token reference (<c>file:</c>, <c>env:</c> or a literal) is
    /// no blob, so its line names what failed and why (the exception's text names the file, the account and the owner and
    /// mode, or the missing variable) and stops, with the consequence the caller states.
    /// </summary>
    /// <param name="tokenName">The token as the log names it, such as "MCP network token" or "Web dashboard token".</param>
    /// <param name="section">The config section, <c>mcp.network</c> or <c>web.network</c>.</param>
    /// <param name="consequence">What the failure stops, as a sentence.</param>
    /// <param name="detail">The exception's message.</param>
    /// <param name="encrypted">Whether <c>encryptedToken</c> is set, so it is the setting that was read.</param>
    internal static string DescribeTokenReadFailure(string tokenName, string section, string consequence, string detail, bool encrypted)
        => encrypted
            ? $"{tokenName} ({section}.encryptedToken) could not be decrypted ({detail}). {consequence} "
                + "A token made on another machine or account cannot be read here. "
                + $"Run PerformanceMonitor.Darling.Service.exe --encrypt-password on this machine and put the new value in {section}.encryptedToken. "
                + $"On another OS, remove it and set {section}.token to an env: or file: reference."
            : $"{tokenName} could not be read ({detail}). {consequence}";

    /// <summary>Where compose mounts a service's secrets.</summary>
    internal const string ComposeSecretsMount = "/run/secrets/";

    /// <summary>The host command that lets the container's user read the compose folder's files while
    /// <c>secrets/</c> stays closed to other host users (#8141) and a password key kept there is left alone (#8290);
    /// docs/darling/security.md gives the same line.</summary>
    internal const string ComposeSecretsModeFix = "chmod 700 secrets && chmod 644 darling.json && find secrets -type f ! -name password_key -exec chmod 644 {} +";
}
