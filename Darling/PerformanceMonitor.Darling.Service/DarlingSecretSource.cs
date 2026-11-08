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
    /// The refusal for a <c>file:</c> secret that could not be read (#1804). When the read was denied on Unix and the
    /// service is not root (#8141: the image runs as 1654, and a 3.12 compose install wrote <c>secrets/*</c> as 0600 files
    /// of the host user), it adds what to do: for a compose secret, the two upgrade lines from the Container user section
    /// of docs/darling/security.md and the restart; elsewhere, the path and the user that needs to read it. This read
    /// happens while the config loads, before the credentials-directory refusal can run, so it has to say both lines.
    /// </summary>
    internal static string CouldNotRead(string settingName, string path, Exception ex)
    {
        var refusal = $"{settingName}: secret file '{path}' could not be read: {ex.Message}";
        if (ex is not UnauthorizedAccessException
            || ComposeCredentialDirectoryGuard.Current.UnixModes is not { } modes
            || modes.EffectiveUserId == 0)
        {
            return refusal;
        }

        var euid = modes.EffectiveUserId;
        var lead = $"{refusal.TrimEnd('.')}. The service runs as user {euid.ToString(CultureInfo.InvariantCulture)}, which cannot read it; ";
        return path.StartsWith(ComposeSecretsMount, StringComparison.Ordinal)
            ? lead + "if this install ran as root before, run these once on the host, in the compose folder: "
                + $"{ComposeSecretsModeFix}, then {DarlingManagedRoles.ComposeVolumeOwnershipCommand(euid)}; "
                + "then restart the service (docker compose restart darling). See Container user in docs/darling/security.md."
            : lead + $"give that user read access to '{path}', then restart the service.";
    }

    /// <summary>Where compose mounts a service's secrets.</summary>
    internal const string ComposeSecretsMount = "/run/secrets/";

    /// <summary>The host command that lets the container's user read the compose folder's files while
    /// <c>secrets/</c> stays closed to other host users (#8141); docs/darling/security.md gives the same line.</summary>
    internal const string ComposeSecretsModeFix = "chmod 700 secrets && chmod 644 darling.json secrets/*";
}
