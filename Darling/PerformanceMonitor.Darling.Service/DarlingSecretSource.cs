/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;

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

    /// <summary>True when <paramref name="value"/> is a reference rather than a literal — the callers'
    /// plaintext warnings key on this (a reference is not plaintext-in-config).</summary>
    public static bool IsReference(string? value)
        => value is not null
           && (value.StartsWith(EnvPrefix, StringComparison.Ordinal)
               || value.StartsWith(FilePrefix, StringComparison.Ordinal));

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
                throw new InvalidOperationException(
                    $"{settingName}: secret file '{path}' could not be read: {ex.Message}", ex);
            }

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
}
