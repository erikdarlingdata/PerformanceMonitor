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
using System.Text;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The shipped <c>provision-roles.sql</c> no longer matches what the renderer expects: a site it replaces is
/// missing or appears a different number of times. The message names every such site with its expected and actual
/// counts.</summary>
internal sealed class ProvisionRolesRenderException : InvalidOperationException
{
    public ProvisionRolesRenderException(string message) : base(message)
    {
    }
}

/// <summary>
/// Renders the shipped <c>Darling/tools/provision-roles.sql</c> for a store owner (#8779): SCRAM verifiers in place of the
/// four password placeholders, and the real owner and database names at the sites the script's BEFORE RUNNING section
/// tells a person to change. Nothing else is touched. Each site is matched as full statement text (an ordinal substring,
/// so line endings stay as they are), counted before anything is replaced, and the render refuses when any count differs.
/// Pure: no database access.
/// </summary>
internal static class ProvisionRolesRenderer
{
    private const string OwnerSite = "ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA ";
    private const string RevokeSite = "REVOKE ALL ON DATABASE darling FROM PUBLIC;";
    private const string GrantWebSite = "GRANT CONNECT ON DATABASE darling TO admin, viewer, web;";
    private const string GrantMcpSite = "GRANT CONNECT ON DATABASE darling TO mcp;";
    private const string SearchPathNoteSite = "--       ALTER DATABASE darling SET search_path = collect, config, public;";

    private const int MinPasswordLength = 16;

    internal static IReadOnlyList<(string Text, int ExpectedCount)> Sites { get; } =
    [
        (OwnerSite, 5),
        (RevokeSite, 1),
        (GrantWebSite, 1),
        (GrantMcpSite, 1),
        (SearchPathNoteSite, 1),
        ("CHANGE_ME_ADMIN_PASSWORD", 3),
        ("CHANGE_ME_VIEWER_PASSWORD", 3),
        ("CHANGE_ME_WEB_PASSWORD", 4),
        ("CHANGE_ME_MCP_PASSWORD", 3),
    ];

    /// <summary>The script with every site replaced. <paramref name="rolePasswords"/> holds the keys admin, viewer, web
    /// and mcp.</summary>
    internal static string Render(string shippedScript, string owner, string database, IReadOnlyDictionary<string, string> rolePasswords)
    {
        ArgumentNullException.ThrowIfNull(shippedScript);
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(rolePasswords);

        var nameRefusal = CheckName("owner", owner) ?? CheckName("database", database);
        if (nameRefusal is not null)
        {
            throw new ProvisionRolesRenderException(nameRefusal);
        }

        var mismatches = new List<string>();
        foreach (var (text, expected) in Sites)
        {
            var actual = Count(shippedScript, text);
            if (actual != expected)
            {
                mismatches.Add(string.Create(CultureInfo.InvariantCulture, $"\"{text}\": expected {expected}, found {actual}"));
            }
        }

        if (mismatches.Count > 0)
        {
            throw new ProvisionRolesRenderException(
                "The shipped provision-roles.sql no longer matches the renderer, so it was not rendered. " + string.Join("; ", mismatches) + ".");
        }

        var quotedOwner = ProvisioningTarget.QuoteIdentifier(owner);
        var quotedDatabase = ProvisioningTarget.QuoteIdentifier(database);

        var rendered = shippedScript
            .Replace(OwnerSite, "ALTER DEFAULT PRIVILEGES FOR ROLE " + quotedOwner + " IN SCHEMA ", StringComparison.Ordinal)
            .Replace(RevokeSite, "REVOKE ALL ON DATABASE " + quotedDatabase + " FROM PUBLIC;", StringComparison.Ordinal)
            .Replace(GrantWebSite, "GRANT CONNECT ON DATABASE " + quotedDatabase + " TO admin, viewer, web;", StringComparison.Ordinal)
            .Replace(GrantMcpSite, "GRANT CONNECT ON DATABASE " + quotedDatabase + " TO mcp;", StringComparison.Ordinal)
            .Replace(SearchPathNoteSite, "--       ALTER DATABASE " + quotedDatabase + " SET search_path = collect, config, public;", StringComparison.Ordinal);

        foreach (var role in new[] { "admin", "viewer", "web", "mcp" })
        {
            if (!rolePasswords.TryGetValue(role, out var password))
            {
                throw new ArgumentException("No password was given for role " + role + ".", nameof(rolePasswords));
            }

            var placeholder = "CHANGE_ME_" + role.ToUpperInvariant() + "_PASSWORD";
            rendered = rendered.Replace(placeholder, ScramSha256Verifier.Create(password), StringComparison.Ordinal);
        }

        return rendered;
    }

    /// <summary>Null when the owner or database name is fit to put into the script, otherwise the refusal sentence. A name with any
    /// control character (a line feed or carriage return, a tab, NUL) is refused: one site is a <c>--</c> comment, which a line
    /// break ends, and the rest of the name would run as script lines under the owner login, psql meta-commands included. The
    /// sentence names the kind of name, the character's code point and its position, and never the value.</summary>
    internal static string? CheckName(string kind, string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        for (var i = 0; i < name.Length; i++)
        {
            if (char.IsControl(name[i]))
            {
                return string.Create(CultureInfo.InvariantCulture,
                    $"The {kind} name contains a control character (U+{(int)name[i]:X4} at position {i}); it is refused, because the script would run what follows it as statements under the owner login.");
            }
        }

        return null;
    }

    /// <summary>Null when the password is accepted, otherwise the refusal sentence (which names the role and never the
    /// value). One trailing newline is dropped, as <c>file:</c> secrets do.</summary>
    internal static string? CheckRolePassword(string role, string raw, out string password)
    {
        ArgumentNullException.ThrowIfNull(raw);
        password = raw.EndsWith('\n') ? raw[..^1] : raw;

        if (password.Length == 0)
        {
            return "The password for role " + role + " is empty.";
        }

        if (password.Contains('\r', StringComparison.Ordinal))
        {
            return "The password for role " + role + " contains a carriage return; write the file with Unix line endings.";
        }

        if (password.Length < MinPasswordLength)
        {
            return "The password for role " + role + " is shorter than " + MinPasswordLength.ToString(CultureInfo.InvariantCulture) + " characters.";
        }

        if (!password.All(IsAllowedPasswordCharacter))
        {
            return "The password for role " + role + " has a character outside A-Z a-z 0-9 . _ ~ + / = -.";
        }

        return null;
    }

    /// <summary>One <c>.pgpass</c> line; a backslash and a colon in a field are escaped with a backslash.</summary>
    internal static string PgPassLine(string host, int port, string database, string user, string password) =>
        string.Join(':', EscapePgPass(host), port.ToString(CultureInfo.InvariantCulture), EscapePgPass(database), EscapePgPass(user), EscapePgPass(password));

    /// <summary>The libpq environment, one <c>KEY='value'</c> line each, for a shell that sources it.</summary>
    internal static string LibpqEnvFile(string host, int port, string database, string user, string libpqSslMode, string? rootCertificate)
    {
        var sb = new StringBuilder();
        AppendEnv(sb, "PGHOST", host);
        AppendEnv(sb, "PGPORT", port.ToString(CultureInfo.InvariantCulture));
        AppendEnv(sb, "PGDATABASE", database);
        AppendEnv(sb, "PGUSER", user);
        AppendEnv(sb, "PGSSLMODE", libpqSslMode);
        if (!string.IsNullOrEmpty(rootCertificate))
        {
            AppendEnv(sb, "PGSSLROOTCERT", rootCertificate);
        }

        AppendEnv(sb, "PGAPPNAME", "darling-provision-roles");
        return sb.ToString();
    }

    internal static string ToLibpqSslMode(SslMode mode) => mode switch
    {
        SslMode.Disable => "disable",
        SslMode.Allow => "allow",
        SslMode.Prefer => "prefer",
        SslMode.Require => "require",
        SslMode.VerifyCA => "verify-ca",
        SslMode.VerifyFull => "verify-full",
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Not an Npgsql SslMode."),
    };

    private static bool IsAllowedPasswordCharacter(char c) =>
        c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '.' or '_' or '~' or '+' or '/' or '=' or '-';

    private static string EscapePgPass(string field) =>
        field.Replace("\\", "\\\\", StringComparison.Ordinal).Replace(":", "\\:", StringComparison.Ordinal);

    private static void AppendEnv(StringBuilder sb, string key, string value) =>
        sb.Append(key).Append("='").Append(value.Replace("'", "'\\''", StringComparison.Ordinal)).Append("'\n");

    private static int Count(string text, string site)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(site, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += site.Length;
        }

        return count;
    }
}
