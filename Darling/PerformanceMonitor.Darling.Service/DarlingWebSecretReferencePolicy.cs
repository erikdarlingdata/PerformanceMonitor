/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What the web add-server route may resolve as a secret reference: the operator's allowlist, plus the paths and
/// environment names the service itself owns (refused even when an allowlist entry would cover them).
/// </summary>
public sealed record WebSecretReferenceScope(
    IReadOnlyList<string> Allowed,
    IReadOnlyCollection<string> ServiceOwnedPaths,
    IReadOnlyCollection<string> ServiceOwnedEnvNames)
{
    /// <summary>The default: no reference is accepted.</summary>
    public static WebSecretReferenceScope Empty { get; } = new(Array.Empty<string>(), Array.Empty<string>(), Array.Empty<string>());
}

/// <summary>
/// Gate for <c>env:</c> / <c>file:</c> passwords on the web add-server route. The host resolves a reference while it
/// tests the connection, so an unrestricted reference would let an editor read any host file or environment
/// variable. A reference is accepted only when an entry of <c>web.serverAddSecretReferences</c> covers it, and never
/// when it names something the service owns.
/// </summary>
internal static class DarlingWebSecretReferencePolicy
{
    private const string FilePrefix = "file:";
    private const string EnvPrefix = "env:";

    /// <summary>One sentence for every refusal, so the answer never reveals whether a path exists or which rule fired.</summary>
    internal const string RefusalText =
        "This dashboard isn't configured to accept that secret reference. Add the server through MCP, or ask the operator to allow its path in web.serverAddSecretReferences.";

    /// <summary>Environment variables the service itself reads. A census test keeps this complete.</summary>
    internal static readonly IReadOnlySet<string> ServiceEnvNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "DARLING_CONFIG",
        "DARLING_OUTPUT_FORMAT",
        "DARLING_STOPPED_MARKER",
        "DOTNET_RUNNING_IN_CONTAINER",
        "SystemDrive",
        "USERPROFILE",
    };

    private static StringComparison EnvComparison =>
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>Null when the value is not a reference or an allowlist entry covers it; otherwise <see cref="RefusalText"/>.</summary>
    internal static string? Refusal(string? password, WebSecretReferenceScope scope) =>
        Refusal(password, scope.Allowed, scope.ServiceOwnedPaths, scope.ServiceOwnedEnvNames);

    internal static string? Refusal(
        string? password,
        IReadOnlyList<string> allowed,
        IReadOnlyCollection<string> serviceOwnedPaths,
        IReadOnlyCollection<string> serviceOwnedEnvNames)
    {
        if (string.IsNullOrWhiteSpace(password))
        {
            return null;
        }

        var value = password.Trim();
        if (value.StartsWith(FilePrefix, StringComparison.OrdinalIgnoreCase))
        {
            return FileAllowed(value[FilePrefix.Length..].Trim(), allowed, serviceOwnedPaths) ? null : RefusalText;
        }

        if (value.StartsWith(EnvPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return EnvAllowed(value[EnvPrefix.Length..].Trim(), allowed, serviceOwnedEnvNames) ? null : RefusalText;
        }

        return null;
    }

    private static bool FileAllowed(string path, IReadOnlyList<string> allowed, IReadOnlyCollection<string> owned)
    {
        if (!TryCanonical(path, out var full))
        {
            return false;
        }

        foreach (var ownedPath in owned)
        {
            if (TryCanonical(ownedPath, out var ownedFull) && Covers(ownedFull, full))
            {
                return false;
            }
        }

        foreach (var entry in allowed)
        {
            if (entry.StartsWith(FilePrefix, StringComparison.OrdinalIgnoreCase)
                && TryCanonical(entry[FilePrefix.Length..].Trim(), out var dir)
                && Covers(dir, full))
            {
                return true;
            }
        }

        return false;
    }

    private static bool EnvAllowed(string name, IReadOnlyList<string> allowed, IReadOnlyCollection<string> owned)
    {
        if (name.Length == 0)
        {
            return false;
        }

        foreach (var ownedName in owned)
        {
            if (string.Equals(ownedName, name, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        foreach (var entry in allowed)
        {
            if (entry.StartsWith(EnvPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var prefix = entry[EnvPrefix.Length..].Trim();
                if (prefix.Length > 0 && name.StartsWith(prefix, EnvComparison))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>True when <paramref name="path"/> is the directory itself or lies under it (ends at a separator).</summary>
    private static bool Covers(string directory, string path)
    {
        var dir = directory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (path.Equals(dir, PathComparison))
        {
            return true;
        }

        return path.Length > dir.Length
            && path.StartsWith(dir, PathComparison)
            && (path[dir.Length] == Path.DirectorySeparatorChar || path[dir.Length] == Path.AltDirectorySeparatorChar);
    }

    private static bool TryCanonical(string path, out string full)
    {
        full = "";
        if (path.Length == 0 || path.Contains('\0') || !Path.IsPathRooted(path))
        {
            return false;
        }

        foreach (var segment in path.Split('/', '\\'))
        {
            if (segment == "..")
            {
                return false;
            }
        }

        try
        {
            full = Path.GetFullPath(path);
            return true;
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return false;
        }
    }

    /// <summary>The paths and env names the loaded configuration makes the service's own.</summary>
    internal static WebSecretReferenceScope ScopeFor(DarlingConfig config, string configPath)
    {
        var paths = new List<string>();
        var envNames = new List<string>(ServiceEnvNames);

        try
        {
            var dir = Path.GetDirectoryName(Path.GetFullPath(configPath));
            if (!string.IsNullOrEmpty(dir))
            {
                paths.Add(dir);
            }
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            /* An unresolvable config path owns nothing we can name; the other sources still apply. */
        }

        void AddReference(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            var v = value.Trim();
            if (v.StartsWith(FilePrefix, StringComparison.OrdinalIgnoreCase))
            {
                paths.Add(v[FilePrefix.Length..].Trim());
            }
            else if (v.StartsWith(EnvPrefix, StringComparison.OrdinalIgnoreCase))
            {
                envNames.Add(v[EnvPrefix.Length..].Trim());
            }
        }

        AddReference(config.Smtp?.Password);
        AddReference(config.Mcp?.Network?.Token);
        AddReference(config.Web?.Network?.Token);
        AddReference(config.Web?.Network?.Tls?.PfxPassword);
        AddReference(config.Postgres?.ConnectionString);
        AddReference(config.Postgres?.WebConnectionString);
        AddReference(config.Postgres?.McpConnectionString);
        if (!string.IsNullOrWhiteSpace(config.Postgres?.DataDirectory))
        {
            paths.Add(config.Postgres.DataDirectory);
        }

        var tls = config.Web?.Network?.Tls;
        foreach (var p in new[] { tls?.PfxPath, tls?.CertPath, tls?.KeyPath })
        {
            if (!string.IsNullOrWhiteSpace(p))
            {
                paths.Add(p);
            }
        }

        foreach (var server in config.Servers ?? new List<MonitoredServer>())
        {
            AddReference(server.Password);
        }

        return new WebSecretReferenceScope(
            config.Web?.ServerAddSecretReferences ?? new List<string>(), paths, envNames);
    }
}
