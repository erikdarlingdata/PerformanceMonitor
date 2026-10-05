/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The files and environment variables that belong to Darling itself.</summary>
public sealed record DarlingOwnedSet(IReadOnlyList<string> Paths, IReadOnlyList<string> EnvNames)
{
    public static DarlingOwnedSet Empty { get; } = new(Array.Empty<string>(), Array.Empty<string>());
}

/// <summary>
/// Process-wide holder for the owned set. A static is used because <c>DarlingConfig.Load</c> is called
/// separately by the worker, the MCP host and the web host, and the server-add core is a static method with
/// no dependency injection: each host populates the holder the same way from <c>Load</c>, and the add core
/// reads it. Written once at startup (<see cref="Set"/>), then read; the reference swap is atomic.
/// </summary>
public static class DarlingOwnedSecrets
{
    private static volatile DarlingOwnedSet s_current = DarlingOwnedSet.Empty;
    private const int MaxDepth = 8;

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

    /// <summary>The current owned set (empty until a config has been loaded).</summary>
    public static DarlingOwnedSet Current => s_current;

    public static void Set(DarlingOwnedSet set) => s_current = set ?? DarlingOwnedSet.Empty;

    /// <summary>One sentence for every refusal; it names no path and no variable.</summary>
    internal const string ReferenceRefusalText =
        "That password reference points at this service's own configuration or secrets.";

    private const int MaxLinkHops = 40;

    private static StringComparison EnvComparison =>
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>Null when <paramref name="password"/> is not an <c>env:</c>/<c>file:</c> reference or the reference
    /// points at nothing Darling owns; otherwise <see cref="ReferenceRefusalText"/>.</summary>
    internal static string? ReferenceRefusal(string? password) => ReferenceRefusal(password, s_current);

    internal static string? ReferenceRefusal(string? password, DarlingOwnedSet owned)
    {
        if (string.IsNullOrWhiteSpace(password))
        {
            return null;
        }

        var value = password.Trim();
        if (value.StartsWith("file:", StringComparison.Ordinal))
        {
            return FileRefused(value["file:".Length..].Trim(), owned) ? ReferenceRefusalText : null;
        }

        if (value.StartsWith("env:", StringComparison.Ordinal))
        {
            var name = value["env:".Length..].Trim();
            return owned.EnvNames.Any(n => string.Equals(n, name, EnvComparison)) ? ReferenceRefusalText : null;
        }

        return null;
    }

    private static bool FileRefused(string path, DarlingOwnedSet owned)
    {
        if (IsRefusedForm(path))
        {
            return true;
        }

        var real = RealPath(path);
        if (real is null)
        {
            return true;
        }

        foreach (var ownedPath in owned.Paths)
        {
            if (string.IsNullOrWhiteSpace(ownedPath) || ownedPath.Contains('\0'))
            {
                continue;
            }

            var ownedReal = RealPath(ownedPath);
            if (ownedReal is not null && Covers(ownedReal, real))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Forms no real secret path needs, refused before any comparison.</summary>
    private static bool IsRefusedForm(string path)
    {
        if (path.Length == 0 || path.Contains('\0') || !Path.IsPathRooted(path) || path[0] == '~')
        {
            return true;
        }

        if (path.StartsWith("\\\\", StringComparison.Ordinal) || path.StartsWith("//", StringComparison.Ordinal))
        {
            return true; /* UNC root, and the \\?\ and \\.\ prefixes */
        }

        if (path.Split('/', '\\').Contains(".."))
        {
            return true;
        }

        var hasDrive = path.Length >= 2 && char.IsAsciiLetter(path[0]) && path[1] == ':';
        if (path.IndexOf(':', hasDrive ? 2 : 0) >= 0)
        {
            return true; /* alternate data stream */
        }

        return path.Equals("/proc", StringComparison.Ordinal) || path.StartsWith("/proc/", StringComparison.Ordinal)
            || path.Equals("/sys", StringComparison.Ordinal) || path.StartsWith("/sys/", StringComparison.Ordinal);
    }

    /// <summary>The path with every symbolic link followed (capped; null on a cycle or an unresolvable path).</summary>
    private static string? RealPath(string path)
    {
        try
        {
            var full = Path.GetFullPath(path);
            var root = Path.GetPathRoot(full) ?? "";
            var queue = new LinkedList<string>(full[root.Length..].Split(
                new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries));
            var current = root;
            var hops = 0;
            while (queue.First is { } head)
            {
                queue.RemoveFirst();
                var part = head.Value;
                if (part == ".")
                {
                    continue;
                }

                if (part == "..")
                {
                    current = Path.GetDirectoryName(current) ?? root;
                    continue;
                }

                var next = Path.Combine(current, part);
                var target = new DirectoryInfo(next).LinkTarget;
                if (target is null)
                {
                    current = next;
                    continue;
                }

                if (++hops > MaxLinkHops)
                {
                    return null;
                }

                var targetFull = Path.IsPathRooted(target) ? target : Path.Combine(current, target);
                var targetRoot = Path.GetPathRoot(targetFull) ?? "";
                current = targetRoot;
                var parts = targetFull[targetRoot.Length..].Split(
                    new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);
                for (var i = parts.Length - 1; i >= 0; i--)
                {
                    queue.AddFirst(parts[i]);
                }
            }

            return Path.GetFullPath(current);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException or IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static bool Covers(string directory, string path)
    {
        var dir = Path.TrimEndingDirectorySeparator(directory);
        if (path.Equals(dir, PathComparison) || path.Equals(directory, PathComparison))
        {
            return true;
        }

        return path.Length > dir.Length
            && path.StartsWith(dir, PathComparison)
            && (path[dir.Length] == Path.DirectorySeparatorChar || path[dir.Length] == Path.AltDirectorySeparatorChar);
    }

    /// <summary>Walks string properties of the graph by reflection and returns every env:/file: value as written.</summary>
    internal static List<string> CollectReferences(object root)
    {
        var found = new List<string>();
        Walk(root, found, new HashSet<object>(ReferenceEqualityComparer.Instance), 0, pathsOnly: false, paths: null);
        return found;
    }

    private static bool IsOurs(Type t) => t.Assembly == typeof(DarlingOwnedSecrets).Assembly;

    private static void Walk(object? node, List<string> refs, HashSet<object> seen, int depth, bool pathsOnly, List<string>? paths)
    {
        if (node is null || depth > MaxDepth || !seen.Add(node))
        {
            return;
        }

        if (node is string)
        {
            return;
        }

        if (node is IEnumerable items)
        {
            foreach (var item in items)
            {
                if (item is string str)
                {
                    Visit(str, null, refs, paths);
                }
                else if (item is not null && IsOurs(item.GetType()))
                {
                    Walk(item, refs, seen, depth + 1, pathsOnly, paths);
                }
            }

            return;
        }

        if (!IsOurs(node.GetType()))
        {
            return;
        }

        foreach (var prop in node.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (prop.GetIndexParameters().Length != 0 || !prop.CanRead)
            {
                continue;
            }

            if (Attribute.IsDefined(prop, typeof(System.Text.Json.Serialization.JsonIgnoreAttribute)))
            {
                continue;
            }

            object? value;
            try
            {
                value = prop.GetValue(node);
            }
            catch (Exception ex) when (ex is TargetInvocationException or InvalidOperationException)
            {
                continue;
            }

            if (value is string s)
            {
                Visit(s, prop.Name, refs, paths);
            }
            else if (value is not null && !value.GetType().IsPrimitive && !value.GetType().IsEnum)
            {
                Walk(value, refs, seen, depth + 1, pathsOnly, paths);
            }
        }
    }

    private static void Visit(string value, string? propName, List<string> refs, List<string>? paths)
    {
        if (DarlingSecretSource.IsReference(value))
        {
            refs.Add(value);
        }
        else if (paths is not null && propName is not null && value.Length > 0
                 && (propName.EndsWith("Path", StringComparison.Ordinal) || propName.EndsWith("Directory", StringComparison.Ordinal)))
        {
            paths.Add(value);
        }
    }

    /// <summary>Builds the owned set for a loaded config: config directory, every referenced file/env var,
    /// path-valued settings, the store data directory, and the managed store's credential/key/log files that
    /// sit in that directory's PARENT.</summary>
    internal static DarlingOwnedSet Compute(DarlingConfig config, string configPath)
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
            /* An unresolvable config path owns nothing we can name. */
        }

        foreach (var r in config.SecretReferencesAsWritten)
        {
            if (r.StartsWith("file:", StringComparison.Ordinal))
            {
                paths.Add(r["file:".Length..].Trim());
            }
            else
            {
                envNames.Add(r["env:".Length..].Trim());
            }
        }

        var pathValues = new List<string>();
        Walk(config, new List<string>(), new HashSet<object>(ReferenceEqualityComparer.Instance), 0, false, pathValues);
        paths.AddRange(pathValues);

        var data = config.Postgres?.DataDirectory;
        if (!string.IsNullOrWhiteSpace(data))
        {
            paths.Add(data);
            try
            {
                var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(data)));
                if (!string.IsNullOrEmpty(parent))
                {
                    foreach (var name in new[]
                    {
                        DarlingManagedPostgres.CredentialFileName,
                        DarlingManagedPostgres.AdminCredentialFileName,
                        DarlingManagedPostgres.ViewerCredentialFileName,
                        DarlingManagedPostgres.McpCredentialFileName,
                        DarlingManagedPostgres.ServerCertFileName,
                        DarlingManagedPostgres.ServerKeyFileName,
                        DarlingManagedPostgres.ServerLogFileName,
                    })
                    {
                        paths.Add(Path.Combine(parent, name));
                    }
                }
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
            {
                /* Unresolvable data directory: nothing nameable beyond the raw value above. */
            }
        }

        return new DarlingOwnedSet(
            paths.Where(p => !string.IsNullOrWhiteSpace(p)).Distinct(StringComparer.Ordinal).ToList(),
            envNames.Where(e => !string.IsNullOrWhiteSpace(e)).Distinct(StringComparer.OrdinalIgnoreCase).ToList());
    }
}
