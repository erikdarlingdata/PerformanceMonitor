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

    /// <summary>The current owned set (empty until a config has been loaded).</summary>
    public static DarlingOwnedSet Current => s_current;

    public static void Set(DarlingOwnedSet set) => s_current = set ?? DarlingOwnedSet.Empty;

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
        var envNames = new List<string>(DarlingWebSecretReferencePolicy.ServiceEnvNames);

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
