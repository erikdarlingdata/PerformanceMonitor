/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Command-line argument rules shared by startup and the headless <c>--test</c> run.</summary>
public static class ViewerArgs
{
    /// <summary>True when <paramref name="args"/>[<paramref name="i"/>] is <paramref name="option"/> and the next
    /// argument is its value: present and not itself a <c>--</c> flag (a dangling option has no value).</summary>
    private static bool HasValue(string[] args, int i, string option) =>
        string.Equals(args[i], option, StringComparison.OrdinalIgnoreCase)
        && i + 1 < args.Length
        && !args[i + 1].StartsWith("--", StringComparison.Ordinal);

    /// <summary>
    /// The explicit config path for a <c>--test</c> run, PURE: an explicit <c>--config &lt;path&gt;</c> pair
    /// wins; otherwise the startup convention applies (first argument that is not an option flag or an
    /// option pair's value — the startup rule, which skips
    /// any <c>--</c>-prefixed flag so <c>--test</c> itself, or the upgrade-takeover flag, is never
    /// mistaken for a file path). Null falls through to <see cref="ViewerSettings.ResolveConfigLocation"/>'s
    /// remaining rules (DARLING_CONFIG, then the conventional locations), exactly like startup.
    /// An option whose next argument is a <c>--</c> flag is dangling and has no value.
    /// </summary>
    public static string? ExplicitConfigPath(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            if (HasValue(args, i, "--config"))
            {
                return args[i + 1];
            }
        }

        for (var i = 0; i < args.Length; i++)
        {
            if (HasValue(args, i, "--open-server") || HasValue(args, i, "--config"))
            {
                i++; /* Skip the option's value too. */
                continue;
            }

            if (args[i].StartsWith("--", StringComparison.Ordinal))
            {
                continue; /* A bare flag, or a dangling option, is never a config path. */
            }

            return args[i];
        }

        return null;
    }

    /// <summary>The value following <c>--open-server</c>, or null when absent or dangling.</summary>
    public static string? OpenServerName(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            if (HasValue(args, i, "--open-server"))
            {
                return args[i + 1];
            }
        }

        return null;
    }

    /// <summary>
    /// The servers a <c>--open-server</c> name answers to, PURE: every server whose <see cref="DarlingServer.ServerName"/>
    /// equals the name, in any letter case. The caller opens a server only when exactly one answers.
    ///
    /// <para>Several databases on one Azure SQL Database server are separate servers. Once the service has
    /// connected to one, its name is its storage name (<c>host:database</c>) and is its own. Until then the list
    /// names it by its host (the managed-servers read falls back to the host for a server with no registry row yet),
    /// and the host is the same for all of them, so a name can answer to several servers and must not open the
    /// first of them.</para>
    /// </summary>
    public static IReadOnlyList<DarlingServer> ServersNamed(IEnumerable<DarlingServer> servers, string name) =>
        servers.Where(s => string.Equals(s.ServerName, name, StringComparison.OrdinalIgnoreCase)).ToList();
}
