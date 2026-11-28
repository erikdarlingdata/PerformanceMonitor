/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The log line for a collector a target's gate turns off, and the memory that writes it at INFO once.
/// <see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/> decides whether the collector runs; this type only
/// describes the target that decision was made on (#9938). The line names every fact on the target that a gate can read
/// (hosting platform, RDS edition class, msdb access), then the engine edition and major version, so a skip on an RDS
/// instance reads as RDS rather than as a statement about SQL Server 2022 Enterprise.
/// <para>The skip repeats on every cycle for as long as the target stays the same. <see cref="ShouldLogAtInfo"/> is true for
/// the first line of a server and collector, and again only when the line's text, which carries every fact, changes (a
/// reconnect that finds a different platform, a GRANT that gives msdb access). The caller writes every other repeat at DEBUG.</para>
/// <para>State is in memory, keyed by server id and collector name, so it is bounded by servers times collectors. A restart
/// writes each line once more at INFO. <see cref="Forget"/> and <see cref="ForgetExcept"/> drop a removed server's entries.</para>
/// </summary>
internal sealed class CollectorSkipNotice
{
    private readonly object _lock = new();
    private readonly Dictionary<(int ServerId, string CollectorName), string> _lastLine = new();

    /// <summary>
    /// The line for <paramref name="collectorName"/> skipped on <paramref name="target"/>:
    /// <c>agent_status not collected on this target (AWS RDS, engine edition 3, version 16)</c>. The facts that are true come
    /// first, in a fixed order; with none true the parentheses hold only the edition and version, which is then the whole
    /// description of the target (a version-floor gate).
    /// </summary>
    internal static string Describe(string collectorName, CollectorTargetInfo target, int engineEdition)
    {
        var facts = new List<string>(5);

        if (target.IsAzureSqlDb)
        {
            facts.Add("Azure SQL Database");
        }

        if (target.IsAzureManagedInstance)
        {
            facts.Add("Azure SQL Managed Instance");
        }

        if (target.IsAwsRds)
        {
            facts.Add("AWS RDS");
        }

        if (target.IsAwsRdsWebOrExpress)
        {
            facts.Add("RDS Web or Express edition");
        }

        if (!target.HasMsdbAccess)
        {
            facts.Add("no msdb access");
        }

        facts.Add(string.Create(CultureInfo.InvariantCulture, $"engine edition {engineEdition}"));
        facts.Add(string.Create(CultureInfo.InvariantCulture, $"version {target.SqlMajorVersion}"));

        return $"{collectorName} not collected on this target ({string.Join(", ", facts)})";
    }

    /// <summary>
    /// True when <paramref name="line"/> is the first one for this server and collector, or differs from the last one
    /// recorded for them; records it either way. Thread-safe: collectors on different servers run in parallel.
    /// </summary>
    internal bool ShouldLogAtInfo(int serverId, string collectorName, string line)
    {
        lock (_lock)
        {
            var key = (serverId, collectorName);
            if (_lastLine.TryGetValue(key, out var previous) && string.Equals(previous, line, StringComparison.Ordinal))
            {
                return false;
            }

            _lastLine[key] = line;
            return true;
        }
    }

    /// <summary>Drops every entry for a server that was removed, so a server added again logs its skips at INFO again.</summary>
    internal void Forget(int serverId)
    {
        lock (_lock)
        {
            foreach (var key in new List<(int ServerId, string CollectorName)>(_lastLine.Keys))
            {
                if (key.ServerId == serverId)
                {
                    _lastLine.Remove(key);
                }
            }
        }
    }

    /// <summary>Drops every entry for a server not in <paramref name="activeServerIds"/>.</summary>
    internal void ForgetExcept(HashSet<int> activeServerIds)
    {
        lock (_lock)
        {
            foreach (var key in new List<(int ServerId, string CollectorName)>(_lastLine.Keys))
            {
                if (!activeServerIds.Contains(key.ServerId))
                {
                    _lastLine.Remove(key);
                }
            }
        }
    }

    /// <summary>The number of server and collector pairs held; for tests.</summary>
    internal int Count
    {
        get
        {
            lock (_lock)
            {
                return _lastLine.Count;
            }
        }
    }
}
