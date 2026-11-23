/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingCliCommands
{
    /// <summary>The verb <see cref="ObjectUsageCompareAsync(string[], TextWriter, TextWriter, CancellationToken)"/> handles (#9349).</summary>
    public static bool IsObjectUsageCompareVerb(string arg) =>
        string.Equals(arg, "--object-usage-compare", StringComparison.OrdinalIgnoreCase);

    /// <summary>The exit code when the two reads gave different answers (#9349).</summary>
    internal const int ObjectUsageCompareDiffers = 3;

    /// <summary>The exit code when the stored chain cannot answer the window, so there is nothing to compare (#9349).</summary>
    internal const int ObjectUsageCompareNoChain = 4;

    /// <summary>What <c>--object-usage-compare</c> was asked for.</summary>
    internal sealed record ObjectUsageCompareArgs(string ServerName, string DatabaseName, int Days, string? ConfigPath);

    /// <summary>Parses <c>--object-usage-compare &lt;server&gt; &lt;database&gt; [days] [--config path]</c>. Days default to the tool's own default.</summary>
    internal static bool TryParseObjectUsageCompareArgs(string[] rest, out ObjectUsageCompareArgs? parsed, out string? errorMessage)
    {
        parsed = null;
        errorMessage = null;
        string? configPath = null;
        var positional = new List<string>();
        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            if (string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= rest.Length || rest[i + 1].StartsWith("--", StringComparison.Ordinal) || configPath is not null)
                {
                    errorMessage = configPath is not null ? "--config was given twice." : "--config needs a path.";
                    return false;
                }

                configPath = rest[++i];
                continue;
            }

            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                errorMessage = $"Unknown option {arg}.";
                return false;
            }

            positional.Add(arg);
        }

        if (positional.Count is < 2 or > 3)
        {
            errorMessage = "--object-usage-compare takes a server, a database and optionally a number of days.";
            return false;
        }

        var days = DarlingMcpObjectUsageTools.DefaultDaysBack;
        if (positional.Count == 3
            && (!int.TryParse(positional[2], NumberStyles.None, CultureInfo.InvariantCulture, out days)
                || days < 1 || days > CollectorScheduleDefaults.MaxRetentionDays))
        {
            errorMessage = $"days must be a whole number from 1 to {CollectorScheduleDefaults.MaxRetentionDays}.";
            return false;
        }

        parsed = new ObjectUsageCompareArgs(positional[0], positional[1], days, configPath);
        return true;
    }

    /// <summary>
    /// <c>--object-usage-compare &lt;server&gt; &lt;database&gt; [days]</c> (#9349): reads one database's object usage window both ways, from the
    /// stored chain and from every snapshot, and prints "identical" with both timings, or each difference. It reads only, so a login that can
    /// read the store is enough, and it never builds or changes a chain. Exit codes: 0 identical, 1 usage or configuration, 2 the store cannot
    /// be read, 3 the answers differ, 4 the chain cannot answer the window (nothing to compare).
    /// </summary>
    public static async Task<int> ObjectUsageCompareAsync(string[] rest, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        const string Verb = "--object-usage-compare";
        if (!TryParseObjectUsageCompareArgs(rest, out var args, out var parseError))
        {
            error.WriteLine(parseError);
            output.WriteLine($"Usage: {Verb} <server> <database> [days] [--config <path to darling.json>]");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var created = TryCreateCollectorStoreDataSource(Verb, args!.ConfigPath, error);
        if (created is null)
        {
            return CollectorToggleExitCode.UsageOrConfig;
        }

        await using var dataSource = created;
        try
        {
            var (serverId, label, serverError) = await InvestigationFeedbackExport.ResolveServerAsync(dataSource, args.ServerName, cancellationToken);
            if (serverError is not null || serverId is not { } id)
            {
                error.WriteLine(serverError ?? "Name a server.");
                return CollectorToggleExitCode.UsageOrConfig;
            }

            return await ObjectUsageCompareAsync(
                dataSource, id, label ?? args.ServerName, args.DatabaseName, DateTime.UtcNow.AddDays(-args.Days), output, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not read the store: {ex.Message}");
            return CollectorToggleExitCode.StoreUnavailable;
        }
    }

    /// <summary>The comparison on an open store, so a live test drives exactly what the command line does.</summary>
    internal static async Task<int> ObjectUsageCompareAsync(
        NpgsqlDataSource dataSource, int serverId, string serverLabel, string database, DateTime windowStartUtc, TextWriter output,
        CancellationToken cancellationToken)
    {
        output.WriteLine($"Object usage for {serverLabel}, database {database}, window from {windowStartUtc:yyyy-MM-dd HH:mm:ss} UTC.");

        var persistedClock = Stopwatch.StartNew();
        var (persisted, reason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, serverId, database, windowStartUtc, cancellationToken);
        persistedClock.Stop();

        var fullClock = Stopwatch.StartNew();
        var full = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, serverId, database, windowStartUtc, cancellationToken);
        fullClock.Stop();

        var timings = $"persisted path {persistedClock.ElapsedMilliseconds} ms, every snapshot {fullClock.ElapsedMilliseconds} ms";
        if (persisted is null)
        {
            output.WriteLine($"The stored chain cannot answer this window ({reason}), so get_object_usage reads every snapshot for it. Nothing to compare. {timings}.");
            return ObjectUsageCompareNoChain;
        }

        var differences = ObjectUsageCompare.Differences(full, persisted);
        if (differences.Count == 0)
        {
            output.WriteLine($"identical: {full.SnapshotCount} snapshots, {full.Objects.Count} objects, {full.Resets.Count} resets. {timings}.");
            return CollectorToggleExitCode.Success;
        }

        output.WriteLine($"{differences.Count} differences between the two reads. {timings}.");
        foreach (var difference in differences)
        {
            output.WriteLine("  " + difference);
        }

        return ObjectUsageCompareDiffers;
    }
}

/// <summary>Lists how two <see cref="ObjectUsageWindowResult"/>s differ (#9349). The text names the field and both values.</summary>
internal static class ObjectUsageCompare
{
    /// <summary>Every difference between the full read (<paramref name="full"/>) and the persisted read; empty when they are record-equal.</summary>
    public static List<string> Differences(ObjectUsageWindowResult full, ObjectUsageWindowResult persisted)
    {
        ArgumentNullException.ThrowIfNull(full);
        ArgumentNullException.ThrowIfNull(persisted);
        var found = new List<string>();

        void Scalar<T>(string name, T expected, T actual)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
            {
                found.Add($"{name}: every snapshot gave {expected}, the chain gave {actual}");
            }
        }

        Scalar("CoveredFrom", full.CoveredFrom, persisted.CoveredFrom);
        Scalar("CoveredTo", full.CoveredTo, persisted.CoveredTo);
        Scalar("SnapshotCount", full.SnapshotCount, persisted.SnapshotCount);
        Scalar("LargestGap", full.LargestGap, persisted.LargestGap);
        Scalar("IndexResets", full.IndexResets, persisted.IndexResets);
        Scalar("BlindTime", full.BlindTime, persisted.BlindTime);

        if (!full.AnsweringServerNames.SequenceEqual(persisted.AnsweringServerNames))
        {
            found.Add($"AnsweringServerNames: every snapshot gave [{string.Join(", ", full.AnsweringServerNames)}], the chain gave [{string.Join(", ", persisted.AnsweringServerNames)}]");
        }

        for (var i = 0; i < Math.Max(full.Resets.Count, persisted.Resets.Count); i++)
        {
            var expected = i < full.Resets.Count ? full.Resets[i] : null;
            var actual = i < persisted.Resets.Count ? persisted.Resets[i] : null;
            if (!Equals(expected, actual))
            {
                found.Add($"Resets[{i}]: every snapshot gave {expected?.ToString() ?? "none"}, the chain gave {actual?.ToString() ?? "none"}");
            }
        }

        var fullById = full.Objects.GroupBy(o => o.ObjectId).ToDictionary(g => g.Key, g => g.First());
        var persistedById = persisted.Objects.GroupBy(o => o.ObjectId).ToDictionary(g => g.Key, g => g.First());
        foreach (var id in fullById.Keys.Union(persistedById.Keys).OrderBy(k => k))
        {
            fullById.TryGetValue(id, out var expected);
            persistedById.TryGetValue(id, out var actual);
            if (!Equals(expected, actual))
            {
                found.Add($"Object {id}: every snapshot gave {expected?.ToString() ?? "none"}, the chain gave {actual?.ToString() ?? "none"}");
            }
        }

        /* Same objects and same totals in a different order is still a difference: the tool pages the list in this order. */
        if (found.Count == 0 && !full.Objects.SequenceEqual(persisted.Objects))
        {
            found.Add("Objects: the same objects in a different order");
        }

        return found;
    }
}
