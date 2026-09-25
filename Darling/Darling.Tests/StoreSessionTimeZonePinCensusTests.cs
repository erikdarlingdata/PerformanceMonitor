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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4277: every STORE connection string is pinned to session <c>timezone = 'UTC'</c>
/// (<c>PerformanceMonitor.Darling.Storage.DarlingStoreConnection.PinSessionTimeZoneUtc</c>) at the moment the
/// Npgsql connection or data source is built from it, and no MONITORED-target connection string is — a
/// monitored target's own session zone is read on purpose (see <c>PgTargetBaselineProvider.Clock.cs</c>). A
/// source scan rather than a live probe, because every developer and CI store runs UTC, so a missed call site
/// is invisible there — the same reasoning <see cref="StoreSqlClockDisciplineTests"/> gives for scanning
/// source instead of trusting a green suite.
///
/// <para>Found by a full-repo grep for the four ways this codebase opens an Npgsql connection —
/// <c>NpgsqlDataSource.Create(</c>, <c>new NpgsqlDataSourceBuilder(</c>, <c>new NpgsqlConnection(</c> and the
/// fully qualified <c>new Npgsql.NpgsqlConnection(</c> — the same list #4277's own PR body used to find the 25
/// STORE and 4 MONITORED-target call sites this test holds to a floor.</para>
///
/// <para><b>Every pin is direct, on the call line itself.</b> Every call site passes
/// <c>DarlingStoreConnection.PinSessionTimeZoneUtc(...)</c> straight into the constructor argument, which the
/// scan finds textually. Round-1 review on #4285's PR found that an earlier version of this scan also
/// accepted an INDIRECT pin — a bare identifier argument counted as pinned if it was assigned from a
/// <c>PinSessionTimeZoneUtc(...)</c> call anywhere else in the same file, which let an unrelated unpinned
/// reassignment or a second same-named variable pass on trust (<c>DarlingWorker.cs</c> has two
/// <c>storeConnectionString</c> variables). <c>DarlingWorker.RunCollectionLoopAsync</c> now writes the pin
/// inline on the <c>NpgsqlDataSource.Create(...)</c> line itself, so every STORE site the scan finds must show
/// the pin on its own line, with no indirection rule left to fool.</para>
/// </summary>
public sealed class StoreSessionTimeZonePinCensusTests
{
    /* Measured on dev at #4277: 562 .cs files across the four store-side projects, 29 call sites (25 STORE,
       4 MONITORED-target). Floored well below both so ordinary file churn does not trip them, but a broken
       glob or a desynchronised regex fails loudly instead of reporting a vacuous clean bill of health. */
    private const int MinimumFilesScanned = 400;
    private const int MinimumCallSitesFound = 25;

    /// <summary>
    /// MONITORED-target call sites, named and reasoned — every one connects to a server this service WATCHES,
    /// not to the store, so its own session zone is read on purpose and pinning it would make that read
    /// wrong. Keyed <c>file:member</c>, the same key <see cref="StoreSqlClockDisciplineTests"/>'s own waiver
    /// list uses.
    /// </summary>
    private static readonly HashSet<string> MonitoredTargetSites = new(StringComparer.Ordinal)
    {
        /* MonitoredServer connect-and-probe: onboarding/health-check reads the TARGET's own clock. */
        "DarlingServerConnector.cs:ConnectPostgresAsync",

        /* ITargetProvider's generic collector connection factory — every PostgreSQL collector's connection,
           always against a MONITORED target, never the store. */
        "PostgresTargetProvider.cs:CreateConnection",

        /* Both read runtime.ConnectionString — a ServerRuntime is a MONITORED target's own resolved
           connection, not the store's. */
        "DarlingWorker.cs:ReadPgStatementTextAsync",
        "DarlingWorker.cs:RunTestHypotheticalIndexAsync",
    };

    [Fact]
    public void EveryStoreConnectionStringSourcePinsItsSessionTimeZoneToUtc()
    {
        var scan = Scan();

        Assert.True(
            scan.FilesScanned >= MinimumFilesScanned,
            $"scanned only {scan.FilesScanned} store-side source files (floor {MinimumFilesScanned}) — the "
            + "scan would pass vacuously. Check the roots in StoreSourceFiles.");
        Assert.True(
            scan.Sites.Count >= MinimumCallSitesFound,
            $"found only {scan.Sites.Count} Npgsql connection call sites (floor {MinimumCallSitesFound}) — "
            + "check the four patterns in CallSiteRegex.");

        var offenders = scan.Sites
            .Where(s => !s.Pinned && !MonitoredTargetSites.Contains(s.Key))
            .Select(s => $"{s.Key} ({s.File}:{s.Line}) — {s.Argument}")
            .ToList();

        Assert.True(
            offenders.Count == 0,
            "a STORE connection string is not pinned to session timezone UTC:" + Environment.NewLine
            + string.Join(Environment.NewLine, offenders) + Environment.NewLine
            + "Route it through DarlingStoreConnection.PinSessionTimeZoneUtc(...) — or, if it truly connects "
            + "to a MONITORED target, add its file:member key to MonitoredTargetSites with the reason.");
    }

    [Fact]
    public void NoMonitoredTargetConnectionStringIsPinned()
    {
        var scan = Scan();
        var byKey = scan.Sites.ToLookup(s => s.Key);

        var missing = MonitoredTargetSites.Where(key => !byKey.Contains(key)).ToList();

        Assert.True(
            missing.Count == 0,
            "a listed MONITORED-target site was not found by the scan (renamed, removed, or moved) — update "
            + "MonitoredTargetSites: " + string.Join(", ", missing));

        var wronglyPinned = scan.Sites
            .Where(s => MonitoredTargetSites.Contains(s.Key) && s.Pinned)
            .Select(s => $"{s.Key} ({s.File}:{s.Line})")
            .ToList();

        Assert.True(
            wronglyPinned.Count == 0,
            "a MONITORED-target connection string is pinned to UTC — its own session zone must be read on "
            + "purpose (see PgTargetBaselineProvider.Clock.cs), so pinning it makes that read wrong: "
            + string.Join(", ", wronglyPinned));
    }

    /* ---------------- scan ---------------- */

    private readonly record struct CallSite(string Key, string File, int Line, string Argument, bool Pinned);

    private readonly record struct CensusScan(int FilesScanned, IReadOnlyList<CallSite> Sites);

    private static readonly Regex CallSiteRegex = new(
        @"NpgsqlDataSource\.Create\(|new\s+NpgsqlDataSourceBuilder\(|new\s+NpgsqlConnection\(|new\s+Npgsql\.NpgsqlConnection\(",
        RegexOptions.Compiled);

    private static CensusScan Scan([CallerFilePath] string thisFile = "")
    {
        var files = 0;
        var sites = new List<CallSite>();

        foreach (var path in StoreSourceFiles(thisFile))
        {
            files++;
            var text = File.ReadAllText(path);
            var name = Path.GetFileName(path);
            CSharpMemberMap.MemberMap? members = null;

            foreach (Match match in CallSiteRegex.Matches(text))
            {
                var openParen = match.Index + match.Length - 1;
                var argument = ArgumentSpan(text, openParen)
                    ?? throw new InvalidOperationException($"{name}: unterminated call at offset {match.Index}");

                members ??= CSharpMemberMap.Of(text);
                var member = CSharpMemberMap.EnclosingMember(members, match.Index);
                var line = CSharpMemberMap.LineOf(text, match.Index);

                sites.Add(new CallSite(
                    name + ":" + member,
                    name,
                    line,
                    Collapse(argument),
                    ArgumentIsPinned(argument)));
            }
        }

        return new CensusScan(files, sites);
    }

    /// <summary>Whether <paramref name="argument"/> (the balanced-paren span passed to the connection
    /// constructor) is pinned — DIRECTLY, by calling <c>PinSessionTimeZoneUtc</c> inline on the same call. No
    /// indirect form counts (round-1 review on #4285's PR): a bare identifier used to pass if it was assigned
    /// from a pin call anywhere else in the file, which couldn't tell that assignment apart from an unrelated
    /// same-named variable or a later unpinned reassignment of the same one.</summary>
    private static bool ArgumentIsPinned(string argument)
    {
        return argument.Contains("PinSessionTimeZoneUtc", StringComparison.Ordinal);
    }

    /// <summary>The balanced-paren span starting at <paramref name="openParen"/> (which must hold <c>(</c>),
    /// through its matching close — the connection constructor's whole argument list, however many calls are
    /// nested inside it.</summary>
    private static string? ArgumentSpan(string text, int openParen)
    {
        var depth = 0;

        for (var i = openParen; i < text.Length; i++)
        {
            if (text[i] == '(')
            {
                depth++;
            }
            else if (text[i] == ')')
            {
                depth--;

                if (depth == 0)
                {
                    return text[openParen..(i + 1)];
                }
            }
        }

        return null;
    }

    private static string Collapse(string span)
    {
        var one = Regex.Replace(span.Trim(), @"\s+", " ");

        return one.Length <= 160 ? one : one[..160] + "…";
    }

    /// <summary>The store-side projects that can open an Npgsql connection: the storage helpers, the service
    /// (worker, MCP/web hosts, CLI verbs, managed-runtime bootstrap), the analysis project, and the viewer.
    /// Lite is excluded — it has no Npgsql dependency at all (DuckDB), see the Lite check in #4277's PR
    /// body.</summary>
    private static IEnumerable<string> StoreSourceFiles(string thisFile)
    {
        var darlingDir = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

        var roots = new[]
        {
            Path.Combine(darlingDir, "PerformanceMonitor.Darling.Storage"),
            Path.Combine(darlingDir, "PerformanceMonitor.Darling.Service"),
            Path.Combine(darlingDir, "PerformanceMonitor.Darling.Viewer"),
            Path.Combine(darlingDir, "PerformanceMonitor.Darling.Analysis"),
        };

        foreach (var root in roots)
        {
            foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return path;
            }
        }
    }
}
