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
/// #4479: every STORE connection string names its surface in <c>ApplicationName</c> — either directly, by
/// wrapping <c>DarlingStoreConnection.WithApplicationName(...)</c> around the construction, or indirectly, by
/// resolving from a builder that already sets <c>ApplicationName</c> (<c>DarlingManagedPostgres.BuildRoleConnectionString</c>,
/// <c>ViewerSettings.DeriveManagedConnectionString</c>, <c>DarlingStoreLogins.BuildComposeStoreRoleConnectionString</c>).
/// A source scan rather than a live probe, for the same reason
/// <see cref="StoreSessionTimeZonePinCensusTests"/> gives: every developer and CI store would show the
/// same result whether or not a call site is named, so a missed site is invisible to a live suite.
///
/// <para>Modelled directly on <see cref="StoreSessionTimeZonePinCensusTests"/> — same four-pattern scan, same
/// roster-of-exceptions shape, over the same four store-side projects. Every STORE call site the TZ census
/// finds must ALSO carry the naming pin, unless rostered here with a reason: a MONITORED-target connection
/// (never named — its own session belongs to the target, not this service), or a bare identifier already
/// built by one of the three naming builders above (this scan only recognises the DIRECT
/// <c>WithApplicationName(</c> call on the same line, the same round-1-review discipline the TZ census
/// applies to its own pin).</para>
/// </summary>
public sealed class StoreApplicationNameCensusTests
{
    /* Same floors as StoreSessionTimeZonePinCensusTests: this scan finds the identical 29 call sites (the
       naming question is asked of every one of them), so the same floor catches a desynchronised scan. */
    private const int MinimumFilesScanned = 400;
    private const int MinimumCallSitesFound = 25;

    /// <summary>
    /// Sites this scan finds but does not require to carry a DIRECT <c>WithApplicationName(</c> call, keyed
    /// <c>file:line</c> — by LINE, not by enclosing member the way
    /// <see cref="StoreSessionTimeZonePinCensusTests.MonitoredTargetSites"/> keys its own roster: two of this
    /// census's sites share an enclosing member with a DIRECTLY-pinned sibling (both
    /// <c>DarlingWorker.RunCollectionLoopAsync</c>'s own store connection and its unrelated custom-alert
    /// viewer source live in that one method), and a member-only key waived BOTH the moment either dropped
    /// its pin — the mutation this test's own review caught, below. Each entry carries a one-line reason.
    /// </summary>
    private static readonly HashSet<string> Waived = new(StringComparer.Ordinal)
    {
        /* MONITORED-target sites (the TZ census's own MonitoredTargetSites, reused): these connect to a
           server this service WATCHES, never to the store, so naming them in the store's own convention
           makes no sense — a monitored target's own pg_stat_activity is the target's business. */
        "DarlingServerConnector.cs:449",   // ConnectPostgresAsync
        "PostgresTargetProvider.cs:31",    // CreateConnection
        "DarlingWorker.cs:4194",           // ReadPgStatementTextAsync
        "DarlingWorker.cs:9663",           // RunTestHypotheticalIndexAsync

        /* DarlingWorker's custom-alert viewer source (#4479): its connection string comes from either
           DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential (BuildRoleConnectionString,
           WebApplicationName) or DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync (which returns
           ConnectionStringFor(Surface.Web), built by BuildComposeStoreRoleConnectionString) — both name the
           connection through a builder, not through a WithApplicationName call visible on this line. */
        "DarlingWorker.cs:2354",           // customAlertViewerSource, inside RunCollectionLoopAsync

        /* The viewer's own diagnostic self-test (#1954/#1966): a short-lived, unpooled probe connection the
           Viewer opens on demand from its "Run self-test" button, never part of the pooled read path
           pg_stat_activity's ApplicationName column exists to distinguish. Both layers open and close inside
           one call; there is nothing here for a store operator's convention to distinguish from another
           surface's pooled backend. */
        "StoreConnectionSelfTest.cs:206",  // RunAsync
        "StoreConnectionSelfTest.cs:306",  // ProbeStoreShapeAsync

        /* Store-role provisioning and bootstrap probes: short-lived, unpooled, ADMINISTRATIVE connections that
           exist only around a single start's role/database setup, never the long-lived surfaces the naming
           convention exists to tell apart in a live pg_stat_activity. */
        "DarlingManagedPostgres.cs:2988",  // MigrateManagedConfAsync's snapshot connection
        "DarlingManagedPostgres.cs:4603",  // OpenProbedMaintenanceConnectionAsync
        "DarlingManagedPostgres.cs:5300",  // VerifyPgHbaAsync
        "DarlingManagedPostgres.cs:5392",  // GuardAdoptedListenAsync
        "DarlingStoreLogins.cs:470",       // AcceptsLoginAsync

        /* The major-version upgrade's own internal probes (#3908/#1706): short-lived connections against the
           OLD or NEW cluster during the upgrade window itself, before/after the owner connection string
           (already named via DarlingStoreConnection.WithApplicationName at its own call sites) is the one the
           running service uses. */
        "DarlingStoreUpgrade.cs:1577",     // OpenWithTransportRetryAsync
        "DarlingStoreUpgrade.cs:4151",     // ReadClusterIdentityAsync
        "DarlingStoreUpgrade.cs:4206",     // BridgeTimescaleAsync's database-listing connection
        "DarlingStoreUpgrade.cs:4402",     // CompleteAfterStartAsync
    };

    [Fact]
    public void EveryStoreConnectionStringSourceNamesItsApplicationName()
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
            .Where(s => !s.Pinned && !IsWaived(s))
            .Select(s => $"{s.Key} — {s.Argument}")
            .ToList();

        Assert.True(
            offenders.Count == 0,
            "a STORE connection string does not name its ApplicationName:" + Environment.NewLine
            + string.Join(Environment.NewLine, offenders) + Environment.NewLine
            + "Route it through DarlingStoreConnection.WithApplicationName(...), through a builder that sets "
            + "ApplicationName, or add its file:line key to Waived with the reason.");
    }

    private static bool IsWaived(CallSite site) => Waived.Contains(site.Key);

    /* ---------------- scan (identical shape to StoreSessionTimeZonePinCensusTests.Scan) ---------------- */

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

            foreach (Match match in CallSiteRegex.Matches(text))
            {
                var openParen = match.Index + match.Length - 1;
                var argument = ArgumentSpan(text, openParen)
                    ?? throw new InvalidOperationException($"{name}: unterminated call at offset {match.Index}");

                var line = CSharpMemberMap.LineOf(text, match.Index);

                sites.Add(new CallSite(
                    name + ":" + line,
                    name,
                    line,
                    Collapse(argument),
                    ArgumentIsNamed(argument)));
            }
        }

        return new CensusScan(files, sites);
    }

    /// <summary>Whether <paramref name="argument"/> is named DIRECTLY, by calling <c>WithApplicationName</c>
    /// inline on the same construction — the same round-1-review discipline the TZ census applies to its own
    /// pin (no indirect "assigned elsewhere in the file" credit).</summary>
    private static bool ArgumentIsNamed(string argument)
        => argument.Contains("WithApplicationName", StringComparison.Ordinal);

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
