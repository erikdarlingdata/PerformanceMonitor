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
    /// <c>file:member</c> — or <c>file:member:disambiguator</c> when a member holds several call sites,
    /// the disambiguator being the assigned variable's name at that site. A LINE key breaks the moment an
    /// unrelated edit adds or removes a line above the waived site anywhere in the file (#4480 added ~38
    /// lines above one of these before this fix landed); a member key survives that, and still keeps the
    /// one-waiver-covers-one-site property
    /// <see cref="StoreSessionTimeZonePinCensusTests.MonitoredTargetSites"/> keys by, because
    /// <c>DarlingWorker.RunCollectionLoopAsync</c> — the one member here with two call sites — needs its
    /// own site told apart from its waived sibling's: the mutation below removes the pin from the
    /// member's OTHER (directly-pinned) site and expects that alone to red, which a member-only key without
    /// a disambiguator cannot do. Each entry carries a one-line reason.
    /// </summary>
    private static readonly HashSet<string> Waived = new(StringComparer.Ordinal)
    {
        /* MONITORED-target sites (the TZ census's own MonitoredTargetSites, reused): these connect to a
           server this service WATCHES, never to the store, so naming them in the store's own convention
           makes no sense — a monitored target's own pg_stat_activity is the target's business. */
        "DarlingServerConnector.cs:ConnectPostgresAsync",
        "PostgresTargetProvider.cs:CreateConnection",
        "DarlingWorker.cs:ReadPgStatementTextAsync",
        "DarlingWorker.cs:RunTestHypotheticalIndexAsync",

        /* DarlingWorker's custom-alert viewer source (#4479): its connection string comes from either
           DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential (BuildRoleConnectionString,
           WebApplicationName) or DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync (which returns
           ConnectionStringFor(Surface.Web), built by BuildComposeStoreRoleConnectionString) — both name the
           connection through a builder, not through a WithApplicationName call visible on this line.
           Disambiguated from the method's OTHER call site (the service's own store connection, directly
           pinned, not waived) by the assigned variable's name. */
        "DarlingWorker.cs:RunCollectionLoopAsync:customAlertViewerSource",

        /* The viewer's own diagnostic self-test (#1954/#1966): a short-lived, unpooled probe connection the
           Viewer opens on demand from its "Run self-test" button, never part of the pooled read path
           pg_stat_activity's ApplicationName column exists to distinguish. Both layers open and close inside
           one call; there is nothing here for a store operator's convention to distinguish from another
           surface's pooled backend. */
        "StoreConnectionSelfTest.cs:RunAsync",
        "StoreConnectionSelfTest.cs:ProbeStoreShapeAsync",

        /* Store-role provisioning and bootstrap probes: short-lived, unpooled, ADMINISTRATIVE connections that
           exist only around a single start's role/database setup, never the long-lived surfaces the naming
           convention exists to tell apart in a live pg_stat_activity. */
        "DarlingManagedPostgres.cs:MigrateManagedConfAsync",       // snapshot connection
        "DarlingManagedPostgres.cs:OpenProbedMaintenanceConnectionAsync",
        "DarlingManagedPostgres.cs:VerifyPgHbaAsync",
        "DarlingManagedPostgres.cs:GuardAdoptedListenAsync",
        "DarlingStoreLogins.cs:AcceptsLoginAsync",

        /* The major-version upgrade's own internal probes (#3908/#1706): short-lived connections against the
           OLD or NEW cluster during the upgrade window itself, before/after the owner connection string
           (already named via DarlingStoreConnection.WithApplicationName at its own call sites) is the one the
           running service uses. */
        "DarlingStoreUpgrade.cs:OpenWithTransportRetryAsync",
        "DarlingStoreUpgrade.cs:ReadClusterIdentityAsync",
        "DarlingStoreUpgrade.cs:BridgeTimescaleAsync",              // database-listing connection
        "DarlingStoreUpgrade.cs:CompleteAfterStartAsync",
    };

    /// <summary>No entry in <see cref="Waived"/> keys by line — a line number breaks the moment an
    /// unrelated edit shifts anything above it in the same file (#4480's own ~38 lines is the case that
    /// found this). Every key is <c>file:member</c> or <c>file:member:disambiguator</c>.</summary>
    [Fact]
    public void NoWaiverKeyIsLineNumbered()
    {
        var lineKeyed = Waived.Where(key => Regex.IsMatch(key, @":\d+$")).ToList();

        Assert.True(
            lineKeyed.Count == 0,
            "a Waived key still ends in a line-number suffix, which breaks the moment an unrelated edit "
            + "adds or removes a line above it: " + string.Join(", ", lineKeyed));
    }

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

    /* ---------------- scan (same four-pattern shape as StoreSessionTimeZonePinCensusTests.Scan, plus a
       disambiguator for the one member here that holds more than one call site) ---------------- */

    private readonly record struct CallSite(string Key, string File, int Line, string Argument, bool Pinned);

    private readonly record struct CensusScan(int FilesScanned, IReadOnlyList<CallSite> Sites);

    private static readonly Regex CallSiteRegex = new(
        @"NpgsqlDataSource\.Create\(|new\s+NpgsqlDataSourceBuilder\(|new\s+NpgsqlConnection\(|new\s+Npgsql\.NpgsqlConnection\(",
        RegexOptions.Compiled);

    /// <summary>The assigned variable's name, or the first identifier inside the construction call, read
    /// backward from the call site to the nearest statement boundary (<c>;</c> or <c>{</c>) — the same span
    /// the roster's disambiguated keys are written against. Null when the call is not a simple
    /// assignment (nothing to disambiguate with, and none of this scan's multi-site members need one).</summary>
    private static string? Disambiguator(string text, int matchIndex)
    {
        var windowStart = Math.Max(0, matchIndex - 400);
        var window = text[windowStart..matchIndex];
        var boundary = Math.Max(window.LastIndexOf(';'), window.LastIndexOf('{'));
        var statement = boundary >= 0 ? window[(boundary + 1)..] : window;
        var assignment = Regex.Match(statement, @"\bvar\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*=");

        return assignment.Success ? assignment.Groups["name"].Value : null;
    }

    private static CensusScan Scan([CallerFilePath] string thisFile = "")
    {
        var files = 0;
        var raw = new List<(string File, string Member, string? Disambiguator, int Line, string Argument, bool Pinned)>();

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
                var disambiguator = Disambiguator(text, match.Index);

                raw.Add((name, member, disambiguator, line, Collapse(argument), ArgumentIsNamed(argument)));
            }
        }

        /* file:member alone is the key UNLESS that member holds more than one call site, in which case the
           disambiguator (the assigned variable's name) joins the key — the shape that keeps one waiver
           covering exactly one site when a member has several. */
        var perMember = raw.GroupBy(r => (r.File, r.Member)).ToDictionary(g => g.Key, g => g.Count());

        var sites = raw
            .Select(r =>
            {
                var multi = perMember[(r.File, r.Member)] > 1;
                var key = multi ? $"{r.File}:{r.Member}:{r.Disambiguator}" : $"{r.File}:{r.Member}";

                return new CallSite(key, r.File, r.Line, r.Argument, r.Pinned);
            })
            .ToList();

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
