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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2495: the window ANCHOR. Every windowed read took <c>hours_back</c> measured from now, so nothing
/// could ask about a past incident — and widening <c>hours_back</c> until the incident falls inside is
/// NOT the same question, because for an aggregate read a wider window is a different answer rather than
/// the same answer with more rows.
///
/// <para>These are the ungated halves: the shared resolver's contract, and the two surfaces agreeing about
/// which reads carry the parameter. The live half — seed a past window, read it anchored, read it
/// unanchored, prove the two disagree — is <see cref="AsOfWindowAnchorLivePostgresTests"/>.</para>
/// </summary>
public sealed class AsOfWindowAnchorTests
{
    /* ── the resolver's contract ── */

    [Fact]
    public void NoAnchor_ResolvesToNow_WhichIsThePreChangeBehaviour()
    {
        var before = DateTime.UtcNow;
        Assert.Null(McpHelpers.ResolveAsOf(null, out var end));
        var after = DateTime.UtcNow;

        Assert.InRange(end, before, after);
        Assert.Equal(DateTimeKind.Utc, end.Kind);
    }

    [Fact]
    public void BlankAnchor_IsTreatedAsAbsent_NotAsAParseFailure()
    {
        var before = DateTime.UtcNow;
        Assert.Null(McpHelpers.ResolveAsOf("   ", out var end));
        Assert.True(end >= before);
    }

    /// <summary>
    /// The four accepted spellings all land on ONE instant. The offset-less form matters most: it is read
    /// as UTC, never as the SERVICE HOST's local time — the store is UTC throughout and the caller is an
    /// agent on some other machine, so a local-time reading would silently shift the window by the host's
    /// offset with nothing in the result to show for it.
    /// </summary>
    [Theory]
    [InlineData("2026-08-18T14:30:00Z")]
    [InlineData("2026-08-18T14:30:00")]
    [InlineData("2026-08-18T16:30:00+02:00")]
    [InlineData("2026-08-18T09:30:00-05:00")]
    [InlineData("2026-08-18T14:30Z")]
    [InlineData("2026-08-18T14:30:00.000Z")]
    [InlineData("  2026-08-18T14:30:00Z  ")]
    public void EveryAcceptedSpelling_ResolvesToTheSameUtcInstant(string asOf)
    {
        Assert.Null(McpHelpers.ResolveAsOf(asOf, out var end));
        Assert.Equal(new DateTime(2026, 8, 18, 14, 30, 0, DateTimeKind.Utc), end);
        Assert.Equal(DateTimeKind.Utc, end.Kind);
    }

    [Fact]
    public void DateOnly_IsMidnightUtc()
    {
        Assert.Null(McpHelpers.ResolveAsOf("2026-08-18", out var end));
        Assert.Equal(new DateTime(2026, 8, 18, 0, 0, 0, DateTimeKind.Utc), end);
    }

    /// <summary>
    /// An anchor we cannot use is REFUSED, following <see cref="McpHelpers.ValidateTop"/>. Silently falling
    /// back to now is the one outcome this parameter exists to prevent: a read answering "the last 4 hours"
    /// when it was asked for "the 4 hours ending Tuesday 03:00" is indistinguishable from a correct answer.
    ///
    /// <para>The slash forms are the interesting half, and the reason the parser is an ISO-8601 ALLOWLIST
    /// rather than a general date parse (review catch): a plain <c>DateTime.TryParse</c> under the invariant
    /// culture accepts <c>01/02/2026</c> as <c>M/d/yyyy</c>, so a caller who meant 1 February gets a window
    /// around 2 January and nothing anywhere says so. That is the same defect one step removed — an answer to
    /// a question nobody asked — so it is refused rather than guessed at.</para>
    /// </summary>
    [Theory]
    [InlineData("last tuesday")]
    [InlineData("2026-13-45")]
    [InlineData("4 hours ago")]
    [InlineData("08/18/2026")]
    [InlineData("01/02/2026")]
    [InlineData("2026/08/18")]
    [InlineData("18 Aug 2026")]
    [InlineData("2026-08-18 14:30:00")]
    public void AnUnusableAnchor_IsRefused_NotSilentlyTreatedAsNow(string asOf)
    {
        var error = McpHelpers.ResolveAsOf(asOf, out _);
        Assert.NotNull(error);
        Assert.Contains("Invalid as_of", error, StringComparison.Ordinal);
        Assert.Contains(asOf, error, StringComparison.Ordinal);
    }

    [Fact]
    public void AFutureAnchor_IsRefused_BecauseTheStoreCannotHoldDataNotYetCollected()
    {
        var error = McpHelpers.ResolveAsOf(DateTime.UtcNow.AddHours(2).ToString("o"), out _);
        Assert.NotNull(error);
        Assert.Contains("future", error, StringComparison.Ordinal);
    }

    /// <summary>
    /// The future refusal has a clock-skew allowance, and it is not a grace period for asking about the
    /// future: an agent that computes "now" from its own clock and sends it must not be refused because
    /// that clock runs a minute fast, on a stored read whose newest row is minutes old anyway.
    /// </summary>
    [Fact]
    public void AClientClockRunningSlightlyFast_IsStillAccepted()
    {
        var justInside = DateTime.UtcNow + McpHelpers.AsOfFutureTolerance - TimeSpan.FromMinutes(1);
        Assert.Null(McpHelpers.ResolveAsOf(justInside.ToString("o"), out _));
    }

    /// <summary>
    /// There is deliberately NO lower bound. An anchor older than anything the store holds is a legitimate
    /// question whose honest answer is the read's own <c>empty</c> / <c>unavailable</c> status — and the
    /// caller knows the anchor they sent, so that status is unambiguous. A hardcoded floor would have to
    /// guess at retention, which is per-deployment, per-server and per-collector.
    /// </summary>
    [Fact]
    public void AnAnchorOlderThanAnyRetention_IsAccepted_AndLeftToTheReadsOwnMissVocabulary()
    {
        Assert.Null(McpHelpers.ResolveAsOf("1999-01-01T00:00:00Z", out var end));
        Assert.Equal(1999, end.Year);
    }

    /* ── the two knobs together ── */

    [Fact]
    public void ValidateWindow_ReportsTheSpanBeforeTheAnchor_SoTheOldMessageIsUnchanged()
    {
        /* A caller who sent both wrong is told about hours_back first, exactly as before as_of existed. */
        var zeroHours = McpHelpers.ValidateWindow(0, "not a date", out _);
        Assert.NotNull(zeroHours);
        Assert.Contains("hours_back", zeroHours, StringComparison.Ordinal);

        var tooManyHours = McpHelpers.ValidateWindow(McpHelpers.MaxHoursBack + 1, "not a date", out _);
        Assert.NotNull(tooManyHours);
        Assert.Contains("exceeds maximum", tooManyHours, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateWindow_WithNoAnchor_IsExactlyTheOldValidateHoursBack()
    {
        foreach (var hours in new[] { -1, 0, 1, 24, McpHelpers.MaxHoursBack, McpHelpers.MaxHoursBack + 1 })
        {
            Assert.Equal(McpHelpers.ValidateHoursBack(hours), McpHelpers.ValidateWindow(hours, null, out _));
        }
    }

    [Fact]
    public void TheWindow_IsHoursBackEndingAtTheAnchor()
    {
        Assert.Null(McpHelpers.ValidateWindow(3, "2026-08-18T15:30:00Z", out var end));

        Assert.Equal(new DateTime(2026, 8, 18, 15, 30, 0, DateTimeKind.Utc), end);
        Assert.Equal(new DateTime(2026, 8, 18, 12, 30, 0, DateTimeKind.Utc), end.AddHours(-3));
    }

    /* ── the two surfaces carry the same convention ── */

    /// <summary>
    /// A read reachable over MCP but not over <c>/api/read/{name}</c> is the failure mode the catalog exists
    /// to prevent: the descriptor is the ONLY input truth for the web surface, so a parameter missing from it
    /// is simply unreachable there, and an unknown query key is ignored rather than rejected — the panel
    /// quietly gets the read's default window and nothing anywhere says so.
    /// </summary>
    [Fact]
    public void EveryDispatchedReadWhoseToolTakesAnAnchor_AdvertisesItInTheCatalog()
    {
        var missing = DarlingWebEndpoints.BuildReadDispatch().Keys
            .Where(ToolTakesAnAnchor)
            .Where(name => !DarlingWebEndpoints.CatalogDescriptors[name].Params.Any(p => p.Name == "as_of"))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            missing.Length == 0,
            "these reads take as_of over MCP but do not advertise it in the catalog, so it cannot be sent " +
            "over /api/read: " + string.Join(", ", missing));
    }

    /// <summary>The other direction: nothing advertises a parameter its tool would ignore.</summary>
    [Fact]
    public void NoCatalogEntry_AdvertisesAnAnchorItsToolDoesNotTake()
    {
        var extra = DarlingWebEndpoints.BuildReadDispatch().Keys
            .Where(name => DarlingWebEndpoints.CatalogDescriptors[name].Params.Any(p => p.Name == "as_of"))
            .Where(name => !ToolTakesAnAnchor(name))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.True(extra.Length == 0, "catalog advertises as_of for reads whose tool ignores it: " + string.Join(", ", extra));
    }

    /// <summary>
    /// The convention itself: a read that windows takes the anchor. The exclusions are named rather than
    /// discovered, so removing one from the list is a deliberate act — and adding a new windowed read
    /// without an anchor fails here rather than shipping half the convention.
    /// </summary>
    [Fact]
    public void EveryWindowedRead_TakesTheAnchor_ExceptTheNamedExclusions()
    {
        /* The analysis family came OFF this list in #2506 — the anchor now reaches the engine, not just
           the tool, and analyze_server refuses to persist when it is anchored rather than declining the
           anchor. What is left are the two permanent kinds. get_pvs_stats and get_fleet_overview mix a
           latest-snapshot measurement with a windowed one: anchoring only the windowed half would return a
           result whose two halves describe different instants, which is worse than not offering it.
           get_store_metrics windows in days over the store's own growth series. */
        var excluded = new[]
        {
            "get_pvs_stats", "get_fleet_overview", "get_store_metrics",
        };

        var unanchored = DarlingWebEndpoints.BuildReadDispatch().Keys
            .Where(name => DarlingWebEndpoints.CatalogDescriptors[name].Params.Any(p => p.Name == "hours"))
            .Where(name => !DarlingWebEndpoints.CatalogDescriptors[name].Params.Any(p => p.Name == "as_of"))
            .Where(name => !excluded.Contains(name, StringComparer.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            unanchored.Length == 0,
            "these reads window but cannot be anchored, and are not on the named exclusion list: " +
            string.Join(", ", unanchored));
    }

    /// <summary>
    /// Every tool that ADVERTISES <c>as_of</c> actually USES the instant it resolved.
    ///
    /// <para><b>Why this exists.</b> The convention's failure mode is not a compile error and not a wrong
    /// number — it is a tool that takes the parameter, validates it, refuses a bad one correctly, and then
    /// computes its window from <see cref="DateTime.UtcNow"/> anyway. The caller gets "now" labelled as their
    /// window, the validation succeeding is what makes them believe it, and nothing in the result says
    /// otherwise. That is #2495's own defect, one level up.</para>
    ///
    /// <para>It is not hypothetical: rebasing this work onto a moving <c>dev</c> produced <b>eight</b> of
    /// them at once, across both SKUs, every one of which compiled and passed every other test here. Review
    /// caught them; this catches the next eight. Both SKUs are scanned from ONE test because the category is
    /// not per-SKU — the two copies drifted together.</para>
    ///
    /// <para>A source scan rather than a behavioural assertion, for the reason
    /// <see cref="ServerPageTabsTests"/> gives about the JS it pins: the alternative is ~110 live round-trips
    /// to prove a property that is visible in the text, and a property nobody checks is the one that breaks.
    /// It reads the SHIPPED files, so it cannot drift into agreeing with a stale copy of itself.</para>
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp")]
    [InlineData("Lite/Mcp")]
    public void EveryToolThatTakesTheAnchor_ActuallyUsesIt(string mcpDirectory)
    {
        var offenders = new List<string>();
        var examined = 0;
        var anchorable = AnchorableServiceMethods();
        var anchorableAnalysis = AnchorableAnalysisServiceMethods();

        foreach (var file in RepoFilesIn(mcpDirectory))
        {
            var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var marks = Regex.Matches(source, @"McpServerTool\(Name = ""([a-z_0-9]+)""");

            for (var i = 0; i < marks.Count; i++)
            {
                var end = i + 1 < marks.Count ? marks[i + 1].Index : source.Length;
                var declaration = source.IndexOf("public static", marks[i].Index, StringComparison.Ordinal);
                if (declaration < 0 || declaration > end)
                {
                    continue;
                }

                var signatureEnd = CloseParenAfter(source, source.IndexOf('(', declaration));
                if (signatureEnd < 0)
                {
                    continue;
                }

                if (!source[declaration..signatureEnd].Contains("as_of", StringComparison.Ordinal))
                {
                    continue;
                }

                examined++;
                var body = source[signatureEnd..end];

                /* The anchor reaches the window either as the resolved local, or by being forwarded whole
                   to a shared collector (the system_health family resolves it one level down). */
                var reaches = body.Contains("windowEnd", StringComparison.Ordinal)
                    || body.Contains("anchorEnd", StringComparison.Ordinal)
                    || Regex.IsMatch(body, @"\w+Async\([^;]*\bas_of\b");

                /*
                    An anchored tool's ONLY source of "now" is the anchor it resolved, so it must not name
                    DateTime.UtcNow at all. Written as an absolute rather than as "does not ASSIGN UtcNow"
                    on evidence: the assignment form passed while get_file_io_trend was demonstrably broken,
                    because the body still contained the word windowEnd from its own `out var`. A check that
                    green-lights the defect it was written for is worse than none.
                */
                var namesNow = body.Contains("DateTime.UtcNow", StringComparison.Ordinal);

                /*
                    The other half, and the one the reaches-check cannot see: Lite's tools resolve the anchor
                    and then hand it to LocalDataService. A read that CAN take asOfUtc and is not given it is
                    a tool that validated an anchor, believed itself anchored, and queried the present. Five
                    of those shipped past a green suite before this line existed.
                */
                var dropped = Regex.Matches(body, @"dataService\.(\w+)\(")
                    .Where(m => anchorable.Contains(m.Groups[1].Value) && !CallPasses(body, m.Index, "asOfUtc"))
                    .Select(m => m.Groups[1].Value)
                    .Distinct()
                    .ToArray();

                /*
                    #2506: the same category, one seam over. The analysis family's window is built inside
                    AnalysisService / DarlingAnalysisService rather than in the tool, so its tools resolve
                    the anchor and then hand it to a SERVICE — and a tool that resolves an anchor and calls
                    AnalyzeAsync without it satisfies every other check on this list (the body still names
                    windowEnd, from its own `out var`) while answering as of now. That is precisely the
                    shape #2495's review found eight of, so it gets its own arm rather than a comment.
                */
                var droppedAnalysis = Regex.Matches(body, @"analysisService\.(\w+)\(")
                    .Where(m => anchorableAnalysis.Contains(m.Groups[1].Value) && !CallPasses(body, m.Index, "asOfUtc"))
                    .Select(m => m.Groups[1].Value)
                    .Distinct()
                    .ToArray();

                if (!reaches || namesNow || dropped.Length > 0 || droppedAnalysis.Length > 0)
                {
                    offenders.Add($"{Path.GetFileName(file)}:{marks[i].Groups[1].Value}" +
                        (reaches ? "" : " (never uses the resolved anchor)") +
                        (namesNow ? " (names DateTime.UtcNow)" : "") +
                        (dropped.Length > 0 ? $" (anchor not passed to {string.Join(", ", dropped)})" : "") +
                        (droppedAnalysis.Length > 0 ? $" (anchor not passed to analysisService.{string.Join(", ", droppedAnalysis)})" : ""));
                }
            }
        }

        /* A scan that parsed nothing passes for free, which is the worst outcome a check like this can have:
           it converts an open question into confidence. Both SKUs anchor dozens of reads, so a handful means
           the signature extraction broke rather than that the surface shrank. */
        Assert.True(
            examined >= 40,
            $"only {examined} anchored tools were found under {mcpDirectory} — the scan is broken, not the surface");

        Assert.True(
            offenders.Count == 0,
            "these tools advertise as_of and then answer as of NOW, which is worse than not offering it — " +
            "the validation succeeds, so the caller believes the window moved: " + string.Join("; ", offenders));
    }

    /// <summary>
    /// The <c>LocalDataService</c> reads that ACCEPT the anchor, read off the shipped source rather than
    /// listed here — a transcribed list would go stale in exactly the direction that makes the pin pass.
    /// </summary>
    private static HashSet<string> AnchorableServiceMethods()
    {
        var methods = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in RepoFilesIn("Lite/Services").Where(f => Path.GetFileName(f).StartsWith("LocalDataService", StringComparison.Ordinal)))
        {
            var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            foreach (Match m in Regex.Matches(source, @"public (?:async )?[\w<>?,\[\]\(\) ]+ (\w+)\(([^{]*?)\)\s*(?:=>|\n\s*\{)"))
            {
                if (m.Groups[2].Value.Contains("asOfUtc", StringComparison.Ordinal))
                {
                    methods.Add(m.Groups[1].Value);
                }
            }
        }

        Assert.True(methods.Count >= 40, $"only {methods.Count} anchor-taking service methods found — the scan is broken");
        return methods;
    }

    /// <summary>
    /// The ANALYSIS-service entry points that accept the anchor (#2506) — the same trick as
    /// <see cref="AnchorableServiceMethods"/>, read off both SKUs' shipped sources.
    ///
    /// <para>Both SKUs are scanned into ONE set because the two services are deliberate twins with the
    /// same member names, and the tools that call them are twins too. A per-SKU set would let one side
    /// drop the anchor while the other kept it and still report nothing, which is the drift the
    /// cross-SKU shape of this whole test exists to catch.</para>
    /// </summary>
    private static HashSet<string> AnchorableAnalysisServiceMethods()
    {
        var methods = new HashSet<string>(StringComparer.Ordinal);
        foreach (var directory in new[] { "Lite/Analysis", "Darling/PerformanceMonitor.Darling.Analysis" })
        {
            foreach (var file in RepoFilesIn(directory).Where(f => Path.GetFileName(f).EndsWith("AnalysisService.cs", StringComparison.Ordinal)))
            {
                var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
                foreach (Match m in Regex.Matches(source, @"public (?:async )?[\w<>?,\[\]\(\) ]+ (\w+)\(([^{]*?)\)\s*(?:=>|\n\s*\{)"))
                {
                    if (m.Groups[2].Value.Contains("asOfUtc", StringComparison.Ordinal))
                    {
                        methods.Add(m.Groups[1].Value);
                    }
                }
            }
        }

        /* AnalyzeAsync, CollectAndScoreFactsAsync, GetRecentFindingsAsync — the three the analysis tools
           reach the window through. Asserted so a regex that stopped matching cannot empty the set and
           make the arm above pass by finding nothing to check. */
        Assert.True(methods.Count >= 3, $"only {methods.Count} anchor-taking analysis-service methods found — the scan is broken");
        return methods;
    }

    /// <summary>Whether one call expression passes an argument whose text contains <paramref name="argument"/>.</summary>
    private static bool CallPasses(string body, int callStart, string argument)
    {
        var open = body.IndexOf('(', callStart);
        var close = CloseParenAfter(body, open);
        return close > open && body[open..close].Contains(argument, StringComparison.Ordinal);
    }

    private static int CloseParenAfter(string source, int open)
    {
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '(') depth++;
            else if (source[i] == ')' && --depth == 0) return i;
        }

        return -1;
    }

    private static string[] RepoFilesIn(string relativeDirectory, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relativeDirectory);
            if (Directory.Exists(candidate))
            {
                var files = Directory.GetFiles(candidate, "*.cs");
                Assert.NotEmpty(files);
                return files;
            }
        }

        throw new DirectoryNotFoundException($"Could not locate {relativeDirectory} walking up from {thisFile}");
    }

    /// <summary>
    /// Whether the <c>[McpServerTool]</c> method behind a read name takes the anchor.
    ///
    /// <para>Both pins above are of the form "no read is in set A but not set B", so a lookup that quietly
    /// returned false for a read it could not FIND would empty the failing set and make them pass for the
    /// wrong reason. The method is therefore asserted to exist rather than defaulted, and a partially
    /// loadable assembly is reported rather than silently enumerated as the types that happened to load.</para>
    /// </summary>
    private static bool ToolTakesAnAnchor(string readName)
    {
        Type[] types;
        try
        {
            types = typeof(DarlingMcpDataTools).Assembly.GetTypes();
        }
        catch (System.Reflection.ReflectionTypeLoadException ex)
        {
            Assert.Fail(
                "the service assembly did not fully load, so a missing tool would look like a passing pin: " +
                string.Join("; ", ex.LoaderExceptions.Where(e => e is not null).Select(e => e!.Message).Distinct()));
            return false;
        }

        var method = types
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .FirstOrDefault(m => m.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false)
                .Cast<ModelContextProtocol.Server.McpServerToolAttribute>()
                .Any(a => a.Name == readName));

        Assert.True(method is not null, $"no [McpServerTool] method is named '{readName}', which the read dispatch serves");
        return method!.GetParameters().Any(p => p.Name == "as_of");
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) proof of the whole point: rows seeded in a PAST window come back when the read
/// is anchored there and do NOT come back on the default anchor — and widening <c>hours_back</c> instead is
/// a demonstrably different answer, not the same one with more rows.
/// </summary>
[Collection("live-postgres")]
public sealed class AsOfWindowAnchorLivePostgresTests
{
    private const string ServerName = "darling-asof-anchor-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* The incident: 30 hours ago, i.e. OUTSIDE every default window on the surface (the widest default is
       24). "Now" carries a different wait type so the two windows are told apart by content, not by count. */
    private const string IncidentWait = "PAGEIOLATCH_SH";
    private const string RecentWait = "CXPACKET";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnAnchoredRead_SeesThePastWindow_AndTheDefaultAnchorDoesNot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live as_of test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var now = Truncate(DateTime.UtcNow);
            var incident = now.AddHours(-30);
            var anchor = incident.AddMinutes(30).ToString("o");   /* the incident sits inside a 4-hour window ending here */

            await PlantWaitAsync(connection, incident, IncidentWait, 900_000L, ct);
            await PlantWaitAsync(connection, incident.AddMinutes(5), IncidentWait, 800_000L, ct);
            await PlantWaitAsync(connection, now.AddMinutes(-10), RecentWait, 1_000L, ct);

            /* 1. The default anchor is unchanged: the last 24 hours, which the incident is not in. */
            var live = await DarlingMcpDataTools.GetWaitStats(postgres, ServerName);
            Assert.Contains(RecentWait, live, StringComparison.Ordinal);
            Assert.DoesNotContain(IncidentWait, live, StringComparison.Ordinal);

            /* 2. Anchored at the incident, the SAME read returns the incident and nothing since. This is
                  the capability the issue says was unreachable. */
            var anchored = await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 4, 20, anchor);
            Assert.Contains(IncidentWait, anchored, StringComparison.Ordinal);
            Assert.DoesNotContain(RecentWait, anchored, StringComparison.Ordinal);

            /* 3. And widening hours_back is NOT the same question. A 48-hour window reaches the incident,
                  but it reaches everything since too — the aggregate now describes both, so the top-N and
                  the totals are answers to a question nobody asked. */
            var widened = await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 48);
            Assert.Contains(IncidentWait, widened, StringComparison.Ordinal);
            Assert.Contains(RecentWait, widened, StringComparison.Ordinal);
            Assert.NotEqual(WaitTypesIn(anchored).Count, WaitTypesIn(widened).Count);

            /* 4. The collection log — the read the gap was noticed on — moves with the anchor too, and its
                  two-branch miss still tells a quiet window from a server that never collected. */
            await PlantCollectionLogAsync(connection, incident, ct);
            var logAnchored = await DarlingMcpDataTools.GetCollectionLog(postgres, ServerName, 4, 200, anchor);
            Assert.Contains("wait_stats", logAnchored, StringComparison.Ordinal);
            Assert.Equal("empty", StatusOf(await DarlingMcpDataTools.GetCollectionLog(postgres, ServerName)));

            /* 5. Refusals reach the caller as the tool's own message, not as a silently-different answer. */
            var future = await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 4, 20, DateTime.UtcNow.AddDays(1).ToString("o"));
            Assert.Contains("future", future, StringComparison.Ordinal);
            Assert.StartsWith("Invalid as_of", await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 4, 20, "last tuesday"), StringComparison.Ordinal);

            /* 6. An anchor older than anything the store holds is the read's honest empty, not a refusal. */
            Assert.Equal(
                "unavailable",
                StatusOf(await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 4, 20, "1999-01-01T00:00:00Z")));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static System.Collections.Generic.List<string> WaitTypesIn(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("waits").EnumerateArray()
            .Select(w => w.GetProperty("wait_type").GetString()!)
            .ToList();
    }

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    private static DateTime Truncate(DateTime value) =>
        new(value.Year, value.Month, value.Day, value.Hour, value.Minute, value.Second, DateTimeKind.Utc);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, string waitType, long deltaMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(Naive(at));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(deltaMs);
        command.Parameters.AddWithValue(deltaMs / 10);
        command.Parameters.AddWithValue(50L);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantCollectionLogAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 120, 'SUCCESS', NULL, 7, 90, 30)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("wait_stats");
        command.Parameters.AddWithValue(Naive(at));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "wait_stats", "collection_log" }
            .Select(table => $"DELETE FROM {table} WHERE server_id = {ServerId};"));
        sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
