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
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// <para>Pins the level of the per-cycle collector timing telemetry (#3102) — the run lines that decompose
/// a collector's cost, which are emitted once per collector per server per cycle and once per DATABASE on
/// the collectors that fan out.</para>
///
/// <para><b>Why a level is worth a test at all.</b> The cost of these lines was never disk. At fleet scale
/// they are ~2M lines a day of near-uniform text, and the collector <c>ERROR</c>, <c>PERMISSIONS</c> and
/// store-cancellation lines the log is actually opened for are dozens a day inside that. The consequence is
/// not "the log is large", it is that an empty grep result against this file stopped meaning absence: the
/// signal is rare, the surrounding text is repetitive, and the interesting lines are the irregular ones a
/// pattern written from memory is likeliest to miss. A log whose empty results cannot be trusted has lost
/// the property it exists for, which is why this is pinned rather than left to review.</para>
///
/// <para><b>The rule being applied is already the repo's,</b> stated on the fault path:
/// <see cref="DarlingCollectorRunner.LogPerDatabaseFaultSplit"/>'s remarks say a phase line must not be
/// louder than the error it decomposes, and give the generic arm Debug because one database being offline is
/// routine. A run that SUCCEEDED has no error beside it at all, so the success-path splits take the same
/// treatment. Nothing is deleted: the sample is the artifact for per-cycle attribution, and a periodic
/// distribution cannot answer "what did this collector do at 04:12".</para>
///
/// <para><b>The seam that makes a level change work here is not obvious, and the wrong reading of it says
/// this change is inert.</b> <c>DarlingFileLoggerProvider</c>'s own <c>IsEnabled</c> accepts every level
/// except <c>None</c>, so read alone it looks like a provider that writes Debug unconditionally. The gate is
/// upstream, in the logger FACTORY: <c>AddLogging</c> registers a default
/// <c>LoggerFilterOptions.MinLevel</c> of <c>Information</c>, and the service adds no configuration that
/// lowers it, so a Debug call is dropped before any provider sees it. Both halves are asserted below,
/// because either one alone is consistent with the opposite conclusion.</para>
///
/// <para><b>What this does NOT cover.</b> No line RATE is measured here — the emission is asserted as a
/// level, and the ~2M/day figure comes from a field log, not from anything runnable. The source scan reads
/// the two files the collector run path lives in and identifies a timing line by the parameter names in its
/// message template, so a future timing line spelled with different placeholders is outside its net; the
/// floor assertion is what keeps a net that has stopped matching from reading as a clean pass. Lite's twin
/// line is deliberately untouched and is NOT covered: <c>AppLoggerAdapter</c> is constructed directly rather
/// than through a factory and answers <c>IsEnabled(Debug)</c> true, so the same edit there suppresses
/// nothing and would need a gate of its own first.</para>
/// </summary>
public sealed class PerCycleTimingLogLevelTests : IDisposable
{
    /// <summary>
    /// The two files the collector run path emits from. Both are read, so a line moved from one to the other
    /// stays inside the net.
    /// </summary>
    private static readonly string[] EmitFiles =
    {
        "DarlingWorker.cs",
        "DarlingCollectorRunner.cs",
    };

    /// <summary>
    /// What makes a message template a per-cycle TIMING line: a phase or total named with its own
    /// placeholder. Matched against the literal's body rather than against source lines, so a template split
    /// across lines is still one match and a doc comment that discusses <c>drain:</c> in prose is not a
    /// match at all.
    /// </summary>
    private static readonly string[] TimingMarkers =
    {
        "sql:{SqlMs}ms",
        "open:{OpenMs}ms",
        "drain:{DrainMs}ms",
        "plan_fetch:{PlanFetchMs}ms",
        "text_fetch:{TextFetchMs}ms",
        "pg:{PgMs}ms",
    };

    /// <summary>
    /// How many timing templates the two files hold today: eight success-path lines plus the fault split.
    /// A FLOOR rather than an equality, so adding a tenth at Debug is not a failure while a net that has
    /// stopped matching — the silent way a scan like this rots — drops below it and fails. The number is the
    /// only frozen thing here, and it fails toward red.
    /// </summary>
    private const int KnownTimingTemplates = 9;

    /// <summary>The nearest <c>.LogSomething(</c> ahead of a literal, which is the call emitting it.</summary>
    private static readonly Regex LogCall = new(@"\.(Log[A-Za-z]*)\s*\(", RegexOptions.CultureInvariant);

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "darling-timing-level-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempRoot))
            {
                Directory.Delete(_tempRoot, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    /// <summary>
    /// Every per-cycle timing template in the collector run path is emitted BELOW the default level. The
    /// success sites are Debug outright; the one exception is the fault split, which takes its arm's level
    /// from the caller by design (pinned in <see cref="PerDatabaseFaultPathSplitTests"/>) and so is
    /// identified by its method rather than exempted by its text.
    /// </summary>
    [Fact]
    public void EveryPerCycleTimingLineIsEmittedBelowTheDefaultLevel()
    {
        var found = new List<(string File, string Method, string Template)>();

        foreach (var file in EmitFiles)
        {
            var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", file);
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);

            foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
            {
                if (!TimingMarkers.Any(marker => body.Contains(marker, StringComparison.Ordinal)))
                {
                    continue;
                }

                /* The emitting call sits ahead of the literal, and the literal is the first argument on the
                   success sites and the second on the fault split, so the LAST call ahead of it is the one.
                   Read out of the stripped text so a `.LogInformation(` mentioned in a comment above the
                   site cannot be mistaken for the site's own call. */
                var call = LogCall.Matches(code[..start]).LastOrDefault();

                found.Add((
                    file,
                    call is null ? "(no log call found)" : call.Groups[1].Value,
                    body.Trim()));
            }
        }

        /* The floor first: a marker set that has stopped matching returns an empty list, and every
           assertion below it passes vacuously on an empty list. This is the one that fails instead. */
        Assert.True(
            found.Count >= KnownTimingTemplates,
            $"expected at least {KnownTimingTemplates} per-cycle timing templates across {string.Join(", ", EmitFiles)}, "
                + $"found {found.Count} — the marker set has stopped matching the emit sites, so the levels below were not checked:"
                + string.Concat(found.Select(f => $"{Environment.NewLine}  {f.File}: {f.Method}")));

        var louder = found
            .Where(f => f.Method is not ("LogDebug" or "LogTrace" or "Log"))
            .ToList();

        Assert.True(
            louder.Count == 0,
            "per-cycle collector timing must not be emitted at or above the default Information level (#3102):"
                + string.Concat(louder.Select(f => $"{Environment.NewLine}  {f.File}: {f.Method} — {f.Template}")));

        /* And the success sites specifically are Debug, not merely "not Information". Without this, deleting
           the lines outright would also satisfy the assertion above. */
        var debugSites = found.Count(f => f.Method == "LogDebug");

        Assert.True(
            debugSites >= KnownTimingTemplates - 1,
            $"expected at least {KnownTimingTemplates - 1} timing templates at LogDebug (every success-path site; "
                + $"the fault split takes its caller's level), found {debugSites}");
    }

    /// <summary>
    /// <para>The shipped default drops Debug before any provider sees it, AND the file provider is not
    /// itself the gate — so raising the minimum is what puts these lines in the FILE rather than only on a
    /// console.</para>
    ///
    /// <para>Carries its own POSITIVE CONTROL: the same call at Information is asserted to land in the same
    /// file through the same harness. Without it, a Debug line missing from the file is equally consistent
    /// with the provider never having been wired up, which is the failure this whole issue is about — an
    /// empty read that cannot be told from a real absence.</para>
    /// </summary>
    [Fact]
    public void TheDefaultLevelDropsDebug_AndTheFileProviderIsNotTheGate()
    {
        var provider = new DarlingFileLoggerProvider(Path.Combine(_tempRoot, "default"), _ => { });

        using (var factory = LoggerFactory.Create(builder => builder.AddProvider(provider)))
        {
            var logger = factory.CreateLogger("PerformanceMonitor.Darling.Service.DarlingWorker");

            Assert.False(logger.IsEnabled(LogLevel.Debug));
            Assert.True(logger.IsEnabled(LogLevel.Information));

            logger.LogDebug("SUPPRESSED sql:{SqlMs}ms", 12);
            logger.LogInformation("POSITIVE-CONTROL connect edge");
            provider.Flush();
        }

        var written = File.ReadAllText(provider.CurrentLogFile());

        /* The control proves this harness can see a line at all, so the absence below is an absence. */
        Assert.Contains("POSITIVE-CONTROL connect edge", written, StringComparison.Ordinal);
        Assert.DoesNotContain("SUPPRESSED", written, StringComparison.Ordinal);

        /* The provider itself accepts Debug. So the suppression above is the FACTORY's minimum, and turning
           that minimum up delivers Debug to this file rather than to the console alone. */
        Assert.True(provider.CreateLogger("PerformanceMonitor.Darling.Service.DarlingWorker")
            .IsEnabled(LogLevel.Debug));
    }

    /// <summary>
    /// The capability is not lost, it is configured — and it is reachable scoped to the service's own
    /// namespace, so measuring the collector run path does not also turn on every other component's Debug
    /// output. This is the claim <c>Darling/README.md</c>'s "Logs" section makes to an operator.
    /// </summary>
    [Fact]
    public void NamespaceScopedConfigurationRestoresTheTimingLinesToTheFile()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Logging:LogLevel:PerformanceMonitor.Darling.Service"] = "Debug",
            })
            .Build();

        var provider = new DarlingFileLoggerProvider(Path.Combine(_tempRoot, "raised"), _ => { });

        using (var factory = LoggerFactory.Create(builder =>
        {
            builder.AddConfiguration(configuration.GetSection("Logging"));
            builder.AddProvider(provider);
        }))
        {
            var collector = factory.CreateLogger("PerformanceMonitor.Darling.Service.DarlingCollectorRunner");
            Assert.True(collector.IsEnabled(LogLevel.Debug));

            collector.LogDebug("RESTORED  [{Server}] {Collector} => {Rows} rows (sql:{SqlMs}ms, pg:{PgMs}ms)",
                "alpha", "wait_stats", 42, 7, 3);

            /* Scoping is the point of naming the namespace rather than Default: an unrelated component's
               Debug output stays off, so raising this one does not re-bury the log from another direction. */
            var unrelated = factory.CreateLogger("PerformanceMonitor.Darling.Viewer.ViewerDataService");
            Assert.False(unrelated.IsEnabled(LogLevel.Debug));
            unrelated.LogDebug("UNRELATED should stay suppressed");

            provider.Flush();
        }

        var written = File.ReadAllText(provider.CurrentLogFile());

        Assert.Contains("RESTORED", written, StringComparison.Ordinal);
        Assert.Contains("sql:7ms", written, StringComparison.Ordinal);
        Assert.DoesNotContain("UNRELATED", written, StringComparison.Ordinal);
    }
}
