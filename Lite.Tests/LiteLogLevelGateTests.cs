/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Darling.Tests;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <para>Pins that Lite's per-database collection timing line is SUPPRESSED at the default configuration
/// (#3104), and that the level it carries and the gate deciding its fate are the same decision.</para>
///
/// <para><b>Why suppression rather than the call site's level.</b> A pin reading "this site is
/// <c>LogDebug</c>" passes whether or not anything is quieter, which is the failure this issue is about:
/// Lite has no logger factory, so there is no <c>LoggerFilterOptions</c> in the path and nothing upstream
/// of <see cref="AppLoggerAdapter{T}"/> to filter on. Every assertion here therefore runs the level the
/// source actually carries through the real gate, instead of comparing it to a name.</para>
///
/// <para><b>Two gates had to agree, and they are the reason this file is four pins rather than one.</b>
/// <see cref="AppLoggerAdapter{T}.IsEnabled"/> decides whether a component's call is admitted, and
/// <see cref="AppLogger"/>'s static methods decide whether the admitted call is written. A minimum level
/// consulted by only one of them is worse than none: a level change would silence the line while the
/// setting that is supposed to bring it back does nothing, and both halves read as working from either
/// side alone. So the adapter's answer, the sink's answer, their agreement, and the round trip back up are
/// each asserted.</para>
///
/// <para><b>What this does NOT cover.</b> No log VOLUME is measured — the emission is asserted as an
/// admission decision, not a rate. The source scan reads one file and identifies the timing line by the
/// placeholders in its message template, so a future timing line spelled with different placeholders is
/// outside its net; the floor assertion is what keeps a net that has stopped matching from reading as a
/// clean pass. Nothing here exercises <c>App.LoadLogMinimumLevel</c>'s settings read — that runs inside
/// WPF startup, and its key is covered by <c>SettingsSampleTests</c> in both directions.</para>
/// </summary>
public sealed class LiteLogLevelGateTests : IDisposable
{
    /// <summary>The file the per-database collection timing line is emitted from.</summary>
    private const string EmitFile = "Lite/Services/RemoteCollectorService.DefinitionRunner.cs";

    /// <summary>
    /// What makes a message template the per-cycle TIMING line: the two phase totals named with their own
    /// placeholders. Matched against the literal's BODY rather than against source lines, so a template
    /// split across lines is still one match and a comment discussing <c>sql:</c> in prose is not a match
    /// at all.
    /// </summary>
    private static readonly string[] TimingMarkers =
    {
        "sql:{SqlMs}ms",
        "duckdb:{DuckMs}ms",
    };

    /// <summary>
    /// The POSITIVE CONTROL's template, taken from the same callback so it shares every condition with the
    /// timing line except the one under test. Its job is to fail when the resolution machinery below has
    /// stopped working: without it, "the timing line is not admitted" is equally consistent with a scan
    /// that matched nothing useful and a gate that admits nothing at all — an empty read that cannot be
    /// told from a real absence, which is the shape of the defect this file exists for.
    /// </summary>
    private const string ControlMarker = "hit its per-database collection bound";

    /// <summary>
    /// How many timing templates that file holds today. A FLOOR rather than an equality, so adding another
    /// one below the default level is not a failure while a net that has stopped matching — the silent way
    /// a scan like this rots — drops beneath it and fails. The only frozen number here, and it fails toward
    /// red.
    /// </summary>
    private const int KnownTimingTemplates = 1;

    /// <summary>The nearest <c>.LogSomething(</c> ahead of a literal, which is the call emitting it.</summary>
    private static readonly Regex LogCall = new(@"\.(Log[A-Za-z]*)\s*\(", RegexOptions.CultureInvariant);

    /// <summary>
    /// The level in force is process-wide, so every pin that moves it puts it back. Restored to the
    /// DEFAULT rather than to a captured value on purpose: nothing else in this suite touches it, so a
    /// value captured at construction that was not the default would mean an earlier test leaked, and
    /// carrying that leak forward would hide it.
    /// </summary>
    public void Dispose() => AppLogger.SetMinimumLevel(AppLogger.DefaultMinimumLevel);

    /// <summary>
    /// The default, which every other pin here is relative to. <c>Information</c> matches what a default
    /// .NET logging factory filters at and what Darling's service is gated at (#3102), so a level chosen
    /// against one SKU's log means the same thing against the other's.
    /// </summary>
    [Fact]
    public void TheDefaultMinimum_IsInformation()
    {
        Assert.Equal(LogLevel.Information, AppLogger.DefaultMinimumLevel);
        Assert.Equal(LogLevel.Information, AppLogger.MinimumLevel);
    }

    /// <summary>
    /// The acceptance criterion: the per-database timing line, at whatever level the source actually
    /// carries, is not admitted by the gate a default install runs with — and the warning beside it is.
    /// </summary>
    [Fact]
    public void ThePerDatabaseTimingLine_IsSuppressedAtTheDefaultConfiguration()
    {
        var sites = TimingSites(out var control);
        var gate = new AppLoggerAdapter<RemoteCollectorService>();

        /* The control first. A resolution failure makes every "not admitted" assertion below pass for the
           wrong reason, so the thing that would mask it is what fails. */
        Assert.NotNull(control);
        Assert.True(
            gate.IsEnabled(control!.Value.Level),
            $"the collection-bound warning resolved to {control.Value.Method} ({control.Value.Level}), which the "
                + "default gate does not admit. Either the gate suppresses everything — in which case the "
                + "timing assertion below proves nothing — or the level resolution is wrong.");

        Assert.True(
            sites.Count >= KnownTimingTemplates,
            $"expected at least {KnownTimingTemplates} per-cycle timing template in {EmitFile}, found "
                + $"{sites.Count} — the marker set has stopped matching the emit site, so the levels below "
                + "were not checked.");

        var admitted = sites.Where(s => gate.IsEnabled(s.Level)).ToList();

        Assert.True(
            admitted.Count == 0,
            "per-database collection timing must not be admitted at Lite's default log level (#3104):"
                + string.Concat(admitted.Select(s =>
                    $"{Environment.NewLine}  {s.Method} ({s.Level}) — {s.Template}")));
    }

    /// <summary>
    /// <para>End to end: the timing line does not reach the log at the default, and the warning emitted
    /// beside it in the same callback does. Driven through the real <see cref="AppLoggerAdapter{T}"/> into
    /// the real <see cref="AppLogger"/>, so it answers for the whole path rather than for either gate's
    /// opinion of it.</para>
    ///
    /// <para>The warning is the POSITIVE CONTROL and it travels the same adapter, the same sink and the same
    /// drain, so the timing line's absence is an absence rather than a logger that was never wired up.</para>
    /// </summary>
    [Fact]
    public void TheTimingLine_DoesNotReachTheLog_AndTheWarningBesideItDoes()
    {
        var sites = TimingSites(out _);
        Assert.True(sites.Count >= KnownTimingTemplates, "the timing site was not located; see the pin above");

        var emitted = sites[0].Level;
        ILogger<RemoteCollectorService> logger = new AppLoggerAdapter<RemoteCollectorService>();

        /* Tagged so concurrently-running tests logging into the same process-wide buffer cannot be mistaken
           for this one's lines, and so this one's cannot be mistaken for theirs. */
        var tag = Guid.NewGuid().ToString("N");

        AppLogger.DrainBufferedLines();
        logger.Log(emitted, default, $"timing {tag}", null, (s, _) => s);
        logger.LogWarning("control {Tag}", tag);
        var written = Tagged(tag);

        Assert.Single(written);
        Assert.Contains("control", written[0], StringComparison.Ordinal);
        Assert.DoesNotContain("timing", written[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// <para>The sink's OWN gate, and the adapter's agreement with it, swept across every minimum rather
    /// than probed at the default.</para>
    ///
    /// <para><b>Why this is separate from the end-to-end pin above.</b> The adapter short-circuits on its
    /// own <c>IsEnabled</c> before it ever calls a static method here, so a sink that had no gate — or one
    /// that disagreed with the adapter — is completely invisible from that direction. That is the failure
    /// mode worth catching: the level a caller sets would gate admission while the sink wrote or dropped on
    /// some other basis, and the two would only diverge for a caller that reaches the sink directly, which
    /// twenty of Lite's log sites do. So this calls the static entry points themselves and asserts the
    /// property rather than a case: a line is written exactly when <see cref="AppLogger.IsEnabled"/> says
    /// so, at every minimum, for every level.</para>
    ///
    /// <para>The sweep carries its own controls at both ends. At <c>Trace</c> all four entry points must
    /// write, so a drain that observes nothing fails here instead of reading as four correct suppressions;
    /// at <c>None</c> none of them may, so a gate that cannot suppress fails too.</para>
    /// </summary>
    [Fact]
    public void EveryStaticEntryPoint_HonoursTheMinimum_AndTheAdapterAgrees()
    {
        var levels = new[]
        {
            LogLevel.Trace, LogLevel.Debug, LogLevel.Information,
            LogLevel.Warning, LogLevel.Error, LogLevel.Critical, LogLevel.None,
        };

        var gate = new AppLoggerAdapter<RemoteCollectorService>();

        foreach (var minimum in levels)
        {
            AppLogger.SetMinimumLevel(minimum);

            foreach (var level in levels)
            {
                Assert.Equal(AppLogger.IsEnabled(level), gate.IsEnabled(level));
            }

            var tag = Guid.NewGuid().ToString("N");
            AppLogger.DrainBufferedLines();

            AppLogger.Debug("L3104", $"debug {tag}");
            AppLogger.Info("L3104", $"info {tag}");
            AppLogger.Warn("L3104", $"warn {tag}");
            AppLogger.Error("L3104", $"error {tag}");

            var written = string.Join(Environment.NewLine, Tagged(tag));

            /* The entry point is named by the level it writes at, so the expectation is IsEnabled itself
               rather than a second table of levels that could drift away from it. */
            var expected = new (string Word, LogLevel Level)[]
            {
                ("debug", LogLevel.Debug),
                ("info", LogLevel.Information),
                ("warn", LogLevel.Warning),
                ("error", LogLevel.Error),
            };

            foreach (var (word, level) in expected)
            {
                var appears = written.Contains($"{word} {tag}", StringComparison.Ordinal);

                Assert.True(
                    appears == AppLogger.IsEnabled(level),
                    $"with the minimum at {minimum}, AppLogger.{word} "
                        + (appears ? "wrote" : "wrote nothing")
                        + $" while IsEnabled({level}) is {AppLogger.IsEnabled(level)}. The sink and the gate "
                        + "must be one decision (#3104).");
            }
        }
    }

    /// <summary>
    /// Raising the minimum puts the line back, which is the half a gate alone does not deliver. Suppressing
    /// a diagnostic and deleting one look identical in a log; the difference is whether an operator can
    /// reach it, and that is what this asserts.
    /// </summary>
    [Fact]
    public void RaisingTheMinimum_RestoresTheTimingLine()
    {
        var sites = TimingSites(out _);
        Assert.True(sites.Count >= KnownTimingTemplates, "the timing site was not located; see the pin above");

        var emitted = sites[0].Level;
        ILogger<RemoteCollectorService> logger = new AppLoggerAdapter<RemoteCollectorService>();
        var tag = Guid.NewGuid().ToString("N");

        AppLogger.SetMinimumLevel(emitted);

        Assert.True(
            logger.IsEnabled(emitted),
            $"the gate still rejects {emitted} after the minimum was set to it, so the setting cannot bring "
                + "the timing lines back and is a knob that does nothing.");

        AppLogger.DrainBufferedLines();
        logger.Log(emitted, default, $"timing {tag}", null, (s, _) => s);
        var written = Tagged(tag);

        Assert.Single(written);
        Assert.Contains("timing", written[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// This test's own lines out of the buffer. Filtered by tag rather than taken wholesale because the
    /// buffer is process-wide: any test class running in parallel is logging into it, and a count over
    /// everything drained would be a count of the suite's timing.
    /// </summary>
    private static List<string> Tagged(string tag) =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains(tag, StringComparison.Ordinal))
            .ToList();

    /// <summary>
    /// <para>No logging decision in <see cref="AppLogger"/> is made by the preprocessor. This is a SOURCE
    /// pin because no runtime assertion can be: a test is compiled in one configuration and can only ever
    /// observe that one, so a <c>#if DEBUG</c> around a write is invisible to every other pin in this file
    /// while making all of them configuration-dependent — green under Release, and speaking for nothing but
    /// Release, which is not the configuration a developer runs locally.</para>
    ///
    /// <para>It is also the difference between suppressing a line and deleting it. Conditional compilation
    /// selects a verbosity at build time, so the shipped Release artifact has exactly one and no setting can
    /// change it; a level lowered onto a compiled-out write is silenced permanently rather than by
    /// default.</para>
    /// </summary>
    [Fact]
    public void NoLoggingDecision_IsMadeByThePreprocessor()
    {
        /* Stripped, so the discussion of conditional compilation in this type's own remarks is not read as
           conditional compilation. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Lite/Services/AppLogger.cs"));

        var directives = Regex
            .Matches(code, @"^[ \t]*#(if|elif|else|endif)\b", RegexOptions.Multiline)
            .Select(m => m.Value.Trim())
            .ToList();

        Assert.True(
            directives.Count == 0,
            "AppLogger carries conditional compilation: " + string.Join(", ", directives)
                + ". Verbosity has to be a runtime value — a preprocessor gate cannot be configured on the "
                + "shipped Release build, and it makes every suppression pin in this file answer for one "
                + "configuration only (#3104).");
    }

    /// <summary>One timing emit site: the call, the level it maps to, and the template it emits.</summary>
    private readonly record struct Site(string Method, LogLevel Level, string Template);

    /// <summary>
    /// Every per-cycle timing emit site in the run path, plus the control site, resolved out of the real
    /// source. The emitting call sits ahead of the literal and the literal is its first argument, so the
    /// LAST call ahead of it is the one. Read out of the comment-and-literal-stripped text so a
    /// <c>.LogInformation(</c> named in a comment above the site cannot be mistaken for the site's own call.
    /// </summary>
    private static List<Site> TimingSites(out Site? control)
    {
        var source = ReadRepoFile(EmitFile);
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var sites = new List<Site>();
        control = null;

        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            var isTiming = TimingMarkers.All(m => body.Contains(m, StringComparison.Ordinal));
            var isControl = body.Contains(ControlMarker, StringComparison.Ordinal);

            if (!isTiming && !isControl)
            {
                continue;
            }

            var call = LogCall.Matches(code[..start]).LastOrDefault();
            var method = call is null ? "(no log call found)" : call.Groups[1].Value;
            var site = new Site(method, LevelOf(method), body.Trim());

            if (isTiming)
            {
                sites.Add(site);
            }
            else
            {
                control ??= site;
            }
        }

        return sites;
    }

    /// <summary>
    /// The <c>ILogger</c> extension's name as the level it emits at. An unrecognised name maps to
    /// <see cref="LogLevel.Information"/> — the default minimum, so it reads as ADMITTED and fails the
    /// suppression assertion rather than passing it. A resolution failure must not look like a quiet line.
    /// </summary>
    private static LogLevel LevelOf(string method) => method switch
    {
        "LogTrace" => LogLevel.Trace,
        "LogDebug" => LogLevel.Debug,
        "LogInformation" => LogLevel.Information,
        "LogWarning" => LogLevel.Warning,
        "LogError" => LogLevel.Error,
        "LogCritical" => LogLevel.Critical,
        _ => LogLevel.Information,
    };

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }
}
