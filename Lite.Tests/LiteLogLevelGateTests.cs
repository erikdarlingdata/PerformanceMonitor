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
/// clean pass. <c>App.LoadLogMinimumLevel</c>'s file read is not exercised — that runs inside WPF startup
/// — so what is pinned of it is the part that decides: the token vocabulary, through the same
/// <see cref="AppLogger.TryParseMinimumLevel"/> the loader calls. The key's presence in the shipped sample
/// is covered by <c>SettingsSampleTests</c> in both directions.</para>
/// </summary>
/// <remarks>
/// The collection exists because these pins move <see cref="AppLogger"/>'s process-wide minimum, briefly
/// as far as <see cref="LogLevel.None"/>, and a concurrently-running class whose service logs during that
/// window loses the line — unrecoverably, since tag-filtering a buffer cannot bring back a line never
/// enqueued. That is #1965, which <c>app-alert-statics</c> was created for. No other class makes runtime
/// <c>AppLogger</c> calls today, and a collection name only serialises classes that SHARE it, so this
/// serialises nothing yet: it is the named place for the next class touching this static to join, and it
/// is worth stating that it protects nothing on its own rather than implying it already does.
/// </remarks>
[Collection("app-logger-statics")]
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
    /// The POSITIVE CONTROL's template. Its job is to fail when the resolution machinery below has stopped
    /// working: without it, "the timing line is not admitted" is equally consistent with a scan that
    /// matched nothing useful and a gate that admits nothing at all — an empty read that cannot be told
    /// from a real absence, which is the shape of the defect this file exists for.
    ///
    /// <para><b>Two sites in that file carry this template</b> — the Azure SQL DB per-database loop and the
    /// fan-out completion callback the timing line is emitted from — so the one NEAREST the matched timing
    /// site is the one taken, rather than the first met walking the file. Both resolve to
    /// <c>LogWarning</c> today, so the choice changes no result now; it is what makes the control share the
    /// timing line's surroundings, which is the property that makes it a control rather than another
    /// arbitrary warning. Selecting the first would silently attach it to a different branch the moment
    /// either site moved.</para>
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
    /// <para>The same property through the ADAPTER, at every level and every minimum: an
    /// <c>ILogger&lt;T&gt;</c> call reaches the log exactly when <see cref="AppLogger.IsEnabled"/> says its
    /// level is on.</para>
    ///
    /// <para><b>Why the static sweep above is not enough.</b> That one calls the four entry points by
    /// name, so it never exercises the adapter's MAPPING — and the mapping is where the two gates can
    /// disagree, because it collapses two levels onto one sink twice. Those two collapses are not
    /// symmetric: <c>Trace</c>/<c>Debug</c> lands on the higher of the pair, so admitting the outer level
    /// already admits the sink's, while <c>Error</c>/<c>Critical</c> lands on the LOWER, where it does not.
    /// A minimum of <c>Critical</c> therefore admitted a Critical line at the adapter and dropped it at the
    /// sink, making a documented level silence the log as completely as <c>None</c>. Sweeping the product
    /// of levels and minima is what makes that reachable rather than relying on someone noticing the
    /// asymmetry.</para>
    /// </summary>
    [Fact]
    public void EveryLevelThroughTheAdapter_ReachesTheLogExactlyWhenItIsEnabled()
    {
        var levels = new[]
        {
            LogLevel.Trace, LogLevel.Debug, LogLevel.Information,
            LogLevel.Warning, LogLevel.Error, LogLevel.Critical,
        };

        var minima = levels.Append(LogLevel.None).ToArray();
        ILogger<RemoteCollectorService> logger = new AppLoggerAdapter<RemoteCollectorService>();

        foreach (var minimum in minima)
        {
            AppLogger.SetMinimumLevel(minimum);

            foreach (var level in levels)
            {
                var tag = Guid.NewGuid().ToString("N");
                AppLogger.DrainBufferedLines();

                logger.Log(level, default, $"probe {tag}", null, (s, _) => s);

                var reached = Tagged(tag).Count > 0;

                Assert.True(
                    reached == AppLogger.IsEnabled(level),
                    $"with the minimum at {minimum}, an ILogger call at {level} "
                        + (reached ? "reached" : "did not reach")
                        + $" the log while IsEnabled({level}) is {AppLogger.IsEnabled(level)}. The adapter's "
                        + "mapping must not re-decide a level the gate has already answered (#3104).");
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
    /// <para>No logging decision in <see cref="AppLogger"/> is made by the preprocessor. Conditional
    /// compilation selects a verbosity at BUILD time, so the shipped Release artifact has exactly one and
    /// no setting can reach it: a level lowered onto a compiled-out write is silenced permanently rather
    /// than by default, which is the difference between suppressing a diagnostic and deleting it.</para>
    ///
    /// <para><b>What this adds over the sweep above, measured rather than assumed.</b> A <c>#if DEBUG</c>
    /// around <c>Debug</c>'s write does fail
    /// <see cref="EveryStaticEntryPoint_HonoursTheMinimum_AndTheAdapterAgrees"/> — but a DIFFERENT
    /// assertion in each configuration, because the two configurations disagree about what the sink does: a
    /// Debug build fails at minimum <c>Information</c> (it wrote when the gate said no) and a Release build
    /// fails at minimum <c>Trace</c> (it wrote nothing when the gate said yes). So the sweep reports a level
    /// table that does not match, and which mismatch you are shown depends on how you built; this names the
    /// cause, identically in both. The sweep also only covers the four entry points and seven minima it
    /// enumerates, so a directive around any other decision here is invisible to it and visible to this.</para>
    ///
    /// <para>Read out of stripped source, so this type's own discussion of conditional compilation — and
    /// <see cref="AppLogger"/>'s — is not itself read as conditional compilation.</para>
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

    /// <summary>
    /// <para>The accepted vocabulary is exactly the seven names the sample documents — because the
    /// rejected half is what decides whether a typo costs one setting or the whole log.</para>
    ///
    /// <para><c>Enum.TryParse</c> alone accepts a NUMBER, and an undefined one is the dangerous case:
    /// <c>"999"</c> parses to <c>(LogLevel)999</c>, which no real level can reach, so every line
    /// including <see cref="LogLevel.Error"/> is dropped — logging off entirely, from a setting whose
    /// stated contract is that a bad value costs itself and is named at startup. Asserted as the pair that
    /// matters: the token is rejected AND the level handed back is the default, since a caller reading the
    /// level without the bool is how the silent version happens.</para>
    /// </summary>
    [Fact]
    public void TheSettingsVocabulary_TakesTheDocumentedNamesAndNothingElse()
    {
        foreach (var (token, expected) in new (string?, LogLevel)[]
        {
            ("Trace", LogLevel.Trace),
            ("Debug", LogLevel.Debug),
            ("information", LogLevel.Information),
            ("WARNING", LogLevel.Warning),
            ("Error", LogLevel.Error),
            ("Critical", LogLevel.Critical),
            ("None", LogLevel.None),
        })
        {
            Assert.True(AppLogger.TryParseMinimumLevel(token, out var got), $"'{token}' was rejected");
            Assert.Equal(expected, got);
        }

        /* "3" and "6" are DEFINED levels spelled as numbers: undocumented vocabulary rather than a hazard,
           rejected so the accepted set is the set the failure message names. "999" and "-1" are the ones
           that would silence everything. */
        foreach (var token in new string?[] { "999", "-1", "42", "3", "6", "banana", "Debug ", "", "  ", null })
        {
            Assert.False(
                AppLogger.TryParseMinimumLevel(token, out var got),
                $"'{token}' was accepted as a log level");

            Assert.Equal(AppLogger.DefaultMinimumLevel, got);
        }
    }

    /// <summary>
    /// One emit site: the call, the level it maps to, the template it emits, and where in the file it sits.
    /// The offset is carried only to pick the control nearest a timing site.
    /// </summary>
    private readonly record struct Site(string Method, LogLevel Level, string Template, int Offset);

    /// <summary>
    /// Every per-cycle timing emit site in the run path, plus the control site nearest the first of them,
    /// resolved out of the real source. The emitting call sits ahead of the literal and the literal is its
    /// first argument, so the LAST call ahead of it is the one. Read out of the comment-and-literal-stripped
    /// text so a <c>.LogInformation(</c> named in a comment above the site cannot be mistaken for the site's
    /// own call.
    /// </summary>
    private static List<Site> TimingSites(out Site? control)
    {
        var source = ReadRepoFile(EmitFile);
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var sites = new List<Site>();
        var controls = new List<Site>();

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
            var site = new Site(method, LevelOf(method), body.Trim(), start);

            (isTiming ? sites : controls).Add(site);
        }

        /* Nearest the timing site, so the control shares its surroundings. With no timing site there is
           nothing to be near, and the floor assertion is what should fail there rather than this. */
        var ranked = sites.Count > 0
            ? controls.OrderBy(c => Math.Abs(c.Offset - sites[0].Offset)).ToList()
            : controls;

        control = ranked.Count > 0 ? ranked[0] : null;

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
