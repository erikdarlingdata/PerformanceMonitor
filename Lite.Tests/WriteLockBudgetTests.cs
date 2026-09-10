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
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Darling.Tests;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3134: <c>LocalDataService.OpenWriteConnectionAsync</c>'s write-lock wait is the host's number, not the
/// method's.
///
/// <para>The wait exists to keep a WPF dispatcher responsive behind an in-flight archival, and
/// <c>DuckDbInitializer</c>'s lock is process-wide — so in a host with no dispatcher, a store call queues
/// behind every other class in the process and then fails on a budget sized for a user who is not there.
/// That is a real failure with nothing behind it, and it is what these pin against: the app keeps the
/// dispatcher's five seconds, this host declares its own, and the declaration demonstrably ARRIVES rather
/// than falling back to the default while looking configured.</para>
/// </summary>
public sealed class WriteLockBudgetTests
{
    /// <summary>
    /// The one project file in the repo that states a budget, and it is not the app's.
    ///
    /// <para>Whole-tree rather than "the app's csproj does not declare it": the value is a runtime
    /// configuration property, so a shared <c>Directory.Build.props</c> or an imported <c>.targets</c>
    /// would reach the shipped app just as effectively as its own project file, and the acceptance
    /// criterion is that the app's protection is unchanged — not that one file was left alone.</para>
    /// </summary>
    [Fact]
    public void OnlyTheTestHostDeclaresABudget()
    {
        var declaring = BuildFiles()
            .Where(f => File.ReadAllText(f).Contains(LocalDataService.WriteLockBudgetConfigKey, StringComparison.Ordinal))
            .Select(Relative)
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(new[] { "Lite.Tests/Lite.Tests.csproj" }, declaring);
    }

    /// <summary>
    /// The budget a host that declares nothing gets — which, by the pin above, is the shipped app.
    /// </summary>
    [Fact]
    public void TheAppKeepsTheDispatchersFiveSeconds()
    {
        Assert.Equal(TimeSpan.FromSeconds(5), LocalDataService.DefaultWriteLockBudget);
        Assert.Equal(LocalDataService.DefaultWriteLockBudget, LocalDataService.ResolveWriteLockBudget(null));
    }

    /// <summary>
    /// The end-to-end one, and the only one here that can catch a seam that is wired but inert.
    ///
    /// <para>A <c>RuntimeHostConfigurationOption</c> that never reaches <c>AppContext</c> — a renamed key,
    /// a value MSBuild declines to emit, an option written into the wrong project — resolves the default
    /// SILENTLY, which is indistinguishable from the pre-#3134 behaviour and would leave the flake in place
    /// while every other pin here stayed green. So the expected value is read out of the project file and
    /// compared against what this running host actually resolved, and the budget is additionally required to
    /// DIFFER from the default: matching the default is exactly what a dead seam looks like.</para>
    /// </summary>
    [Fact]
    public void ThisHostResolvedTheBudgetItsProjectFileDeclares()
    {
        var declared = DeclaredBudgetSeconds("Lite.Tests/Lite.Tests.csproj");

        Assert.Equal(TimeSpan.FromSeconds(declared), LocalDataService.WriteLockBudget);
        Assert.NotEqual(LocalDataService.DefaultWriteLockBudget, LocalDataService.WriteLockBudget);
    }

    /// <summary>
    /// Every duration-shaped call site in a file that holds this lock is one the scan below can READ.
    ///
    /// <para>This is the arm that makes the bound a measurement rather than a shape. The scan recognises a
    /// set of spellings, and a spelling outside it does not merely go uncounted — the file still matches the
    /// write-lock scan, so it joins the population and contributes ZERO. <c>gate.Wait(TimeSpan.FromSeconds(30))</c>
    /// and <c>gate.Wait(30000)</c> are the same thirty-second hold; only one of them used to be visible, and
    /// the other reported itself as no hold at all. A zero that looks like a measurement is worse than an
    /// absence, because the sum then names a population it did not measure.</para>
    ///
    /// <para>So an unreadable site FAILS here rather than being silently scored, which is the same move as
    /// checking the <c>TimeSpan</c> the resolver constructed instead of the double it parsed: make the
    /// derivation total, rather than enumerate the ways it can be wrong. A file with no duration site at all
    /// is a real zero — it holds the lock across straight-line code — and that is distinguishable from a
    /// file whose duration could not be read, which is the distinction that was missing.</para>
    ///
    /// <para>Read off <c>CSharpSourceWalker.StripCommentsAndStrings</c> rather than raw text, so a duration
    /// written in a comment cannot raise the bar and <c>string.Join</c> cannot be mistaken for
    /// <c>Thread.Join</c>.</para>
    /// </summary>
    [Fact]
    public void EveryDeliberateHoldInThisSuiteIsReadable()
    {
        var (holds, unreadable) = ScanDeliberateHolds();

        Assert.NotEmpty(holds);
        Assert.True(
            unreadable.Count == 0,
            "these call sites bound a deliberate hold of the process-wide lock in a spelling the hold scan " +
            "cannot read, so they would be counted as no hold at all. Teach the scan the spelling, or give " +
            "the site a duration it already reads: " + string.Join("; ", unreadable));
    }

    /// <summary>
    /// This host's budget outlasts every deliberate lock hold its own suite can queue in front of one
    /// acquisition, which is what makes the wait BOUNDED rather than merely longer.
    ///
    /// <para>The bar is derived, not restated: for each test file that takes the write lock with no timeout,
    /// the largest duration in that file stands in for how long it can hold, and the bar is their SUM — a
    /// writer can queue behind all of them. The remaining approximation rounds the bar UP, since an
    /// unrelated duration in a holder's file inflates that file's share and nothing here can shrink it. What
    /// keeps that honest is the companion pin above: a duration this scan cannot read is a failure there
    /// rather than a zero here.</para>
    /// </summary>
    [Fact]
    public void TheBudgetOutlastsEveryDeliberateHoldInThisSuite()
    {
        var (holds, _) = ScanDeliberateHolds();
        Assert.NotEmpty(holds);

        var queued = TimeSpan.FromSeconds(holds.Values.Sum(h => h.TotalSeconds));

        Assert.True(
            LocalDataService.WriteLockBudget > queued,
            $"this host's write-lock budget is {LocalDataService.WriteLockBudget.TotalSeconds:F0}s, but its own " +
            $"tests can hold the process-wide lock for {queued.TotalSeconds:F0}s in front of one acquisition: " +
            string.Join(", ", holds.OrderByDescending(h => h.Value).Select(h => $"{h.Key} {h.Value.TotalSeconds:F0}s")));
    }

    /// <summary>
    /// The resolver is TOTAL: every input returns a budget, none throws.
    ///
    /// <para>This is the one arm that discriminates a THIRD outcome from the two the change is argued on.
    /// A budget either lets the wait succeed or fails it loudly — unless resolution itself throws, and it
    /// can: <c>NumberStyles.Float</c> admits exponents and .NET parses the invariant <c>Infinity</c>
    /// symbol whatever the style, so <c>"1e300"</c> and <c>"Infinity"</c> arrive as positive doubles that
    /// overflow <see cref="TimeSpan.FromSeconds"/>. Out of a static initializer that is a
    /// <c>TypeInitializationException</c> on the first store call anywhere in the process, which
    /// is strictly worse than the timeout it replaces. Positivity alone does not reach it — it bounds the
    /// other end.</para>
    ///
    /// <para>Asserted over inputs rather than by construction, and the band is read off
    /// <c>MaxWriteLockBudget</c> rather than restated, so a ceiling that moves moves the bar with it.</para>
    /// </summary>
    [Fact]
    public void TheResolverIsTotalAndAlwaysReturnsAUsableBudget()
    {
        var hostile = new object?[]
        {
            null, "", "   ", "later", "0", "-0", "-30", "1,5", "NaN", "Infinity", "-Infinity",
            "1e300", "-1e300", "1E+15", "  120  ", "+120", ".5",
            TimeSpan.MaxValue.TotalSeconds.ToString("R", CultureInfo.InvariantCulture),
            double.MaxValue.ToString("R", CultureInfo.InvariantCulture),
            double.Epsilon.ToString("R", CultureInfo.InvariantCulture),
            120, 120d, TimeSpan.FromSeconds(120), new object(),
        };

        var broke = new List<string>();
        foreach (var input in hostile)
        {
            var name = input is null ? "<null>" : $"{input.GetType().Name} \"{input}\"";
            try
            {
                var resolved = LocalDataService.ResolveWriteLockBudget(input);
                if (resolved <= TimeSpan.Zero || resolved > LocalDataService.MaxWriteLockBudget)
                {
                    broke.Add($"{name} -> {resolved}");
                }
            }
            catch (Exception ex)
            {
                broke.Add($"{name} -> {ex.GetType().Name}");
            }
        }

        Assert.True(
            broke.Count == 0,
            "resolving the write-lock budget must never throw and must never leave " +
            $"(TimeSpan.Zero, {LocalDataService.MaxWriteLockBudget}]: " + string.Join("; ", broke));
    }

    /// <summary>
    /// The ceiling is inclusive and anything past it resolves the default, derived from the constant so
    /// neither figure is written down twice.
    ///
    /// <para>The bound is deliberately NOT <see cref="TimeSpan"/>'s representable range, which reaches
    /// about 29,000 years and would accept a budget that makes a wedged lock hang for the life of the
    /// process — the silent outcome, where the whole point of a number is the loud one.</para>
    /// </summary>
    [Fact]
    public void TheCeilingIsAcceptedAndAnythingPastItResolvesTheDefault()
    {
        var ceiling = LocalDataService.MaxWriteLockBudget;

        Assert.Equal(ceiling, Resolve(ceiling.TotalSeconds));
        Assert.Equal(LocalDataService.DefaultWriteLockBudget, Resolve(ceiling.TotalSeconds * 2));
        Assert.Equal(LocalDataService.DefaultWriteLockBudget, Resolve(TimeSpan.MaxValue.TotalSeconds));

        /* And this host is inside the band, so the pin above is not passing on a ceiling nothing meets. */
        Assert.InRange(LocalDataService.WriteLockBudget, TimeSpan.FromTicks(1), ceiling);

        static TimeSpan Resolve(double seconds) =>
            LocalDataService.ResolveWriteLockBudget(seconds.ToString("R", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A declaration is honoured only when it is usable, and every unusable shape resolves the app's budget
    /// rather than disabling the wait. Losing dispatcher protection is the failure worth refusing; a host
    /// that merely fails to lengthen its own wait gets a flake back, which is loud.
    /// </summary>
    [Theory]
    [InlineData(null, 5d)]
    [InlineData("", 5d)]
    [InlineData("   ", 5d)]
    [InlineData("later", 5d)]
    [InlineData("0", 5d)]
    [InlineData("-30", 5d)]
    /* A project file is not written in the machine's locale, so a decimal comma is not a decimal point. */
    [InlineData("1,5", 5d)]
    [InlineData("120", 120d)]
    [InlineData("0.5", 0.5d)]
    public void OnlyAUsableDeclarationDisplacesTheDefault(string? declared, double expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), LocalDataService.ResolveWriteLockBudget(declared));
    }

    /// <summary>
    /// The parse reads the project file's spelling, not the machine's. A build agent or a workstation with a
    /// comma-decimal locale must resolve the same budget as this one, and the only way to tell an invariant
    /// parse from an ambient one is to make the ambient culture disagree.
    /// </summary>
    [Fact]
    public void TheDeclarationIsReadInTheInvariantCulture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            /* de-DE reads "1,5" as one and a half and "1.5" as fifteen - both the opposite of this parse. */
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");

            Assert.Equal(TimeSpan.FromSeconds(1.5), LocalDataService.ResolveWriteLockBudget("1.5"));
            Assert.Equal(LocalDataService.DefaultWriteLockBudget, LocalDataService.ResolveWriteLockBudget("1,5"));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    /// <summary>
    /// The host hands the property back as a string whatever JSON type MSBuild wrote, which is why the
    /// resolver parses rather than casts. A value arriving as anything else resolves the default, so this
    /// records that a caller passing a number gets the app's budget and not its own.
    /// </summary>
    [Fact]
    public void ANonStringDeclarationResolvesTheDefault()
    {
        Assert.Equal(LocalDataService.DefaultWriteLockBudget, LocalDataService.ResolveWriteLockBudget(120));
        Assert.Equal(LocalDataService.DefaultWriteLockBudget, LocalDataService.ResolveWriteLockBudget(TimeSpan.FromSeconds(120)));
    }

    /// <summary>
    /// The call site takes the budget. A literal timeout there is the regression this whole file exists to
    /// notice, and it is invisible to every behavioural pin above when the host happens to declare 5.
    /// </summary>
    [Fact]
    public void TheCallSiteTakesTheBudgetRatherThanALiteral()
    {
        var source = ParitySource.ReadFile("Lite/Services/LocalDataService.cs");

        Assert.Contains("AcquireWriteLock(timeout: WriteLockBudget)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AcquireWriteLock(timeout: TimeSpan.", source, StringComparison.Ordinal);
    }

    /* ── derivations ── */

    /// <summary>
    /// Every build file in the repo, minus build output — where restore writes <c>.props</c> and
    /// <c>.targets</c> of its own, and where a copy of a project file proves nothing about what ships.
    /// Segment-wise rather than a substring match, so a directory merely NAMED like output stays in.
    /// </summary>
    private static IEnumerable<string> BuildFiles()
    {
        var root = ParitySource.RepoRoot();
        var options = new EnumerationOptions
        {
            RecurseSubdirectories = true,
            IgnoreInaccessible = true,
        };

        return new[] { "*.csproj", "*.props", "*.targets" }
            .SelectMany(pattern => Directory.EnumerateFiles(root, pattern, options))
            .Where(f => !IsBuildOutput(f));
    }

    private static string Relative(string full) =>
        Path.GetRelativePath(ParitySource.RepoRoot(), full).Replace(Path.DirectorySeparatorChar, '/');

    /// <summary>The seconds a project file declares for the budget property.</summary>
    private static double DeclaredBudgetSeconds(string projectRelativePath)
    {
        var project = XDocument.Parse(ParitySource.ReadFile(projectRelativePath));

        var value = project.Descendants("RuntimeHostConfigurationOption")
            .Where(o => (string?)o.Attribute("Include") == LocalDataService.WriteLockBudgetConfigKey)
            .Select(o => (string?)o.Attribute("Value"))
            .SingleOrDefault();

        Assert.False(
            string.IsNullOrWhiteSpace(value),
            $"{projectRelativePath} declares no {LocalDataService.WriteLockBudgetConfigKey}");

        return double.Parse(value!, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The durations named at every wait in every test file that takes the write lock with no timeout, plus
    /// the sites whose duration could not be read.
    ///
    /// <para>Recursive, matching <see cref="BuildFiles"/>: <c>Lite.Tests</c> has subdirectories, and a helper
    /// under one of them taking this lock would otherwise never be scanned at all.</para>
    /// </summary>
    private static (Dictionary<string, TimeSpan> Holds, List<string> Unreadable) ScanDeliberateHolds()
    {
        var holds = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);
        var unreadable = new List<string>();
        var options = new EnumerationOptions { RecurseSubdirectories = true, IgnoreInaccessible = true };

        foreach (var file in Directory.EnumerateFiles(
            Path.Combine(ParitySource.RepoRoot(), "Lite.Tests"), "*.cs", options))
        {
            if (IsBuildOutput(file))
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            if (!Regex.IsMatch(code, @"AcquireWriteLock\(\s*\)"))
            {
                continue;
            }

            var name = Path.GetFileName(file);
            var durations = new List<TimeSpan>();
            var namedDurations = new HashSet<string>(StringComparer.Ordinal);

            /* TimeSpan.From*(literal), wherever it appears: it is also how a named hold constant such as
               StatusBarSizeReadLockTests.WriteLockHold gets its value, so taking the file's maximum counts
               the constant without having to resolve the identifier at its use site. */
            foreach (Match constructed in Regex.Matches(code, ConstructedDurationPattern))
            {
                if (TryScale(constructed.Groups["unit"].Value, constructed.Groups["value"].Value, out var span))
                {
                    durations.Add(span);
                    var declared = Regex.Match(
                        constructed.Value,
                        @"^(?<name>\w+)\s*=",
                        RegexOptions.None);
                    if (declared.Success)
                    {
                        namedDurations.Add(declared.Groups["name"].Value);
                    }
                }
                else
                {
                    unreadable.Add($"{name}: {Collapse(constructed.Value)}");
                }
            }

            foreach (Match declaration in Regex.Matches(code, DurationConstantPattern))
            {
                namedDurations.Add(declaration.Groups["name"].Value);
            }

            foreach (Match site in Regex.Matches(code, WaitSitePattern))
            {
                var arguments = site.Groups["args"].Value;
                var first = FirstArgument(arguments);

                if (first.Length == 0)
                {
                    /* Nothing readable in the first position. With further arguments behind it that is a
                       separator-shaped call the walker blanked — string.Join, not Thread.Join — and not a
                       wait at all. Alone, it is a wait with no bound, which no hold of this lock may be. */
                    if (arguments.Contains(',', StringComparison.Ordinal))
                    {
                        continue;
                    }

                    unreadable.Add($"{name}: {Collapse(site.Value)} names no duration");
                    continue;
                }

                if (double.TryParse(first, NumberStyles.Float, CultureInfo.InvariantCulture, out var milliseconds))
                {
                    /* Wait, Join, Sleep and Delay all take bare milliseconds, which is the spelling that
                       used to read as no hold. */
                    durations.Add(TimeSpan.FromMilliseconds(milliseconds));
                    continue;
                }

                /* A constructed duration was already counted above; a named one is counted through its
                   declaration. Anything else is a duration this scan cannot see the value of. */
                if (!first.Contains("TimeSpan.", StringComparison.Ordinal) && !namedDurations.Contains(first))
                {
                    unreadable.Add($"{name}: {Collapse(site.Value)}");
                }
            }

            holds[name] = durations.Count == 0 ? TimeSpan.Zero : durations.Max();
        }

        return (holds, unreadable);
    }

    /// <summary><c>TimeSpan.From&lt;unit&gt;(&lt;value&gt;)</c>, with one level of nesting allowed inside the
    /// argument so a non-literal argument is captured rather than skipped.</summary>
    private const string ConstructedDurationPattern =
        @"(?:(?<name>\w+)\s*=\s*)?TimeSpan\.From(?<unit>Days|Hours|Minutes|Seconds|Milliseconds|Ticks)" +
        @"\(\s*(?<value>(?:[^()]|\([^()]*\))*?)\s*\)";

    /// <summary>A field or local bound to a constructed duration, so a wait naming it reads as bounded.</summary>
    private const string DurationConstantPattern =
        @"\b(?<name>\w+)\s*=\s*TimeSpan\.From(?:Days|Hours|Minutes|Seconds|Milliseconds|Ticks)\(";

    /// <summary>The calls that make a hold last: a wait, a join, a sleep, a delay, or a timed lock entry.</summary>
    private const string WaitSitePattern =
        @"\.(?:Wait|WaitAsync|WaitOne|Join|Sleep|Delay|TryEnterWriteLock|TryEnterReadLock)" +
        @"\(\s*(?<args>(?:[^()]|\([^()]*\))*?)\s*\)";

    private static bool TryScale(string unit, string value, out TimeSpan span)
    {
        span = TimeSpan.Zero;
        if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var magnitude))
        {
            return false;
        }

        span = unit switch
        {
            "Days" => TimeSpan.FromDays(magnitude),
            "Hours" => TimeSpan.FromHours(magnitude),
            "Minutes" => TimeSpan.FromMinutes(magnitude),
            "Seconds" => TimeSpan.FromSeconds(magnitude),
            "Milliseconds" => TimeSpan.FromMilliseconds(magnitude),
            _ => TimeSpan.FromTicks((long)magnitude),
        };
        return true;
    }

    /// <summary>The first argument of a call, respecting one level of nesting.</summary>
    private static string FirstArgument(string arguments)
    {
        var depth = 0;
        for (var i = 0; i < arguments.Length; i++)
        {
            var character = arguments[i];
            if (character == '(')
            {
                depth++;
            }
            else if (character == ')')
            {
                depth--;
            }
            else if (character == ',' && depth == 0)
            {
                return arguments[..i].Trim();
            }
        }

        return arguments.Trim();
    }

    /// <summary>One line, for a failure message: the walker leaves blanks where it removed text.</summary>
    private static string Collapse(string text) =>
        Regex.Replace(text, @"\s+", " ").Trim();

    private static bool IsBuildOutput(string path) =>
        Relative(path).Split('/').Any(segment =>
            segment.Equals("bin", StringComparison.OrdinalIgnoreCase) ||
            segment.Equals("obj", StringComparison.OrdinalIgnoreCase));
}
