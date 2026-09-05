/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A store read that FAILS must never be recorded as a store read that succeeded. Two places on the
/// control plane did exactly that, and they are one defect in two shapes — surfaced while deriving
/// <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/>, whose whole floor rests on the fact that a
/// deadline on this thread does not delay a reload, it DISCARDS one.
///
/// <para><b>Shape one: the config_version watermark.</b> <c>DarlingWorker</c>'s reload beacon assigned
/// <c>_lastConfigVersion</c> on DETECTING a new version, then reloaded. A reload whose store read failed
/// had therefore already recorded the version as applied — the operator's change stayed live in the
/// store, absent from the process, and unreported until some later write bumped the version again,
/// because the beacon's next tick compared equal and never retried.</para>
///
/// <para><b>Shape two: the role statement_timeout.</b>
/// <c>DarlingManagedRoles.ReadComposeStatementTimeoutAsync</c> caught every exception with a bare,
/// unlogged <c>catch (Exception)</c> and returned 15 — swallowing cancellation, saying nothing, and
/// handing its caller a value that then went onto the viewer/mcp roles as
/// <c>ALTER ROLE … SET statement_timeout</c>. Since #2931 the CLIENT side of that same backstop reads the
/// SAME <c>config_service.compose_statement_timeout_seconds</c> column live per run, so a silent default
/// desynced the two halves: an operator who set 120 got a client deadline of 120, a server ceiling of 15,
/// every composed query dying at 15 s, and 120 visible in the UI throughout.</para>
///
/// <para><b>No Lite twin for either</b>, checked rather than assumed, because a one-SKU fix is the drift
/// this codebase keeps paying for. Lite has no login roles and no <c>statement_timeout</c> concept at all
/// (its store is embedded DuckDB, and <c>compose_statement_timeout</c>, <c>ManagedRoles</c> and
/// <c>BuildProvisioningSql</c> appear nowhere under <c>Lite/</c>, <c>Lite.Tests/</c> or
/// <c>PerformanceMonitor.Common/</c>), and it has no <c>config_version</c> beacon — its settings are
/// applied in-process by the Settings window, so there is no store-side watermark to advance early. The
/// only <c>config_version</c> mention under <c>Lite/</c> is a comment in <c>AppAlertEngineSettings</c>
/// describing Darling's behaviour. <see cref="NeitherFixHasALiteTwinToDriftFrom"/> pins that, so a Lite
/// twin appearing later fails asking for the same fix rather than drifting quietly.</para>
/// </summary>
public sealed class ControlPlaneReloadDurabilityTests
{
    /// <summary>
    /// The reload's own <c>ALTER ROLE</c> pair, as the single renderer emits it. Matched on the shape
    /// rather than a literal so the assertions below cannot pass against a hand-written near-copy.
    /// </summary>
    private static readonly Regex s_statementTimeoutGrant = new(
        @"ALTER ROLE \w+\s+SET statement_timeout = '(\d+)s';",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void AnUnreadableComposeTimeout_LeavesTheRolesHorizonAlone()
    {
        var readable = DarlingManagedRoles.BuildProvisioningSql(
            "AdminPassword01", "ViewerPassword02", "McpPassword03", 120);

        var unreadable = DarlingManagedRoles.BuildProvisioningSql(
            "AdminPassword01", "ViewerPassword02", "McpPassword03", null);

        /* Positive control FIRST, through the identical regex the negative below uses: a scan that cannot
           see the statements it is asserting the absence of would pass on any input. */
        var grants = s_statementTimeoutGrant.Matches(readable);
        Assert.Equal(2, grants.Count);
        Assert.All(grants, m => Assert.Equal("120", m.Groups[1].Value));

        Assert.Empty(s_statementTimeoutGrant.Matches(unreadable));

        /* The OMISSION has to be surgical — everything else provisioning does still has to happen, or
           "do not overwrite the horizon" would have become "do not provision", which is a much worse
           trade than the one it replaces. Asserted as an exact difference rather than by spot-checking a
           few statements: strip the two ALTER ROLE lines from the readable batch and what is left has to
           be the unreadable one, modulo the placeholder comment that marks the omission. */
        foreach (var essential in new[] { "CREATE ROLE", "GRANT USAGE ON SCHEMA", "GRANT CONNECT ON DATABASE" })
        {
            Assert.Contains(essential, unreadable, StringComparison.Ordinal);
        }

        static string[] Statements(string sql) =>
            sql.Split('\n')
                .Select(l => l.TrimEnd('\r'))
                .Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal))
                .Where(l => l.Trim().Length > 0)
                .ToArray();

        Assert.Equal(
            Statements(readable).Where(l => !l.Contains("SET statement_timeout", StringComparison.Ordinal)).ToArray(),
            Statements(unreadable));
    }

    /// <summary>
    /// The sentinel and the gate that consumes it, composed. This is what makes the omission above
    /// self-healing rather than merely non-destructive: provisioning reports "I wrote nothing", and the
    /// next control-plane reload therefore re-asserts instead of concluding the roles are already right.
    /// </summary>
    [Theory]
    [InlineData(5)]
    [InlineData(15)]
    [InlineData(120)]
    [InlineData(600)]
    public void TheUnknownSentinel_CannotBeMistakenForAnAppliedValue(int storeSeconds)
    {
        /* Every value the store can hand out has been through the clamp, so the sentinel is only safe if
           it lies outside the clamp's range in a way no input can reach. */
        Assert.Equal(storeSeconds, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(storeSeconds));

        Assert.True(
            DarlingManagedRoles.ShouldReassertComposeStatementTimeout(
                storeSeconds, DarlingManagedRoles.ComposeStatementTimeoutUnknown,
                managedStore: true, isWindows: true),
            $"a store value of {storeSeconds}s compared against the unknown sentinel must re-assert — "
            + "otherwise provisioning that could not read the column leaves the roles stale forever, since "
            + "#2918's gate only fires on a difference and a value set before the restart bumps no version");

        /* And the mirror image, so the assertion above is not passing because the gate always fires. */
        Assert.False(
            DarlingManagedRoles.ShouldReassertComposeStatementTimeout(
                storeSeconds, storeSeconds, managedStore: true, isWindows: true));
    }

    /// <summary>
    /// The sentinel must stay negative. A future edit moving it into the clamp's range would silently
    /// stop the convergence above, and every value assertion in this file would still pass.
    /// </summary>
    [Fact]
    public void TheUnknownSentinel_StaysOutsideEveryReachableValue()
    {
        var sentinel = DarlingManagedRoles.ComposeStatementTimeoutUnknown;

        Assert.True(sentinel < 0, $"the unknown sentinel is {sentinel}, which a stored value could equal");

        Assert.NotEqual(sentinel, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(sentinel));
        Assert.NotEqual(sentinel, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(0));
        Assert.NotEqual(sentinel, StoreConfigProvider.MinComposeStatementTimeoutSeconds);
    }

    /// <summary>
    /// What provisioning REPORTS after a failed read, which is the half that makes the omission
    /// self-healing. Returning the shipped default here would compile, pass every value assertion above,
    /// and leave the roles stale forever — #2918's gate only fires on a difference, so a baseline equal to
    /// the store's value is indistinguishable from "already applied".
    /// </summary>
    [Fact]
    public void ProvisioningReportsTheUnknownSentinel_WhenItCouldNotReadTheColumn()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(SourceText("DarlingManagedRoles.cs"));

        var member = code.IndexOf("public static async Task<int> EnsureProvisionedAsync(", StringComparison.Ordinal);
        Assert.True(member > 0, "EnsureProvisionedAsync could not be located, so this check proves nothing");

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', member)));

        Assert.Contains("ComposeStatementTimeoutUnknown", body, StringComparison.Ordinal);

        /* And the read it branches on has to be nullable — an int cannot express "I could not find out",
           which is exactly why the original returned 15. */
        Assert.Contains(
            "private static async Task<int?> ReadComposeStatementTimeoutAsync(",
            code,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// No unfiltered <c>catch (Exception)</c> anywhere in <c>DarlingManagedRoles</c>. The specific defect
    /// was one that also swallowed <see cref="OperationCanceledException"/> — so a shutdown arriving
    /// mid-start was reported as "the store said 15" — and the fix is the
    /// <c>when (ex is not OperationCanceledException)</c> filter this file's own callers already use.
    /// Swept over the file rather than pinned at the one line, because the failure mode to guard is the
    /// next one being added.
    /// </summary>
    [Fact]
    public void RoleProvisioning_HasNoUnfilteredCatch()
    {
        var source = SourceText("DarlingManagedRoles.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        /* The lookahead has to sit immediately after the ')' and swallow the whitespace ITSELF. Written as
           `\s*(?!when)` it backtracks: the engine consumes the space, fails the lookahead at "when", then
           gives the space back and succeeds at " " — so the FILTERED form matched too, which the positive
           control below caught on the first run. */
        var unfiltered = new Regex(
            @"catch\s*\(\s*Exception(?:\s+\w+)?\s*\)(?!\s*when)",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        var offenders = unfiltered.Matches(code)
            .Select(m => LineOf(code, m.Index))
            .ToArray();

        /* Positive control on the negative, through the identical regex: both spellings of the defect,
           and the filtered form must NOT match. */
        Assert.Equal(2, unfiltered.Matches("catch (Exception)\n{\n}\ncatch (Exception ex)\n{\n}\n").Count);
        Assert.Empty(unfiltered.Matches("catch (Exception ex) when (ex is not OperationCanceledException)\n{\n}\n"));

        Assert.True(
            offenders.Length == 0,
            $"DarlingManagedRoles has {offenders.Length} unfiltered catch(Exception) at line(s) "
            + $"{string.Join(", ", offenders)} — an unfiltered catch here swallows cancellation and then "
            + "writes a guessed statement_timeout onto the login roles");
    }

    /// <summary>
    /// The watermark ordering, pinned by parsing the beacon because there is no other way to reach it: it
    /// lives inside <c>RunCollectionLoopAsync</c>, a private member of a <c>BackgroundService</c> that
    /// needs a configured host, a store and a fleet to run at all — the same reason
    /// <c>HostHeaderGuardTests</c> and <c>DarlingStoreUpgrade</c>'s post-commit-catch ordering are parsed
    /// rather than exercised. It is a WIRING invariant, and wiring is invisible to a logic test.
    ///
    /// <para>Asserted structurally, not by string equality: the assignment to <c>_lastConfigVersion</c>
    /// must sit INSIDE the braces of the <c>if (appliedVersion.HasValue)</c> that guards it, and the
    /// <c>appliedVersion</c> it reads must come from the <c>ReloadFromStoreAsync</c> call immediately
    /// above. A scan for "does the assignment appear after the call" would pass on the defect the moment
    /// someone moved the call one line up, which is exactly the count-invariant relocation this sweep
    /// keeps finding.</para>
    ///
    /// <para>It also pins WHICH version is stamped, not merely that one is. The first version of this fix
    /// stamped <c>configVersion.Value</c> — the beacon's own earlier read — while
    /// <c>ReloadFromStoreAsync</c> re-reads <c>config_version</c> inside <c>LoadViewAsync</c>, so a write
    /// landing between the two left the watermark behind a config that was already live. This test froze
    /// that imprecision until review caught it, which is why the negative against the outer read is here
    /// and is positive-controlled.</para>
    /// </summary>
    [Fact]
    public void TheConfigVersionWatermark_AdvancesOnlyInsideTheReloadSuccessBranch()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(SourceText("DarlingWorker.cs"));

        var guard = new Regex(
            @"var appliedVersion = await ReloadFromStoreAsync\s*\([^)]*\)\s*;\s*if\s*\(\s*appliedVersion\.HasValue\s*\)\s*\{",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        var match = guard.Match(code);
        Assert.True(
            match.Success,
            "the reload beacon no longer guards on ReloadFromStoreAsync's applied version, so a reload "
            + "that read nothing can record its config_version as applied and will never be retried");

        var branch = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', match.Index + match.Length - 1));

        /* Stamped from the APPLIED version, not from the beacon's own earlier read. Review caught that the
           first version of this fix used `configVersion.Value` — correct about ordering, imprecise about
           WHICH version: LoadViewAsync re-reads config_version, so a write landing between the two makes
           the applied view newer than the beacon saw, and the older stamp leaves the watermark behind a
           live config. Self-healing rather than lossy, but the same class of imprecision as the defect
           this class exists for, and the startup path already stamped it correctly. */
        Assert.Contains("_lastConfigVersion = appliedVersion.Value;", branch, StringComparison.Ordinal);

        /* The beacon's own outer read must NOT be what lands on the watermark anywhere — this is the
           assertion that would have failed on the first version of the fix. */
        Assert.Empty(Regex.Matches(code, @"_lastConfigVersion\s*=\s*configVersion\.Value\s*;"));

        /* And exactly one assignment from the applied version, so a second one outside the branch cannot
           re-introduce the early advance alongside the correct one. */
        Assert.Equal(
            1,
            Regex.Matches(code, @"_lastConfigVersion\s*=\s*appliedVersion\.Value\s*;").Count);

        /* Positive control on the negative above, through the identical pattern: an assertion that can
           only pass is not an assertion. */
        Assert.Single(Regex.Matches(
            "_lastConfigVersion = configVersion.Value;",
            @"_lastConfigVersion\s*=\s*configVersion\.Value\s*;"));

        /* ReloadFromStoreAsync has to REPORT failure for the guard to mean anything — a method that
           always returned true would satisfy every assertion above, which is why the signature check is
           not the end of it. */
        var reload = code.IndexOf("private async Task<long?> ReloadFromStoreAsync(", StringComparison.Ordinal);
        Assert.True(reload > 0, "ReloadFromStoreAsync no longer reports WHICH config_version it applied");

        var reloadBody = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', reload)));

        /* The unreadable-view branch must return FALSE. This is the assertion that separates "the plumbing
           is shaped right" from "the plumbing carries the answer". */
        var nullBranch = new Regex(
            @"if\s*\(\s*view is null\s*\)\s*\{[^}]*return null\s*;[^}]*\}",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        Assert.True(
            nullBranch.IsMatch(reloadBody),
            "ReloadFromStoreAsync does not return null when LoadViewAsync could not read the store, so the "
            + "caller's guard always succeeds and the config_version watermark advances on a reload that "
            + "applied nothing — the original defect, reachable again through the new return value");

        Assert.Equal(1, Regex.Matches(reloadBody, @"return null\s*;").Count);

        /* And what it returns on SUCCESS is the view's own version, so the watermark, the log line and the
           applied config can never disagree. A method returning any other long would satisfy every
           assertion above. */
        Assert.Contains("return view.ConfigVersion;", reloadBody, StringComparison.Ordinal);
    }

    /// <summary>
    /// The retry the ordering fix creates must not become a log flood. With the watermark held back, a
    /// persistently unreachable store is re-attempted on every 15 s tick — roughly 5,760 attempts a day —
    /// and <c>LoadViewAsync</c> used to warn on each one. It now warns on the first failure of a streak,
    /// drops to Debug, and reports recovery once.
    /// </summary>
    [Fact]
    public void ThePendingReloadRetry_ReportsTheTransitionRatherThanEveryAttempt()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(SourceText("StoreConfigProvider.cs"));

        Assert.Contains("private int _viewReadFailureStreak;", code, StringComparison.Ordinal);

        /* The warning is GATED on the streak's first increment rather than emitted unconditionally. */
        Assert.Matches(new Regex(@"\+\+_viewReadFailureStreak\s*==\s*1"), code);

        /* Exactly one reset, and it is on the success path — a reset in the catch would make every failure
           look like a first failure and restore the flood. */
        Assert.Equal(1, Regex.Matches(code, @"_viewReadFailureStreak\s*=\s*0\s*;").Count);

        /* Scoped to LoadViewAsync's own body. Searching the FILE for that catch signature finds an
           earlier member's copy of it — which made this comparison meaningless on the first run, and is
           the same file-versus-member scoping error #2928 generalised. */
        var member = code.IndexOf("public async Task<StoreConfigView?> LoadViewAsync(", StringComparison.Ordinal);
        Assert.True(member > 0, "LoadViewAsync could not be located, so the ordering check below proves nothing");

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', member)));

        var reset = body.IndexOf("_viewReadFailureStreak = 0;", StringComparison.Ordinal);
        var catchStart = body.IndexOf("catch (Exception ex) when (ex is not OperationCanceledException)", StringComparison.Ordinal);

        Assert.True(reset > 0, "the streak reset is not inside LoadViewAsync");
        Assert.True(catchStart > 0, "LoadViewAsync's catch could not be located, so the check below proves nothing");
        Assert.True(
            reset < catchStart,
            "the failure streak is reset inside or after the catch rather than on the success path, so a "
            + "persistent failure would report every attempt as the first");
    }

    /// <summary>
    /// Both fixes are Darling-only, and that is asserted rather than remembered. If a Lite twin of either
    /// mechanism ever appears, this fails and points at the fix that has to be mirrored.
    /// </summary>
    [Fact]
    public void NeitherFixHasALiteTwinToDriftFrom()
    {
        var root = RepoRoot();
        var liteSources = new[] { "Lite", "Lite.Tests", "PerformanceMonitor.Common" }
            .Select(d => Path.Combine(root, d))
            .Where(Directory.Exists)
            .SelectMany(d => Directory.EnumerateFiles(d, "*.cs", SearchOption.AllDirectories))
            .Where(p => !IsBuildOutput(p))
            .ToArray();

        Assert.True(liteSources.Length > 100, $"the Lite sweep found only {liteSources.Length} files — the layout has moved");

        var twins = new Regex(
            @"compose_statement_timeout|BuildProvisioningSql|ManagedRoles|_lastConfigVersion",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        /* Comment-stripped, because Lite DOES discuss Darling's config_version behaviour in prose
           (AppAlertEngineSettings) and prose is not a twin. That distinction is the whole reason this
           assertion can be stated at all. */
        var hits = liteSources
            .Select(p => (Path: p, Code: CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(p))))
            .SelectMany(f => twins.Matches(f.Code).Select(m => $"{Path.GetFileName(f.Path)}:{LineOf(f.Code, m.Index)}"))
            .ToArray();

        /* Positive control through the identical regex, so an empty result means "nothing there" rather
           than "the scan is broken". */
        Assert.Equal(2, twins.Matches("var x = compose_statement_timeout; _lastConfigVersion = 1;").Count);

        Assert.True(
            hits.Length == 0,
            "Lite now carries a twin of one of the two control-plane mechanisms fixed here, so the fix has "
            + $"to be mirrored rather than left one-SKU: {string.Join(", ", hits)}");
    }

    private static string SourceText(string fileName)
    {
        var path = Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", fileName);

        Assert.True(File.Exists(path), $"source not found: {path}");

        return File.ReadAllText(path);
    }

    private static bool IsBuildOutput(string path) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(s => string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase));

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
