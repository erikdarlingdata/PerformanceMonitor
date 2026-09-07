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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Central package management pins the DIRECT versions in <c>Directory.Packages.props</c>; the committed
/// <c>packages.lock.json</c> files pin the TRANSITIVE closure, which is the half nobody reviews. CI rejects a
/// change that moves one without the other by restoring <c>--locked-mode</c>, and this holds the two
/// properties that rejection rests on: every locked-mode restore CI runs is able to fail its step, and every
/// committed lock file is reached by one (#3143).
///
/// <para><b>Neither property held.</b> The <c>build</c> job restored six projects in one multi-line
/// <c>run:</c> block. A block like that runs under the runner's default shell — <c>pwsh</c> on Windows —
/// which does not stop on a native command's non-zero exit and reports the block's exit code as the LAST
/// command's, so the five restores ahead of it could print <c>NU1004</c> and leave the step green. Not
/// hypothetical: on the Dependabot pull request #3143 was filed from, the required <c>build</c> check passed
/// with <c>error NU1004</c> in its own log, and the only job that went red was the one whose restore is a
/// single-command step.</para>
///
/// <para><b>And one lock file was reached by nothing.</b> <c>deprecated/Installer</c> carries a lock file, is
/// referenced by no project, and was restored by no workflow. Measured rather than reasoned: perturbing that
/// file left all six restores CI ran green, and failed only a direct restore of that project.</para>
///
/// <para><b>Coverage is derived, not listed.</b> The requirement comes from the lock files on disk and the
/// <c>ProjectReference</c> edges between projects, so a tenth lock file arriving makes its restore mandatory
/// instead of remembered. A referenced project's own lock file IS validated by a locked-mode restore of the
/// referencing project — verified against NuGet by perturbing <c>Installer.Core</c>'s lock file and watching
/// a locked-mode restore of <c>Installer.Tests</c> fail <c>NU1004</c> — so the closure arm encodes measured
/// behaviour rather than an assumption about it.</para>
///
/// <para><b>What this cannot see.</b> Whether the restores CI runs cover the packages a particular change
/// moves; that is NuGet's resolution, not text. What it holds is the shape that lets those restores report a
/// mismatch at all, plus #3143's refusal to reach green by dropping <c>--locked-mode</c>. Both detectors are
/// exercised against synthetic input, because a parser that matched nothing would report every step as safe
/// and every lock file as covered — and would do it in green.</para>
/// </summary>
public sealed class LockedModeRestoreCoverageTests
{
    private static readonly string[] s_buildSegments = { ".github", "workflows", "build.yml" };

    private static readonly string[] s_nightlySegments = { ".github", "workflows", "nightly.yml" };

    /// <summary>
    /// The package whose version is the SDK's own rather than one anyone in this repository chose. The SDK
    /// adds it implicitly to a project that publishes single-file, at the version bundled with whichever SDK
    /// performed the restore — so a lock file recording it is pinned to an SDK patch, and cannot be right for
    /// two patches at once.
    /// </summary>
    private const string SdkCoupledPackage = "Microsoft.NET.ILLink.Tasks";

    /// <summary>
    /// The lock files that cannot be validated in locked mode while the SDK patch floats — held as a ratchet
    /// on the DERIVED set below, not as the requirement itself. Without the ratchet, a project newly
    /// publishing single-file would take the exemption silently, which is the same shape of quiet widening
    /// this class exists to stop.
    /// </summary>
    private static readonly string[] s_sdkCoupledLockFiles = { "deprecated/Installer" };

    /// <summary>
    /// One <c>dotnet restore</c> invocation and what it names. The target is captured so the locked-mode
    /// requirement can be read per invocation rather than per file.
    /// </summary>
    private static readonly Regex RestoreInvocation =
        new(@"dotnet restore\s+(?<target>\S+)", RegexOptions.Compiled);

    /// <summary>
    /// One <c>&lt;ProjectReference Include="..." /&gt;</c>. MSBuild spells these with backslashes on every
    /// platform, so the path is normalised at the use site. <c>DarlingPathFilterGateTests</c> reads the same
    /// edges for a different question — which shared libraries a path gate has to name — and answers it in
    /// library names; this one answers in directories, because a directory is what holds a lock file.
    /// </summary>
    private static readonly Regex ProjectReference =
        new(@"<ProjectReference\s+Include=""(?<path>[^""]+)""", RegexOptions.Compiled);

    /// <summary>
    /// A <c>set -e</c>-family line: bash abandoning the block at the first non-zero exit. Both spellings,
    /// because <c>set -o errexit</c> has the same effect and a check that only knew the short form would
    /// report a safe step as unsafe.
    /// </summary>
    private static readonly Regex SetsErrExit =
        new(@"^\s*set\s+(-[a-z]*e[a-z]*|-o\s+errexit)(\s|$)", RegexOptions.Compiled);

    /// <summary>
    /// A workflow-command annotation line — the one place a restore command appears as TEXT rather than as
    /// something the runner executes, because the remediation message quotes the command to run.
    ///
    /// <para>The exemption is deliberately this narrow rather than "a line mentioning echo", and the
    /// detector it exempts from is deliberately broad: any line carrying <c>dotnet restore</c> is treated as
    /// an invocation. A restore smuggled onto the end of another command still gets read, which is the
    /// direction to be wrong in — the alternative, anchoring the pattern to the start of a command, would
    /// quietly return "no bare restores" for a workflow that had one.</para>
    /// </summary>
    private static readonly Regex Annotation =
        new(@"::(error|warning|notice)[ :]", RegexOptions.Compiled);

    /// <summary>
    /// Every step that runs <c>dotnet restore</c> reports a failure from EVERY restore in it.
    ///
    /// <para>A step that runs several and reports only the last one's exit code is not a weaker guard, it is
    /// an absent one for everything ahead of the last: the log carries the error and the check carries a
    /// tick.</para>
    /// </summary>
    [Theory]
    [InlineData(".github/workflows/build.yml")]
    [InlineData(".github/workflows/nightly.yml")]
    public void EveryRestoreStep_ReportsAFailureFromEveryRestoreInIt(string workflow)
    {
        var steps = RestoreSteps(WorkflowText(workflow), workflow);

        var swallowing = steps
            .Where(step => !PropagatesEveryFailure(step))
            .Select(FirstStepName)
            .ToList();

        Assert.True(
            swallowing.Count == 0,
            $"{workflow} has steps that run more than one command and report only the last one's exit code, "
            + "so a locked-mode restore ahead of it can print NU1004 while the step reports success — give "
            + "each `shell: bash` and open its run block with `set -euo pipefail`:\n  "
            + string.Join("\n  ", swallowing));
    }

    /// <summary>
    /// The step-shape check reads the SHAPE of a step rather than its name, and the cases it must get right
    /// are pinned here instead of left to whatever the two workflows happen to contain.
    ///
    /// <para>The last case is the one a simpler rule gets wrong. "Exactly one restore in the block" is not
    /// sufficient: a block whose restore is followed by anything else reports that other command's exit
    /// code, so the restore is unable to fail the step even though it is the only one there.</para>
    /// </summary>
    [Theory]
    // Several restores, runner default shell: only the last can fail the step.
    [InlineData(false, "\n      - name: Restore\n        run: |\n          dotnet restore A.csproj --locked-mode\n          dotnet restore B.csproj --locked-mode\n")]
    // bash without errexit is the same block with a different interpreter.
    [InlineData(false, "\n      - name: Restore\n        shell: bash\n        run: |\n          dotnet restore A.csproj --locked-mode\n          dotnet restore B.csproj --locked-mode\n")]
    // bash with errexit, set before the first restore.
    [InlineData(true, "\n      - name: Restore\n        shell: bash\n        run: |\n          set -euo pipefail\n          dotnet restore A.csproj --locked-mode\n          dotnet restore B.csproj --locked-mode\n")]
    // errexit set AFTER the first restore protects every restore but that one.
    [InlineData(false, "\n      - name: Restore\n        shell: bash\n        run: |\n          dotnet restore A.csproj --locked-mode\n          set -euo pipefail\n          dotnet restore B.csproj --locked-mode\n")]
    // The single-command form: the step's exit code IS the restore's.
    [InlineData(true, "\n      - name: Restore\n        run: dotnet restore A.csproj --locked-mode\n")]
    // A folded plain scalar reads like the single-command form and is not: the build's exit code reports.
    [InlineData(false, "\n      - name: Restore\n        run: dotnet restore A.csproj --locked-mode\n          && dotnet build A.csproj --no-restore\n")]
    // The same step's sibling keys sit at or above the run key's indent and do not make it a folded scalar.
    [InlineData(true, "\n      - name: Restore\n        run: dotnet restore A.csproj --locked-mode\n        env:\n          CI: 'true'\n")]
    // One restore, but not the whole command — the build's exit code is what reports.
    [InlineData(false, "\n      - name: Restore\n        run: |\n          dotnet restore A.csproj --locked-mode\n          dotnet build A.csproj --no-restore\n")]
    public void TheStepShapeCheck_ReadsTheShapeRatherThanTheStepName(bool propagates, string step) =>
        Assert.Equal(propagates, PropagatesEveryFailure(step));

    /// <summary>
    /// Every committed <c>packages.lock.json</c> is reached by a locked-mode restore the <c>build</c>
    /// workflow runs, directly or through a <c>ProjectReference</c> — bar the ones whose contents follow the
    /// SDK rather than this repository's own choices.
    ///
    /// <para>Scoped to <c>build.yml</c> deliberately: that is the workflow whose checks gate a merge.
    /// <c>nightly.yml</c> restores what it publishes and is a backstop, so requiring whole-tree coverage
    /// there would assert something that workflow is not for.</para>
    /// </summary>
    [Fact]
    public void EveryCommittedLockFile_IsReachedByALockedModeRestoreInTheBuildWorkflow()
    {
        var buildYaml = ReadRepoFileLf(s_buildSegments);
        var globalJson = ReadRepoFileLf("global.json");

        /* Non-vacuity, in the direction that matters: an empty lock-file set or an empty restore set both
           report perfect coverage. */
        var lockFiles = LockFileDirectories();
        Assert.Contains("Darling/Darling.Tests", lockFiles);
        Assert.Contains("deprecated/Installer", lockFiles);
        Assert.NotEmpty(RestoredProjects(buildYaml));

        /* The closure arm carries real weight in the tree as it stands, rather than being a spare wheel that
           would never turn: deprecated/Dashboard holds a lock file, is named by no restore line in the
           workflow, and is covered only because Dashboard.Tests references it. */
        Assert.DoesNotContain("dotnet restore deprecated/Dashboard/Dashboard.csproj", buildYaml, StringComparison.Ordinal);
        Assert.DoesNotContain("deprecated/Dashboard", UncoveredLockFiles(buildYaml, globalJson));

        var uncovered = UncoveredLockFiles(buildYaml, globalJson);

        Assert.True(
            uncovered.Count == 0,
            "Committed packages.lock.json files that no locked-mode restore in .github/workflows/build.yml "
            + "reaches, so a lock file left inconsistent with Directory.Packages.props under one of them "
            + "merges green — add a `dotnet restore <project> --locked-mode` for each, or reference it from "
            + "a project already restored:\n  "
            + string.Join("\n  ", uncovered));
    }

    /// <summary>
    /// The lock files exempt from that requirement are exactly the ones whose contents follow the SDK, and
    /// there is one.
    ///
    /// <para><b>Why an exemption exists at all.</b> <c>deprecated/Installer</c> is the only project
    /// publishing single-file, so the SDK adds an implicit <c>Microsoft.NET.ILLink.Tasks</c> reference at the
    /// version bundled with whichever SDK ran the restore. <c>global.json</c> rolls forward across patches,
    /// so a runner on 10.0.303 wants <c>[10.0.11, )</c> where a lock file written under 10.0.302 records
    /// <c>[10.0.10, )</c>. No restore line can make that file right for both, and nothing had restored the
    /// project, so the drift accumulated unseen until #3143 pointed a restore at it.</para>
    ///
    /// <para><b>Why it is derived and ratcheted rather than a name in a list.</b> Derived, so pinning the SDK
    /// exactly in <c>global.json</c> removes the exemption and makes the restore mandatory — the fix cancels
    /// the excuse instead of leaving it behind. Ratcheted, so a second project taking an SDK-coupled
    /// reference fails here rather than inheriting the exemption in silence, which is the widening this class
    /// exists to catch. Both halves have to hold: the exemption is not the decision about whether that lock
    /// file should exist, only an accurate statement that CI cannot currently check it.</para>
    /// </summary>
    [Fact]
    public void TheExemptLockFiles_AreExactlyTheOnesWhoseContentsFollowTheSdk()
    {
        var globalJson = ReadRepoFileLf("global.json");

        Assert.Equal(s_sdkCoupledLockFiles, SdkCoupledLockFiles(globalJson).ToArray());

        /* The reason, read off the tree rather than restated: the exempt project publishes single-file, which
           is what makes the SDK add the reference, and its lock file is the only one carrying it. */
        Assert.Contains(
            "<PublishSingleFile>true</PublishSingleFile>",
            ReadRepoFileLf("deprecated", "Installer", "PerformanceMonitorInstaller.csproj"),
            StringComparison.Ordinal);

        Assert.Equal(
            s_sdkCoupledLockFiles,
            LockFileDirectories()
                .Where(directory => ReadRepoFileLf(directory, "packages.lock.json").Contains(SdkCoupledPackage, StringComparison.Ordinal))
                .ToArray());

        /* And that the float is real: an SDK pinned exactly would leave nothing exempt. */
        Assert.DoesNotContain("\"rollForward\": \"disable\"", globalJson, StringComparison.Ordinal);
    }

    /// <summary>
    /// The coverage check reports an injected gap, in all three of its arms.
    ///
    /// <para>Removing the restore of a project nothing references must uncover it; removing the restore of a
    /// project that others hang off must uncover THEM; and pinning the SDK must uncover the one the SDK float
    /// is what excuses. Without the third, the exemption could be unconditional and read as derived.</para>
    /// </summary>
    [Fact]
    public void TheCoverageCheck_ReportsAnInjectedGap()
    {
        var real = ReadRepoFileLf(s_buildSegments);
        var globalJson = ReadRepoFileLf("global.json");

        Assert.Empty(UncoveredLockFiles(real, globalJson));

        /* The direct arm: Lite.Tests is covered only by its own restore line. */
        var withoutLiteTests = real.Replace(
            "          dotnet restore Lite.Tests/Lite.Tests.csproj --locked-mode\n",
            string.Empty,
            StringComparison.Ordinal);
        Assert.NotEqual(real, withoutLiteTests);
        Assert.Contains("Lite.Tests", UncoveredLockFiles(withoutLiteTests, globalJson));

        /* The closure arm: Dashboard.Tests is the only restore that reaches deprecated/Dashboard, and it
           reaches it only by reference. */
        var withoutDashboardTests = real.Replace(
            "          dotnet restore deprecated/Dashboard.Tests/Dashboard.Tests.csproj --locked-mode\n",
            string.Empty,
            StringComparison.Ordinal);
        Assert.NotEqual(real, withoutDashboardTests);
        Assert.Contains("deprecated/Dashboard", UncoveredLockFiles(withoutDashboardTests, globalJson));

        /* The exemption arm: with the SDK pinned exactly, deprecated/Installer stops being excusable and the
           requirement reports it — which is the state a decision to keep that lock file has to reach. */
        var pinnedSdk = globalJson.Replace(
            "\"rollForward\": \"latestPatch\"",
            "\"rollForward\": \"disable\"",
            StringComparison.Ordinal);
        Assert.NotEqual(globalJson, pinnedSdk);
        Assert.Empty(SdkCoupledLockFiles(pinnedSdk));
        Assert.Contains("deprecated/Installer", UncoveredLockFiles(real, pinnedSdk));
    }

    /// <summary>
    /// No restore any workflow executes reaches green by leaving locked mode, and no project file switches it
    /// off.
    ///
    /// <para>#3143 refused this option on the merits — central package management pins the direct versions,
    /// the lock files pin the transitive closure, and a build that re-resolves that closure on every run has
    /// stopped guaranteeing what ships. Pinning the refusal is what makes it survive the next red Dependabot
    /// pull request, because the error NuGet prints names the opt-out as one of its own two remedies. A
    /// deliberate reversal edits this test; an expedient one fails it.</para>
    /// </summary>
    [Fact]
    public void NoRestoreAWorkflowExecutes_LeavesLockedMode()
    {
        var scanned = 0;
        var invocations = 0;
        var offending = new List<string>();

        foreach (var file in Directory.GetFiles(Path.Combine(Root, ".github", "workflows"), "*.yml").OrderBy(path => path, StringComparer.Ordinal))
        {
            var yaml = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var name = Path.GetFileName(file);
            scanned++;

            Assert.DoesNotContain("RestoreLockedMode", yaml, StringComparison.Ordinal);

            invocations += RestoreLines(yaml).Count;
            offending.AddRange(RestoresOutsideLockedMode(yaml).Select(line => $"{name}: {line}"));
        }

        /* Non-vacuity: the sweep read the workflow directory and found the restores that are in it. A glob
           matching nothing, or a detector matching nothing, satisfies the emptiness assertion below by
           never having anything to judge. */
        Assert.True(scanned >= 2, $"only {scanned} workflow files were scanned");
        Assert.True(invocations >= 10, $"only {invocations} dotnet restore lines were seen across {scanned} workflow files");

        Assert.True(
            offending.Count == 0,
            "Restores a workflow executes without --locked-mode, or with --force-evaluate, which resolve the "
            + "transitive closure freshly instead of against the committed lock file:\n  "
            + string.Join("\n  ", offending));

        foreach (var project in Directory.GetFiles(Root, "*.csproj", SearchOption.AllDirectories))
        {
            if (IsBuildOutput(project))
            {
                continue;
            }

            Assert.DoesNotContain("RestoreLockedMode", File.ReadAllText(project), StringComparison.Ordinal);
        }

        Assert.DoesNotContain("RestoreLockedMode", ReadRepoFileLf("Directory.Packages.props"), StringComparison.Ordinal);
    }

    /// <summary>
    /// The locked-mode requirement is read per line, reports a restore that opts out however it is spelled,
    /// and leaves the remediation message alone.
    ///
    /// <para>The last two cases are the pair that matters. A message quoting the command a human should run
    /// is not a restore CI performs, and the workflow carries one; a restore appended to another command IS
    /// one, and a detector narrowed enough to excuse the message would stop seeing it.</para>
    /// </summary>
    [Theory]
    [InlineData(0, "        run: dotnet restore A.csproj --locked-mode\n")]
    [InlineData(1, "        run: dotnet restore A.csproj\n")]
    [InlineData(1, "        run: dotnet restore A.csproj --force-evaluate\n")]
    [InlineData(1, "        run: |\n          dotnet restore A.csproj --locked-mode\n          dotnet restore B.csproj\n")]
    [InlineData(0, "          echo \"::error title=T::Regenerate with 'dotnet restore Solution.sln --force-evaluate' and commit them.\"\n")]
    [InlineData(1, "          dotnet build A.csproj && dotnet restore B.csproj\n")]
    public void TheLockedModeRequirement_ReadsEveryRestoreAndOnlyTheMessageIsExempt(int offending, string yaml) =>
        Assert.Equal(offending, RestoresOutsideLockedMode(yaml).Count);

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// One of the two scanned workflows, LF-normalised, chosen by its repo-relative path so the
    /// <c>[InlineData]</c> above names the file it is talking about.
    /// </summary>
    private static string WorkflowText(string workflow) => workflow switch
    {
        ".github/workflows/build.yml" => ReadRepoFileLf(s_buildSegments),
        ".github/workflows/nightly.yml" => ReadRepoFileLf(s_nightlySegments),
        _ => throw new ArgumentOutOfRangeException(nameof(workflow), workflow, "not a scanned workflow"),
    };

    /// <summary>
    /// The steps of one workflow that run <c>dotnet restore</c>, split at the six-space indent every step in
    /// these two files sits at.
    /// </summary>
    private static List<string> RestoreSteps(string yaml, string workflow)
    {
        var steps = Regex.Split(yaml, @"(?=\n {6}- )")
            .Where(step => RestoreInvocation.IsMatch(step))
            .ToList();

        Assert.True(steps.Count > 0, $"{workflow} runs no dotnet restore — find where it moved before editing this test");

        /* Counted two ways, because the two fail differently. A step boundary this split does not recognise
           leaves its restore lines outside every block returned, and the check then reports the steps it did
           find as safe while saying nothing about the one it lost — a partial answer wearing a green tick.
           The file's own invocation count cannot lose a step to a boundary it never looks for. */
        Assert.Equal(
            RestoreInvocation.Matches(yaml).Count,
            steps.Sum(step => RestoreInvocation.Matches(step).Count));

        return steps;
    }

    /// <summary>
    /// Whether every <c>dotnet restore</c> in one step block is able to fail that step.
    /// </summary>
    private static bool PropagatesEveryFailure(string step)
    {
        var lines = step.Split('\n');
        var firstRestore = Array.FindIndex(lines, line => RestoreInvocation.IsMatch(line));

        if (firstRestore < 0)
        {
            return true;
        }

        /* The single-command form, `run: dotnet restore ...` on the key's own line: the step's exit code IS
           that restore's, whatever shell interprets it.

           The continuation clause is not decoration. A YAML plain scalar may fold across lines, so
           `run: dotnet restore X --locked-mode` followed by a more-indented `&& dotnet build Y` is ONE
           command whose exit code is the build's — the same swallow wearing the shape of the safe form.
           What identifies it is the CONTIGUOUS run of more-indented lines directly beneath the key, not
           "any deeper line later in the step": a sibling `env:` key sits at the run key's own indent and
           carries deeper lines of its own, so a check that looked further down the step would call every
           step with an env block unsafe. Measured against the key's indentation rather than a fixed column,
           so a step nested one level deeper does not take this branch for free. */
        var runIndent = Indent(lines[firstRestore]);

        var continuation = lines
            .Skip(firstRestore + 1)
            .TakeWhile(line => line.Trim().Length > 0 && Indent(line) > runIndent)
            .ToList();

        if (lines.Count(line => RestoreInvocation.IsMatch(line)) == 1
            && Regex.IsMatch(lines[firstRestore], @"^\s*run: dotnet restore ")
            && continuation.Count == 0)
        {
            return true;
        }

        /* Otherwise the block runs more than one command, and the only thing that makes each of them able to
           fail the step is bash abandoning the block on the first non-zero exit — declared, and set before
           the first restore rather than anywhere in the block. */
        return lines.Take(firstRestore).Any(line => Regex.IsMatch(line, @"^\s*shell: bash\s*$"))
            && lines.Take(firstRestore).Any(line => SetsErrExit.IsMatch(line));
    }

    /// <summary>
    /// Every line of a fragment of YAML that carries a <c>dotnet restore</c>, annotation lines included, as
    /// their own text — the population the locked-mode requirement is read against, and the count its
    /// non-vacuity floor is taken from.
    /// </summary>
    private static List<string> RestoreLines(string yaml) =>
        yaml.Replace("\r\n", "\n", StringComparison.Ordinal)
            .Split('\n')
            .Where(line => RestoreInvocation.IsMatch(line))
            .Select(line => line.Trim())
            .ToList();

    /// <summary>
    /// The restore lines that opt out of locked mode: no <c>--locked-mode</c>, or a <c>--force-evaluate</c>
    /// that overrides it. Annotation lines are excluded because the runner prints them rather than running
    /// them.
    /// </summary>
    private static List<string> RestoresOutsideLockedMode(string yaml) =>
        RestoreLines(yaml)
            .Where(line => !Annotation.IsMatch(line))
            .Where(line => !line.Contains("--locked-mode", StringComparison.Ordinal)
                        || line.Contains("--force-evaluate", StringComparison.Ordinal))
            .ToList();

    /// <summary>
    /// The name of the step a block belongs to, for a message that names what to go and edit.
    /// </summary>
    private static string FirstStepName(string step)
    {
        var match = Regex.Match(step, @"- name: (?<name>.+)");
        return match.Success ? match.Groups["name"].Value.Trim() : step.Split('\n').FirstOrDefault(l => l.Trim().Length > 0)?.Trim() ?? "(unnamed step)";
    }

    /// <summary>
    /// Every directory holding a committed <c>packages.lock.json</c>, repo-relative with forward slashes.
    /// </summary>
    private static SortedSet<string> LockFileDirectories()
    {
        var found = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var lockFile in Directory.GetFiles(Root, "packages.lock.json", SearchOption.AllDirectories))
        {
            if (IsBuildOutput(lockFile))
            {
                continue;
            }

            found.Add(RepoRelative(Path.GetDirectoryName(lockFile)!));
        }

        Assert.NotEmpty(found);
        return found;
    }

    /// <summary>
    /// The project files one workflow restores by name, as absolute paths.
    /// </summary>
    private static List<string> RestoredProjects(string yaml) =>
        RestoreInvocation.Matches(yaml)
            .Select(match => match.Groups["target"].Value)
            .Where(target => target.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase))
            .Select(target => Path.GetFullPath(Path.Combine(Root, target.Replace('/', Path.DirectorySeparatorChar))))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>
    /// The lock-file directories whose contents follow the SDK's version rather than this repository's
    /// choices, and which therefore cannot satisfy a locked-mode restore while <c>global.json</c> lets the
    /// SDK patch float. Empty when the SDK is pinned exactly, which is what makes the exemption a statement
    /// about the current configuration rather than about those projects.
    /// </summary>
    private static List<string> SdkCoupledLockFiles(string globalJson)
    {
        if (globalJson.Contains("\"rollForward\": \"disable\"", StringComparison.Ordinal))
        {
            return new List<string>();
        }

        return LockFileDirectories()
            .Where(directory => ReadRepoFileLf(directory, "packages.lock.json").Contains(SdkCoupledPackage, StringComparison.Ordinal))
            .ToList();
    }

    /// <summary>
    /// The lock-file directories no restore in one workflow reaches, excluding the ones the SDK float
    /// excuses.
    /// </summary>
    private static List<string> UncoveredLockFiles(string yaml, string globalJson)
    {
        var reached = new HashSet<string>(StringComparer.Ordinal);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new Queue<string>(RestoredProjects(yaml));

        while (pending.Count > 0)
        {
            var project = pending.Dequeue();

            if (!seen.Add(project))
            {
                continue;
            }

            /* A ProjectReference that does not resolve fails loudly rather than silently shrinking the
               closure: a short closure is how this check would start reporting coverage it does not have. */
            Assert.True(File.Exists(project), $"a restore names, or a ProjectReference points at, a file that is not there: {project}");

            var directory = Path.GetDirectoryName(project)!;
            reached.Add(RepoRelative(directory));

            foreach (Match match in ProjectReference.Matches(File.ReadAllText(project)))
            {
                var include = match.Groups["path"].Value
                    .Replace('\\', Path.DirectorySeparatorChar)
                    .Replace('/', Path.DirectorySeparatorChar);

                pending.Enqueue(Path.GetFullPath(Path.Combine(directory, include)));
            }
        }

        var excused = SdkCoupledLockFiles(globalJson);

        return LockFileDirectories()
            .Where(directory => !reached.Contains(directory))
            .Where(directory => !excused.Contains(directory, StringComparer.Ordinal))
            .ToList();
    }

    /// <summary>
    /// How far one line is indented, which is what tells a folded scalar's continuation from the next key.
    /// </summary>
    private static int Indent(string line) => line.Length - line.TrimStart().Length;

    /// <summary>
    /// An absolute path as a repo-relative one with forward slashes, so the two sides of the coverage
    /// comparison and the failure message all spell a directory the same way.
    /// </summary>
    private static string RepoRelative(string absolute) =>
        Path.GetRelativePath(Root, absolute).Replace(Path.DirectorySeparatorChar, '/');

    /// <summary>
    /// Whether a path sits under a <c>bin</c> or <c>obj</c> directory below the repository root.
    ///
    /// <para>No build output in this tree holds a <c>packages.lock.json</c> or a <c>.csproj</c> today —
    /// restore writes <c>project.assets.json</c> and the generated props and targets, not copies of these —
    /// so this filter currently changes no answer, and that is said out loud rather than left to look like a
    /// measurement. It is here because the sweeps above are recursive globs over the whole repository, and a
    /// copy appearing under <c>obj</c> would be reported as a lock-file directory no restore reaches: a red
    /// requirement produced by a local artefact, failing for whoever had built the tree and passing for
    /// whoever had not. The repository already excludes build output by construction for the same reason.</para>
    /// </summary>
    private static bool IsBuildOutput(string absolute) =>
        Path.GetRelativePath(Root, absolute)
            .Split(Path.DirectorySeparatorChar, '/')
            .Any(segment => segment.Equals("bin", StringComparison.OrdinalIgnoreCase)
                         || segment.Equals("obj", StringComparison.OrdinalIgnoreCase));
}
