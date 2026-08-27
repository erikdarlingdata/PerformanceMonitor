/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins #2529: <c>upgrade-darling.ps1</c> can name — and with <c>-RemoveStaleFiles</c>, delete — the files
/// an earlier build shipped and the new one does not, and it can NEVER touch the store, the config, the
/// credentials, the rollback backups, or a file the new build ships.
///
/// <para><b>Why the feature exists, measured rather than assumed.</b> An in-place upgrade is an OVERLAY:
/// <c>Expand-Archive -Force</c> writes what the new build ships and deletes nothing else. Diffing the file
/// lists of consecutive release zips says how often that matters: the Lite package dropped 44 shipped files
/// across twelve consecutive releases — 43 of them in one step, where a target-framework move stranded
/// every <c>runtimes\*\lib\net8.0\</c> assembly including two copies of
/// <c>Microsoft.Data.SqlClient.dll</c>. Those are .NET PROBING directories, so a stale assembly there is a
/// candidate for loading rather than inert clutter.</para>
///
/// <para><b>Why the tests are shaped like this.</b> The whole design rests on one property — every path the
/// procedure can nominate provably came out of one of our own build payloads, because the manifest is
/// derived from the payload rather than maintained by hand. A test that asserted on the script's TEXT could
/// never see that property break. So these run the shipped functions, lifted out of the <c>.ps1</c> by
/// brace matching and executed under Windows PowerShell against planted trees, and the load-bearing ones
/// hand the delete a DELIBERATELY POISONED list holding every path that must survive it — then check the
/// filesystem, not the script's own report of what it did.</para>
///
/// <para><b>No C# twin of the exclusion rule, deliberately.</b> <c>DarlingRollbackBackups</c> exists
/// because the SERVICE genuinely participates in that convention: it recognises those directories so it can
/// report them on one line. Nothing in the service participates in this one — the manifest is written and
/// read by the deploy script alone — so a second implementation in C# would be a copy that nothing ships,
/// and a copy drifts while its test keeps passing. The shared case table below is run against the shipped
/// PowerShell, which is the artifact that actually decides.</para>
/// </summary>
public sealed class DarlingDeployStaleFileTests
{
    private static string DeployScriptPath => Path.Combine(RepoRoot, "Darling", "tools", "upgrade-darling.ps1");

    private static string DeployScript => File.ReadAllText(DeployScriptPath);

    /// <summary>The manifest's name, written down here so the pin below can prove the script agrees with
    /// itself: <c>$manifestName</c> is what gets created, and the exclusion rule carries the same spelling
    /// as a literal so the predicate answers alike when it is lifted out of the script and run alone. Two
    /// spellings of one name is the #2525 failure with a new subject.</summary>
    private const string ManifestFileName = "darling-install-manifest.txt";

    /// <summary>
    /// Every function the probes need. Extracted together so a probe never runs against a partially
    /// assembled copy of the script's own rules.
    /// </summary>
    private static readonly string[] s_functions =
    [
        "Test-DarlingRollbackBackupName",
        "Format-DarlingBytes",
        "ConvertTo-DarlingManifestPath",
        "Join-DarlingInstallPath",
        "Test-DarlingPathIsAbsolute",
        "Test-DarlingManifestPathIsPreserved",
        "Get-DarlingPayloadFiles",
        "Read-DarlingInstallManifest",
        "Write-DarlingInstallManifest",
        "Select-DarlingStaleFiles",
        "Remove-DarlingStaleFiles",
    ];

    /// <summary>
    /// The shared case table for the exclusion rule. Each row says what must happen and WHY, because the
    /// why is the part a future edit has to argue with.
    ///
    /// <para>The <c>pg-runtime</c> rows are the ones that matter most and there are five of them, including
    /// a name we do not ship today (<c>pg-runtime-2027</c>). The rule is a PREFIX on the top-level name
    /// rather than an enumeration of the two directories in the layout, precisely so that a third one
    /// arriving later is covered before it exists: an enumeration is a list somebody has to maintain, and
    /// the cost of it being one entry out of date here is the monitoring store.</para>
    ///
    /// <para>The near misses are deliberate too. <c>pgruntime\</c> and <c>rollback_manual_…</c> are how a
    /// too-eager prefix test would swallow a directory that is genuinely ours to remove, and
    /// <c>darling.sample.json</c> is protected by the DIFF rather than by this rule — it is shipped by every
    /// build, so it is on both sides of every comparison. A rule that preserved it here would be hiding a
    /// hole in the diff.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string Path, bool Preserved, string Because)> PreservedPathCases =
    [
        (@"pg-runtime\pgsql\bin\pg_ctl.exe", true, "THE STORE — deleting this destroys every collected row"),
        (@"pg-runtime\PG_VERSION", true, "the store, at its own root"),
        (@"PG-RUNTIME\pgsql\bin\psql.exe", true, "Windows paths are case-insensitive and so is this rule"),
        (@"pg-runtime-prev\pgsql\bin\pg_ctl.exe", true, "the rescued previous runtime"),
        (@"pg-runtime-2027\anything.dll", true, "a future name in the same namespace, covered before it exists"),
        (@"pg-runtime.zip", true, "shipped by every build, and inside the namespace either way"),
        (@"darling.json", true, "operator config — the zip ships only darling.sample.json"),
        (@"DARLING.JSON", true, "the same file, spelled loudly"),
        (@"darling.json.bak-20260819-120000", true, "a byte-for-byte copy of every secret in the config"),
        (@"pg-credential.dpapi", true, "a DPAPI blob, recoverable from nowhere else once deleted"),
        (@"_rollback_manual_20260819-120000\PerformanceMonitor.Darling.Service.exe", true, "the prune owns those directories"),
        (ManifestFileName, true, "the file that decides the deletes is not deletable by them"),
        (@"..\..\Windows\System32\kernel32.dll", true, "escapes the install root"),
        (@"viewer\..\..\evil.dll", true, "escapes it from further in"),
        (@"C:\Windows\System32\kernel32.dll", true, "drive-qualified, so it did not come from a payload"),
        (@"\\fileserver\share\x.dll", true, "UNC — refused, not restripped into an ordinary-looking relative path"),
        (@"\Windows\System32\kernel32.dll", true, "root-relative, and refused BEFORE the leading slash is stripped"),
        ("/usr/lib/x.so", true, "the same, forward-slash spelled"),
        ("", true, "nothing at all"),
        ("   ", true, "whitespace"),
        (@"runtimes\win\lib\net8.0\Microsoft.Data.SqlClient.dll", false, "the measured real case, in a probing path"),
        (@"Microsoft.Extensions.Logging.dll", false, "a dropped dependency at the install root"),
        (@"de\PerformanceMonitor.Darling.Service.resources.dll", false, "a satellite for a culture nothing localizes into now"),
        (@"viewer\Old.Assembly.dll", false, "inside a product directory, which the layout report cannot see into"),
        ("wwwroot/js/pages/gone.js", false, "a zip-spelled path, forward slashes and all"),
        (@"darling.sample.json", false, "shipped, so the DIFF protects it — this rule must not have to"),
        (@"pgruntime\x.dll", false, "a near miss that is not the pg-runtime namespace"),
        (@"rollback_manual_20260819\x.dll", false, "no leading underscore, so not the backup namespace"),
    ];

    /// <summary>
    /// THE exclusion pin. Runs the shipped predicate over the table above.
    ///
    /// <para>This is the rule standing between a manifest and a recursive-free delete inside a monitoring
    /// host's install directory. Everything else in this file is a consequence of it being right.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_NeverNominatesAPathThatMustSurviveAnUpgrade()
    {
        var probe = new StringBuilder();
        probe.AppendLine(Functions());

        foreach (var (path, _, _) in PreservedPathCases)
        {
            /* Single-quoted so nothing in a path is expanded. The table holds no quote characters and the
               count assertion below fails loudly if one is ever added. */
            probe.AppendLine($"Test-DarlingManifestPathIsPreserved '{path}'");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(PreservedPathCases.Count, answers.Count);

        var wrong = new List<string>();
        for (var i = 0; i < PreservedPathCases.Count; i++)
        {
            var (path, preserved, because) = PreservedPathCases[i];
            var actual = string.Equals(answers[i], "True", StringComparison.OrdinalIgnoreCase);
            if (actual != preserved)
            {
                wrong.Add($"'{path}': the script said {actual}, the table says {preserved} ({because})");
            }
        }

        Assert.True(wrong.Count == 0,
            "upgrade-darling.ps1's exclusion rule no longer answers the way it must (#2529):\n  " + string.Join("\n  ", wrong));
    }

    /// <summary>
    /// The one that would cost a customer their history. A DELIBERATELY POISONED list — every path the rule
    /// exists to stop, plus a containment escape resolving to a real file outside the tree — is handed
    /// straight to the shipped delete, against a planted install tree.
    ///
    /// <para>The assertions are against the FILESYSTEM, never against the function's own report of what it
    /// did. A delete that miscounts is a bug; a delete that removed the store and said it did not is the
    /// failure this test exists for, and only the disk can tell them apart.</para>
    ///
    /// <para>Such a list cannot be produced by a manifest this script wrote — preserved paths are filtered
    /// on the way IN as well as out. That is the argument for testing it: the guard's whole job is to be
    /// there on the day something upstream is wrong, so it has to be exercised with an upstream that is.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_LeavesTheStoreAndTheConfigAlone_EvenWhenTheListItIsHandedNamesThem()
    {
        var root = Directory.CreateTempSubdirectory("darling-2529-poison-");
        try
        {
            var stale = Plant(root.FullName, Path.Combine("runtimes", "win", "lib", "net8.0", "Microsoft.Data.SqlClient.dll"));
            var satellite = Plant(root.FullName, Path.Combine("de", "PerformanceMonitor.Darling.Service.resources.dll"));
            var shippedFile = Plant(root.FullName, Path.Combine("runtimes", "win", "lib", "net10.0", "Microsoft.Data.SqlClient.dll"));
            var store = Plant(root.FullName, Path.Combine("pg-runtime", "pgsql", "bin", "pg_ctl.exe"));
            var storeVersion = Plant(root.FullName, Path.Combine("pg-runtime", "PG_VERSION"));
            var previousRuntime = Plant(root.FullName, Path.Combine("pg-runtime-prev", "pgsql", "bin", "pg_ctl.exe"));
            var config = Plant(root.FullName, "darling.json");
            var configBackup = Plant(root.FullName, "darling.json.bak-20260819-120000");
            var credential = Plant(root.FullName, "pg-credential.dpapi");
            var backup = Plant(root.FullName, Path.Combine("_rollback_manual_20260819-120000", "PerformanceMonitor.Darling.Service.exe"));

            /* Outside the install root entirely, reached by a relative escape. */
            var outside = Path.Combine(Path.GetDirectoryName(root.FullName)!, $"darling-2529-outside-{Guid.NewGuid():N}.dll");
            File.WriteAllText(outside, "outside");

            try
            {
                var hostile = new[]
                {
                    @"runtimes\win\lib\net8.0\Microsoft.Data.SqlClient.dll",
                    @"de\PerformanceMonitor.Darling.Service.resources.dll",
                    @"runtimes\win\lib\net10.0\Microsoft.Data.SqlClient.dll",
                    @"pg-runtime\pgsql\bin\pg_ctl.exe",
                    @"pg-runtime\PG_VERSION",
                    @"pg-runtime-prev\pgsql\bin\pg_ctl.exe",
                    @"darling.json",
                    @"darling.json.bak-20260819-120000",
                    @"pg-credential.dpapi",
                    @"_rollback_manual_20260819-120000\PerformanceMonitor.Darling.Service.exe",
                    @"..\" + Path.GetFileName(outside),
                };

                var shipped = new[] { @"runtimes\win\lib\net10.0\Microsoft.Data.SqlClient.dll", "PerformanceMonitor.Darling.Service.exe" };

                RunRemoval(root.FullName, hostile, shipped);

                Assert.True(File.Exists(store), "THE STORE was deleted: pg-runtime\\pgsql\\bin\\pg_ctl.exe");
                Assert.True(File.Exists(storeVersion), "THE STORE was deleted: pg-runtime\\PG_VERSION");
                Assert.True(Directory.Exists(Path.Combine(root.FullName, "pg-runtime")), "the pg-runtime directory itself was removed");
                Assert.True(File.Exists(previousRuntime), "the rescued previous runtime was deleted");
                Assert.True(File.Exists(config), "darling.json was deleted");
                Assert.True(File.Exists(configBackup), "a darling.json.bak-* copy was deleted");
                Assert.True(File.Exists(credential), "a DPAPI credential blob was deleted");
                Assert.True(File.Exists(backup), "a rollback backup's contents were deleted");
                Assert.True(File.Exists(outside), "a file OUTSIDE the install root was reached by a relative escape");
                Assert.True(File.Exists(shippedFile), "a file the NEW BUILD SHIPS was deleted");

                /* And it still did the job it is for. A guard that achieves safety by doing nothing is not
                   the thing being pinned here. */
                Assert.False(File.Exists(stale), "the stale assembly the new build does not ship was left behind");
                Assert.False(File.Exists(satellite), "the stale satellite assembly was left behind");
            }
            finally
            {
                TryDelete(outside);
            }
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Directory pruning, and the line it must not cross. Emptying <c>runtimes\win\lib\net8.0\</c> removes
    /// that directory and walks up only while each parent is also empty — so <c>runtimes\win\lib\</c>, which
    /// still holds <c>net10.0\</c>, survives.
    ///
    /// <para>Pruning at all is not tidiness. The service's layout report classifies a satellite-resource
    /// directory STRUCTURALLY — one holding nothing but <c>*.resources.dll</c> — and an EMPTY directory
    /// deliberately fails that test, so it would be reported as "not part of the product's layout" on every
    /// start. Removing the last file out of <c>de\</c> and leaving the shell behind would turn one stale
    /// file into a permanent startup warning: #2525's too-loud-to-be-useful outcome, caused by our own
    /// cleanup.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_PrunesADirectoryItEmptied_AndNotOneThatStillHasSomethingInIt()
    {
        var root = Directory.CreateTempSubdirectory("darling-2529-prune-");
        try
        {
            Plant(root.FullName, Path.Combine("runtimes", "win", "lib", "net8.0", "a.dll"));
            Plant(root.FullName, Path.Combine("runtimes", "win", "lib", "net10.0", "a.dll"));
            Plant(root.FullName, Path.Combine("de", "x.resources.dll"));

            RunRemoval(
                root.FullName,
                [@"runtimes\win\lib\net8.0\a.dll", @"de\x.resources.dll"],
                ["PerformanceMonitor.Darling.Service.exe"]);

            Assert.False(Directory.Exists(Path.Combine(root.FullName, "runtimes", "win", "lib", "net8.0")), "the emptied framework directory was left behind");
            Assert.False(Directory.Exists(Path.Combine(root.FullName, "de")), "the emptied culture directory was left behind");
            Assert.True(Directory.Exists(Path.Combine(root.FullName, "runtimes", "win", "lib")), "a directory that still held net10.0 was pruned anyway");
            Assert.True(Directory.Exists(root.FullName), "the install root itself was removed");
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The diff, run as shipped. A file the NEW build ships is never selected — including when the two
    /// builds spell it in different CASES, which is the shape that would delete what the copy had just
    /// written: Windows filenames are case-insensitive, so a build that respelled
    /// <c>wwwroot\js\App.js</c> as <c>app.js</c> ships the same file under a name an ordinal comparison
    /// calls absent.
    /// </summary>
    [Fact]
    public void TheDeployScript_NeverSelectsAFileTheNewBuildShips_WhateverTheCase()
    {
        var selected = RunSelect(
            [@"a.dll", @"b.dll", @"runtimes\win\lib\net8.0\x.dll", @"wwwroot\js\App.js"],
            [@"a.dll", @"runtimes\win\lib\net10.0\x.dll", "wwwroot/js/app.js"]);

        Assert.Equal([@"b.dll", @"runtimes\win\lib\net8.0\x.dll"], selected);
    }

    /// <summary>
    /// The input that must not be expressible, in both places that can express it — the same shape as
    /// <c>Select-DarlingRollbackBackupsToPrune</c>'s floor of 1.
    ///
    /// <para>An empty "what this build ships" makes EVERY file an earlier build shipped stale at once,
    /// which is the largest delete this code can produce. It cannot come from a real payload, so arriving
    /// at either function with one means something upstream failed and reported success — and selecting
    /// nothing is the answer that is right either way.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_RemovesNothing_WhenItCannotSayWhatTheNewBuildShips()
    {
        Assert.Empty(RunSelect([@"a.dll", @"b.dll", @"viewer\c.dll"], []));

        var root = Directory.CreateTempSubdirectory("darling-2529-blind-");
        try
        {
            var planted = Plant(root.FullName, "a.dll");
            RunRemoval(root.FullName, [@"a.dll"], []);
            Assert.True(File.Exists(planted), "a delete that did not know what the build ships removed a file anyway");
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The manifest round trip, and the three ways it is refused instead of half-believed: absent, from a
    /// format this script does not understand, and disagreeing with its own <c>file-count</c>.
    ///
    /// <para>Also the write-side filter, which is the one that matters most and is invisible from the read
    /// side. The ONE route by which <c>pg-runtime</c> could ever reach a manifest is a <c>-Source</c> FOLDER
    /// that is a copy of a live install tree — somebody's staging directory made from a box that has run —
    /// and that walk feeds this function directly. A path that cannot be written down cannot be read back
    /// and acted on by a later version of this script.</para>
    /// </summary>
    [Fact]
    public void TheDeployScriptsManifest_RefusesWhatItCannotVouchFor_AndNeverRecordsThePreservedPaths()
    {
        var root = Directory.CreateTempSubdirectory("darling-2529-manifest-");
        try
        {
            var manifest = Path.Combine(root.FullName, ManifestFileName);

            var probe = new StringBuilder();
            probe.AppendLine(Functions());
            probe.AppendLine($"$manifest = '{manifest}'");

            /* absent */
            probe.AppendLine("(Read-DarlingInstallManifest $manifest).Ok");

            /* written, then read back — the preserved paths and the duplicate must not survive the write */
            probe.AppendLine(@"(Write-DarlingInstallManifest $manifest @('a.dll', 'viewer\b.dll', 'pg-runtime\pgsql\bin\pg_ctl.exe', 'darling.json', 'pg-credential.dpapi', 'a.dll') 'PerformanceMonitorDarling-9.9.9.zip' ([datetime]::UtcNow)).Count");
            probe.AppendLine("$read = Read-DarlingInstallManifest $manifest");
            probe.AppendLine("$read.Ok");
            probe.AppendLine("($read.Files -join '|')");
            /* And the raw file, not just the parse: 'pg_ctl' appears nowhere in the manifest's own header
               text, so a hit here means a store path was genuinely written down. */
            probe.AppendLine("((Get-Content -LiteralPath $manifest -Raw) -match 'pg_ctl')");

            /* truncated: drop the last path line, leaving file-count claiming one more than is there */
            probe.AppendLine("$lines = @(Get-Content -LiteralPath $manifest)");
            probe.AppendLine("Set-Content -LiteralPath $manifest -Value $lines[0..($lines.Count - 2)]");
            probe.AppendLine("(Read-DarlingInstallManifest $manifest).Ok");

            /* a format from the future */
            probe.AppendLine("Write-DarlingInstallManifest $manifest @('a.dll') 'x.zip' ([datetime]::UtcNow) | Out-Null");
            probe.AppendLine("$raw = (Get-Content -LiteralPath $manifest -Raw).Replace('manifest-version 1', 'manifest-version 2')");
            probe.AppendLine("Set-Content -LiteralPath $manifest -Value $raw -NoNewline");
            probe.AppendLine("(Read-DarlingInstallManifest $manifest).Ok");

            var answers = RunWindowsPowerShell(probe.ToString());

            Assert.Equal("False", answers[0]);
            Assert.Equal("2", answers[1]);
            Assert.Equal("True", answers[2]);
            Assert.Equal(@"a.dll|viewer\b.dll", answers[3]);
            Assert.Equal("False", answers[4]);
            Assert.Equal("False", answers[5]);
            Assert.Equal("False", answers[6]);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// An absolute path is refused where the RAW TEXT is — by the manifest reader, from a real file on disk,
    /// and by the payload reader, from a real zip.
    ///
    /// <para><b>This is the test the first two attempts at the rule did not have, and the reason the rule
    /// moved.</b> Review caught it twice. The check began life inside
    /// <c>Test-DarlingManifestPathIsPreserved</c>, then was reordered inside it to run before
    /// normalization — both read correctly and neither could fire, because every caller of that predicate
    /// normalizes first and normalization strips the leading separators that say "absolute". A case table
    /// calling the predicate directly proved it correct and proved nothing about the script, which is
    /// exactly how a check that cannot fire keeps its green.</para>
    ///
    /// <para>So these go through <c>Read-DarlingInstallManifest</c> and <c>Get-DarlingPayloadFiles</c>, the
    /// two places raw text enters the script, and each manifest is written BY HAND with a consistent
    /// <c>file-count</c>. That last part is not fussiness: the first version of this fixture appended a line
    /// to a manifest the writer had produced, which left <c>file-count</c> one short — so it went green on
    /// the TRUNCATION check while the rule under test was absent. The control row is here for the same
    /// reason.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_RefusesAnAbsolutePathWhereTheRawTextIs_NotOnlyInThePredicate()
    {
        var root = Directory.CreateTempSubdirectory("darling-2529-absolute-");
        try
        {
            var manifest = Path.Combine(root.FullName, ManifestFileName);

            var probe = new StringBuilder();
            probe.AppendLine(Functions());
            probe.AppendLine($"$manifest = '{manifest}'");
            probe.AppendLine(
                "function PlantManifest([string[]]$entries) { Set-Content -LiteralPath $manifest -Value (@("
                + "'manifest-version 1', 'build-source x.zip', 'written-utc 2026-08-23T00:00:00Z', "
                + "('file-count ' + $entries.Count), '--- files ---') + $entries) }");

            probe.AppendLine(@"PlantManifest @('a.dll', '\\fileserver\share\x.dll')");
            probe.AppendLine("(Read-DarlingInstallManifest $manifest).Ok");

            probe.AppendLine(@"PlantManifest @('a.dll', 'C:\Windows\System32\kernel32.dll')");
            probe.AppendLine("(Read-DarlingInstallManifest $manifest).Ok");

            probe.AppendLine(@"PlantManifest @('a.dll', '\Windows\System32\kernel32.dll')");
            probe.AppendLine("(Read-DarlingInstallManifest $manifest).Ok");

            /* THE CONTROL. Without it, all three rows above could be passing because the fixture is broken
               rather than because the rule works. */
            probe.AppendLine(@"PlantManifest @('a.dll', 'viewer\b.dll')");
            probe.AppendLine("$control = Read-DarlingInstallManifest $manifest");
            probe.AppendLine("$control.Ok");
            probe.AppendLine("$control.Files.Count");

            /* And the payload reader, against a real archive carrying a rooted entry. */
            probe.AppendLine($"$zip = '{Path.Combine(root.FullName, "hostile.zip")}'");
            probe.AppendLine("Add-Type -AssemblyName 'System.IO.Compression.FileSystem' -ErrorAction SilentlyContinue");
            probe.AppendLine("$archive = [IO.Compression.ZipFile]::Open($zip, 'Create')");
            probe.AppendLine("$entry = $archive.CreateEntry('/etc/passwd')");
            probe.AppendLine("$writer = New-Object IO.StreamWriter($entry.Open())");
            probe.AppendLine("$writer.Write('x')");
            probe.AppendLine("$writer.Dispose()");
            probe.AppendLine("$archive.Dispose()");
            probe.AppendLine("(Get-DarlingPayloadFiles $zip $true).Ok");

            var answers = RunWindowsPowerShell(probe.ToString());

            Assert.Equal(["False", "False", "False", "True", "2", "False"], answers);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A UNC or root-relative path is refused THROUGH <c>Select-DarlingStaleFiles</c> and
    /// <c>Write-DarlingInstallManifest</c>, not only when the predicate is asked about it directly.
    ///
    /// <para><b>This is the gap the case table could not see, and review found it.</b> The predicate's
    /// absolute-path half only works on RAW text — normalization strips the leading separators that make a
    /// path UNC or root-relative, so <c>\\fileserver\share\x.dll</c> arrives as the perfectly ordinary
    /// <c>fileserver\share\x.dll</c>. Both of these functions used to normalize first and hand the
    /// normalized value over, which left that half of the rule doing nothing at either site. It was safe
    /// only because the readers upstream refuse an absolute path before it gets this far — an invariant
    /// living in the CALLERS rather than in the functions, which is a guard with an expiry date nobody
    /// wrote down.</para>
    ///
    /// <para>So this exercises the two functions, which is the thing
    /// <see cref="PreservedPathCases"/> cannot do: that table proves the predicate correct when called with
    /// raw text, and proves nothing about whether anyone calls it that way. A drive-qualified path was
    /// never affected — the colon survives normalization — so the rows here are deliberately the two that
    /// were.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_RefusesARawUncPath_ThroughTheDiffAndTheManifestWriter()
    {
        var root = Directory.CreateTempSubdirectory("darling-2529-rawcallers-");
        try
        {
            var manifest = Path.Combine(root.FullName, ManifestFileName);

            var probe = new StringBuilder();
            probe.AppendLine(Functions());

            /* Through the diff: neither absolute path is nominated, and the ordinary one still is — so a
               function that simply selected nothing could not pass this. */
            probe.AppendLine(@"$selected = Select-DarlingStaleFiles @('\\fileserver\share\x.dll', '\Windows\System32\kernel32.dll', 'b.dll') @('other.dll')");
            probe.AppendLine("($selected -join '|')");

            /* Through the writer: only the ordinary path is recorded. */
            probe.AppendLine($"$manifest = '{manifest}'");
            probe.AppendLine(@"(Write-DarlingInstallManifest $manifest @('a.dll', '\\fileserver\share\x.dll', '\Windows\System32\kernel32.dll') 'x.zip' ([datetime]::UtcNow)).Count");
            probe.AppendLine("((Read-DarlingInstallManifest $manifest).Files -join '|')");

            var answers = RunWindowsPowerShell(probe.ToString());

            Assert.Equal(["b.dll", "1", "a.dll"], answers);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The previous build's manifest is read BEFORE the copy, and the post-copy block does not read it
    /// again.
    ///
    /// <para><b>Why the order is load-bearing, which review had to point out.</b> A folder source is copied
    /// wholesale — <c>Copy-Item -Path "$Source\*" -Recurse -Force</c> takes everything in it, unfiltered —
    /// so a staging directory made from a box that has already run carries THAT box's manifest, and
    /// <c>-Force</c> lays it over this install's. Reading afterwards computes this run's stale-file list
    /// against another install's history. It self-heals on the next upgrade, because the manifest written
    /// at the end comes from the real payload, but one cycle of a wrong answer is one too many for a list
    /// that feeds a delete. <c>$configHashBefore</c> is captured before the copy for exactly this reason;
    /// the manifest now sits beside it.</para>
    ///
    /// <para>A text pin rather than a behavioural one, and the limitation is worth stating: this asserts
    /// the ORDER of two statements in the shipped script, which is the thing that regressed and the thing a
    /// reader would most plausibly "tidy" back. The behaviour itself is exercised locally against planted
    /// trees, where reverting the order makes the run report "no files from the previous build were
    /// dropped" — a confident wrong answer — while naming a file only the other box ever had.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_ReadsThePreviousManifestBeforeTheCopy_NotAfterIt()
    {
        var script = DeployScript;

        var read = script.IndexOf("$previousManifest = Read-DarlingInstallManifest $manifestPath", StringComparison.Ordinal);
        Assert.True(read >= 0, "upgrade-darling.ps1 no longer reads the previous install manifest (#2529)");

        var copy = script.IndexOf("Expand-Archive -LiteralPath $Source -DestinationPath $InstallRoot -Force", StringComparison.Ordinal);
        Assert.True(copy >= 0, "upgrade-darling.ps1 no longer lays the new build over the install root");

        Assert.True(read < copy,
            "upgrade-darling.ps1 reads the previous install manifest AFTER the copy. A folder source is copied "
            + "wholesale, so a staging directory made from a live install overwrites this install's manifest first, "
            + "and the stale-file diff is then computed against another box's history (#2529).");

        /* And it is read exactly once. A second read after the copy would take the foreign manifest even
           with the first one in place, which is the same defect wearing a belt. */
        Assert.Equal(1, CountOccurrences(script, "Read-DarlingInstallManifest $manifestPath"));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /// <summary>
    /// The manifest's NAME, in the two places the script spells it. <c>$manifestName</c> is what a run
    /// creates and reads; the exclusion rule carries the same string as a LITERAL so the predicate answers
    /// identically when it is lifted out of the script and run on its own — which is what every probe in
    /// this file does, and what a future reader will do at a prompt.
    ///
    /// <para>Two spellings of one name is #2525 with a new subject: the rename would look completely
    /// harmless, every test here would keep passing against the lifted copy, and on a real box the manifest
    /// would become a file the procedure no longer protects from its own delete.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_SpellsTheManifestNameTheSameWayInBothPlaces()
    {
        var script = DeployScript;

        Assert.Contains($"$manifestName = '{ManifestFileName}'", script, StringComparison.Ordinal);
        Assert.Contains($"$leaf.Equals('{ManifestFileName}', [StringComparison]::OrdinalIgnoreCase)", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// The PowerShell that runs on a Windows box rather than on a developer's: the three the release zip
    /// ships and an operator is told to run, plus the pg-runtime build script the workflows invoke.
    /// <c>new-upgraded-store-fixture.ps1</c> is deliberately absent — it generates a test fixture and never
    /// leaves the repo.
    /// </summary>
    private static readonly string[] s_shippedScripts =
    [
        "upgrade-darling.ps1",
        "install-darling.ps1",
        "uninstall-darling.ps1",
        "fetch-pg-runtime.ps1",
    ];

    /// <summary>
    /// Every shipped script PARSES under Windows PowerShell, and none of them binds a PowerShell operator
    /// as a command parameter.
    ///
    /// <para>Two different bugs, one check, and this pin has already earned itself twice over.</para>
    ///
    /// <para><b>The operator half.</b> <c>$x | ForEach-Object { } -join ', '</c> binds <c>-join</c> to
    /// <c>ForEach-Object</c> as a PARAMETER. It parses clean, it reads correctly, and it throws at RUNTIME —
    /// which in <c>upgrade-darling.ps1</c> means after the service has been stopped, leaving a monitoring
    /// host down over a formatting mistake. Two comments in that file warn about it; a warning is not a
    /// check, and the AST can see it directly.</para>
    ///
    /// <para><b>The parse half is what this actually caught (#2529).</b> It ran red on the FIRST CI round
    /// against lines nobody had touched, and the cause was real:
    /// <c>Parser::ParseFile</c> under Windows PowerShell 5.1 reads a BOM-less file using the ANSI code page,
    /// which is exactly what <c>powershell.exe</c> itself does when it RUNS one. The em dashes #2528 put
    /// inside double-quoted strings are three UTF-8 bytes, and the third of them decodes in CP1252 to
    /// <c>&#x201D;</c> — a character PowerShell honours as a closing double quote. So every one of them
    /// terminated its string early and the file did not parse at all, on the default shell of the operating
    /// system it targets. Reproduced byte for byte, down to the same "The '&lt;' operator is reserved for
    /// future use" the CI run reported.</para>
    ///
    /// <para>This is deliberately NOT fixed by adding a byte-order mark, which would work and would decay:
    /// a BOM is one careless save away from being gone, and its absence is invisible. <see
    /// cref="TheShippedScripts_HoldNoByteAbove127"/> pins the version that cannot decay instead.</para>
    /// </summary>
    [Fact]
    public void TheShippedScripts_ParseAndBindNoOperatorAsACommandParameter()
    {
        var probe = new StringBuilder();
        probe.AppendLine("$found = @()");

        foreach (var name in s_shippedScripts)
        {
            var path = Path.Combine(RepoRoot, "Darling", "tools", name);
            Assert.True(File.Exists(path), $"expected {path} to exist");

            probe.AppendLine("$errors = $null");
            probe.AppendLine("$tokens = $null");
            probe.AppendLine($"$ast = [System.Management.Automation.Language.Parser]::ParseFile('{path}', [ref]$tokens, [ref]$errors)");
            probe.AppendLine($"foreach ($e in @($errors)) {{ $found += \"{name} PARSE-ERROR line $($e.Extent.StartLineNumber): $($e.Message)\" }}");
            probe.AppendLine("if (-not $errors -or $errors.Count -eq 0) {");
            probe.AppendLine("  $operators = @('join', 'f', 'replace', 'split', 'match', 'contains', 'eq', 'ne', 'like', 'is', 'as')");
            probe.AppendLine("  $commands = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] }, $true)");
            probe.AppendLine("  foreach ($command in $commands) {");
            probe.AppendLine("    foreach ($element in $command.CommandElements) {");
            probe.AppendLine("      if ($element -is [System.Management.Automation.Language.CommandParameterAst]) {");
            probe.AppendLine("        if ($operators -contains $element.ParameterName) {");
            probe.AppendLine($"          $found += \"{name} OPERATOR-AS-PARAMETER line $($element.Extent.StartLineNumber): -$($element.ParameterName) on $($command.GetCommandName())\"");
            probe.AppendLine("        }");
            probe.AppendLine("      }");
            probe.AppendLine("    }");
            probe.AppendLine("  }");
            probe.AppendLine("}");
        }

        probe.AppendLine("foreach ($f in @($found)) { $f }");
        probe.AppendLine("'DONE'");

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.True(
            answers.Count == 1 && answers[0] == "DONE",
            "a shipped PowerShell script does not parse under Windows PowerShell, or binds an operator as a command parameter:\n  "
            + string.Join("\n  ", answers));
    }

    /// <summary>
    /// No shipped PowerShell script holds a byte above 127.
    ///
    /// <para>The rule the parse pin above proved we need, in the form that cannot be got wrong. Windows
    /// PowerShell 5.1 decodes a BOM-less script using the machine's ANSI code page, so a single em dash
    /// inside a double-quoted string ends that string early and the script stops parsing — which is what
    /// <c>upgrade-darling.ps1</c> was doing when this test was written, on the default shell of the OS it
    /// ships to. The five em dashes already in <c>install-darling.ps1</c> and <c>fetch-pg-runtime.ps1</c>
    /// were harmless only because they happened to sit in COMMENTS, where a mis-decoded character is never
    /// parsed. That is not a property anybody can maintain by eye.</para>
    ///
    /// <para>So the rule is not "no em dash inside a double-quoted string" — that needs a parser and gets it
    /// wrong once. It is "no byte above 127", which anyone can check, holds under every code page, and does
    /// not care whether the file has a byte-order mark. The same rule keeps the extracted-function probes in
    /// this file and in <c>DarlingDeployRollbackRetentionTests</c> honest: they write extracted PowerShell
    /// to a temp file and run it under <c>powershell.exe</c>, so a non-ASCII byte inside any function they
    /// lift would break the test in a way that looks nothing like its cause.</para>
    ///
    /// <para>Scoped to the scripts we SHIP, not to every .ps1 in the repo. These are the ones an operator
    /// runs on a Windows Server box with whatever shell that box has.</para>
    /// </summary>
    [Fact]
    public void TheShippedScripts_HoldNoByteAbove127()
    {
        var offenders = new List<string>();

        foreach (var name in s_shippedScripts)
        {
            var path = Path.Combine(RepoRoot, "Darling", "tools", name);
            var bytes = File.ReadAllBytes(path);

            var line = 1;
            for (var i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] == (byte)'\n') { line++; continue; }
                if (bytes[i] > 127)
                {
                    offenders.Add($"{name} line {line}: byte 0x{bytes[i]:X2}");
                    /* One report per line is enough — an em dash is three bytes and would otherwise name
                       itself three times. */
                    while (i < bytes.Length && bytes[i] != (byte)'\n') { i++; }
                    line++;
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "a shipped PowerShell script holds a byte above 127. Windows PowerShell 5.1 decodes a BOM-less "
            + "script with the ANSI code page, and the third byte of a UTF-8 em dash becomes a smart closing "
            + "quote there, which ends a double-quoted string early and stops the file parsing (#2529). Use "
            + "ASCII:\n  " + string.Join("\n  ", offenders));
    }

    /// <summary>Runs the shipped <c>Select-DarlingStaleFiles</c> over two lists and returns what it
    /// selected.</summary>
    private static List<string> RunSelect(string[] previous, string[] current)
    {
        var probe = new StringBuilder();
        probe.AppendLine(Functions());
        probe.AppendLine($"$previous = @({Quote(previous)})");
        probe.AppendLine($"$current = @({Quote(current)})");
        probe.AppendLine("$selected = Select-DarlingStaleFiles $previous $current");
        /* Parenthesised before anything downstream: `$x | ForEach-Object { } -join ','` binds -join to
           ForEach-Object and throws, which is the formatting bug this repo has already lost a deploy to. */
        probe.AppendLine("foreach ($s in @($selected)) { $s }");

        return RunWindowsPowerShell(probe.ToString());
    }

    /// <summary>Runs the shipped <c>Remove-DarlingStaleFiles</c> against a real tree. The caller asserts on
    /// the FILESYSTEM afterwards; this returns nothing on purpose, so a test cannot accidentally be written
    /// against the function's own account of what it did.</summary>
    private static void RunRemoval(string installRoot, string[] relativePaths, string[] shippedPaths)
    {
        var probe = new StringBuilder();
        /* The delete reports through Note/Warn, which live in the script's own preamble rather than in any
           function the probe extracts. Stubs, so the extracted copy runs the same code path. */
        probe.AppendLine("function Note([string]$message) { }");
        probe.AppendLine("function Warn([string]$message) { }");
        probe.AppendLine(Functions());
        probe.AppendLine($"$stale = @({Quote(relativePaths)})");
        probe.AppendLine($"$shipped = @({Quote(shippedPaths)})");
        probe.AppendLine($"Remove-DarlingStaleFiles '{installRoot}' $stale $shipped | Out-Null");
        probe.AppendLine("'DONE'");

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(["DONE"], answers);
    }

    private static string Quote(IReadOnlyList<string> values)
    {
        var parts = new List<string>(values.Count);
        foreach (var value in values)
        {
            Assert.DoesNotContain("'", value, StringComparison.Ordinal);
            parts.Add($"'{value}'");
        }

        return string.Join(", ", parts);
    }

    /// <summary>Every function the probes need, lifted out of the shipped script by brace matching.</summary>
    private static string Functions()
    {
        var script = DeployScript;
        var builder = new StringBuilder();
        foreach (var name in s_functions)
        {
            builder.AppendLine(ExtractFunction(script, name));
        }

        return builder.ToString();
    }

    private static string Plant(string root, string relativePath)
    {
        var full = Path.Combine(root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, new string('x', 64));
        return full;
    }

    /// <summary>Runs <paramref name="script"/> under Windows PowerShell 5.1 and returns its non-empty output
    /// lines. Written to a temp file rather than passed with -Command: the script under test is a set of
    /// whole function bodies, and quoting those through a command line fails for reasons that have nothing
    /// to do with what is being tested. Same idiom as
    /// <c>DarlingDeployRollbackRetentionTests</c>.</summary>
    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-2529-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(new ProcessStartInfo("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            Assert.NotNull(process);

            /* BOTH streams drained CONCURRENTLY, and that is not style. Reading stdout to the end and then
               stderr is the classic redirect deadlock: a probe that writes enough to stderr to fill the OS
               pipe buffer blocks the child, which never closes stdout, which blocks this thread forever —
               and WaitForExit's timeout is behind the block, so it never gets to fire. The result is a CI
               job that HANGS rather than fails, which is the worse of the two by a distance. The probes in
               this file are the larger and more failure-prone ones, so the case is not hypothetical here.
               Raised in review on this PR (#2529); DarlingDeployRollbackRetentionTests carried the same
               shape and was fixed with it. */
            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();

            var exited = process.WaitForExit(60_000);
            if (!exited)
            {
                try { process.Kill(entireProcessTree: true); }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or System.ComponentModel.Win32Exception)
                {
                    /* Already gone between the timeout and the kill. Nothing to do. */
                }
            }

            var stdout = stdoutTask.GetAwaiter().GetResult();
            var stderr = stderrTask.GetAwaiter().GetResult();

            Assert.True(exited, $"powershell.exe did not exit within 60 seconds running the extracted functions. Output so far:\n{stdout}\n{stderr}");
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error running the extracted functions:\n{stderr}");

            var lines = new List<string>();
            foreach (var line in stdout.Split('\n'))
            {
                var trimmed = line.Trim();
                if (trimmed.Length > 0) { lines.Add(trimmed); }
            }

            return lines;
        }
        finally
        {
            try { File.Delete(path); } catch (IOException) { /* best-effort */ }
        }
    }

    /// <summary>Returns the full <c>function NAME(...) { ... }</c> definition text from the script —
    /// signature included, so the extracted copy takes its parameters the way the shipped one does.</summary>
    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"upgrade-darling.ps1 no longer defines {name} (#2529)");

        var open = script.IndexOf('{', start);
        Assert.True(open >= 0, $"expected an opening brace after {name}");

        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return script.Substring(start, i - start + 1);
                }
            }
        }

        Assert.Fail($"unbalanced braces while extracting {name} from upgrade-darling.ps1");
        return string.Empty;
    }

    private static void TryDelete(string path)
    {
        try { File.Delete(path); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { /* best-effort */ }
    }

    private static void TryDeleteTree(string path)
    {
        try { Directory.Delete(path, recursive: true); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { /* best-effort */ }
    }

    /// <summary>Walks up from the test output directory to the repo root (the directory holding
    /// <c>PerformanceMonitor.sln</c>) — the same idiom <c>DarlingDeployRollbackRetentionTests</c> uses.</summary>
    private static string RepoRoot
    {
        get
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory);
            for (var i = 0; i < 10 && directory is not null; i++)
            {
                if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
                {
                    return directory.FullName;
                }

                directory = directory.Parent;
            }

            Assert.Fail("could not find the repo root from " + AppContext.BaseDirectory);
            return string.Empty;
        }
    }
}
