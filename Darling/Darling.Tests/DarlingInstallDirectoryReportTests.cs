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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the install-directory report added after a production field instance verified SILENT: #1775's
/// store-copy report scans siblings of the DATA directory under <c>%ProgramData%</c>, and the field's seven
/// hand-made directories are under the INSTALL directory — a different parent, so nothing was ever going to
/// name them. They also carry no <c>PG_VERSION</c> (seven copies of a ~280 GB store do not fit on a 500 GB
/// volume, so they are small snapshots, not clusters), which would have kept them invisible to a
/// store-shaped test even in the right place.
///
/// <para>The whole feature is a classification, so the tests are about classification going BOTH ways: a
/// foreign directory must be named, and a directory the product itself put there must never be called
/// foreign. The second half is the one that decides whether an operator believes the first.</para>
///
/// <para><b>#2525 added a THIRD class, and it moved what some of these tests pin.</b> The directories the
/// field box was carrying turned out to be the deploy procedure's own rollback backups — 46 of them, 5.48
/// GB — so the report now recognises that naming convention and collapses the whole set into one line.
/// Everything the original tests were really pinning is still pinned, but the tests that used
/// <c>_rollback_manual_</c> as their FOREIGN example had to stop, because that name is no longer foreign.
/// Each of them now plants a directory nobody can account for, which is what they were about; the rollback
/// behaviour is pinned separately below, and the pin that matters most — a stray directory stays findable
/// among forty-six backups — is new.</para>
/// </summary>
public sealed class DarlingInstallDirectoryReportTests
{
    /// <summary>The field's own naming, taken from the convention the deploy script and the service share
    /// rather than re-typed, so this suite cannot pass against a spelling nothing else uses.</summary>
    private const string FieldDirectoryPrefix = DarlingRollbackBackups.Prefix;

    [Fact]
    public void Report_NamesForeignDirectories_WithTheirSize()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-foreign-");
        try
        {
            PlantProductLayout(root.FullName);
            var unknown = PlantDirectory(root.FullName, "half-extracted-upgrade", "snapshot.bak", 4096);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            Assert.Contains("are not part of the product's layout", lines, StringComparison.Ordinal);
            Assert.Contains("Directory not part of the product's layout: " + unknown, lines, StringComparison.Ordinal);

            /* Reported, and still there — the never-delete property, asserted rather than assumed. */
            Assert.True(Directory.Exists(unknown));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Seven directories the product cannot account for produce ONE summary line and SEVEN per-directory
    /// lines, and all seven survive.
    ///
    /// <para>This is the original field-acceptance test with its subject corrected. What it was ever
    /// pinning is three properties — the summary appears once and not once per directory, no directory is
    /// silently dropped from the list, and nothing is deleted — and all three still hold here. What it was
    /// NOT pinning, though its fixture made it look that way, is that a <c>_rollback_manual_</c> directory
    /// gets a line of its own; that was the field's naming used as a realistic stand-in, and #2525 is the
    /// discovery that the name was never arbitrary. A directory the product genuinely cannot explain still
    /// gets its own line, because for those the path IS the message.</para>
    /// </summary>
    [Fact]
    public void Report_SevenUnaccountedForDirectories_ProduceOneSummaryAndSevenLines_AndAllSurvive()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-seven-");
        try
        {
            PlantProductLayout(root.FullName);

            var planted = new List<string>();
            foreach (var name in new[] { "staging", "old-viewer", "copy of runtimes", "tmp", "dll-backup", "upgrade-work", "notes" })
            {
                planted.Add(PlantDirectory(root.FullName, name, "snapshot.bak", 1024));
            }

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            Assert.Equal(1, CountOccurrences(lines, "are not part of the product's layout"));
            Assert.Equal(7, CountOccurrences(lines, "Directory not part of the product's layout: "));
            Assert.Contains("7 director(ies) in the install directory", lines, StringComparison.Ordinal);

            foreach (var directory in planted)
            {
                Assert.Contains(directory, lines, StringComparison.Ordinal);
                Assert.True(Directory.Exists(directory), $"{directory} must survive the report");
            }
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The field case as it actually was: 46 rollback backups become ONE line carrying the count, the
    /// total and the command that prunes them — not 46 warnings that are each individually true and
    /// collectively useless.
    /// </summary>
    [Fact]
    public void Report_TheFieldsFortySixRollbackBackups_CollapseToOneLine_AndAllSurvive()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-rollback-");
        try
        {
            PlantProductLayout(root.FullName);

            var planted = new List<string>();
            for (var i = 0; i < 46; i++)
            {
                var backup = PlantDirectory(root.FullName, FieldDirectoryPrefix + $"202607{20 + (i % 10):00}-{i:000000}", "PerformanceMonitor.Darling.Service.exe", 1024);
                Directory.SetLastWriteTimeUtc(backup, new DateTime(2026, 7, 20, 0, 0, 0, DateTimeKind.Utc).AddHours(i));
                planted.Add(backup);
            }

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            /* One line for the whole set... */
            Assert.Equal(1, CountOccurrences(lines, "deploy rollback backups in the install directory"));
            Assert.Contains("46 deploy rollback backups in the install directory", lines, StringComparison.Ordinal);

            /* ...carrying the count, what to do, and how many the fix keeps. */
            Assert.Contains(DarlingRollbackBackups.PruneCommand, lines, StringComparison.Ordinal);
            Assert.Contains("43 more than the deploy procedure keeps", lines, StringComparison.Ordinal);
            Assert.Contains("keeps the newest " + DarlingRollbackBackups.DefaultRetained, lines, StringComparison.Ordinal);

            /* The oldest is named, so an operator learns how long this has been accumulating. */
            Assert.Contains("the oldest of them is " + FieldDirectoryPrefix + "20260720-000000", lines, StringComparison.Ordinal);

            /* ...and NOT 46 lines, nor a foreign-directory summary that counts them. */
            Assert.Equal(0, CountOccurrences(lines, "Directory not part of the product's layout: "));
            Assert.Equal(0, CountOccurrences(lines, "are not part of the product's layout"));

            /* Recognised is not deleted. The service did not create these and never removes one. */
            foreach (var directory in planted)
            {
                Assert.True(Directory.Exists(directory), $"{directory} must survive the report");
            }
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// THE point of the whole change, and the one property worth the most: warning forty-seven stays
    /// findable.
    ///
    /// <para>A real layout problem — a stray DLL directory, a half-extracted upgrade, a folder dropped in by
    /// hand — used to arrive as one more line in a list of forty-six identical ones. That is a guard that
    /// has stopped guarding by being too loud, which is the same failure as a pin that never bites wearing
    /// the opposite clothes. So the assertion is not "the report is quieter": it is that the stray directory
    /// gets its OWN line, that the summary counts one directory and not forty-seven, and that the backups
    /// are still accounted for on a line of their own.</para>
    /// </summary>
    [Fact]
    public void Report_AStrayDirectoryAmongFortySixRollbackBackups_IsStillNamedOnItsOwnLine()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-needle-");
        try
        {
            PlantProductLayout(root.FullName);

            for (var i = 0; i < 46; i++)
            {
                PlantDirectory(root.FullName, FieldDirectoryPrefix + $"20260720-{i:000000}", "PerformanceMonitor.Darling.Service.exe", 1024);
            }

            var stray = PlantDirectory(root.FullName, "Npgsql-8.0.3", "Npgsql.dll", 2048);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            /* The needle, named, on its own line. */
            Assert.Contains("Directory not part of the product's layout: " + stray, lines, StringComparison.Ordinal);
            Assert.Equal(1, CountOccurrences(lines, "Directory not part of the product's layout: "));

            /* The summary counts the stray directory ONLY — the backups are not in this bucket. */
            Assert.Contains("1 director(ies) in the install directory", lines, StringComparison.Ordinal);

            /* And the backups are still reported, once, rather than hidden. */
            Assert.Contains("46 deploy rollback backups in the install directory", lines, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Under retention the report is quiet: a box that has been upgraded three times is in the state the
    /// deploy script intends, and warning about the intended state on every start is how this report would
    /// talk itself back into being ignored. It still SAYS so — informational, not silent — because a total
    /// an operator can find is the difference between "quiet" and "hiding something".
    /// </summary>
    [Fact]
    public void Report_RollbackBackupsWithinRetention_AreNotedRatherThanWarnedAbout()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-retained-");
        try
        {
            PlantProductLayout(root.FullName);

            for (var i = 0; i < DarlingRollbackBackups.DefaultRetained; i++)
            {
                PlantDirectory(root.FullName, FieldDirectoryPrefix + $"20260819-{i:000000}", "PerformanceMonitor.Darling.Service.exe", 1024);
            }

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            Assert.Contains("[Information] 3 deploy rollback backup(s) in the install directory", lines, StringComparison.Ordinal);
            Assert.Contains("within the 3 the deploy procedure keeps", lines, StringComparison.Ordinal);
            Assert.Equal(0, CountOccurrences(lines, "[Warning]"));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// One over retention is a warning. The boundary is asserted from the other side too, because a report
    /// that only ever fires at forty-six would be a report nobody sees until the volume is already full.
    /// </summary>
    [Fact]
    public void Report_OneRollbackBackupPastRetention_IsAWarning()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-overrun-");
        try
        {
            PlantProductLayout(root.FullName);

            for (var i = 0; i <= DarlingRollbackBackups.DefaultRetained; i++)
            {
                PlantDirectory(root.FullName, FieldDirectoryPrefix + $"20260819-{i:000000}", "PerformanceMonitor.Darling.Service.exe", 1024);
            }

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            Assert.Contains("[Warning] 4 deploy rollback backups in the install directory", lines, StringComparison.Ordinal);
            Assert.Contains("1 more than the deploy procedure keeps", lines, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A directory named with the bare prefix and no stamp is NOT one of the deploy procedure's backups —
    /// no procedure produces that name — so it stays in the unaccounted-for report, where a thing nobody
    /// can explain belongs. The recognition rule is a namespace, not a substring, and this is the edge that
    /// proves it did not become one.
    /// </summary>
    [Fact]
    public void Report_ADirectoryNamedWithTheBarePrefix_IsStillUnaccountedFor()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-bareprefix-");
        try
        {
            PlantProductLayout(root.FullName);
            var bare = PlantDirectory(root.FullName, FieldDirectoryPrefix, "something.bin", 512);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            Assert.Contains("Directory not part of the product's layout: " + bare, lines, StringComparison.Ordinal);
            Assert.Equal(0, CountOccurrences(lines, "deploy rollback backup"));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// THE credibility pin. Every directory the product's own layout puts here — shipped, packaged, or
    /// created at runtime — must go unmentioned. Telling an operator the product did not install a directory
    /// the product installed is how the report stops being read, and it is the failure mode that a report
    /// classifying by ELIMINATION invites.
    /// </summary>
    [Fact]
    public void Report_NeverCallsAProductDirectoryForeign()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-owned-");
        try
        {
            var owned = PlantProductLayout(root.FullName);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);
            var lines = log.ToString();

            /* Nothing at all is reported when the install directory holds only the product's own layout. */
            Assert.DoesNotContain("not part of the product's layout", lines, StringComparison.Ordinal);

            foreach (var directory in owned)
            {
                Assert.DoesNotContain(directory, lines, StringComparison.Ordinal);
            }
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A locale directory is recognised STRUCTURALLY — everything in it is a <c>.resources.dll</c> — not by
    /// a list of culture names. A dependency package can add a culture without a line of our code changing,
    /// and a hardcoded list would then have the product reporting its own shipped directory as foreign. The
    /// invented culture below is the point: it is in no list anywhere.
    /// </summary>
    [Fact]
    public void Report_TreatsAnyLocaleShapedDirectoryAsTheProducts_EvenAnUnlistedCulture()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-locale-");
        try
        {
            PlantProductLayout(root.FullName);
            var newCulture = PlantDirectory(root.FullName, "nl-BE", "Npgsql.resources.dll", 128);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);

            Assert.DoesNotContain(newCulture, log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A locale-NAMED directory holding something other than satellite assemblies is still foreign — the
    /// structural test must not become a naming loophole that hides a directory by calling it <c>de</c>.
    /// </summary>
    [Fact]
    public void Report_ALocaleNamedDirectoryHoldingOtherFiles_IsStillForeign()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-fakelocale-");
        try
        {
            PlantProductLayout(root.FullName);
            var impostor = PlantDirectory(root.FullName, "de-AT", "backup.zip", 2048);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);

            Assert.Contains("Directory not part of the product's layout: " + impostor, log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Budget exhaustion degrades a directory's SIZE and never its presence. A report whose only job is to
    /// tell an operator these directories exist must not drop one because a walk ran long — so a spent
    /// budget produces an "at least" line, not silence. Deadline in the past forces the exhausted path.
    /// </summary>
    [Fact]
    public void Report_WhenTheSizeProbeIsExhausted_StillReportsEveryDirectory()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-budget-");
        try
        {
            PlantProductLayout(root.FullName);

            /* Enough files that the deadline check — every 512 entries — is actually reached. */
            var big = Path.Combine(root.FullName, "unexplained-and-enormous");
            Directory.CreateDirectory(big);
            for (var i = 0; i < 1200; i++)
            {
                File.WriteAllText(Path.Combine(big, $"f{i}.bin"), "x");
            }

            /* An already-spent budget, so the exhausted branch is entered rather than approximated. Running
               the REPORT this way is the point: measuring the helper alone would leave the branch that
               decides whether a directory still gets NAMED completely unexercised. */
            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log, TimeSpan.Zero);
            var lines = log.ToString();

            /* Named, with an honest lower bound and the budget that ran out... */
            Assert.Contains("Directory not part of the product's layout: " + big, lines, StringComparison.Ordinal);
            Assert.Contains("at least", lines, StringComparison.Ordinal);
            Assert.Contains("size probe did not finish walking it", lines, StringComparison.Ordinal);

            /* ...and the summary total is marked approximate too, rather than stating a number it did not
               finish computing. */
            Assert.Contains("are holding at least ", lines, StringComparison.Ordinal);

            Assert.True(Directory.Exists(big));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The rollback line's total obeys the same rule: a size that stopped being measured is reported as a
    /// lower bound, never as though it had been measured.
    ///
    /// <para>It matters more here than anywhere else in this report, because this number is the one an
    /// operator sizes a cleanup against — "5.48 GB" is what makes someone go and prune, and a confidently
    /// wrong small number is what makes them not bother. The set collapsing to a single line is exactly why
    /// the honesty of that line has to be pinned: there is no longer a list of per-directory sizes to
    /// cross-check it against.</para>
    /// </summary>
    [Fact]
    public void Report_WhenTheSizeProbeIsExhausted_TheRollbackTotalIsMarkedApproximate()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-rollbackbudget-");
        try
        {
            PlantProductLayout(root.FullName);

            for (var i = 0; i <= DarlingRollbackBackups.DefaultRetained; i++)
            {
                var backup = Path.Combine(root.FullName, FieldDirectoryPrefix + $"20260819-{i:000000}");
                Directory.CreateDirectory(backup);
                for (var f = 0; f < 1200; f++)
                {
                    File.WriteAllText(Path.Combine(backup, $"f{f}.bin"), "x");
                }
            }

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log, TimeSpan.Zero);
            var lines = log.ToString();

            Assert.Contains("4 deploy rollback backups in the install directory", lines, StringComparison.Ordinal);

            /* The assertion is on the ROLLBACK line specifically, not on the log as a whole. A bare
               Assert.Contains("holding at least") passes on the unaccounted-for summary, which carries the
               same words — so it went green against a build that had no rollback line at all. A pin that
               passes for a reason other than the one it names is worse than no pin. */
            var rollbackLine = SingleLineContaining(lines, "deploy rollback backups in the install directory");
            Assert.Contains("holding at least ", rollbackLine, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// An empty directory is reported rather than excused. It cannot be a satellite-resource directory (the
    /// product ships files in those), and naming something harmless is the safe direction for a report that
    /// never deletes.
    /// </summary>
    [Fact]
    public void Report_AnEmptyDirectory_IsReported()
    {
        var root = Directory.CreateTempSubdirectory("darling-installdir-empty-");
        try
        {
            PlantProductLayout(root.FullName);
            var empty = Path.Combine(root.FullName, "leftover");
            Directory.CreateDirectory(empty);

            var log = new CapturingLogger();
            DarlingInstallDirectoryReport.Report(root.FullName, log);

            Assert.Contains("Directory not part of the product's layout: " + empty, log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    [Fact]
    public void Report_AMissingInstallDirectory_IsSilentRatherThanThrowing()
    {
        var log = new CapturingLogger();
        DarlingInstallDirectoryReport.Report(Path.Combine(Path.GetTempPath(), "darling-does-not-exist-" + Guid.NewGuid().ToString("N")), log);
        Assert.Equal(string.Empty, log.ToString());
    }

    /// <summary>
    /// The product's own layout: the packaged directories, the runtime-created ones, and a representative
    /// satellite-resource directory. Returns them so a caller can assert none was named.
    /// </summary>
    private static List<string> PlantProductLayout(string root)
    {
        var planted = new List<string>();

        /* viewer + runtimes + wwwroot ship; pg-runtime is extracted on first run and pg-runtime-prev is the
           rescued previous runtime. */
        planted.Add(PlantDirectory(root, "viewer", "PerformanceMonitor.Darling.Viewer.exe", 64));
        planted.Add(PlantDirectory(root, "runtimes", Path.Combine("win-x64", "native", "e_sqlite3.dll"), 64));
        planted.Add(PlantDirectory(root, "wwwroot", "index.html", 64));
        planted.Add(PlantDirectory(root, "pg-runtime", Path.Combine("pgsql", "bin", "pg_ctl.exe"), 64));
        planted.Add(PlantDirectory(root, "pg-runtime" + DarlingStoreUpgrade.PreviousRuntimeSuffix, Path.Combine("pgsql", "bin", "pg_ctl.exe"), 64));

        /* One real satellite-resource directory from the measured publish. */
        planted.Add(PlantDirectory(root, "ja", "Microsoft.Data.SqlClient.resources.dll", 64));

        /* Files at the root are not directories and are never candidates. */
        File.WriteAllText(Path.Combine(root, "darling.json"), "{}");
        File.WriteAllText(Path.Combine(root, "pg-runtime.zip"), "zip");

        return planted;
    }

    private static string PlantDirectory(string root, string name, string relativeFile, int bytes)
    {
        var directory = Path.Combine(root, name);
        var file = Path.Combine(directory, relativeFile);
        Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        File.WriteAllText(file, new string('x', bytes));
        return directory;
    }

    /// <summary>The one logged line containing <paramref name="needle"/>, so an assertion can be made about
    /// THAT line rather than about the log as a whole — several lines here share vocabulary, and a match
    /// anywhere is not a match where it was meant.</summary>
    private static string SingleLineContaining(string log, string needle)
    {
        var matches = new List<string>();
        foreach (var line in log.Split('\n'))
        {
            if (line.Contains(needle, StringComparison.Ordinal))
            {
                matches.Add(line);
            }
        }

        Assert.Single(matches);
        return matches[0];
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }
        return count;
    }

    private static void TryDeleteTree(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A leftover temp tree is not a test failure. */
        }
    }

    /// <summary>Records every line the report writes, so the classification can be asserted rather than assumed.</summary>
    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly System.Text.StringBuilder _lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel,
            Microsoft.Extensions.Logging.EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Append('[').Append(logLevel).Append("] ").AppendLine(formatter(state, exception));
            }
        }

        public override string ToString()
        {
            lock (_lines)
            {
                return _lines.ToString();
            }
        }
    }
}
