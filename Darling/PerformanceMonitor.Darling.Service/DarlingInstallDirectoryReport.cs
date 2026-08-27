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
using System.Runtime.Versioning;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Names the directories sitting in the INSTALL directory that are not part of the product's layout,
/// with what they cost in disk — and deletes none of them, ever.
///
/// <para><b>Why this exists separately from the store-copy report.</b> #1775 reports store-shaped
/// directories beside the DATA directory, which lives under <c>%ProgramData%\PerformanceMonitorDarling</c>.
/// A production field instance's seven hand-made directories are under the INSTALL directory — a different
/// parent entirely — so that report was structurally incapable of seeing them and stayed silent. They are
/// also almost certainly not store copies: seven copies of a ~280 GB store cannot fit on a 500 GB volume,
/// so they are small hand-made snapshots carrying no <c>PG_VERSION</c>, invisible to a store-shaped test
/// even in the right parent. Different class, different place, so: different report.</para>
///
/// <para><b>This report carries no store semantics.</b> It cannot say what a directory is, only that the
/// product did not put it there — so it says exactly that and nothing more. No cluster language, no
/// which-one-is-live warnings; those belong to the store-copy report, where they are true.</para>
///
/// <para><b>It never deletes.</b> Same absolutism as the store-copy report, and for a stronger reason here:
/// this report identifies by ELIMINATION against a known layout rather than by any positive structural
/// test, so a directory it cannot account for may be anything at all — an operator's snapshot, a deploy
/// tool's staging folder, someone's notes. Reporting is the only verdict that stays harmless when the
/// classification is wrong, and here it will sometimes be wrong by construction.</para>
///
/// <para><b>Rollback backups are recognised, and that is not the same as being owned (#2525).</b> A
/// dogfood box was found carrying forty-six <c>_rollback_manual_*</c> directories holding 5.48 GB, the
/// oldest three weeks old — and this report named every one of them on its own line, every start. Each
/// line was individually true and the pile of them was useless: a real layout problem, a stray DLL or a
/// half-extracted upgrade, arrives as warning forty-seven in a list of forty-six identical ones. That is a
/// guard that has stopped guarding by being too loud, which is the same failure as a pin that never bites
/// wearing the opposite clothes. So a directory in the deploy procedure's own namespace
/// (<see cref="DarlingRollbackBackups"/>) now gets ONE line for the whole set — count, total size, the age
/// of the oldest, and the command that prunes them — leaving the per-directory lines for the directories
/// that genuinely have something to diagnose. Nothing is hidden and nothing is deleted: the set is still
/// reported every start, and the size still accumulates in a total an operator can act on.</para>
///
/// <para><b>Silent in the healthy state, loud exactly once when there is a backlog.</b> Under the deploy
/// script's retention the steady state is a handful of backups by design, and warning about the correct
/// state on every start is how this report would talk itself back into being ignored. At or under
/// <see cref="DarlingRollbackBackups.DefaultRetained"/> the line drops to informational; past it, it is a
/// warning that names the excess. The recognition half matters even where retention is already running,
/// because the boxes carrying the backlog today got it before any script pruned anything, and no upgrade
/// removes a directory the product did not create.</para>
/// </summary>
[SupportedOSPlatform("windows")]
internal static class DarlingInstallDirectoryReport
{
    /// <summary>
    /// Its own budget rather than one shared with the store-copy report, deliberately. The two scan
    /// disjoint trees for different reasons, and a single multi-hundred-GB store copy is quite capable of
    /// spending a shared budget by itself — which would leave this report's directories unmeasured because
    /// of work done somewhere else entirely. Bounding each report separately costs a worse case of ten
    /// seconds instead of five at startup, and buys that one report's cost can never degrade another
    /// report's content.
    /// </summary>
    private static readonly TimeSpan s_sizeProbeBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The directories the product's own layout puts at the top level of the install directory.
    ///
    /// <para>Enumerated from the shipped layout rather than remembered, and each entry is cited:</para>
    /// <list type="bullet">
    /// <item><c>viewer</c> — the packaging step copies the viewer publish into it
    /// (<c>.github/workflows/build.yml</c> "Stage Darling for signing", and the same layout in
    /// <c>nightly.yml</c>).</item>
    /// <item><c>runtimes</c> — .NET's native-asset directory, present in a measured
    /// <c>dotnet publish</c> of the service.</item>
    /// <item><c>wwwroot</c> — the web dashboard's static assets, a csproj <c>Content</c> copy
    /// (<c>PerformanceMonitor.Darling.Service.csproj</c>).</item>
    /// <item><c>pg-runtime</c> — extracted from <c>pg-runtime.zip</c> beside the service exe on first run
    /// (<c>DarlingManagedPostgres</c>, <c>AppContext.BaseDirectory</c> + "pg-runtime").</item>
    /// <item><c>pg-runtime-prev</c> — the rescued previous runtime
    /// (<c>DarlingStoreUpgrade.PreviousRuntimeRootFor</c>, which appends
    /// <see cref="DarlingStoreUpgrade.PreviousRuntimeSuffix"/> to the runtime root).</item>
    /// </list>
    ///
    /// <para>Satellite-resource directories (<c>cs</c>, <c>de</c>, <c>ja</c>, <c>zh-Hans</c> and the rest)
    /// are deliberately NOT listed: see <see cref="IsSatelliteResourceDirectory"/>. The service log
    /// directory is not listed either, and that is not an omission — logs live under
    /// <c>%ProgramData%\PerformanceMonitorDarling\logs</c>, not here (<c>uninstall-darling.ps1</c> says so
    /// explicitly).</para>
    /// </summary>
    private static readonly string[] s_productDirectories =
    [
        "viewer",
        "runtimes",
        "wwwroot",
        "pg-runtime",
        "pg-runtime" + DarlingStoreUpgrade.PreviousRuntimeSuffix,
    ];

    /// <summary>
    /// Reports every top-level directory of <paramref name="installDirectory"/> that the product's layout
    /// does not account for. Never throws: a report that cannot be produced must not cost the service its
    /// start.
    /// </summary>
    internal static void Report(string installDirectory, ILogger logger)
        => Report(installDirectory, logger, s_sizeProbeBudget);

    /// <summary>
    /// The budget is a parameter here purely so a test can force the exhausted path, which is otherwise
    /// unreachable without a directory large enough to spend five real seconds. It has to be reachable: the
    /// property that a spent budget degrades a directory's SIZE and never drops the directory itself is the
    /// one this report cannot afford to get wrong, and a pin that cannot enter the branch does not pin it.
    /// Production has exactly one caller, and it passes <see cref="s_sizeProbeBudget"/>.
    /// </summary>
    internal static void Report(string installDirectory, ILogger logger, TimeSpan sizeProbeBudget)
    {
        try
        {
            var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(installDirectory));
            if (!Directory.Exists(root))
            {
                return;
            }

            var deadline = DateTime.UtcNow + sizeProbeBudget;
            var unaccountedFor = new List<string>();
            var rollbackBackups = new List<string>();

            foreach (var candidate in Directory.GetDirectories(root))
            {
                if (IsProductDirectory(candidate))
                {
                    continue;
                }

                /* Ahead of the satellite test on purpose, and the order is worth a line. The satellite test
                   is a directory WALK; the rollback test is a string comparison. On the box that produced
                   #2525 that ordering is the difference between classifying forty-six backups for nothing
                   and walking 5.48 GB of binaries to learn what their name already said. */
                if (DarlingRollbackBackups.IsRollbackBackup(candidate))
                {
                    rollbackBackups.Add(candidate);
                    continue;
                }

                if (IsSatelliteResourceDirectory(candidate))
                {
                    continue;
                }

                unaccountedFor.Add(candidate);
            }

            /* Classify everything first, then measure the unaccounted-for directories BEFORE the backups.
               The budget is finite and shared, and the enumeration order of a directory is the filesystem's
               business — so measuring in discovery order lets forty-six backups spend the whole budget and
               leave the one stray directory, the only line here anybody has to act on, reporting "at least
               0 bytes". The report that carries the signal gets first call on the budget. */
            ReportUnaccountedFor(root, Measure(unaccountedFor, deadline), logger, sizeProbeBudget);
            ReportRollbackBackups(root, Measure(rollbackBackups, deadline), logger);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                "Could not check the install directory {Root} for directories outside the product's layout ({Message}).",
                installDirectory, ex.Message);
        }
    }

    /// <summary>
    /// Sizes each directory against the shared <paramref name="deadline"/>, carrying whether the walk
    /// finished so a caller can say "at least" rather than state a number it did not finish computing.
    /// </summary>
    private static List<(string Path, long Bytes, bool Measured)> Measure(List<string> directories, DateTime deadline)
    {
        var measured = new List<(string Path, long Bytes, bool Measured)>(directories.Count);
        foreach (var directory in directories)
        {
            var bytes = DarlingStoreUpgrade.MeasureDirectoryBytes(directory, deadline, out var complete);
            measured.Add((directory, bytes, complete));
        }

        return measured;
    }

    /// <summary>
    /// The original report, unchanged in shape: one summary line and one line per directory, biggest first,
    /// naming every directory the product's layout cannot account for.
    ///
    /// <para>Per-directory lines are right HERE and only here. The product does not know what any of these
    /// are, so the path is the whole message — there is nothing to summarise and nobody can act on a count.
    /// The rollback backups moved out from under this exactly so these lines stay findable.</para>
    /// </summary>
    private static void ReportUnaccountedFor(
        string root,
        List<(string Path, long Bytes, bool Measured)> found,
        ILogger logger,
        TimeSpan sizeProbeBudget)
    {
        if (found.Count == 0)
        {
            return;
        }

        long total = 0;
        var allMeasured = true;
        foreach (var (_, bytes, measured) in found)
        {
            total += bytes;
            allMeasured &= measured;
        }

        found.Sort(static (left, right) => right.Bytes.CompareTo(left.Bytes));

        logger.LogWarning(
            "{Count} director(ies) in the install directory {Root} are not part of the product's layout, and are holding {Approximately}{Size}. NONE of them is deleted automatically — the product does not know what they are, only that it did not put them there. Remove the ones you no longer need: a major store upgrade in copy mode needs roughly twice the data directory in free space.",
            found.Count, root, allMeasured ? string.Empty : "at least ", DarlingStoreUpgrade.FormatBytes(total));

        foreach (var (path, bytes, measured) in found)
        {
            /* An exhausted budget degrades a directory's SIZE and never its presence in this list. The
               whole point of the report is that the operator learns these exist; letting a slow walk
               silence one would be the report failing at the only job it has. */
            if (!measured)
            {
                logger.LogWarning(
                    "Directory not part of the product's layout: {Path} (at least {Size}; the {Budget}-second size probe did not finish walking it). It is never deleted automatically.",
                    path, DarlingStoreUpgrade.FormatBytes(bytes), (int)sizeProbeBudget.TotalSeconds);
                continue;
            }

            logger.LogWarning(
                "Directory not part of the product's layout: {Path} ({Size}). It is never deleted automatically.",
                path, DarlingStoreUpgrade.FormatBytes(bytes));
        }
    }

    /// <summary>
    /// The whole set of deploy rollback backups, in ONE line: how many, what they cost together, which is
    /// the oldest, and the command that prunes them (#2525).
    ///
    /// <para>No per-directory lines, deliberately. Their name says what they are and our own deploy script
    /// put every one of them there, so a line each adds a path and no information — while costing the
    /// per-directory lines above the only property that makes them worth reading, which is that a line in
    /// this report means something happened that nobody planned.</para>
    ///
    /// <para>Severity is the retention decision, not the disk cost. At or under the deploy script's
    /// retention this is the intended state of a box that has been upgraded a few times, and a warning
    /// about the intended state is noise with extra steps; past it the box is carrying backups that can
    /// only roll back to a version nobody wants, which is worth one warning a start until someone prunes
    /// them.</para>
    ///
    /// <para>The total still says "at least" when the budget ran out. A size that stopped being measured
    /// must never be reported as though it were measured — the same rule the unaccounted-for report follows,
    /// and it matters more here because this number is the one an operator sizes a cleanup against.</para>
    /// </summary>
    private static void ReportRollbackBackups(
        string root,
        List<(string Path, long Bytes, bool Measured)> backups,
        ILogger logger)
    {
        if (backups.Count == 0)
        {
            return;
        }

        long total = 0;
        var allMeasured = true;
        foreach (var (_, bytes, measured) in backups)
        {
            total += bytes;
            allMeasured &= measured;
        }

        var approximately = allMeasured ? string.Empty : "at least ";

        if (backups.Count <= DarlingRollbackBackups.DefaultRetained)
        {
            logger.LogInformation(
                "{Count} deploy rollback backup(s) in the install directory {Root}, holding {Approximately}{Size} — within the {Keep} the deploy procedure keeps. It creates and prunes them; the service never touches one.",
                backups.Count, root, approximately, DarlingStoreUpgrade.FormatBytes(total), DarlingRollbackBackups.DefaultRetained);
            return;
        }

        logger.LogWarning(
            "{Count} deploy rollback backups in the install directory {Root}, holding {Approximately}{Size} — {Excess} more than the deploy procedure keeps, and the oldest of them is {Oldest}. Reported once with a running total rather than one warning each: our own deploy script made every one of these, so there is nothing in the list to diagnose, and a line each would bury the directories above that DO need looking at. Prune them with {Prune}, which keeps the newest {Keep}. The service never creates or deletes one.",
            backups.Count,
            root,
            approximately,
            DarlingStoreUpgrade.FormatBytes(total),
            backups.Count - DarlingRollbackBackups.DefaultRetained,
            OldestBackupName(backups),
            DarlingRollbackBackups.PruneCommand,
            DarlingRollbackBackups.DefaultRetained);
    }

    /// <summary>
    /// The NAME of the oldest backup, chosen by last-write time rather than by sorting the stamps in the
    /// names.
    ///
    /// <para>The filesystem knows when a directory was written; the name only carries whatever spelling the
    /// procedure used that month, and #2525's box holds more than one spelling. Ordering on the timestamp
    /// and printing the name gives an operator both — a stamp they can recognise, chosen by something that
    /// cannot be wrong about which came first — and it is the same ordering <c>upgrade-darling.ps1</c>
    /// prunes by, so the directory this line calls oldest is the one the script would remove first.</para>
    /// </summary>
    private static string OldestBackupName(List<(string Path, long Bytes, bool Measured)> backups)
    {
        var oldestPath = backups[0].Path;
        var oldestWhen = Directory.GetLastWriteTimeUtc(oldestPath);

        for (var i = 1; i < backups.Count; i++)
        {
            var when = Directory.GetLastWriteTimeUtc(backups[i].Path);
            if (when < oldestWhen)
            {
                oldestWhen = when;
                oldestPath = backups[i].Path;
            }
        }

        return Path.GetFileName(oldestPath);
    }

    private static bool IsProductDirectory(string candidate)
    {
        var name = Path.GetFileName(candidate);
        foreach (var known in s_productDirectories)
        {
            if (string.Equals(name, known, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// True for a .NET satellite-resource directory — one holding nothing but <c>*.resources.dll</c>.
    ///
    /// <para>Structural rather than a list of culture names, and that is the load-bearing choice. A
    /// measured publish ships thirteen of them today (<c>cs</c>, <c>de</c>, <c>es</c>, <c>fr</c>,
    /// <c>it</c>, <c>ja</c>, <c>ko</c>, <c>pl</c>, <c>pt-BR</c>, <c>ru</c>, <c>tr</c>, <c>zh-Hans</c>,
    /// <c>zh-Hant</c>), but that set is decided by whichever cultures our DEPENDENCIES localize into — a
    /// package update can add one without a line of our code changing. Hardcoding the thirteen would mean
    /// the product starts reporting its own shipped directory as something it did not install, which is the
    /// exact credibility failure this whole report has to avoid. Every one of the thirteen was verified to
    /// contain only <c>.resources.dll</c> files and nothing else.</para>
    ///
    /// <para>An EMPTY directory is not one of these. Something the product shipped would have files in it,
    /// so an empty directory is left to be reported — which is the safe direction: it is named, not
    /// deleted.</para>
    ///
    /// <para><b>The residual, seen and accepted rather than discovered later.</b> A dependency that ships a
    /// NON-satellite file inside a culture-named directory — a localized native library, or a data file
    /// sitting beside the satellite assembly — fails this test and gets reported as foreign. No such
    /// directory exists in the layout today (all thirteen were checked), the cost is one wrong line in a
    /// report that never deletes anything, and the alternative is trusting a directory because of its NAME,
    /// which is the loophole this rule exists to close. Bounded to report-only by design, and written down
    /// here so the next reader knows it was a decision.</para>
    /// </summary>
    private static bool IsSatelliteResourceDirectory(string candidate)
    {
        try
        {
            var any = false;
            foreach (var file in Directory.EnumerateFiles(candidate, "*", SearchOption.AllDirectories))
            {
                any = true;
                if (!file.EndsWith(".resources.dll", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return any;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Unreadable means unclassifiable, and unclassifiable must fall to REPORTED rather than
               silently excused — a directory the service cannot even enumerate is more worth an
               operator's attention, not less. */
            return false;
        }
    }
}
