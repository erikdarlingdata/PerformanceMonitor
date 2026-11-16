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
using System.Text;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What <see cref="DataRootMigration.Migrate"/> did.
/// </summary>
internal enum DataRootMigrationStatus
{
    /// <summary>The legacy root is absent, or holds none of Lite's artifacts. A fresh install lands here.</summary>
    NothingToMigrate,

    /// <summary>The new root already holds a complete store, so nothing from the legacy root went live. What
    /// was still in there was quarantined out of the install directory rather than left to be deleted.</summary>
    AlreadyMigrated,

    /// <summary>Every artifact Lite owns in the legacy root moved across.</summary>
    Migrated,

    /// <summary>At least one move failed. What failed stays in the legacy root and is retried next launch.</summary>
    PartiallyMigrated
}

/// <summary>
/// The outcome of one migration attempt. <see cref="Kept"/> names artifacts that existed in BOTH roots and
/// could not be relocated — the new root's copy wins and the legacy one is left where it is.
/// <see cref="Rescued"/> names the ones that existed in both and WERE relocated, out of the install
/// directory and into the new root's quarantine folder, where the next Setup.exe cannot delete them.
/// </summary>
internal sealed class DataRootMigrationResult
{
    public DataRootMigrationStatus Status { get; init; }
    public IReadOnlyList<string> Moved { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> Kept { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> Rescued { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> Failed { get; init; } = Array.Empty<string>();
}

/// <summary>
/// #1832: moves Lite's per-user data out of <c>%LOCALAPPDATA%\PerformanceMonitorLite</c> — which is also
/// Velopack's install root — into the sibling <c>%LOCALAPPDATA%\PerformanceMonitorLite-Data</c>.
///
/// Re-running Setup.exe over an existing install renames the install root aside and deletes it, so every
/// release installed that way destroyed the DuckDB store, the Parquet archive, the logs, and settings.json.
/// In-app updates never did (Velopack updates the <c>current\</c> subfolder in place), which is why the
/// data loss looked random.
///
/// The migration is deliberately an explicit allow-list rather than "move the folder contents": the legacy
/// root ALSO holds Velopack's own <c>Update.exe</c>, <c>current\</c>, <c>packages\</c> and
/// <c>velopack.log</c>. Moving those would break the installed app and its updater. Only the artifacts
/// listed here belong to us, and only those move.
/// </summary>
internal static class DataRootMigration
{
    /// <summary>The Velopack install root, which is where data used to live.</summary>
    internal const string LegacyRootName = "PerformanceMonitorLite";

    /// <summary>The new per-user data root. A LOCAL sibling — the DuckDB store must never roam.</summary>
    internal const string DataRootName = "PerformanceMonitorLite-Data";

    /// <summary>Signpost left in the legacy root so someone browsing it can find their data.</summary>
    internal const string MarkerFileName = "DATA-MOVED.txt";

    /// <summary>
    /// Quarantine under the new root for legacy artifacts that could not become the live copy because one
    /// was already there. Nothing reads from it; it exists so those artifacts stop living in the folder
    /// Setup.exe deletes. Named rather than timestamped so a second run is a no-op, not a pile.
    /// </summary>
    internal const string RescuedDirName = "recovered-from-install-dir";

    /// <summary>
    /// Directories Lite owns under its data root. <c>monitor.duckdb.tmp</c> is DuckDB's scratch directory —
    /// it only survives an unclean shutdown, and it belongs with the database file it spilled for.
    /// </summary>
    private static readonly string[] s_directories = { "config", "archive", "logs", "monitor.duckdb.tmp" };

    /// <summary>Files Lite owns directly under its data root.</summary>
    private static readonly string[] s_files = { "monitor.duckdb", "monitor.duckdb.wal", "alert_state.json", "store-health-history.json" };

    /// <summary>
    /// The two artifacts that make a root "the live install": user settings and the store. If BOTH are
    /// already in the new root, a previous launch finished the job and the legacy root is stale.
    /// </summary>
    private static bool HasCompleteStore(string root) =>
        File.Exists(Path.Combine(root, "config", "settings.json"))
        && File.Exists(Path.Combine(root, "monitor.duckdb"));

    /// <summary>
    /// Moves everything Lite owns from <paramref name="legacyRoot"/> to <paramref name="newRoot"/>, then
    /// leaves a marker behind. Never deletes the legacy root itself — Velopack owns it.
    ///
    /// Nothing in the new root is ever overwritten: the new root's copy always stays live. Per-artifact
    /// rather than all-or-nothing, so a run interrupted halfway (a locked store file, a killed process)
    /// finishes on the next launch.
    ///
    /// <para>An artifact present in BOTH roots does not simply stay put, though (#1842 review). "Already at
    /// the target" cannot be told apart from "recreated at the target after the move failed" — and the
    /// second is easy to reach: one locked <c>monitor.duckdb</c> and DuckDB makes a fresh empty one at the
    /// new root the same session, or one locked <c>config\</c> and <c>ConfigSeeder</c> fills a new one with
    /// defaults. Bare existence would then read as "done" forever while the real store sat in the folder
    /// the next Setup.exe deletes. So a collision is resolved instead of accepted: an EMPTY directory or a
    /// zero-byte file at the target is treated as the placeholder it is and moved over, and anything with
    /// real content is left live while the legacy copy is relocated into <see cref="RescuedDirName"/> under
    /// the new root. The live copy never changes; the losing copy just stops sitting somewhere deletable.
    /// Swapping a bigger legacy store in for a live one is deliberately NOT attempted — bytes are no
    /// evidence of which one the user wants — so that decision stays theirs, with the data still there to
    /// make it with.</para>
    ///
    /// Runs before the logger is initialized, so <paramref name="log"/> is the only channel out. Never
    /// throws: a failed migration must not stop the app from starting.
    /// </summary>
    internal static DataRootMigrationResult Migrate(string legacyRoot, string newRoot, Action<string> log)
    {
        var moved = new List<string>();
        var kept = new List<string>();
        var rescued = new List<string>();
        var failed = new List<string>();

        try
        {
            if (!Directory.Exists(legacyRoot)
                || string.Equals(
                    Path.TrimEndingDirectorySeparator(Path.GetFullPath(legacyRoot)),
                    Path.TrimEndingDirectorySeparator(Path.GetFullPath(newRoot)),
                    StringComparison.OrdinalIgnoreCase))
            {
                return new DataRootMigrationResult { Status = DataRootMigrationStatus.NothingToMigrate };
            }

            var pending = new List<(string Name, bool IsDirectory)>();
            foreach (var name in s_directories)
            {
                if (Directory.Exists(Path.Combine(legacyRoot, name))) pending.Add((name, true));
            }
            foreach (var name in s_files)
            {
                if (File.Exists(Path.Combine(legacyRoot, name))) pending.Add((name, false));
            }

            if (pending.Count == 0)
            {
                return new DataRootMigrationResult { Status = DataRootMigrationStatus.NothingToMigrate };
            }

            Directory.CreateDirectory(newRoot);

            /* The new root is complete and live, so none of the leftovers can become the live copy — but
               they are still real data in a folder Setup.exe deletes. Quarantine the lot rather than
               merging: an artifact the new root happens to lack (a stale archive\, an old alert_state.json)
               would otherwise silently become live data belonging to an older install. */
            if (HasCompleteStore(newRoot))
            {
                foreach (var (name, isDirectory) in pending)
                {
                    if (TryRescue(Path.Combine(legacyRoot, name), newRoot, name, isDirectory, log)) rescued.Add(name);
                    else kept.Add(name);
                }

                /* Each arm is gated on its own list. Claiming a move that did not happen is worse than
                   saying nothing: this log is the ONLY channel this early, and an operator who reads
                   "moved it to the quarantine" and finds an empty folder stops looking. */
                if (rescued.Count > 0)
                {
                    log($"Data root '{newRoot}' is already populated and is the one in use, so the older copy in " +
                        $"'{legacyRoot}' cannot go live. Moved it to '{Path.Combine(newRoot, RescuedDirName)}' so " +
                        $"the next Setup.exe cannot delete it ({string.Join(", ", rescued)}). If the data you " +
                        "expected to see is in there rather than in the live store, close Lite and swap it in by " +
                        "hand; otherwise the folder is safe to delete.");
                }

                if (kept.Count > 0)
                {
                    log($"Data root '{newRoot}' is already populated and is the one in use, so the older copy in " +
                        $"'{legacyRoot}' cannot go live - and it could not be moved out of there either: " +
                        $"{string.Join(", ", kept)}. Copy them somewhere safe by hand; re-running Setup.exe " +
                        "deletes that folder.");
                }

                /* Written as soon as anything leaves, not only once nothing is left: the marker now states
                   what is STILL in this folder, so it can no longer read as a premature "all done". */
                if (rescued.Count > 0)
                {
                    TryWriteMarker(legacyRoot, newRoot, log);
                }

                return new DataRootMigrationResult
                {
                    Status = DataRootMigrationStatus.AlreadyMigrated,
                    Rescued = rescued,
                    Kept = kept
                };
            }

            foreach (var (name, isDirectory) in pending)
            {
                var source = Path.Combine(legacyRoot, name);
                var target = Path.Combine(newRoot, name);

                if ((isDirectory ? Directory.Exists(target) : File.Exists(target))
                    && !TryClearPlaceholder(target, isDirectory, log))
                {
                    if (TryRescue(source, newRoot, name, isDirectory, log)) rescued.Add(name);
                    else kept.Add(name);
                    continue;
                }

                try
                {
                    if (isDirectory)
                    {
                        Directory.Move(source, target);
                    }
                    else
                    {
                        File.Move(source, target);
                    }

                    moved.Add(name);
                }
                catch (Exception ex)
                {
                    failed.Add(name);
                    log($"Could not move '{source}' to '{target}': {ex.Message}. It stays where it is and the " +
                        "move is retried on the next start.");
                }
            }

            /* Every arm logs BEFORE any early return. A run where each artifact collided and each rescue
               then failed leaves moved/rescued/failed all empty and kept full — and returning on that
               emptiness without saying so would strand real data in the deletable folder in total silence,
               which is the failure this whole class exists to prevent. */
            if (moved.Count > 0)
            {
                log($"Moved Lite's data out of the install directory '{legacyRoot}' and into '{newRoot}' (#1832): " +
                    $"{string.Join(", ", moved)}. Re-running Setup.exe deletes the install directory, which is why " +
                    "data kept there did not survive an installer upgrade.");
            }

            if (rescued.Count > 0)
            {
                log($"'{newRoot}' already had its own copy of these, so the '{legacyRoot}' ones could not go live " +
                    $"and were moved to '{Path.Combine(newRoot, RescuedDirName)}' instead: {string.Join(", ", rescued)}. " +
                    "Nothing was overwritten. That folder is safe to delete once you are satisfied nothing is missing.");
            }

            if (kept.Count > 0)
            {
                log($"Left in '{legacyRoot}' because '{newRoot}' already had a copy and the old one could not be " +
                    $"moved aside: {string.Join(", ", kept)}. Copy them somewhere safe by hand - re-running " +
                    "Setup.exe deletes that folder.");
            }

            if (moved.Count == 0 && rescued.Count == 0 && failed.Count == 0)
            {
                return new DataRootMigrationResult
                {
                    Status = DataRootMigrationStatus.AlreadyMigrated,
                    Kept = kept
                };
            }

            /* Written as soon as anything leaves this folder, whether it went live or into the quarantine.
               Waiting for a clean run was the old rule — it kept the marker from claiming a completeness it
               did not have — but the marker now names what is still here, so it is honest mid-migration and
               a partial run gets a signpost instead of silence. */
            if (moved.Count > 0 || rescued.Count > 0)
            {
                TryWriteMarker(legacyRoot, newRoot, log);
            }

            return new DataRootMigrationResult
            {
                Status = failed.Count > 0 ? DataRootMigrationStatus.PartiallyMigrated : DataRootMigrationStatus.Migrated,
                Moved = moved,
                Kept = kept,
                Rescued = rescued,
                Failed = failed
            };
        }
        catch (Exception ex)
        {
            log($"Data directory migration failed: {ex.Message}. Lite starts against '{newRoot}' regardless; " +
                $"anything left in '{legacyRoot}' is untouched.");
            return new DataRootMigrationResult
            {
                /* Rescued counts as progress here exactly like Moved: an artifact already relocated before
                   the throw really did leave the legacy root, and reporting it empty would understate what
                   happened on the one path where the caller has least information. */
                Status = failed.Count > 0 || moved.Count > 0 || rescued.Count > 0
                    ? DataRootMigrationStatus.PartiallyMigrated
                    : DataRootMigrationStatus.NothingToMigrate,
                Moved = moved,
                Kept = kept,
                Rescued = rescued,
                Failed = failed
            };
        }
    }

    /// <summary>Which of Lite's own artifacts are sitting in <paramref name="root"/> right now, in the
    /// declaration order of the allow-list so the marker reads the same way every time.</summary>
    private static List<string> OurArtifactsIn(string root)
    {
        var names = new List<string>();
        foreach (var name in s_directories)
        {
            if (Directory.Exists(Path.Combine(root, name))) names.Add(name);
        }
        foreach (var name in s_files)
        {
            if (File.Exists(Path.Combine(root, name))) names.Add(name);
        }

        return names;
    }

    private static void AppendNames(StringBuilder text, List<string> names)
    {
        foreach (var name in names)
        {
            text.Append("    ").AppendLine(name);
        }
    }

    /// <summary>
    /// Deletes a target that is only a PLACEHOLDER — an empty directory or a zero-byte file — so the
    /// legacy artifact can take its place; false for anything holding content, and for any failure.
    ///
    /// <para>This is the narrow half of the collision fix. A target created by a race rather than by a
    /// finished migration is usually empty at first (a <c>Directory.CreateDirectory</c> that ran before its
    /// seeder, a touched WAL), and treating that as "already migrated" is what strands the real copy. An
    /// empty target has nothing to lose, so replacing it is safe in a way replacing a populated one is not.
    /// Everything with content goes down the rescue path instead.</para>
    /// </summary>
    private static bool TryClearPlaceholder(string target, bool isDirectory, Action<string> log)
    {
        try
        {
            if (isDirectory)
            {
                using (var entries = Directory.EnumerateFileSystemEntries(target).GetEnumerator())
                {
                    if (entries.MoveNext()) return false;
                }

                Directory.Delete(target);
                return true;
            }

            if (new FileInfo(target).Length != 0) return false;

            File.Delete(target);
            return true;
        }
        catch (Exception ex)
        {
            log($"Could not clear the empty '{target}': {ex.Message}. Treating it as real and leaving it alone.");
            return false;
        }
    }

    /// <summary>
    /// Moves a legacy artifact that lost the collision out of the install directory and into
    /// <see cref="RescuedDirName"/> under the new root, so re-running Setup.exe cannot delete it. Returns
    /// false when it could not be moved, which leaves it exactly where it was — no worse than before.
    /// </summary>
    private static bool TryRescue(string source, string newRoot, string name, bool isDirectory, Action<string> log)
    {
        var rescueTarget = Path.Combine(newRoot, RescuedDirName, name);

        if (isDirectory ? Directory.Exists(rescueTarget) : File.Exists(rescueTarget))
        {
            /* A previous rescue already claimed the name and its source is somehow back. Inventing a second
               name would let repeated failures pile up copies of a multi-GB store; say so instead. */
            log($"'{rescueTarget}' already exists, so '{source}' was left where it is. Move it somewhere safe " +
                "by hand - re-running Setup.exe deletes that folder.");
            return false;
        }

        try
        {
            Directory.CreateDirectory(Path.Combine(newRoot, RescuedDirName));
            if (isDirectory) Directory.Move(source, rescueTarget);
            else File.Move(source, rescueTarget);
            return true;
        }
        catch (Exception ex)
        {
            log($"Could not move '{source}' to '{rescueTarget}': {ex.Message}. It stays where it is and the " +
                "move is retried on the next start.");
            return false;
        }
    }

    /// <summary>
    /// Drops a plain-text signpost in the legacy root. The old root is NOT deleted — it is the Velopack
    /// install directory and still holds Update.exe, current\ and packages\.
    ///
    /// <para>Lists what is at each of the three locations under its own heading, because they mean different
    /// things and someone reads this file precisely because they are hunting for ONE artifact by name. A
    /// marker naming only the artifacts that went live would read, to the person whose <c>settings.json</c>
    /// collided and got quarantined, as proof it was never touched at all.</para>
    ///
    /// <para><b>The lists are read off the filesystem, not from the calling run's <c>moved</c>/<c>rescued</c>
    /// (#1842 review, second pass).</b> Those are local to one <c>Migrate</c> call, and the marker is written
    /// on the first run that leaves nothing behind — which may not be the run that moved most of it. Launch 1
    /// rescues A and fails on B, so it correctly writes nothing; launch 2 sees only B pending, succeeds, and
    /// would have written a marker naming B alone, with A safe in the quarantine and unmentioned forever
    /// (a later launch finds nothing pending and returns before any marker logic). Ground truth at write time
    /// has no such hole and needs no state carried between runs.</para>
    /// </summary>
    private static void TryWriteMarker(string legacyRoot, string newRoot, Action<string> log)
    {
        try
        {
            var live = OurArtifactsIn(newRoot);
            var quarantined = OurArtifactsIn(Path.Combine(newRoot, RescuedDirName));
            var stillHere = OurArtifactsIn(legacyRoot);

            var marker = Path.Combine(legacyRoot, MarkerFileName);
            var text = new StringBuilder();
            text.AppendLine("Performance Monitor Lite data has MOVED.");
            text.AppendLine();
            text.AppendLine("This folder is the application install directory. The installer owns it: running");
            text.AppendLine("Setup.exe again renames it aside and deletes it, which used to destroy the");
            text.AppendLine("monitoring history stored here (issue #1832).");
            text.AppendLine();
            text.Append("As of ").Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)).AppendLine(", your data lives in:");
            text.AppendLine();
            text.AppendLine("    " + newRoot);
            text.AppendLine();
            AppendNames(text, live);

            if (quarantined.Count > 0)
            {
                text.AppendLine();
                text.AppendLine("These already existed at the new location, so the copies from this folder did NOT");
                text.AppendLine("go live. Nothing was overwritten - they were moved here instead, out of reach of");
                text.AppendLine("the installer:");
                text.AppendLine();
                text.AppendLine("    " + Path.Combine(newRoot, RescuedDirName));
                text.AppendLine();
                AppendNames(text, quarantined);
            }

            if (stillHere.Count > 0)
            {
                text.AppendLine();
                text.AppendLine("These could NOT be moved and are still in this folder. Re-running Setup.exe will");
                text.AppendLine("delete them - copy them somewhere safe, or start Lite again to retry the move:");
                text.AppendLine();
                AppendNames(text, stillHere);
            }

            text.AppendLine();
            text.AppendLine("Nothing was deleted. This file is only a signpost and is safe to remove.");

            File.WriteAllText(marker, text.ToString());
        }
        catch (Exception ex)
        {
            /* The signpost is a courtesy. Failing to write it must not fail the migration that already
               succeeded. */
            log($"Could not write '{MarkerFileName}' in '{legacyRoot}': {ex.Message}");
        }
    }
}
