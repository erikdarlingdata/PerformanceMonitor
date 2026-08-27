/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The single place the deploy procedure's rollback-backup naming convention is written down. Two very
/// different pieces of the product need to agree on it: <c>upgrade-darling.ps1</c>, which CREATES one of
/// these directories before it overwrites the install tree and PRUNES the ones past retention, and
/// <see cref="DarlingInstallDirectoryReport"/>, which has to RECOGNISE them so it can stop shouting about
/// them one line at a time.
///
/// <para><b>Why a shared constant and not two literals.</b> #2525 is what happens when the two halves do
/// not agree: the deploy procedure had been writing <c>_rollback_manual_&lt;stamp&gt;</c> into the install
/// root since before the layout report existed, the layout report classifies by ELIMINATION against the
/// product's own directories, and so every backup the procedure made came back as a directory nobody could
/// account for. Forty-six of them, forty-six warnings, every start. If the script's spelling and the
/// service's matcher are ever allowed to drift apart the same failure returns silently, so the C# side owns
/// the string and <c>DarlingDeployRollbackRetentionTests</c> runs the script's own predicate against this
/// one to prove they still answer alike.</para>
///
/// <para><b>What this does NOT cover.</b> The store-copy report (#1775) sees directories named
/// <c>&lt;datadir&gt;_rollback_manual_&lt;stamp&gt;</c> beside the store's DATA directory under
/// <c>%ProgramData%</c> — the prefix as a suffix, on a different tree, holding a whole PostgreSQL cluster.
/// Those stay reported individually and deliberately: one of them can be hundreds of gigabytes, they are
/// identified structurally by <c>PG_VERSION</c> rather than by name, and no script of ours creates or prunes
/// them. This convention is about the INSTALL tree, where a backup is ~120 MB of binaries and the deploy
/// script both makes and removes them.</para>
/// </summary>
internal static class DarlingRollbackBackups
{
    /// <summary>
    /// What the deploy procedure names its pre-copy backup of the install tree. Matched
    /// case-insensitively: Windows paths are, and a matcher stricter than the filesystem is a matcher that
    /// misses a directory the operator will tell you is right there.
    /// </summary>
    internal const string Prefix = "_rollback_manual_";

    /// <summary>
    /// How many backups <c>upgrade-darling.ps1</c> keeps, and the number the report quotes when it says a
    /// box is carrying more than that.
    ///
    /// <para>Three, not one and not ten. One is not a retention policy — it is gone the moment you deploy
    /// the fix for the bad deploy, which is exactly when you want it. Three covers "the release, the one
    /// before it, and the one before that", which is as far back as a rollback is ever actually wanted:
    /// beyond it you are not rolling back, you are restoring a version nobody asked for. The field box in
    /// #2525 had forty-six, and the forty-third could only have returned it to a build from three weeks
    /// earlier.</para>
    ///
    /// <para>The script's <c>-KeepRollbacks</c> default is this number and
    /// <c>DarlingDeployRollbackRetentionTests</c> pins the two together — a report that advertises a
    /// retention the script does not implement is worse than no advice at all.</para>
    /// </summary>
    internal const int DefaultRetained = 3;

    /// <summary>The command an operator runs to act on what the report just told them.</summary>
    internal const string PruneCommand = "upgrade-darling.ps1 -PruneOnly";

    /// <summary>
    /// True when <paramref name="directoryPath"/> is one of the deploy procedure's rollback backups.
    ///
    /// <para>Prefix plus at least one more character, and nothing more clever than that. A stamp format is
    /// deliberately NOT parsed: the backlog this exists to recognise was made over months by a procedure
    /// that has spelled its stamp more than one way, and a matcher that only accepted today's spelling
    /// would leave the old ones being reported one line at a time — which is the entire complaint. The
    /// prefix is a namespace: anything inside it belongs to the deploy procedure, and a directory an
    /// operator named into someone else's namespace is treated as theirs.</para>
    ///
    /// <para>The trailing-character requirement keeps a bare <c>_rollback_manual_</c> — a directory with no
    /// stamp at all, which no procedure produces — outside the convention, so it stays in the
    /// unaccounted-for report where a thing nobody can explain belongs.</para>
    /// </summary>
    internal static bool IsRollbackBackup(string directoryPath)
    {
        if (string.IsNullOrEmpty(directoryPath))
        {
            return false;
        }

        /* GetFileName on a path with a trailing separator returns empty, so the separator comes off first —
           the caller may be handing over a directory path spelled either way. */
        var name = Path.GetFileName(Path.TrimEndingDirectorySeparator(directoryPath));

        return name.Length > Prefix.Length
            && name.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);
    }
}
