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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Which step an outcome came from — used only to report a Failed or Unknown result, since both A
/// and Step B share the same tri-state verification shape.</summary>
internal enum ManagedConfMigrationStep
{
    A,
    B,
}

/// <summary>The tri-state verdict of a Step A run: <c>Verified</c> when
/// every applied setting the after-snapshot reports matches the before snapshot; <c>Failed</c> when a
/// mismatch was found and the restore already ran; <c>Unknown</c> when a snapshot itself could not be taken
/// — which side of the write that failure landed on decides the outcome.</summary>
internal enum ManagedConfVerificationStatus
{
    Verified,
    Failed,
    Unknown,
}

/// <summary>
/// One <see cref="ManagedConfMigrationRunner.RunStepA"/> or <see cref="ManagedConfMigrationRunner.ResumePending"/>
/// call's result: the status, the keys a <c>Failed</c> result named as
/// mismatched (empty otherwise), the backup path once one exists (null only when nothing was ever written —
/// the before-snapshot-throws and pending-write-failed cases), and which step produced this (always
/// <see cref="ManagedConfMigrationStep.A"/> here; Step B is a separate call), and for an <c>Unknown</c>
/// result the exception type, message, and the phase it was caught in (null otherwise; never file contents).
/// </summary>
internal readonly record struct ManagedConfMigrationOutcome(
    ManagedConfVerificationStatus Status,
    IReadOnlyList<string> MismatchedKeys,
    string? BackupPath,
    ManagedConfMigrationStep Step,
    string? Detail = null);

/// <summary>
/// Drives Step A end to end over the pure building blocks
/// <see cref="ManagedConfMigrationSteps"/>, <see cref="ManagedConfFile"/> and <see cref="ManagedConfFileSettings"/>:
/// snapshot the OLD conf's effective values, persist that snapshot so a crash has something to compare
/// against, back up the original, write the two new files, re-snapshot, compare, then stamp or restore. Every
/// I/O call is a delegate the caller supplies (the real one is a live Npgsql query against
/// <see cref="ManagedConfFileSettings.SnapshotSql"/>; tests fake it), so this class stays free of any
/// database dependency and is exercised by a plain xUnit test.
/// </summary>
internal static class ManagedConfMigrationRunner
{
    /// <summary>
    /// Runs Step A once, start to finish, for a conf that has never been migrated before this call.
    /// Order:
    /// <list type="number">
    /// <item>snapshot the OLD conf (a throw here leaves nothing written at all — <c>Unknown</c>);</item>
    /// <item><see cref="ManagedConfMigrationSteps.WritePending"/> (a throw here — the pending file itself
    /// could not be written — aborts before <see cref="ManagedConfMigrationSteps.BackupOriginal"/>, with
    /// nothing changed — <c>Unknown</c>);</item>
    /// <item><see cref="ManagedConfMigrationSteps.BackupOriginal"/>;</item>
    /// <item><see cref="ManagedConfMigration.Rewrite"/> over <paramref name="derived"/> (the freshly DERIVED
    /// map — this is what makes the change log line read "derived &lt;new&gt;"); the keys it dropped that
    /// this render does not itself derive are collected here (#4336);</item>
    /// <item><see cref="ManagedConfFile.RenderWithValues"/> over the BEFORE snapshot's applied values plus
    /// those dropped keys (the new managed file's content);</item>
    /// <item>the rewrite's own log lines, through <paramref name="logger"/>;</item>
    /// <item><see cref="ManagedConfMigrationSteps.WriteTwoSteps"/>;</item>
    /// <item>re-snapshot (a throw here — restore both files to their exact pre-migration
    /// bytes via <see cref="ManagedConfMigrationSteps.RestoreOriginal"/>, delete the pending file, no stamp —
    /// <c>Unknown</c>);</item>
    /// <item><see cref="ManagedConfFileSettings.Compare"/> the before and after snapshots;</item>
    /// <item>a match: <see cref="ManagedConfMigrationSteps.WriteVerifiedStamp"/>, delete the pending file,
    /// <c>Verified</c>; a mismatch: <see cref="ManagedConfMigrationSteps.RestoreOriginal"/>, delete the
    /// pending file, <c>Failed</c> with the mismatched keys.</item>
    /// </list>
    /// </summary>
    internal static async Task<ManagedConfMigrationOutcome> RunStepA(
        string dataDir,
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot,
        IReadOnlyDictionary<string, string> derived,
        ManagedConfFile.RenderInputs inputs,
        int port,
        DateTime utcNow,
        ILogger logger,
        CancellationToken ct)
    {
        var postgresqlConfPath = Path.Combine(dataDir, "postgresql.conf");
        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);
        string? priorManagedText = File.Exists(managedConfPath) ? File.ReadAllText(managedConfPath) : null;

        IReadOnlyList<FileSettingRow> before;
        try
        {
            before = await snapshot(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ct.IsCancellationRequested is false)
        {
            /* Nothing has been written yet — the before-snapshot-throws case. Nothing to restore. */
            var detail = FormattableString.Invariant($"before-snapshot: {ex.GetType().Name}: {ex.Message}");
            logger.LogWarning(
                "The #4215 conf migration's before-snapshot failed for {DataDirectory}: {Detail}", dataDir, detail);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), null, ManagedConfMigrationStep.A, detail);
        }

        try
        {
            ManagedConfMigrationSteps.WritePending(dataDir, before, priorManagedText);
        }
        catch (IOException ex)
        {
            /* The pending file could not be written — abort before BackupOriginal, nothing changed. */
            var detail = FormattableString.Invariant($"pending write: {ex.GetType().Name}: {ex.Message}");
            logger.LogWarning(
                "The #4215 conf migration's pending-file write failed for {DataDirectory}: {Detail}", dataDir, detail);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), null, ManagedConfMigrationStep.A, detail);
        }

        var backupPath = ManagedConfMigrationSteps.BackupOriginal(dataDir, postgresqlConfPath, utcNow);

        var beforeValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in before)
        {
            if (row.Applied && row.Name is not null && row.Setting is not null)
            {
                beforeValues[row.Name] = row.Setting;
            }
        }

        /* #4336: a carried maintenance_work_mem goes through the SAME cap ManagedConfFile.RenderBody's
           v14 block uses for a freshly derived value — NeedsLegacyMaintenanceWorkMemCap over this running major,
           MaintenanceWorkMemCapMb as the ceiling — so a pre-#3909 2048MB BEFORE snapshot lands in
           darling-managed.conf capped, not carried verbatim into a value PostgreSQL 17 and earlier reject. */
        var postgresMajorForCap = inputs.PostgresMajor > 0 ? inputs.PostgresMajor : (int?)null;
        if (beforeValues.TryGetValue(DarlingManagedPostgres.MaintenanceWorkMemSetting, out var maintenanceWorkMemBefore)
            && DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(postgresMajorForCap, maintenanceWorkMemBefore))
        {
            beforeValues[DarlingManagedPostgres.MaintenanceWorkMemSetting] =
                FormattableString.Invariant($"{DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB");
        }

        var postgresqlConfText = File.ReadAllText(postgresqlConfPath);
        var rewrite = ManagedConfMigration.Rewrite(postgresqlConfText, derived, port);

        /* The keys the rewrite dropped as Ours (design rule 2, ManagedConfMigration.Rewrite) that this
           render does not itself derive a value for — v12's min_wal_size when the disk reading is not
           authoritative, for example. Without carrying the BEFORE snapshot's value forward, the managed
           file simply has nothing for that key, the after-snapshot loses it, and Compare reports a mismatch
           on every start. */
        var classifiedBefore = ManagedConfMigration.ClassifyLines(postgresqlConfText, port);
        var droppedKeys = new List<string>();
        foreach (var line in classifiedBefore)
        {
            if (line.Classification == ManagedConfMigration.ConfLineClassification.Ours
                && line.Key is not null
                && !derived.ContainsKey(line.Key)
                && beforeValues.ContainsKey(line.Key)
                && !droppedKeys.Exists(k => string.Equals(k, line.Key, StringComparison.OrdinalIgnoreCase)))
            {
                droppedKeys.Add(line.Key);
            }
        }

        var newManagedConfText = ManagedConfFile.RenderWithValues(inputs, beforeValues, droppedKeys);

        foreach (var entry in rewrite.Log)
        {
            logger.LogInformation("{Message}", entry.Message);
        }

        /* #4336: if any product marker survives the rewrite — an unclassified or hand-edited marker line
           this rewrite kept verbatim — the file classifies Legacy again on the very next start
           (ManagedConfMigrationState.Classify), and EnsureConfAppended then re-appends the missing blocks
           at the end, after the moved operator lines, overriding them. Abort BEFORE any write — nothing on
           disk has changed yet — and report Failed rather than let that happen silently. */
        var survivingMarker = Array.Find(DarlingManagedPostgres.AllManagedConfMarkers,
            marker => rewrite.NewConfText.Contains(marker, StringComparison.Ordinal));
        if (survivingMarker is not null)
        {
            var markerDetail = FormattableString.Invariant($"rewrite: a product marker survived the rewrite: {survivingMarker}");
            logger.LogWarning(
                "The #4215 conf migration's rewrite for {DataDirectory} left a product marker in the new postgresql.conf; aborting before any write. {Detail}",
                dataDir, markerDetail);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Failed, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A, markerDetail);
        }

        ManagedConfMigrationSteps.WriteTwoSteps(dataDir, newManagedConfText, rewrite.NewConfText);

        IReadOnlyList<FileSettingRow> after;
        try
        {
            after = await snapshot(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ct.IsCancellationRequested is false)
        {
            /* The after-snapshot throws — restore both files to their exact pre-migration bytes, no
               stamp, delete the pending file, report Unknown. */
            var detail = FormattableString.Invariant($"after-snapshot: {ex.GetType().Name}: {ex.Message}");
            logger.LogWarning(
                "The #4215 conf migration's after-snapshot failed for {DataDirectory}: {Detail}", dataDir, detail);
            ManagedConfMigrationSteps.RestoreOriginal(dataDir, backupPath, priorManagedText);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A, detail);
        }

        return CompareAndFinish(dataDir, backupPath, priorManagedText, newManagedConfText, before, after);
    }

    /// <summary>
    /// Resumes a run left <c>PendingVerify</c> by a crash after <see cref="ManagedConfMigrationSteps.WriteTwoSteps"/>
    /// but before the stamp or a restore: reads back the persisted BEFORE snapshot
    /// (<see cref="ManagedConfMigrationSteps.TryReadPending"/>), takes a fresh AFTER snapshot, and runs the
    /// same compare-then-stamp-or-restore tail <see cref="RunStepA"/> does. A snapshot throw here follows the
    /// same path as <see cref="RunStepA"/>'s after-snapshot: restore, delete pending, <c>Unknown</c>. A
    /// pending file that exists but cannot be parsed (#4336) is a DIFFERENT case from either of those: there
    /// is a backup (<paramref name="backupPath"/> is always given to this call), so this restores from it,
    /// deletes the pending file, and reports <c>Failed</c> — not <c>Unknown</c> — so the store-settings
    /// alert fires rather than staying silent forever on a stuck PendingVerify.
    /// </summary>
    internal static async Task<ManagedConfMigrationOutcome> ResumePending(
        string dataDir,
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot,
        string backupPath,
        CancellationToken ct,
        ILogger? logger = null)
    {
        if (!ManagedConfMigrationSteps.TryReadPending(dataDir, out var before, out var priorManagedText))
        {
            const string detail = "resume: the pending file exists but could not be parsed";
            logger?.LogWarning(
                "The #4215 conf migration's pending file for {DataDirectory} could not be parsed; restoring from backup and reporting Failed.", dataDir);
            ManagedConfMigrationSteps.RestoreOriginal(dataDir, backupPath, priorManagedText: null);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Failed, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A, detail);
        }

        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);
        var currentManagedConfText = File.Exists(managedConfPath) ? File.ReadAllText(managedConfPath) : string.Empty;

        IReadOnlyList<FileSettingRow> after;
        try
        {
            after = await snapshot(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ct.IsCancellationRequested is false)
        {
            var detail = FormattableString.Invariant($"resume: {ex.GetType().Name}: {ex.Message}");
            logger?.LogWarning(
                "The #4215 conf migration's resume after-snapshot failed for {DataDirectory}: {Detail}", dataDir, detail);
            ManagedConfMigrationSteps.RestoreOriginal(dataDir, backupPath, priorManagedText);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A, detail);
        }

        return CompareAndFinish(dataDir, backupPath, priorManagedText, currentManagedConfText, before, after);
    }

    private static ManagedConfMigrationOutcome CompareAndFinish(
        string dataDir,
        string backupPath,
        string? priorManagedText,
        string newManagedConfText,
        IReadOnlyList<FileSettingRow> before,
        IReadOnlyList<FileSettingRow> after)
    {
        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        if (match)
        {
            ManagedConfMigrationSteps.WriteVerifiedStamp(dataDir, newManagedConfText);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Verified, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A);
        }

        ManagedConfMigrationSteps.RestoreOriginal(dataDir, backupPath, priorManagedText);
        ManagedConfMigrationSteps.DeletePending(dataDir);
        return new ManagedConfMigrationOutcome(
            ManagedConfVerificationStatus.Failed, mismatches, backupPath, ManagedConfMigrationStep.A);
    }

    /// <summary>
    /// Step B: after a normal derivation has already rendered and written
    /// <c>darling-managed.conf</c>, checks that <paramref name="rows"/> (a fresh <c>pg_file_settings</c>
    /// snapshot) shows every key <paramref name="renderedText"/> declares with the rendered value and no
    /// error, from a row whose <c>sourcefile</c> is exactly <see cref="ManagedConfFile.FileName"/>, by name
    /// (#4336; <see cref="Path.GetFileName(string)"/>, not a suffix match — a stray <c>old-darling-managed.conf</c>
    /// in an include directory is a different file). A row's
    /// <c>applied</c> may be false — an operator line below the include can legitimately override it; only
    /// the value and the absence of an error matter here. All keys match: stamp the new text as verified. Any
    /// key fails: restore <paramref name="previousText"/> (when there was one) so the old stamp matches
    /// again, and report <c>Failed</c>.
    /// </summary>
    internal static ManagedConfMigrationOutcome VerifyStepB(
        string dataDir,
        IReadOnlyList<FileSettingRow> rows,
        string renderedText,
        string? previousText)
    {
        var byKey = new Dictionary<string, List<FileSettingRow>>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows)
        {
            if (row.Name is null || row.SourceFile is null
                || !string.Equals(Path.GetFileName(row.SourceFile), ManagedConfFile.FileName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!byKey.TryGetValue(row.Name, out var list))
            {
                list = new List<FileSettingRow>();
                byKey[row.Name] = list;
            }

            list.Add(row);
        }

        var mismatchedKeys = new List<string>();
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(renderedText))
        {
            var ok = byKey.TryGetValue(name, out var candidates)
                && candidates.Exists(r => r.Error is null && string.Equals(r.Setting, value, StringComparison.Ordinal));
            if (!ok)
            {
                mismatchedKeys.Add(name);
            }
        }

        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);

        if (mismatchedKeys.Count == 0)
        {
            ManagedConfMigrationSteps.WriteVerifiedStamp(dataDir, renderedText);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Verified, Array.Empty<string>(), null, ManagedConfMigrationStep.B);
        }

        if (previousText is not null)
        {
            if (!ManagedConfFile.TryReplaceAtomic(
                    managedConfPath,
                    previousText,
                    ManagedConfFile.DefaultMaxReplaceAttempts,
                    ManagedConfFile.DefaultReplaceRetryDelay,
                    out var restoreError))
            {
                throw new IOException(
                    FormattableString.Invariant($"Failed to restore {managedConfPath}."), restoreError);
            }
        }

        return new ManagedConfMigrationOutcome(
            ManagedConfVerificationStatus.Failed, mismatchedKeys, null, ManagedConfMigrationStep.B);
    }

    /// <summary>
    /// One Information line per key that changed between <paramref name="previousText"/> and the fresh
    /// render, for Step B's log: the key, its old value (<c>(unset)</c> when it had none), the
    /// new value, and the <see cref="ManagedConfFile.RenderInputs"/> that produced it — RAM, whether that RAM
    /// figure is authoritative, platform, PostgreSQL major, CPU count, and hypertable count.
    /// </summary>
    internal static string FormatStepBChangeLog(
        IReadOnlyList<(string Key, string? Old, string New)> changes,
        ManagedConfFile.RenderInputs inputs)
    {
        var lines = new List<string>(changes.Count);
        foreach (var (key, old, @new) in changes)
        {
            lines.Add(FormattableString.Invariant(
                $"{key}: {old ?? "(unset)"} -> {@new} (RAM {inputs.RamBytes / (1024 * 1024)} MB, authoritative {inputs.RamAuthoritative}; platform {inputs.Platform}; PG {inputs.PostgresMajor}; CPUs {inputs.ProcessorCount}; hypertables {inputs.HypertableCount})"));
        }

        return string.Join("\n", lines);
    }

    /// <summary>
    /// Diffs <paramref name="previousText"/> (null when there was none) against <paramref name="renderedText"/>
    /// key by key, for <see cref="FormatStepBChangeLog"/>'s input: only keys whose value actually changed —
    /// a key rendered with the same value it already had is not a change.
    /// </summary>
    internal static IReadOnlyList<(string Key, string? Old, string New)> DiffStepBChanges(
        string? previousText, string renderedText)
    {
        var oldValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (previousText is not null)
        {
            foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(previousText))
            {
                oldValues[name] = value;
            }
        }

        var changes = new List<(string Key, string? Old, string New)>();
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(renderedText))
        {
            var hadOld = oldValues.TryGetValue(name, out var old);
            if (!hadOld || !string.Equals(old, value, StringComparison.Ordinal))
            {
                changes.Add((name, hadOld ? old : null, value));
            }
        }

        return changes;
    }
}
