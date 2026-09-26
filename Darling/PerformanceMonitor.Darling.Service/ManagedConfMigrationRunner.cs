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

/// <summary>The step Step A's outcome names — used only to report a Failed or Unknown result, since both A
/// and later Step B (#4336 lane 6) share the same tri-state verification shape.</summary>
internal enum ManagedConfMigrationStep
{
    A,
    B,
}

/// <summary>The tri-state verdict of a Step A run (#4336 lane 5b, design decision (d)): <c>Verified</c> when
/// every applied setting the after-snapshot reports matches the before snapshot; <c>Failed</c> when a
/// mismatch was found and the restore already ran; <c>Unknown</c> when a snapshot itself could not be taken
/// — the rule the plan's ruling settles is which side of the write that failure landed on.</summary>
internal enum ManagedConfVerificationStatus
{
    Verified,
    Failed,
    Unknown,
}

/// <summary>
/// One <see cref="ManagedConfMigrationRunner.RunStepA"/> or <see cref="ManagedConfMigrationRunner.ResumePending"/>
/// call's result (#4336 lane 5b, design decision (d)): the status, the keys a <c>Failed</c> result named as
/// mismatched (empty otherwise), the backup path once one exists (null only when nothing was ever written —
/// the before-snapshot-throws and pending-write-failed cases), and which step produced this (always
/// <see cref="ManagedConfMigrationStep.A"/> from this lane; Step B is #4336 lane 6).
/// </summary>
internal readonly record struct ManagedConfMigrationOutcome(
    ManagedConfVerificationStatus Status,
    IReadOnlyList<string> MismatchedKeys,
    string? BackupPath,
    ManagedConfMigrationStep Step);

/// <summary>
/// Drives Step A (#4336 lane 5b, design decisions (a)-(d)) end to end over the pure building blocks
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
    /// Runs Step A once, start to finish, for a conf that has never been migrated before this call (#4336 lane
    /// 5b). Order, exactly as the plan specifies:
    /// <list type="number">
    /// <item>snapshot the OLD conf (a throw here leaves nothing written at all — <c>Unknown</c>);</item>
    /// <item><see cref="ManagedConfMigrationSteps.WritePending"/> (a throw here — the pending file itself
    /// could not be written — aborts before <see cref="ManagedConfMigrationSteps.BackupOriginal"/>, with
    /// nothing changed — <c>Unknown</c>, per the ruling on the open question);</item>
    /// <item><see cref="ManagedConfMigrationSteps.BackupOriginal"/>;</item>
    /// <item><see cref="ManagedConfFile.RenderWithValues"/> over the BEFORE snapshot's applied values (the
    /// new managed file's content);</item>
    /// <item><see cref="ManagedConfMigration.Rewrite"/> over <paramref name="derived"/> (the freshly DERIVED
    /// map, per design decision (b) — this is what makes the item-4 log line read "derived &lt;new&gt;");</item>
    /// <item>the rewrite's own log lines, through <paramref name="logger"/>;</item>
    /// <item><see cref="ManagedConfMigrationSteps.WriteTwoSteps"/>;</item>
    /// <item>re-snapshot (a throw here — the RULED behavior: restore both files to their exact pre-migration
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
        catch (Exception) when (ct.IsCancellationRequested is false)
        {
            /* Nothing has been written yet — the before-snapshot-throws case. Nothing to restore. */
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), null, ManagedConfMigrationStep.A);
        }

        try
        {
            ManagedConfMigrationSteps.WritePending(dataDir, before, priorManagedText);
        }
        catch (IOException)
        {
            /* Ruled: the pending file could not be written — abort before BackupOriginal, nothing changed. */
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), null, ManagedConfMigrationStep.A);
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

        var newManagedConfText = ManagedConfFile.RenderWithValues(inputs, beforeValues);
        var postgresqlConfText = File.ReadAllText(postgresqlConfPath);
        var rewrite = ManagedConfMigration.Rewrite(postgresqlConfText, derived, port);

        foreach (var entry in rewrite.Log)
        {
            logger.LogInformation("{Message}", entry.Message);
        }

        ManagedConfMigrationSteps.WriteTwoSteps(dataDir, newManagedConfText, rewrite.NewConfText);

        IReadOnlyList<FileSettingRow> after;
        try
        {
            after = await snapshot(ct).ConfigureAwait(false);
        }
        catch (Exception) when (ct.IsCancellationRequested is false)
        {
            /* Ruled: the after-snapshot throws — restore both files to their exact pre-migration bytes, no
               stamp, delete the pending file, report Unknown. */
            ManagedConfMigrationSteps.RestoreOriginal(dataDir, backupPath, priorManagedText);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A);
        }

        return CompareAndFinish(dataDir, backupPath, priorManagedText, newManagedConfText, before, after);
    }

    /// <summary>
    /// Resumes a run left <c>PendingVerify</c> by a crash after <see cref="ManagedConfMigrationSteps.WriteTwoSteps"/>
    /// but before the stamp or a restore (#4336 lane 5b): reads back the persisted BEFORE snapshot
    /// (<see cref="ManagedConfMigrationSteps.TryReadPending"/>), takes a fresh AFTER snapshot, and runs the
    /// same compare-then-stamp-or-restore tail <see cref="RunStepA"/> does. A snapshot throw here follows the
    /// same ruled path as <see cref="RunStepA"/>'s after-snapshot: restore, delete pending, <c>Unknown</c>.
    /// </summary>
    internal static async Task<ManagedConfMigrationOutcome> ResumePending(
        string dataDir,
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot,
        string backupPath,
        CancellationToken ct)
    {
        ManagedConfMigrationSteps.TryReadPending(dataDir, out var before, out var priorManagedText);

        var managedConfPath = Path.Combine(dataDir, ManagedConfFile.FileName);
        var currentManagedConfText = File.Exists(managedConfPath) ? File.ReadAllText(managedConfPath) : string.Empty;

        IReadOnlyList<FileSettingRow> after;
        try
        {
            after = await snapshot(ct).ConfigureAwait(false);
        }
        catch (Exception) when (ct.IsCancellationRequested is false)
        {
            ManagedConfMigrationSteps.RestoreOriginal(dataDir, backupPath, priorManagedText);
            ManagedConfMigrationSteps.DeletePending(dataDir);
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), backupPath, ManagedConfMigrationStep.A);
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
}
