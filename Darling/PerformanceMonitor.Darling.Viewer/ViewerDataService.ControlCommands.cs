/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's typed wrappers over the Stage-2 command plane (<see cref="EnqueueCommandAsync"/> /
/// <see cref="RunCommandAsync"/>) for the operator's imperative actions: <c>pause</c>/<c>resume</c> the whole
/// service (the Settings window's Data Collection toggle), <c>snapshot_now</c> for one server (the "Latest
/// Snapshot" button), <c>analyze_now</c> for one server (the Recommendations "Generate now" button), and
/// <c>purge_now</c> fleet-wide (the Collection Health "Purge Now" button). The
/// command TYPES match <c>DarlingCommandExecutor.ResolvePlan</c> exactly so the two ends agree; Darling.Tests
/// pin the constants against the executor's cases.
///
/// <para>Each is a write (enqueue), so a read-only <c>viewer</c> seat throws
/// <see cref="ViewerReadOnlyException"/> from <see cref="EnqueueCommandAsync"/> — correct: a read-only seat
/// can neither pause the service nor command a capture. Pause/resume only WRITE <c>config_service.paused</c>
/// (the service's reconcile applies it) so a 45s wait is ample; <c>snapshot_now</c>/<c>analyze_now</c> drive
/// the running collection/analysis loop for a server and can run a full sweep, so they use a longer
/// <see cref="ImperativeCommandTimeout"/>. <c>snapshot_now</c> runs EVEN WHILE PAUSED (explicit operator
/// intent — the service honors it), so the button stays enabled while paused.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The control-command types the viewer enqueues (must match the service executor's cases).</summary>
    public const string CommandPause = "pause";
    public const string CommandResume = "resume";
    public const string CommandSnapshotNow = "snapshot_now";
    public const string CommandAnalyzeNow = "analyze_now";
    public const string CommandPurgeNow = "purge_now";

    /// <summary>A full-server snapshot or analysis pass can take longer than a config write's <see
    /// cref="DefaultCommandTimeout"/>, so the imperative per-server commands wait up to three minutes for the
    /// running loop to finish before the UI reports "still running / try again".</summary>
    public static readonly TimeSpan ImperativeCommandTimeout = TimeSpan.FromMinutes(3);

    /// <summary>Enqueues a <c>pause</c> (service stops collecting on its next reconcile) and waits for the
    /// terminal result. Returns null on timeout. Read-only seats throw <see cref="ViewerReadOnlyException"/>.</summary>
    public Task<CommandResult?> PauseServiceAsync(CancellationToken cancellationToken = default) =>
        RunCommandAsync(CommandPause, targetServerId: null, argsJson: null, RequestedBy(), timeout: null, cancellationToken);

    /// <summary>Enqueues a <c>resume</c> (service resumes collecting) and waits for the terminal result.</summary>
    public Task<CommandResult?> ResumeServiceAsync(CancellationToken cancellationToken = default) =>
        RunCommandAsync(CommandResume, targetServerId: null, argsJson: null, RequestedBy(), timeout: null, cancellationToken);

    /// <summary>Enqueues a <c>snapshot_now</c> for one server (run its collectors now, even while paused) and
    /// waits for the terminal result. Returns null on timeout.</summary>
    public Task<CommandResult?> RequestSnapshotNowAsync(int serverId, CancellationToken cancellationToken = default) =>
        RunCommandAsync(CommandSnapshotNow, serverId, argsJson: null, RequestedBy(), ImperativeCommandTimeout, cancellationToken);

    /// <summary>Enqueues an <c>analyze_now</c> for one server (force an analysis pass now) and waits for the
    /// terminal result. Returns null on timeout.</summary>
    public Task<CommandResult?> RequestAnalyzeNowAsync(int serverId, CancellationToken cancellationToken = default) =>
        RunCommandAsync(CommandAnalyzeNow, serverId, argsJson: null, RequestedBy(), ImperativeCommandTimeout, cancellationToken);

    /// <summary>Enqueues a <c>purge_now</c> (start the retention purge over the shared store now) and waits for
    /// the terminal result. A current service starts the purge in the background, paced, and answers at once with
    /// <c>started</c> or <c>alreadyRunning</c> (#4825); an older one runs it inline and answers with the totals.
    /// Fleet-wide over the shared tables, so NO server id. <paramref
    /// name="customRetentionDays"/> (when set) purges every collector to that horizon instead of the configured
    /// fleet retention; null = the configured horizons. Returns null on timeout. Read-only seats throw
    /// <see cref="ViewerReadOnlyException"/> (a purge is a config-write command, like Pause).</summary>
    public Task<CommandResult?> RequestPurgeNowAsync(int? customRetentionDays = null, CancellationToken cancellationToken = default)
    {
        var argsJson = customRetentionDays is int days ? BuildPurgeNowArgs(days) : null;
        return RunCommandAsync(CommandPurgeNow, targetServerId: null, argsJson, RequestedBy(), ImperativeCommandTimeout, cancellationToken);
    }

    /// <summary>
    /// Builds the <c>purge_now</c> <c>args_json</c> carrying a custom retention horizon; the service executor
    /// reads the <c>retention_days</c> key, so the two ends must agree on it. Pure; pinned by Darling.Tests.
    /// </summary>
    public static string BuildPurgeNowArgs(int customRetentionDays) =>
        JsonSerializer.Serialize(new { retention_days = customRetentionDays });

    /// <summary>The audit label stamped on <c>config_command.requested_by</c> — the interactive user + a viewer tag.</summary>
    private static string RequestedBy()
    {
        try
        {
            return $"{Environment.UserName} (viewer)";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return "viewer";
        }
    }
}
