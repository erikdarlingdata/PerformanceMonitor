/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>What the update check on startup came to (#9745).</summary>
internal enum StartupUpdateKind
{
    /// <summary><c>check_for_updates_on_startup</c> is off, so no check ran.</summary>
    Skipped,

    /// <summary>The check read the latest release and this build is not older than it.</summary>
    UpToDate,

    /// <summary>A newer release exists.</summary>
    Available,

    /// <summary>The check could not read the latest release.</summary>
    Failed
}

/// <summary>
/// One outcome of the check. <paramref name="Version"/> is the newer release for <see cref="StartupUpdateKind.Available"/> and the
/// latest release for <see cref="StartupUpdateKind.UpToDate"/>. <paramref name="Reason"/> is set for <see cref="StartupUpdateKind.Failed"/>.
/// </summary>
internal sealed record StartupUpdateOutcome(StartupUpdateKind Kind, string? Version = null, string? Reason = null);

/// <summary>
/// The update check Lite runs after it starts, and the one log line each outcome writes. A headless Lite has no window to show the
/// result in, so the log is the only place an operator sees whether the running build is current (#9745).
/// </summary>
internal static class StartupUpdateCheck
{
    /// <summary>
    /// Runs the check and writes exactly one line for its outcome: Information for a skipped check, a current build and an available
    /// update, Warning for a failed check. The Velopack source is asked first. It finds nothing on an install that Velopack did not
    /// place, such as the portable zip, and a failure there is not an outcome, so the GitHub release check decides. Never throws.
    /// </summary>
    /// <param name="enabled">The <c>check_for_updates_on_startup</c> setting.</param>
    /// <param name="headless">Whether this run has no window; the available line then says how to update.</param>
    /// <param name="runningVersion">The informational version of the running build.</param>
    /// <param name="velopackCheck">The newer version Velopack finds, or null.</param>
    /// <param name="releaseCheck">The GitHub latest-release check.</param>
    /// <param name="info">The Information sink.</param>
    /// <param name="warn">The Warning sink.</param>
    internal static async Task<StartupUpdateOutcome> RunAsync(
        bool enabled,
        bool headless,
        string runningVersion,
        Func<Task<string?>> velopackCheck,
        Func<Task<UpdateCheckResult>> releaseCheck,
        Action<string> info,
        Action<string> warn)
    {
        var outcome = await CheckAsync(enabled, velopackCheck, releaseCheck).ConfigureAwait(false);
        var line = LogLine(outcome, runningVersion, headless);
        if (outcome.Kind == StartupUpdateKind.Failed)
        {
            warn(line);
        }
        else
        {
            info(line);
        }

        return outcome;
    }

    private static async Task<StartupUpdateOutcome> CheckAsync(
        bool enabled,
        Func<Task<string?>> velopackCheck,
        Func<Task<UpdateCheckResult>> releaseCheck)
    {
        if (!enabled)
        {
            return new StartupUpdateOutcome(StartupUpdateKind.Skipped);
        }

        try
        {
            var velopackVersion = await velopackCheck().ConfigureAwait(false);
            if (velopackVersion != null)
            {
                return new StartupUpdateOutcome(StartupUpdateKind.Available, velopackVersion);
            }
        }
        catch
        {
            /* Velopack has no packages for an install it did not place: the release check below decides. */
        }

        UpdateCheckResult result;
        try
        {
            result = await releaseCheck().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            result = new UpdateCheckResult(null, UpdateCheckService.DescribeFailure(ex));
        }

        if (result.Info is not { } release)
        {
            return new StartupUpdateOutcome(StartupUpdateKind.Failed, Reason: result.FailureReason ?? "no answer");
        }

        return new StartupUpdateOutcome(
            release.IsUpdateAvailable ? StartupUpdateKind.Available : StartupUpdateKind.UpToDate,
            release.LatestVersion);
    }

    /// <summary>The log line for an outcome. It names the running build, and the release or the reason the outcome rests on.</summary>
    internal static string LogLine(StartupUpdateOutcome outcome, string runningVersion, bool headless) => outcome.Kind switch
    {
        StartupUpdateKind.Skipped =>
            $"The update check is off (check_for_updates_on_startup is false). This build is {runningVersion}.",
        StartupUpdateKind.UpToDate =>
            $"No update is available. This build is {runningVersion}, and the latest release is {outcome.Version}.",
        StartupUpdateKind.Available when headless =>
            $"Update {outcome.Version} is available. This build is {runningVersion}. This instance runs headless: stop it with --stop, update, and start it again.",
        StartupUpdateKind.Available =>
            $"Update {outcome.Version} is available. This build is {runningVersion}.",
        _ =>
            $"The update check failed: {outcome.Reason}. This build is {runningVersion}."
    };
}
