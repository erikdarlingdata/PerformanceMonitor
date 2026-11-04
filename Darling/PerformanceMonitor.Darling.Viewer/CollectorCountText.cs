/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The status bar's collector count line. The count behind "need setup" is NO_PERMISSIONS plus EXTENSION_MISSING (#6932): the two
/// bands fixed at the monitored server, one by a grant and one by installing an extension. "No permission" named only the first and
/// sent a person looking for a GRANT that would not help an extension (#7384). The Collection Health tab badge keeps "no permission":
/// it counts only refused runs.
/// </summary>
internal static class CollectorCountText
{
    /// <summary>The word every surface uses for the NO_PERMISSIONS plus EXTENSION_MISSING count.</summary>
    public const string NeedSetup = "need setup";

    /// <summary>
    /// #6828: OK counts HEALTHY collectors only, so a WARNING collector is not called OK, and the warning, not-collected and
    /// need-setup counts follow when there are any.
    /// </summary>
    public static string StatusBar(CollectorHealthClassifier.BandTally tally) =>
        $"Collectors: {tally.Healthy} OK"
        + (tally.Warning > 0 ? $", {tally.Warning} warning" : "")
        + (tally.NotCollected > 0 ? $", {tally.NotCollected} not collected" : "")
        + (tally.NoPermission > 0 ? $", {tally.NoPermission} {NeedSetup}" : "");
}
