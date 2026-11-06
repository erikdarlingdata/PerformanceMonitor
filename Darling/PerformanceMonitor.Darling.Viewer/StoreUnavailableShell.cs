/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What stays usable while the full-window store/config failure message shows (#4648). The message covers
/// only the content column; the sidebar entries that read no store stay usable, the rest are disabled.
/// Pure, so the rule is pinned without a window.
/// </summary>
internal static class StoreUnavailableShell
{
    /// <summary>Sidebar footer Click handlers that need the store: disabled while the store is unavailable.</summary>
    internal static readonly IReadOnlyList<string> StoreDependentSidebarHandlers =
    [
        "AddServerButton_Click", "AddMultipleServersButton_Click", "ManageServersButton_Click",
        "ManageTags_Click", "ImportSettingsButton_Click", "SettingsButton_Click",
    ];

    /// <summary>Sidebar footer Click handlers that read no store: usable in every failure state.</summary>
    internal static readonly IReadOnlyList<string> StoreIndependentSidebarHandlers =
    [
        "OpenPlanViewerButton_Click", "ViewLogButton_Click", "OpenLogFolderButton_Click", "AboutButton_Click",
    ];

    /// <summary>The message covers the content column unless the Plan Viewer tab is the one showing.</summary>
    internal static bool OverlayVisible(bool storeUnavailable, bool planViewerShowing) =>
        storeUnavailable && !planViewerShowing;

    /// <summary>Whether a refresh tick retries the server-list read while the message shows: only when that read is what
    /// failed (a connect-path gate or a configuration failure is not cleared by a later list read), and one retry at a
    /// time. A retry that succeeds clears the message; one that fails leaves it for the next tick.</summary>
    internal static bool RetryServerListOnTick(bool storeUnavailable, bool serverListReadFailed, bool retryInFlight) =>
        storeUnavailable && serverListReadFailed && !retryInFlight;
}
