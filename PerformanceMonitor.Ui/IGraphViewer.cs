/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>
/// A graph viewer control hosted by <see cref="GraphViewerWindow"/>. Implementers subscribe to
/// <see cref="ThemeManager.ThemeChanged"/> in their constructor and must unsubscribe in
/// <see cref="Cleanup"/> — the host window calls it on close (mirrors the PlanViewerControl lifecycle).
/// </summary>
public interface IGraphViewer
{
    /// <summary>Unsubscribes from theme changes / releases handlers. Called by the host on window close.</summary>
    void Cleanup();
}
