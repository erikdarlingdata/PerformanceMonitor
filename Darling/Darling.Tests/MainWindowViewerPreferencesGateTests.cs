/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the #2715 fix: whether <see cref="MainWindow"/> persists a <see cref="SettingsWindow"/> session's
/// <see cref="SettingsWindow.Result"/> (Default Time Range + Auto-refresh interval — a purely local, per-user
/// preference file) must depend ONLY on whether Save produced a result, never on the Settings window's own
/// <c>ShowDialog()</c> return value. Before the fix the two were AND-ed together
/// (<c>ShowDialog() == true &amp;&amp; Result is not null</c>), so a read-only viewer seat's
/// <see cref="ViewerReadOnlyException"/> on the UNRELATED shared Postgres/TimescaleDB write (alert engine,
/// notifications, service flags — attempted by the same Save click, after
/// <see cref="SettingsWindow.Result"/> is already built) kept the window from ever setting
/// <c>DialogResult = true</c>, which silently discarded the local preference change too. A reported symptom
/// on the same bug report: the Color Theme control (which self-persists to its own local file BEFORE the
/// store write is attempted, with no dependency on <c>ShowDialog()</c> at all) correctly survived a read-only
/// Save; Default Time Range and Auto-refresh interval did not, purely because of this gate.
/// </summary>
public sealed class MainWindowViewerPreferencesGateTests
{
    [Fact]
    public void ShouldPersistViewerPreferences_ResultCaptured_IsTrue_EvenWhenDialogWasNotAcceptedByCallerConvention()
    {
        /* The regression scenario: Save built a Result (the operator changed Default Time Range / Auto-refresh),
           but the Settings window never reaches DialogResult = true because the LATER, unrelated shared-store
           write threw (a read-only seat, a network blip, a schema-skew error — none of which have anything to
           do with this local preference). The old gate ANDed in ShowDialog()'s return value and would have
           discarded this Result; the fix must not. */
        var result = new ViewerPreferences
        {
            DefaultTimeRangeIndex = 0,
            AutoRefreshEnabled = false,
            AutoRefreshIntervalIndex = 2,
        };

        Assert.True(MainWindow.ShouldPersistViewerPreferences(result));
    }

    [Fact]
    public void ShouldPersistViewerPreferences_NoResult_IsFalse()
    {
        /* Close-without-save (the X button, Esc, or Close) never calls SaveButton_Click, so Result stays null
           and there is nothing to persist — this must stay false. */
        Assert.False(MainWindow.ShouldPersistViewerPreferences(null));
    }
}
