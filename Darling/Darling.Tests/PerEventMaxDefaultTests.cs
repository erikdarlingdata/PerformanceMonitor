/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every Darling copy of the per-event delivery cap ships at <see cref="AlertDefaults.PerEventMax"/>, the one
/// figure both apps read: the service's darling.json default, the viewer's settings-file default and its
/// clamp fallback, and the alert-settings row a fresh store seeds from. Lite pins its own copy to the same
/// name, so the two apps cannot split the same cycle of incidents at different points.
/// </summary>
public sealed class PerEventMaxDefaultTests
{
    [Fact]
    public void TheServiceConfigDefault_IsTheSharedConstant() =>
        Assert.Equal(AlertDefaults.PerEventMax, new DarlingConfig().Alerts.PerEventMax);

    [Fact]
    public void TheServiceSettingsAdapter_ReportsTheSharedConstant_ForACapAndForAnalysisPages()
    {
        var settings = new DarlingAlertSettings(new DarlingConfig());

        Assert.Equal(AlertDefaults.PerEventMax, settings.PerEventMax);
        Assert.Equal(AlertDefaults.PerEventMax, settings.AnalysisPageCap);
    }

    [Fact]
    public void TheViewerSettingsFileDefault_IsTheSharedConstant() =>
        Assert.Equal(AlertDefaults.PerEventMax, new ViewerAppSettings().AlertPerEventMaxPerCycle);

    [Fact]
    public void TheViewerSettingsFile_ClampsAnOutOfRangeCap_BackToTheSharedConstant()
    {
        var settings = new ViewerAppSettings { AlertPerEventMaxPerCycle = 0 }.Normalize();

        Assert.Equal(AlertDefaults.PerEventMax, settings.AlertPerEventMaxPerCycle);
    }

    [Fact]
    public void TheAlertSettingsRowDefault_IsTheSharedConstant() =>
        Assert.Equal(AlertDefaults.PerEventMax, AlertSettingsRow.Defaults().PerEventMax);
}
