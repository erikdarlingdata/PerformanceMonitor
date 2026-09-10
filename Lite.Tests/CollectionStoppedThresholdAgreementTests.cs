/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's side of the #2794 agreement: the Agent-status staleness window ("a reading older than this is not
/// presented as current") documented itself as MIRRORING the headless service's window, at a numerically-equal
/// but independently-editable 30 — the exact drift shape <c>ServerHealthThresholds</c> exists to prevent
/// (#1562). Both now derive from the shared constant; this pin holds the two SYMBOLS equal so a future edit
/// to either goes red instead of silently re-splitting the definition. Proven red against the pre-#2794 tree
/// (30m != 15m).
/// </summary>
public class CollectionStoppedThresholdAgreementTests
{
    [Fact]
    public void TheAgentStalenessWindow_AndTheOfflineBand_AreTheSameWindow() =>
        Assert.Equal(AgentStatusRow.StaleWindow, ServerHealthThresholds.OfflineThreshold);

    [Fact]
    public void TheSharedConstant_IsTheWindowBothDeriveFrom() =>
        Assert.Equal(
            TimeSpan.FromMinutes(ServerHealthThresholds.CollectionStoppedMinutesDefault),
            ServerHealthThresholds.OfflineThreshold);
}
