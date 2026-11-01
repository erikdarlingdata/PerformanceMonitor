/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace Darling.Tests;

/// <summary>
/// The route a test's route hook answers for a level (#7109): the gate and the write read the route that <c>ResolveAgent</c> picks, not
/// only its level. Approval is off, so a Level 3 action in a test that is not about approval runs as it did before approval existed.
/// Null stays null (no route sends the metric to an agent).
/// </summary>
internal static class TestRouteHooks
{
    internal static NotificationRoute? AtLevel(int? level) =>
        level is null ? null : new NotificationRoute(1, "*", "", "", "", "", "", Enabled: true) { AgentLevel = level, ActionApproval = NotificationRouteKinds.ActionApprovalOff };
}
