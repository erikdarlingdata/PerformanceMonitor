/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4051 round-2 review, L-2: a managed target's failed <c>log_timezone</c> read logs at Warning at most once an
/// hour per server, and at Debug in between, instead of a Warning with a stack trace on every cycle.
/// </summary>
public sealed class LogTimezoneReadFailureLevelTests
{
    [Fact]
    public void TheFirstFailureWarns_LaterOnesInsideTheIntervalDoNot_AndEachServerCountsSeparately()
    {
        var warned = new ConcurrentDictionary<int, DateTime>();
        var start = new DateTime(2026, 9, 23, 12, 0, 0, DateTimeKind.Utc);
        var interval = DarlingCollectorRunner.LogTimezoneReadWarningInterval;

        Assert.Equal(LogLevel.Warning, DarlingCollectorRunner.LogTimezoneReadFailureLevel(warned, 1, start));
        Assert.Equal(LogLevel.Debug, DarlingCollectorRunner.LogTimezoneReadFailureLevel(warned, 1, start + interval - TimeSpan.FromSeconds(1)));
        Assert.Equal(LogLevel.Warning, DarlingCollectorRunner.LogTimezoneReadFailureLevel(warned, 2, start + TimeSpan.FromMinutes(1)));
        Assert.Equal(LogLevel.Warning, DarlingCollectorRunner.LogTimezoneReadFailureLevel(warned, 1, start + interval));
        Assert.Equal(LogLevel.Debug, DarlingCollectorRunner.LogTimezoneReadFailureLevel(warned, 1, start + interval + TimeSpan.FromMinutes(1)));
    }
}
