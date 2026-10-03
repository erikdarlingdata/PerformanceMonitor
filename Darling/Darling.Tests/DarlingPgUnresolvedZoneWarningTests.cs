/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: a PostgreSQL server whose <c>TimeZone</c> text does not resolve reads as UTC, and says so once: the warning
/// names the zone text and says run times use UTC. A zone that resolves logs nothing.
/// </summary>
public sealed class DarlingPgUnresolvedZoneWarningTests
{
    private const string PosixZone = "EST5EDT,M3.2.0,M11.1.0";

    [Fact]
    public void AnUnresolvedZone_WarnsOncePerServer_NamingTheZone_AndReadsAsUtc()
    {
        var logger = new CapturingTestLogger();

        var first = DarlingWorker.ResolvePgServerClock(49381, PosixZone, logger);
        DarlingWorker.ResolvePgServerClock(49381, PosixZone, logger);
        DarlingWorker.ResolvePgServerClock(49381, PosixZone, logger);

        var warnings = logger.Lines.Where(l => l.StartsWith("Warning:", System.StringComparison.Ordinal)).ToList();
        Assert.Single(warnings);
        Assert.Contains(PosixZone, warnings[0]);
        Assert.Contains("UTC", warnings[0]);
        Assert.Equal(DarlingWorker.ResolvePgServerClock(49382, PosixZone, null).Id, first.Id);

        /* A second server with the same text warns on its own. */
        DarlingWorker.ResolvePgServerClock(49383, PosixZone, logger);
        Assert.Equal(2, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
    }

    [Fact]
    public void AResolvedZone_LogsNothing()
    {
        var logger = new CapturingTestLogger();

        DarlingWorker.ResolvePgServerClock(49384, "UTC", logger);

        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
    }
}
