/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The behavioural pin for the web Manage Servers grid's version column (#5239), the fourth render site in
/// <c>EngineAwareVersionLabelTests.TheVersionLabel_IsRenderedOnlyByTheKnownEngineAwareSites</c>. It lives in its
/// own class because <c>EngineAwareVersionLabelTests</c> references the WPF Viewer assembly and cannot load on a
/// Mac; this one drives the shipped <c>ToAdminServerRow</c> and runs everywhere.
/// </summary>
public sealed class AdminServersVersionLabelTests
{
    private static readonly DateTime Now = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingAdminServersReader.Row Server(string engine, string? engineKind, int? sqlMajor, int? pgMajor) =>
        new(1, "alpha-01", "alpha-01.example.test", null, false, engine, 0, "sql", true, 0m,
            new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
            "alpha-01", "alpha-01", sqlMajor, pgMajor, null, engineKind, null, null);

    [Fact]
    public void APostgresRow_IsLabelledByItsEngine_NotAsSqlServer()
    {
        var row = DarlingAdminServersReader.ToAdminServerRow(
            Server("postgres", MonitoredEngineKind.Postgres, 0, 16), Now);

        Assert.Equal("PostgreSQL 16", row.version);
        Assert.DoesNotContain("SQL Server", row.version, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ASqlServerRow_KeepsItsSqlServerLabel()
    {
        var row = DarlingAdminServersReader.ToAdminServerRow(
            Server("sqlserver", MonitoredEngineKind.SqlServer, 15, 0), Now);

        Assert.Equal(MonitoredEngineVersion.DescribeEngineVersion(MonitoredEngineKind.SqlServer, 15, 0, null), row.version);
        Assert.StartsWith("SQL Server", row.version, StringComparison.Ordinal);
    }
}
