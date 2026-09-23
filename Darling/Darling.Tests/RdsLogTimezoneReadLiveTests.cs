/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4046 part 1b against a real server (#4051 review H1). The managed route's log_timezone read shipped as a
/// bare expression with no SELECT, so every call failed with 42601. No fake-connection test could see that,
/// because a fake answers whatever text it is sent. This runs the exact statement the RDS and Aurora ingest
/// path sends. It only reads a setting, but it uses the shared DARLING_TEST_PG server, so it sits with the
/// other live classes.
/// </summary>
[Collection("live-postgres")]
public sealed class RdsLogTimezoneReadLiveTests
{
    [Fact]
    public async Task TheManagedRoutesLogTimezoneRead_RunsAndAgreesWithTheServersOwnSetting()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        string shown;
        await using (var show = connection.CreateCommand())
        {
            show.CommandTimeout = 10;
            show.CommandText = "SHOW log_timezone";
            shown = (string)(await show.ExecuteScalarAsync())!;
        }

        var isUtc = await DarlingCollectorRunner.ReadLogTimezoneIsUtcAsync(connection, CancellationToken.None);

        Assert.Equal(PgDeadlockLogParser.IsUtcLogTimezoneSetting(shown), isUtc);
    }
}
