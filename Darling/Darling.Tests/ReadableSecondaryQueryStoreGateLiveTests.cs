/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: no PostgreSQL store here, only a scratch SQL Server database per fact. It shares "remediation-live-sql" with the other
   classes that run on the one test SQL Server. */

/// <summary>
/// #6252 against a REAL SQL Server: the plan correction and Query Store health collectors skip a database whose
/// <c>sys.database_query_store_options.readonly_reason</c> has bit 8 set (a readable secondary that is not in an availability group, for
/// example a cloud read replica), as the Query Store collector does (#1558).
///
/// <para>A test server cannot BE a read replica, so the engine never sets bit 8 here. The seam: the facts run each collector's real per-database
/// statement and real reader, with the one reference to <c>sys.database_query_store_options</c> pointed at a scratch table that is a copy of the
/// real view (<c>SELECT *</c> into <c>dbo.pm_qso</c>) with <c>readonly_reason</c> set to what a replica reports. So these prove what each
/// collector's gate DOES with bit 8 (and with other reasons and with Query Store off). They do not prove that a real replica reports bit 8:
/// that is the engine's documented meaning of the bit, and the field evidence in #6252.</para>
/// </summary>
[Collection("remediation-live-sql")]
public sealed class ReadableSecondaryQueryStoreGateLiveTests
{
    private const string ServerName = "darling-readable-secondary-gate";
    private const string RealView = "sys.database_query_store_options";
    private const string CopyTable = "dbo.pm_qso";

    private static SqlConnectionStringBuilder Admin(string database) => new()
    {
        DataSource = Environment.GetEnvironmentVariable("DARLING_TEST_SQL"),
        InitialCatalog = database,
        UserID = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
        Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
        TrustServerCertificate = true,
        Encrypt = SqlConnectionEncryptOption.Optional,
        Pooling = false,
    };

    private static async Task SqlAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static CollectorContext Context() => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = ServerName,
        Target = new CollectorTargetInfo { SqlMajorVersion = 17 }, Deltas = new CollectorDeltaCalculator(), EnumerationProbeResult = 17,
    };

    /// <summary>Runs the collector's per-database statement from master, optionally with the view swapped for the scratch copy.</summary>
    private static async Task<List<QueryStoreHealthCollector.Row>> HealthAsync(string database, bool useCopy, CancellationToken ct)
    {
        var context = Context();
        var query = QueryStoreHealthCollector.Instance.BuildPerItemQuery(database, context);
        var text = useCopy ? query.Text.Replace(RealView, CopyTable, StringComparison.Ordinal) : query.Text;
        var rows = new List<QueryStoreHealthCollector.Row>();
        await using var connection = new SqlConnection(Admin("master").ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(new CollectorQuery(text, query.Parameters), connection, 120);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await QueryStoreHealthCollector.Instance.ReadItemAsync(database, reader, rows, context, ct);
        return rows;
    }

    private static async Task<List<PlanCorrectionCollector.Row>> PlanCorrectionAsync(string database, bool useCopy, CancellationToken ct)
    {
        var context = Context();
        var query = PlanCorrectionCollector.Instance.BuildPerItemQuery(database, context);
        var text = useCopy ? query.Text.Replace(RealView, CopyTable, StringComparison.Ordinal) : query.Text;
        var rows = new List<PlanCorrectionCollector.Row>();
        await using var connection = new SqlConnection(Admin("master").ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(new CollectorQuery(text, query.Parameters), connection, 120);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await PlanCorrectionCollector.Instance.ReadItemAsync(database, reader, rows, context, ct);
        return rows;
    }

    /// <summary>A scratch database, with Query Store on or off, and a copy of the real options view whose <c>readonly_reason</c> is <paramref name="reason"/> when given.</summary>
    private static async Task RunAsync(bool queryStoreOn, int? reason, Func<string, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL")), "Set DARLING_TEST_SQL (with DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD) to run the readable-secondary gate live tests.");
        var ct = TestContext.Current.CancellationToken;
        var database = "pm_6252_" + Guid.NewGuid().ToString("N")[..12];
        try
        {
            await SqlAsync("master", $"CREATE DATABASE [{database}];", ct);
            if (queryStoreOn)
            {
                await SqlAsync("master", $"ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);", ct);
            }

            await SqlAsync(database, $"SELECT * INTO {CopyTable} FROM {RealView};", ct);
            if (reason is not null)
            {
                await SqlAsync(database, $"UPDATE {CopyTable} SET readonly_reason = {reason.Value}, actual_state = 1, actual_state_desc = N'READ_ONLY';", ct);
            }

            await body(database, ct);
        }
        finally
        {
            try
            {
                await SqlAsync("master", $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;", CancellationToken.None);
            }
            catch (SqlException)
            {
                /* The assertions report a real failure; a cleanup that cannot reach the server leaves a uniquely named scratch database. */
            }
        }
    }

    private const int ReadableSecondary = 8;
    private const int StorageCapReached = 65536;

    [Fact]
    public async Task Health_ReadableSecondary_WritesNoRow()
    {
        await RunAsync(queryStoreOn: true, ReadableSecondary, async (database, ct) =>
        {
            Assert.Empty(await HealthAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task Health_BitEightAmongOtherBits_WritesNoRow()
    {
        await RunAsync(queryStoreOn: true, ReadableSecondary | StorageCapReached, async (database, ct) =>
        {
            Assert.Empty(await HealthAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task Health_StorageCapReadOnly_StillReports_TheFailureThisCollectorExistsFor()
    {
        await RunAsync(queryStoreOn: true, StorageCapReached, async (database, ct) =>
        {
            var row = Assert.Single(await HealthAsync(database, useCopy: true, ct));
            Assert.Equal(StorageCapReached, row.ReadonlyReason);
            Assert.Equal("READ_ONLY", row.ActualState);
        });
    }

    [Fact]
    public async Task Health_NormalDatabase_StillReports_OnTheRealView()
    {
        await RunAsync(queryStoreOn: true, reason: null, async (database, ct) =>
        {
            var row = Assert.Single(await HealthAsync(database, useCopy: false, ct));
            Assert.Equal(database, row.DbName);
            Assert.Equal(0, row.ReadonlyReason);
            Assert.Single(await HealthAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task PlanCorrection_ReadableSecondary_WritesNoRow()
    {
        await RunAsync(queryStoreOn: true, ReadableSecondary, async (database, ct) =>
        {
            Assert.Empty(await PlanCorrectionAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task PlanCorrection_BitEightAmongOtherBits_WritesNoRow()
    {
        await RunAsync(queryStoreOn: true, ReadableSecondary | StorageCapReached, async (database, ct) =>
        {
            Assert.Empty(await PlanCorrectionAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task PlanCorrection_NormalDatabase_StillReportsItsEnablementRow_OnTheRealView()
    {
        await RunAsync(queryStoreOn: true, reason: null, async (database, ct) =>
        {
            var row = Assert.Single(await PlanCorrectionAsync(database, useCopy: false, ct));
            Assert.Equal(database, row.DbName);
            Assert.NotNull(row.DesiredState);
            Assert.Single(await PlanCorrectionAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task PlanCorrection_QueryStoreOff_StillReportsItsEnablementRow()
    {
        /* The database this collector exists to show: FORCE_LAST_GOOD_PLAN asked for, Query Store off (readonly_reason 0). A gate copied from the
           forced-plans collector (state IN (1, 2, 4)) would drop it. */
        await RunAsync(queryStoreOn: false, reason: null, async (database, ct) =>
        {
            Assert.Single(await PlanCorrectionAsync(database, useCopy: false, ct));
            Assert.Single(await PlanCorrectionAsync(database, useCopy: true, ct));
        });
    }

    [Fact]
    public async Task PlanCorrection_StorageCapReadOnly_StillReports()
    {
        await RunAsync(queryStoreOn: true, StorageCapReached, async (database, ct) =>
        {
            Assert.Single(await PlanCorrectionAsync(database, useCopy: true, ct));
        });
    }
}
