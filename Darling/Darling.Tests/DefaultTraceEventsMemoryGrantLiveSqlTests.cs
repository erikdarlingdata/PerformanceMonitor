/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6304: the default trace collector's REAL batch (<see cref="DefaultTraceEventsCollector.BuildQuery"/>) run against a
/// live SQL Server, asking the plan what memory grant the batch requests. <c>sys.fn_trace_gettable</c> carries no
/// row-count statistics, so the optimizer assumed 80,000 rows of about 2 KB and the query's <c>ORDER BY ft.StartTime DESC</c>
/// became a Sort requesting 57-75 MB of workspace memory to return a few dozen rows. A server whose query workspace was
/// smaller than that (a 1 GB VM: 85 MB) queued the batch on RESOURCE_SEMAPHORE at 15 ms of CPU until the host's 60 s
/// command timeout, and the cycle failed with "Execution Timeout Expired" and no rows. Nothing in the repo ran the batch,
/// so no test could see the grant. The request is a property of the plan's estimates, not of how much memory the test
/// server has, so it reads the same on a large server, and the test needs no memory pressure.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth, as the other live SQL tests read them), so it skips where the runner has
/// no SQL Server. It only reads: the batch is the collector's own SELECT, and <c>SET STATISTICS XML ON</c> is a session
/// setting on the test's own connection.</para>
/// </summary>
public sealed class DefaultTraceEventsMemoryGrantLiveSqlTests
{
    /// <summary>
    /// The most the batch may request, in KB. The Sort it replaced requested 57,000-75,000 KB; the hash join that is
    /// left requests about 1,500 KB. 8 MB sits far from both, so a plan that grows a Sort (or any other grant hog) back
    /// fails and ordinary plan variation between versions does not.
    /// </summary>
    private const long MaxRequestedMemoryKb = 8192;

    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-6304-default-trace-grant-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static CollectorContext MakeContext(IReadOnlyDictionary<string, string> state)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo(),
            State = state,
            HasCollectedBefore = true,
        };

    /// <summary>
    /// Runs the collector's real batch with the actual plan switched on and returns every statement's requested grant, in KB.
    /// The plan comes back as an extra one-column result set after the statement it describes.
    /// </summary>
    private static async Task<List<long>> RequestedGrantsKbAsync(
        string connectionString, IReadOnlyDictionary<string, string> state, CancellationToken ct)
    {
        var query = DefaultTraceEventsCollector.Instance.BuildQuery(MakeContext(state));

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using (var setOn = new SqlCommand("SET STATISTICS XML ON;", connection))
        {
            await setOn.ExecuteNonQueryAsync(ct);
        }

        /* The shipped command builder, so the text and the parameter bindings are the host's own, not a copy of them. */
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);

        var grants = new List<long>();
        do
        {
            bool isPlan = reader.FieldCount == 1 && reader.GetName(0).Contains("Showplan", StringComparison.OrdinalIgnoreCase);
            while (await reader.ReadAsync(ct))
            {
                if (!isPlan)
                {
                    continue;
                }

                var plan = XDocument.Parse(Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)!);
                foreach (var info in plan.Descendants().Where(e => e.Name.LocalName == "MemoryGrantInfo"))
                {
                    if (long.TryParse(info.Attribute("RequestedMemory")?.Value, NumberStyles.None, CultureInfo.InvariantCulture, out var kb))
                    {
                        grants.Add(kb);
                    }
                }
            }
        }
        while (await reader.NextResultAsync(ct));

        return grants;
    }

    [Fact]
    public async Task CollectorBatch_RequestsASmallMemoryGrant_WholeRolloverSet()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live default trace grant test.");

        /* No stored state: the fallback that reads the whole rollover set, the widest read the collector makes. */
        var grants = await RequestedGrantsKbAsync(
            connectionString!, new Dictionary<string, string>(StringComparer.Ordinal), TestContext.Current.CancellationToken);

        /* A statement that needs no grant carries no MemoryGrantInfo, so an empty list is a small grant; a plan that cannot
           be read at all would have thrown above. */
        Assert.All(grants, kb => Assert.True(
            kb <= MaxRequestedMemoryKb,
            $"the default trace batch requested {kb} KB of workspace memory; a server with less than that queues the cycle on RESOURCE_SEMAPHORE until its command timeout"));
    }
}
