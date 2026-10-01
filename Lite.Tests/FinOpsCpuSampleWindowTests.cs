/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using Lite.Tests;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every FinOps scenario's CPU samples must sit inside the window the FinOps utilization read uses
/// (<c>GetUtilizationEfficiencyAsync</c>: the last 24 hours from now), at any time of day the suite runs.
///
/// <para>Seeded from <see cref="TestDataSeeder.TestPeriodStart"/>, which is anchored to 04:00 UTC (#4385), the
/// samples sat 24 to 28 hours back between 03:45 and 04:00 UTC. The read found none, so HasCpuSample was false
/// and the CPU and VM right-sizing rules gave no advice: six FinOpsTests failed every day in that quarter hour.
/// The seeder takes a clock, so this class places the samples as a run at 03:50 would, without waiting for the
/// real clock to get there. The source pin below keeps the window this class checks equal to the read's.</para>
/// </summary>
public sealed class FinOpsCpuSampleWindowTests : IClassFixture<SharedDuckDbFixture>
{
    /// <summary>The hours <c>GetUtilizationEfficiencyAsync</c> reads back from now. Pinned to its source below.</summary>
    private const int UtilizationWindowHours = 24;

    private static readonly Dictionary<string, Func<TestDataSeeder, Task>> Scenarios = new()
    {
        ["OverProvisionedEnterprise"] = s => s.SeedOverProvisionedEnterpriseAsync(),
        ["RightSizing"] = s => s.SeedRightSizingScenarioAsync(engineEdition: 3, withCpuSamples: true),
        ["VmRightSizingTarget"] = s => s.SeedVmRightSizingTargetAsync(),
        ["AzureSqlDbVcore"] = s => s.SeedAzureSqlDbVcoreAsync(),
        ["CleanFinOpsServer"] = s => s.SeedCleanFinOpsServerAsync(),
        ["StableCpuForReservedCapacity"] = s => s.SeedStableCpuForReservedCapacityAsync(),
        ["BurstyCpu"] = s => s.SeedBurstyCpuAsync()
    };

    private readonly DuckDbInitializer _duckDb;

    public FinOpsCpuSampleWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>
    /// Each scenario at 03:50 UTC (inside the quarter hour before the 04:00 anchor rolls forward a day, when the
    /// anchored samples were all more than 24 hours old) and at 12:00 UTC (an ordinary hour).
    /// </summary>
    public static TheoryData<string, int, int> ScenariosAtTimesOfDay()
    {
        var data = new TheoryData<string, int, int>();
        foreach (var scenario in Scenarios.Keys)
        {
            data.Add(scenario, 3, 50);
            data.Add(scenario, 12, 0);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(ScenariosAtTimesOfDay))]
    public async Task EveryCpuSampleIsInsideTheUtilizationWindow(string scenario, int hour, int minute)
    {
        var nowUtc = DateTime.UtcNow.Date.AddHours(hour).AddMinutes(minute);

        using (var seeder = new TestDataSeeder(_duckDb, () => nowUtc))
        {
            await Scenarios[scenario](seeder);
        }

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    COUNT(*) AS seeded,
    COUNT(*) FILTER (WHERE collection_time >= $2 AND collection_time <= $3) AS in_window
FROM v_cpu_utilization_stats
WHERE server_id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerId });
        command.Parameters.Add(new DuckDBParameter { Value = nowUtc.AddHours(-UtilizationWindowHours) });
        command.Parameters.Add(new DuckDBParameter { Value = nowUtc });

        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var seeded = Convert.ToInt64(reader.GetValue(0));
        var inWindow = Convert.ToInt64(reader.GetValue(1));

        Assert.True(seeded > 0, $"{scenario} seeded no CPU samples.");
        if (inWindow != seeded)
        {
            Assert.Fail(
                $"{scenario} at {nowUtc:HH:mm} UTC: {inWindow} of its {seeded} CPU samples are inside the " +
                $"{UtilizationWindowHours}-hour utilization read " +
                $"({nowUtc.AddHours(-UtilizationWindowHours):yyyy-MM-dd HH:mm} to {nowUtc:yyyy-MM-dd HH:mm} UTC).");
        }
    }

    /// <summary>
    /// The read's CPU window is the last <see cref="UtilizationWindowHours"/> hours from now: the cutoff, the
    /// parameter that carries it, and the cpu_stats filter that uses it.
    /// </summary>
    [Fact]
    public void UtilizationReadCpuWindowIsTheLast24Hours()
    {
        const string Signature = "Task<UtilizationEfficiencyRow?> GetUtilizationEfficiencyAsync(";

        var source = ParitySource.ReadFile("Lite/Services/LocalDataService.FinOps.Utilization.cs");
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);

        var at = stripped.IndexOf(Signature, StringComparison.Ordinal);
        Assert.True(at >= 0, "GetUtilizationEfficiencyAsync was not found.");
        Assert.Equal(-1, stripped.IndexOf(Signature, at + 1, StringComparison.Ordinal));

        var open = stripped.IndexOf('{', at);
        var body = CSharpSourceWalker.BraceBalanced(stripped, open);
        var code = Regex.Replace(body, @"\s+", " ");

        Assert.Contains($"var cutoff = DateTime.UtcNow.AddHours(-{UtilizationWindowHours});", code);
        Assert.Contains(
            "command.Parameters.Add(new DuckDBParameter { Value = serverId }); " +
            "command.Parameters.Add(new DuckDBParameter { Value = cutoff });",
            code);

        /* The SQL is a string literal, blanked in the stripped copy: read it from the same span of the source. */
        var rawBody = source.Substring(open, body.Length);
        var cte = rawBody.IndexOf("cpu_stats AS (", StringComparison.Ordinal);
        Assert.True(cte >= 0, "The cpu_stats CTE was not found.");

        var depth = 0;
        var end = rawBody.IndexOf('(', cte);
        for (; end < rawBody.Length; end++)
        {
            if (rawBody[end] == '(') depth++;
            else if (rawBody[end] == ')' && --depth == 0) break;
        }

        var cpuStats = Regex.Replace(rawBody[cte..end], @"\s+", " ");
        Assert.Contains("FROM v_cpu_utilization_stats", cpuStats);
        Assert.Contains("collection_time >= $2", cpuStats);
    }
}
