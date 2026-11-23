using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

public sealed class ScratchSetLocalProbe
{
    private static async Task<int> Warnings(string sql, bool alsoBare)
    {
        var ct = TestContext.Current.CancellationToken;
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
        var n = 0;
        await using var c = new NpgsqlConnection(cs);
        c.Notice += (_, e) => n++;
        await c.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
        return n;
    }

    private static async Task<int> Real(string sql)
    {
        var ct = TestContext.Current.CancellationToken;
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
        await using var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        var n = 0;
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        c.Notice += (_, e) => n++;
        await c.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
        return n;
    }

    [Fact] public async Task F_Do_Then_SetLocal_Warns() => Assert.Equal(0, await Warnings("DO $$ BEGIN NULL; END $$; SET LOCAL lock_timeout = '9s'", false));
    [Fact] public async Task G1_CorrectedHourly_Warns() => Assert.Equal(0, await Real(TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedHourlyView]));
    [Fact] public async Task G2_CorrectedDaily_Warns() => Assert.Equal(0, await Real(TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedDailyView]));
    [Fact] public async Task G3_StatsHourlyCoveringIndexes_Warns() => Assert.Equal(0, await Real(TimescaleSupport.CreateQueryStatsHourlyHashCoveringIndexesSql));
    [Fact] public async Task G4_HashIndexV246_Warns() => Assert.Equal(0, await Real(TimescaleSupport.CreateQueryStoreCorrectedHourlyHashIndexSql));
    [Fact] public async Task A_Bare_SetLocal_Warns() => Assert.Equal(0, await Warnings("SET LOCAL lock_timeout = '9s'", false));
    [Fact] public async Task B_SetLocal_Then_Select_Warns() => Assert.Equal(0, await Warnings("SET LOCAL lock_timeout = '9s'; SELECT 1", false));
    [Fact] public async Task C_SetLocal_Then_Do_Warns() => Assert.Equal(0, await Warnings("SET LOCAL lock_timeout = '9s'; DO $$ BEGIN PERFORM 1; END $$;", false));
    [Fact] public async Task D_Select_Then_SetLocal_Warns() => Assert.Equal(0, await Warnings("SELECT 1; SET LOCAL lock_timeout = '9s'", false));
    [Fact] public async Task E_Notice_Delivery_Works() => Assert.Equal(1, await Warnings("DO $$ BEGIN RAISE WARNING 'x'; END $$;", false));
}
