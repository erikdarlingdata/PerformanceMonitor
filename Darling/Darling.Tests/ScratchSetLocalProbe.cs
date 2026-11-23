using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

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

    [Fact] public async Task A_Bare_SetLocal_Warns() => Assert.Equal(0, await Warnings("SET LOCAL lock_timeout = '9s'", false));
    [Fact] public async Task B_SetLocal_Then_Select_Warns() => Assert.Equal(0, await Warnings("SET LOCAL lock_timeout = '9s'; SELECT 1", false));
    [Fact] public async Task C_SetLocal_Then_Do_Warns() => Assert.Equal(0, await Warnings("SET LOCAL lock_timeout = '9s'; DO $$ BEGIN PERFORM 1; END $$;", false));
    [Fact] public async Task D_Select_Then_SetLocal_Warns() => Assert.Equal(0, await Warnings("SELECT 1; SET LOCAL lock_timeout = '9s'", false));
    [Fact] public async Task E_Notice_Delivery_Works() => Assert.Equal(1, await Warnings("DO $$ BEGIN RAISE WARNING 'x'; END $$;", false));
}
