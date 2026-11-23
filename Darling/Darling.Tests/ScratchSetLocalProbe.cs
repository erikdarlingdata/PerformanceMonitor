using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

public sealed class ScratchSetLocalProbe
{
    [Fact]
    public async Task Probe()
    {
        var ct = TestContext.Current.CancellationToken;
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
        var report = new List<string>();
        await using var c = new NpgsqlConnection(cs);
        c.Notice += (_, e) => report.Add("  NOTICE: " + e.Notice.MessageText);
        await c.OpenAsync(ct);
        await using (var v = new NpgsqlCommand("select version()", c)) report.Add((string)(await v.ExecuteScalarAsync(ct))!);
        foreach (var sql in new[] {
            "SET LOCAL lock_timeout = '9s'",
            "SET LOCAL lock_timeout = '9s'; SELECT 1",
            "SET LOCAL lock_timeout = '9s'; DO $$ BEGIN PERFORM 1; END $$;",
            "SET LOCAL lock_timeout = '9s';\nDO $$ BEGIN RAISE NOTICE 'lt=%', current_setting('lock_timeout'); END $$;",
        })
        {
            report.Add("SQL: " + sql.Replace("\n", " "));
            await using var cmd = new NpgsqlCommand(sql, c);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        await using (var cmd = new NpgsqlCommand("SELECT current_setting('lock_timeout')", c)) report.Add("after: " + await cmd.ExecuteScalarAsync(ct));
        System.IO.File.WriteAllText(System.IO.Path.Combine(System.IO.Path.GetTempPath(), "probe9343.txt"), string.Join(Environment.NewLine, report));
    }
}
