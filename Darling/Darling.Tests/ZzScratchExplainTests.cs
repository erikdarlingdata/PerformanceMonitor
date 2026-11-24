using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

public sealed class ZzScratchExplainTests
{
    /// <summary>One-off measurement for #9485, committed only while it runs: the open-time window lookup against a few hundred windows. It fails on purpose so the
    /// plan lands in the run output.</summary>
    [Fact]
    public async Task ZzScratchExplain_OfTheOpenTimeWindowLookup()
    {
        await RunAsync(async (connection, ct) =>
        {
            await ExecAsync(connection, @"
INSERT INTO config.investigation_change_windows (server_ids, starts_at, ends_at, reason, declared_by)
SELECT CASE WHEN g % 5 = 0 THEN NULL ELSE ARRAY[g % 40, (g + 7) % 40] END,
       timestamptz '2026-01-01 00:00:00+00' + make_interval(hours => g * 20),
       timestamptz '2026-01-01 00:00:00+00' + make_interval(hours => g * 20 + 2),
       'scratch ' || g, 'scratch'
FROM generate_series(1, 400) AS g", ct);
            await ExecAsync(connection, "ANALYZE config.investigation_change_windows", ct);
            var sql = "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT " + InvestigationChangeWindows.HeldWindowIdSql("timestamptz '2026-03-10 07:30:00+00'", "7");
            var plan = new List<string>();
            foreach (var setting in new[] { "SET enable_seqscan = on", "SET enable_seqscan = off" })
            {
                await ExecAsync(connection, setting, ct);
                plan.Add(setting);
                await using var command = new NpgsqlCommand(sql, connection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    plan.Add(reader.GetString(0));
            }

            Assert.Fail("PLAN" + Environment.NewLine + string.Join(Environment.NewLine, plan));
        });
    }

    private static async Task RunAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await body(connection, ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
