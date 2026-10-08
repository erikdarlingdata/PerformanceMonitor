// TEMP scaffold: stands in for lane A1's V171/V172 rungs until they land. Delete once they do.
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Darling.Tests;

internal static class TempRungScaffold
{
    public static async Task ApplyAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var name in new[] { "query_store_interval_wide", "query_store_interval_latest" })
        {
            var kind = await ScalarAsync(connection, $"SELECT relkind::text FROM pg_class WHERE oid = 'collect.{name}'::regclass", ct);
            if ((string?)kind == "p")
            {
                continue;
            }

            var indexes = new List<(string Name, string Def)>();
            await using (var cmd = new NpgsqlCommand($"SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'collect' AND tablename = '{name}'", connection))
            await using (var r = await cmd.ExecuteReaderAsync(ct))
            {
                while (await r.ReadAsync(ct))
                {
                    indexes.Add((r.GetString(0), r.GetString(1)));
                }
            }

            string? trigger = null;
            await using (var cmd = new NpgsqlCommand($"SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = 'collect.{name}'::regclass AND NOT tgisinternal", connection))
            {
                trigger = (string?)await cmd.ExecuteScalarAsync(ct);
            }

            await ExecAsync(connection, $"ALTER TABLE collect.{name} RENAME TO {name}_legacy", ct);
            foreach (var (idx, _) in indexes)
            {
                await ExecAsync(connection, $"ALTER INDEX collect.{idx} RENAME TO {idx}_legacy", ct);
            }

            await ExecAsync(connection, $"CREATE TABLE collect.{name} (LIKE collect.{name}_legacy INCLUDING DEFAULTS INCLUDING STORAGE) PARTITION BY RANGE (first_execution_time)", ct);
            await ExecAsync(connection, $"ALTER TABLE collect.{name} ATTACH PARTITION collect.{name}_legacy FOR VALUES FROM (MINVALUE) TO (MAXVALUE)", ct);
            foreach (var (idx, def) in indexes)
            {
                await ExecAsync(connection, def.Replace($" ON collect.{name} ", $" ON ONLY collect.{name} "), ct);
                await ExecAsync(connection, $"ALTER INDEX collect.{idx} ATTACH PARTITION collect.{idx}_legacy", ct);
            }

            if (trigger is not null)
            {
                await ExecAsync(connection, $"DROP TRIGGER trg_plan_regression_daily_late ON collect.{name}_legacy", ct);
                await ExecAsync(connection, trigger, ct);
            }
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        return await cmd.ExecuteScalarAsync(ct);
    }
}
