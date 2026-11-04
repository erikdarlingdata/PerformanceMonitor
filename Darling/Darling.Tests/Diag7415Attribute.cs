using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading;
using Npgsql;
using Xunit;
using Xunit.v3;

[assembly: Diag7415]

/// <summary>
/// DIAGNOSTIC ONLY (#7415), never merged. Fails the first test after which pg_stat_statements exists in the
/// shared test store, and reports the other running tests plus the CREATE EXTENSION log the fixture's event
/// trigger kept.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
public sealed class Diag7415Attribute : BeforeAfterTestAttribute
{
    private static readonly ConcurrentDictionary<string, byte> s_running = new();
    private static int s_fired;

    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        s_running[test.TestDisplayName] = 0;
    }

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        s_running.TryRemove(test.TestDisplayName, out _);
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(cs) || Volatile.Read(ref s_fired) != 0)
        {
            return;
        }

        string log;
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(cs) { Pooling = false };
            using var connection = new NpgsqlConnection(builder.ConnectionString);
            connection.Open();
            using (var check = new NpgsqlCommand("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'", connection))
            {
                if (check.ExecuteScalar() is null)
                {
                    return;
                }
            }

            var sb = new System.Text.StringBuilder();
            try
            {
                using var rows = new NpgsqlCommand(
                    "SELECT id, logged_at, pid, application_name, tag, query FROM public.diag7415_log ORDER BY id", connection);
                using var reader = rows.ExecuteReader();
                while (reader.Read())
                {
                    sb.Append("[#").Append(reader.GetInt32(0)).Append(' ').Append(reader.GetDateTime(1).ToString("O"))
                        .Append(" pid=").Append(reader.GetInt32(2))
                        .Append(" app=").Append(reader.IsDBNull(3) ? "" : reader.GetString(3))
                        .Append(" tag=").Append(reader.GetString(4))
                        .Append(" query=").Append(reader.IsDBNull(5) ? "" : reader.GetString(5).Replace("\n", " ")).Append("] ");
                }
            }
            catch (Exception ex)
            {
                sb.Append("(log read failed: ").Append(ex.Message).Append(')');
            }

            log = sb.ToString();
        }
        catch
        {
            return;
        }

        if (Interlocked.Exchange(ref s_fired, 1) != 0)
        {
            return;
        }

        var others = string.Join(" || ", s_running.Keys.Where(k => k != test.TestDisplayName).OrderBy(k => k, StringComparer.Ordinal));
        throw new InvalidOperationException(
            $"DIAG7415 pg_stat_statements exists after test: {test.TestDisplayName} | utc={DateTime.UtcNow:O} | running now: {others} | create-extension log: {log}");
    }
}
