using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// The result of one straggler-guard check: the last observed count of open writing
/// transactions that started before the window's collection cutoff, and the total time spent
/// waiting for them to close. `WaitSeconds` is 0 when the first check already found none.
/// </summary>
public sealed record StragglerGuardResult(long LastCount, double WaitSeconds);

/// <summary>
/// Before a window is read, checks whether another session (not this exporter) still has an open writing transaction
/// that started before the window's collection cutoff. If so, it waits and re-checks, up to a bounded number of tries,
/// then proceeds regardless: this guard only warns, it never fails or blocks a run past its cap. `WaitAsync` is
/// injectable so tests never sleep for real.
/// </summary>
public static class StragglerGuard
{
    /// <summary>
    /// `/* darling-export:straggler_guard */` tags the statement for easy identification in
    /// pg_stat_statements/logs. `backend_xid IS NOT NULL` limits the count to sessions that
    /// have actually written something (assigned a transaction ID), so Darling's own long
    /// MCP reads never count, and `application_name IS DISTINCT FROM @application_name`
    /// (the exporter's own `application_name`) excludes the exporter's own connections.
    /// </summary>
    public const string Sql = """
        /* darling-export:straggler_guard */ SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
          AND application_name IS DISTINCT FROM @application_name
          AND backend_type = 'client backend' AND state <> 'idle'
          AND backend_xid IS NOT NULL AND xact_start < @collection_to
        """;

    public const int MaxRetries = 5;
    public static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Runs the guard for one window. <paramref name="collectionTo"/> is the window's `--to`
    /// for the fresh family, or `--to + 2h` for Query Store (the same cutoff the exporter
    /// query itself trusts for a complete read). <paramref name="waitAsync"/> defaults to a
    /// real `Task.Delay`; tests pass a no-op so the up-to-5-minute wait never actually runs.
    /// </summary>
    public static Task<StragglerGuardResult> CheckAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        DateTime collectionTo,
        CancellationToken ct,
        Func<TimeSpan, CancellationToken, Task>? waitAsync = null) =>
        CheckAsync(ct => CountStragglersAsync(connectionStringBuilder, collectionTo, ct), ct, waitAsync);

    /// <summary>
    /// Same retry/wait logic as the connection-string overload, but with the count itself
    /// injected, so unit tests can exercise the up-to-5-tries loop without a database.
    /// </summary>
    public static async Task<StragglerGuardResult> CheckAsync(
        Func<CancellationToken, Task<long>> countAsync,
        CancellationToken ct,
        Func<TimeSpan, CancellationToken, Task>? waitAsync = null)
    {
        waitAsync ??= (delay, token) => Task.Delay(delay, token);

        long lastCount = 0;
        var totalWait = TimeSpan.Zero;

        for (var attempt = 0; attempt <= MaxRetries; attempt++)
        {
            lastCount = await countAsync(ct).ConfigureAwait(false);

            if (lastCount == 0)
            {
                break;
            }

            var isLastAttempt = attempt == MaxRetries;
            if (isLastAttempt)
            {
                break;
            }

            await waitAsync(RetryDelay, ct).ConfigureAwait(false);
            totalWait += RetryDelay;
        }

        return new StragglerGuardResult(lastCount, totalWait.TotalSeconds);
    }

    /// <summary>
    /// One count. Both the production overload and the exporter's test-only short-wait path call this, so the two
    /// cannot bind different parameters.
    /// </summary>
    internal static async Task<long> CountStragglersAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, DateTime collectionTo, CancellationToken ct)
    {
        await using var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(Sql, connection);
        command.Parameters.Add(new NpgsqlParameter("application_name", NpgsqlDbType.Text)
        {
            Value = connectionStringBuilder.ApplicationName ?? "",
        });
        command.Parameters.Add(new NpgsqlParameter("collection_to", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(collectionTo, DateTimeKind.Unspecified),
        });
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? 0 : Convert.ToInt64(result);
    }
}
