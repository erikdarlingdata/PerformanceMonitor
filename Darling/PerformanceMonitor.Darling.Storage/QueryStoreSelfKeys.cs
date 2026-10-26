/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The keys of the monitor's own Query Store statements (#6750 part 3): one row per <c>(server_id, database_name, query_id)</c>
/// whose text in <c>collect.query_store_text</c> carries <see cref="QueryStoreSelfStatement.Marker"/>.
///
/// <para><b>Why it exists.</b> The regressions read hides the monitor's own statement before its rank and its cap
/// (<see cref="QueryStoreRegressionBaseline"/>'s <c>qualified</c> CTE), because <c>regression_count</c> and the cap's shares count the
/// rows after that filter. Testing the marker against the stored text there is one primary-key probe of the TOAST-heavy text
/// table per row that passes the flags: measured at about 2,400 rows, 1.95 s and about 2,900 blocks cold on a large store at
/// 168 hours. The text writer already has the text in hand when it lands it, so it records the marker test once, here, and
/// the read probes the key table's primary key instead, which reads no text.</para>
///
/// <para><b>Who writes it.</b> <see cref="QueryStoreTextStore.UpsertSql"/> inserts the key when a landed text carries the marker
/// and deletes it when an overwrite lands text without it (a Query Store reset renumbers ids), in the same statement as the text
/// upsert. <see cref="BackfillSql"/> records the keys of text stored before the table existed. Retention deletes keys whose text
/// row is gone (<see cref="PruneSql"/>).</para>
///
/// <para><b>The gate.</b> A read before the backfill finishes would list the monitor's statement on a store whose text predates
/// this table (#6654 again). So <see cref="StateTableName"/> holds one row once the backfill has run, and the read uses the key
/// table only when that row exists (<see cref="ReadyAsync"/>, read inside the read's own snapshot). Until then it keeps the
/// text probe. The writer keeps the key table current from the moment the rung runs, so the backfill only has to cover the text
/// stored before it.</para>
///
/// <para>A plain table, never a hypertable. It holds one row per stored text that carries the marker. On a large production store
/// that is about 2% of the text rows: an estimated 150,000 keys of 6.8 million rows, from a 1% sample, so a few MB against the text
/// table's 11 GB.</para>
/// </summary>
public static class QueryStoreSelfKeys
{
    public const string TableName = "collect.query_store_self_keys";

    /// <summary>The one-row record that the backfill has run (<c>backfilled_at</c>, naive UTC).</summary>
    public const string StateTableName = "collect.query_store_self_keys_state";

    /// <summary>The rung's SQL (V219). Both tables are new and empty, so the rung moves no data.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_self_keys (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    query_id bigint NOT NULL,
    PRIMARY KEY (server_id, database_name, query_id)
);

CREATE TABLE IF NOT EXISTS collect.query_store_self_keys_state (
    id integer NOT NULL PRIMARY KEY CHECK (id = 1),
    backfilled_at timestamp NOT NULL
);";

    /// <summary>The marker test over a text expression, true for the monitor's own statement. An alias of
    /// <see cref="QueryStoreSelfStatement.SelfOfText"/>, so the key table and the text probe are one definition.</summary>
    internal static string IsSelf(string textExpression) => QueryStoreSelfStatement.SelfOfText(textExpression);

    /// <summary>
    /// Records the keys of every stored text that carries the marker. One sequential scan of the text table (it reads the
    /// TOAST), so it runs in the background after start, never in the startup transaction. Idempotent. <see cref="StampSql"/>
    /// follows it in the same transaction, so a read never sees the gate open over a half-filled table.
    /// </summary>
    public static readonly string BackfillSql = @"
INSERT INTO collect.query_store_self_keys (server_id, database_name, query_id)
SELECT t.server_id, t.database_name, t.query_id
FROM collect.query_store_text AS t
WHERE " + IsSelf("t.query_sql_text") + @"
ORDER BY t.server_id, t.database_name, t.query_id
ON CONFLICT (server_id, database_name, query_id) DO NOTHING";

    /// <summary>The backfill's stamp, the gate's state row. Written after <see cref="BackfillSql"/> in the same transaction.</summary>
    public const string StampSql = @"
INSERT INTO collect.query_store_self_keys_state (id, backfilled_at)
VALUES (1, now() AT TIME ZONE 'UTC')
ON CONFLICT (id) DO NOTHING";

    /// <summary>Whether the backfill has run: the read's gate.</summary>
    public const string ReadySql = "SELECT EXISTS (SELECT 1 FROM collect.query_store_self_keys_state)";

    /// <summary>Deletes the keys whose text row is gone (retention pruned it). It runs with the text prune, so at most once a day
    /// (the text prune's gate), as one anti-join of the key table against the text table's primary key. At about 150,000 keys on a
    /// large store that reads at most that primary key (about 0.8 GB), less than the text prune's own scan of the heap. <c>$1</c> is the retention
    /// pass's cutoff, which every purge statement is bound with. The test does not need it, and it is named only so the bind
    /// matches the statement.</summary>
    public const string PruneSql = @"
DELETE FROM collect.query_store_self_keys AS k
WHERE $1::timestamp IS NOT NULL
AND   NOT EXISTS (
    SELECT 1 FROM collect.query_store_text AS t
    WHERE t.server_id = k.server_id AND t.database_name = k.database_name AND t.query_id = k.query_id)";

    /// <summary>The backfill's own deadline: a sequential scan of a text table that can hold several GB of TOAST.</summary>
    public const int BackfillTimeoutSeconds = 3600;

    /// <summary>The gate, read on the read's own connection and transaction so it and the read are one snapshot.</summary>
    public static async Task<bool> ReadyAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ReadySql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    /// <summary>Runs <see cref="BackfillSql"/> once, unless the state row already exists. Returns the keys it inserted, or
    /// null when the backfill had already run.</summary>
    public static async Task<int?> BackfillAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        if (await ReadyAsync(connection, null, 30, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        int inserted;
        await using (var command = new NpgsqlCommand(BackfillSql, connection, transaction) { CommandTimeout = BackfillTimeoutSeconds })
        {
            inserted = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var stamp = new NpgsqlCommand(StampSql, connection, transaction) { CommandTimeout = 30 })
        {
            await stamp.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return inserted;
    }

    /// <summary>
    /// The background step: waits <paramref name="delay"/> (off the post-restart IO burst, as
    /// <see cref="QueryStoreBackgroundIndexes"/> does), then runs <see cref="BackfillAsync"/> once. Never throws: a failure is a
    /// Warning, the gate stays closed and the read keeps the text probe, and the next start tries again. Cancellation ends it quietly.
    /// </summary>
    public static async Task RunDelayedAsync(NpgsqlDataSource postgres, ILogger logger, TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            var inserted = await BackfillAsync(connection, cancellationToken).ConfigureAwait(false);
            if (inserted is int n)
            {
                logger.LogInformation("Query Store self-statement keys backfilled ({Keys} key(s)); the regressions read now uses them.", n);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug("Query Store self-statement key backfill was cancelled at shutdown; the next start retries.");
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Query Store self-statement key backfill failed: {Message}. The regressions read keeps the text probe; the next start retries.", ex.Message);
        }
        catch (Exception ex)
        {
            logger.LogInformation("Query Store self-statement key backfill stopped at shutdown: {ExceptionType}: {Message}; the next start retries.", ex.GetType().Name, ex.Message);
        }
    }
}
