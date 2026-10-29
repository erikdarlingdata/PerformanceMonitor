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
/// <para><b>Who writes it.</b> Two row triggers on <c>collect.query_store_text</c> (<see cref="TriggerSql"/>, created by the rung), so
/// every writer of text keeps the keys current, whatever build it is: this build's <see cref="QueryStoreTextStore.UpsertSql"/>, an
/// older build run against the same store after a rollback, or an older instance that shares the store (the cases
/// <c>docs/darling/admin.md</c> names under "Rolling back to an older build"). The insert trigger records the key when a new text
/// carries the marker. The update trigger fires only when an update names <c>query_sql_text</c> and the text changed, and it inserts or
/// deletes the key to match the new text (a Query Store reset renumbers ids). The liveness touch sets <c>last_seen</c> only, so it never
/// fires one. <see cref="BackfillSql"/> records the keys of text stored before the rung. Retention deletes keys whose text row is gone
/// (<see cref="PruneSql"/>): the triggers have no DELETE arm, so the text prune stays one statement.</para>
///
/// <para><b>The gate.</b> A read before the backfill finishes would list the monitor's statement on a store whose text predates
/// this table (#6654 again). So <see cref="StateTableName"/> holds one row once the backfill has run, and the read uses the key
/// table only when that row exists (<see cref="ReadyAsync"/>, read inside the read's own snapshot). Until then it keeps the
/// text probe. The triggers keep the key table current from the moment the rung runs, for every writer, so the backfill only has to
/// cover the text stored before it, and the gate can stay open once stamped. The gate read is fail-safe: on a store without the state
/// table it answers closed (<see cref="ReadyAsync"/>).</para>
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

    /// <summary>The tables of the rung (V219). Both are new and empty, so they move no data.</summary>
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

    /// <summary>The trigger function's name, schema-qualified.</summary>
    public const string TrackFunctionName = "collect.query_store_self_keys_track";

    /// <summary>The insert trigger on <c>collect.query_store_text</c>.</summary>
    public const string InsertTriggerName = "trg_query_store_self_keys_ins";

    /// <summary>The update trigger on <c>collect.query_store_text</c>.</summary>
    public const string UpdateTriggerName = "trg_query_store_self_keys_upd";

    /// <summary>
    /// The triggers of the rung (V219) that keep the key table current for every writer of text (see the class summary). The marker
    /// test is <see cref="IsSelf"/>, composed here, so the keys and the text probe they replace are one definition.
    ///
    /// <para><b>Shapes.</b> <c>AFTER INSERT ... WHEN</c> the new text carries the marker, so an ordinary insert never calls the
    /// function. <c>AFTER UPDATE OF query_sql_text ... WHEN</c> the text changed: <c>UPDATE OF</c> keeps it off the liveness touch, which
    /// sets <c>last_seen</c> only and is the text table's busiest write, and the <c>WHEN</c> keeps it off an upsert that landed the same
    /// text again. An <c>ON CONFLICT DO UPDATE</c> fires the update trigger for the rows it updated and the insert trigger only for the
    /// rows it inserted, so each landed row is tested once. A row the upsert's monotonic <c>last_seen</c> guard skipped is not updated,
    /// so it keeps its text and its key.</para>
    ///
    /// <para><b>Idempotent.</b> <c>CREATE OR REPLACE FUNCTION</c>, then <c>DROP TRIGGER IF EXISTS</c> and <c>CREATE TRIGGER</c>, all
    /// schema-qualified. <c>CREATE TRIGGER</c> is a catalog change that reads no row, but it takes a SHARE ROW EXCLUSIVE lock on the
    /// text table, so the rung sets the lock timeout V173 uses for its trigger on an older table.</para>
    /// </summary>
    public static readonly string TriggerSql = @"
CREATE OR REPLACE FUNCTION " + TrackFunctionName + @"() RETURNS trigger
LANGUAGE plpgsql AS $f$
BEGIN
    IF " + IsSelf("NEW.query_sql_text") + @" THEN
        INSERT INTO collect.query_store_self_keys (server_id, database_name, query_id)
        VALUES (NEW.server_id, NEW.database_name, NEW.query_id)
        ON CONFLICT (server_id, database_name, query_id) DO NOTHING;
    ELSE
        DELETE FROM collect.query_store_self_keys AS k
        WHERE k.server_id = NEW.server_id AND k.database_name = NEW.database_name AND k.query_id = NEW.query_id;
    END IF;
    RETURN NULL;
END
$f$;

DROP TRIGGER IF EXISTS " + InsertTriggerName + @" ON collect.query_store_text;
CREATE TRIGGER " + InsertTriggerName + @"
    AFTER INSERT ON collect.query_store_text
    FOR EACH ROW
    WHEN (" + IsSelf("NEW.query_sql_text") + @")
    EXECUTE FUNCTION " + TrackFunctionName + @"();

DROP TRIGGER IF EXISTS " + UpdateTriggerName + @" ON collect.query_store_text;
CREATE TRIGGER " + UpdateTriggerName + @"
    AFTER UPDATE OF query_sql_text ON collect.query_store_text
    FOR EACH ROW
    WHEN (OLD.query_sql_text IS DISTINCT FROM NEW.query_sql_text)
    EXECUTE FUNCTION " + TrackFunctionName + @"();";

    /// <summary>The marker test over a text expression, true for the monitor's own statement. An alias of
    /// <see cref="QueryStoreSelfStatement.SelfOfText"/>, so the key table and the text probe are one definition.</summary>
    internal static string IsSelf(string textExpression) => QueryStoreSelfStatement.SelfOfText(textExpression);

    /// <summary>
    /// Records the keys of every stored text that carries the marker. One sequential scan of the text table (it reads the
    /// TOAST), so it runs in the background after start, never in the startup transaction. Idempotent. <see cref="StampSql"/>
    /// follows it in the same transaction, so a read never sees the gate open over a half-filled table.
    ///
    /// <para><b>The filter runs before the sort (#6750 part 3).</b> With the <c>ORDER BY</c> on the same <c>SELECT</c> as the marker
    /// test, the planner walked <c>query_store_text_pkey</c> in key order over every row and tested the text row by row, so the heap and
    /// TOAST reads were random. Measured cold on a copy of a large production store (6.6 million text rows, an 11 GB table, 16,000
    /// IOPS): 452.9 s, 2,179,401 blocks read (16.6 GB), 420.8 s of it I/O wait, for 156,086 keys. The marker test is now the
    /// <c>MATERIALIZED</c> CTE <c>m</c>, a Seq Scan of the text table that keeps only the matches, and the <c>ORDER BY</c> sorts those
    /// (a 12 MB quicksort of the 156,086 rows). Same keys (0 differ), 157.8 s cold, 1,431,540 blocks read, 142.2 s of I/O wait. The
    /// sort stays so the insert goes into the key table's primary key in key order. A slower disk costs the random form far more
    /// than the sequential one (inferred, not measured). A parallel scan is not available, because <c>INSERT ... SELECT</c> gets no
    /// parallel plan.</para>
    /// </summary>
    public static readonly string BackfillSql = @"
WITH m AS MATERIALIZED (
    SELECT t.server_id, t.database_name, t.query_id
    FROM collect.query_store_text AS t
    WHERE " + IsSelf("t.query_sql_text") + @"
)
INSERT INTO collect.query_store_self_keys (server_id, database_name, query_id)
SELECT m.server_id, m.database_name, m.query_id
FROM m
ORDER BY m.server_id, m.database_name, m.query_id
ON CONFLICT (server_id, database_name, query_id) DO NOTHING";

    /// <summary>The backfill's stamp, the gate's state row. Written after <see cref="BackfillSql"/> in the same transaction.</summary>
    public const string StampSql = @"
INSERT INTO collect.query_store_self_keys_state (id, backfilled_at)
VALUES (1, now() AT TIME ZONE 'UTC')
ON CONFLICT (id) DO NOTHING";

    /// <summary>Whether the state table exists: the gate's first statement (<see cref="ReadyAsync"/>).</summary>
    public const string StateTableExistsSql = "SELECT to_regclass('" + StateTableName + "') IS NOT NULL";

    /// <summary>Whether the backfill has run: the read's gate, read only once <see cref="StateTableExistsSql"/> said the table is there.</summary>
    public const string ReadySql = "SELECT EXISTS (SELECT 1 FROM " + StateTableName + ")";

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

    /// <summary>
    /// The gate, read on the read's own connection and transaction so it and the read are one snapshot.
    ///
    /// <para><b>Fail-safe.</b> It first asks whether the state table exists, as its own statement, and answers closed when it does not,
    /// with no error and the transaction still usable, so the read keeps the text probe. The trigger: the MCP host answers while the
    /// worker's migration ladder is still running (<c>DarlingMcpHostService</c> does not wait for it), so during an upgrade that
    /// applies V219 the read meets a store without the table. The Viewer's connect gate fails open, so it can meet one too.</para>
    /// </summary>
    public static async Task<bool> ReadyAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using (var exists = new NpgsqlCommand(StateTableExistsSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            if (await exists.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
            {
                return false;
            }
        }

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
