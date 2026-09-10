/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Plans one statement twice — once as the server would today, once with a hypothetical index visible —
/// and reports whether the planner would switch (#2612).
///
/// <para>
/// This is the one place the product ACTS on a monitored PostgreSQL server rather than reading it, so the
/// blast radius is worth stating precisely rather than reassuringly. A hypothetical index costs no disk,
/// is visible only inside the session that created it, and is never written anywhere.
/// <c>EXPLAIN</c> without <c>ANALYZE</c> does not execute the statement. The whole experiment runs inside a
/// transaction that is ROLLED BACK, so even the session-local catalog entry does not outlive the call.
/// </para>
///
/// <para>
/// <b><c>GENERIC_PLAN</c> is what makes this possible at all.</b> <c>pg_stat_statements</c> stores
/// NORMALIZED text — literals replaced by <c>$1</c>, <c>$2</c> — which cannot be planned the ordinary way
/// without values nobody has. PostgreSQL 16 added <c>EXPLAIN (GENERIC_PLAN)</c> for exactly this, so the
/// statement is planned as the server would plan it before seeing parameters. On PostgreSQL 15 and older
/// there is no such option and the experiment refuses rather than guessing at values, because inventing a
/// parameter would produce a plan for a query nobody ran.
/// </para>
///
/// <para>
/// <b>A "no" is a result, not a failure.</b> The planner declining the candidate is the answer that saves
/// someone from building an index — measured on the verification rig, where a candidate on an already
/// well-served predicate left the plan and its cost completely unchanged. The report says so plainly
/// rather than presenting an unchanged cost as an inconclusive run.
/// </para>
/// </summary>
public static class HypotheticalIndexExperiment
{
    /// <summary>
    /// The minimum PostgreSQL major for <c>EXPLAIN (GENERIC_PLAN)</c>. Below it the experiment refuses.
    /// </summary>
    public const int MinimumPostgresMajorForGenericPlan = 16;

    /// <summary>How long either EXPLAIN may take. Planning is cheap; a planner that is not is a finding of
    /// its own, and one this call must not sit inside on a server somebody else is using.</summary>
    public const int StatementTimeoutSeconds = 15;

    /// <summary>
    /// The client-side deadline on the experiment's FORWARD path — the <c>SET LOCAL</c> that arms
    /// <see cref="StatementTimeoutSeconds"/>, the hypopg creation, and the six EXPLAIN statements (#2874).
    ///
    /// <para><b>These are MONITORED-TARGET commands, not store commands</b>, so they never inherited
    /// Npgsql's undocumented 30 s: <c>MonitoredServerConnection</c> stamps <c>CommandTimeout = 60</c> on
    /// every PostgreSQL target connection string. The defect here is subtler than an unchosen default — the
    /// value was chosen for a COLLECTOR query and applies to these by adjacency.</para>
    ///
    /// <para><b>It is derived STRICTLY ABOVE <see cref="StatementTimeoutSeconds"/>, and that direction is
    /// the whole point.</b> Seven of the nine statements this method executes run under the server-side
    /// <c>SET LOCAL statement_timeout</c>, which ends an overrun as <c>57014 canceling statement due to
    /// statement timeout</c> — a diagnosable error naming its own cause. A client-side deadline BELOW the
    /// GUC would pre-empt that with Npgsql's <c>Exception while reading from stream</c>, which reads as a
    /// network fault: the exact misdiagnosis #2826 exists to prevent. So the client deadline is only ever a
    /// backstop for "the server's cancel never came back", and the margin it needs over the GUC is one
    /// round trip. Measured against PostgreSQL 17 / hypopg 1.4.3: a statement cancelled by
    /// <c>statement_timeout</c> reaches the client <b>3.1-6.7 ms</b> past the GUC at 1 s, 2 s and 5 s. Five
    /// seconds is ~750x that, which is link margin rather than work margin.</para>
    ///
    /// <para><b>Bounded above by the command plane's claim lease</b>, because that is what actually
    /// encloses this call: the only caller is <c>DarlingWorker.RunTestHypotheticalIndexAsync</c>, and
    /// <c>DarlingCommandExecutor.StaleCommandTimeout</c> reclaims a claim after 5 minutes with NO
    /// heartbeat — past it the reaper has already marked the command failed. Worst case with these two
    /// constants is 15 s of connect budget + 20 s for the unbounded <c>SET LOCAL</c> + 7 x 15 s of
    /// server-bounded statements + <see cref="ResetCommandTimeoutSeconds"/> = <b>260 s</b>, inside the
    /// 300 s lease with 40 s to spare. At the inherited 60 s it is 15 + 60 + 105 + 60 = 240 s — also
    /// inside, which is why this is a tightening rather than a fix for a live failure.</para>
    ///
    /// <para><b>Not derived from the store connection pool.</b> That argument belongs to the store-side
    /// regimes (#2901's <c>MaxPoolSize</c> permit, #2928's connection-seconds); these commands do not
    /// touch the store pool at all, and reusing it here would bound the wrong hop.</para>
    /// </summary>
    public const int ForwardCommandTimeoutSeconds = StatementTimeoutSeconds + 5;

    /// <summary>
    /// The client-side deadline on <c>hypopg_reset()</c> alone — and it is SIX TIMES
    /// <see cref="ForwardCommandTimeoutSeconds"/> rather than equal to it, because this one site's failure
    /// asymmetry is INVERTED relative to every other command in this sweep (#2874).
    ///
    /// <para><b>Three bounds that apply to the forward path do not apply here, and they compound.</b>
    /// (1) The reset runs in the <c>finally</c>, AFTER the rollback, and <c>SET LOCAL</c> dies with its
    /// transaction — measured, not assumed: inside the transaction a 2 s <c>SET LOCAL</c> ends
    /// <c>pg_sleep(30)</c> at 2.01 s with <c>57014</c>, and immediately after <c>ROLLBACK</c>
    /// <c>SHOW statement_timeout</c> reads <c>0</c> and <c>pg_sleep(8)</c> runs to completion in 8.01 s.
    /// (2) It is called with <c>CancellationToken.None</c>, deliberately, because cleanup must run even
    /// when the caller has given up — so no token bounds it either. (3) A monitored target sets no session
    /// <c>statement_timeout</c> of ours. <b>Its <c>CommandTimeout</c> is therefore the ONLY bound that
    /// exists on it.</b></para>
    ///
    /// <para><b>Why the number goes UP where the forward path's goes down.</b> A hypothetical index is held
    /// in the extension's own session memory, not the catalog, so the rollback above cannot remove it and
    /// the reset is what keeps a PHANTOM index out of the next borrower of this pooled session. Measured on
    /// PostgreSQL 17 / hypopg 1.4.3, a client-side <c>CommandTimeout</c> expiring on a RESPONSIVE backend
    /// throws <c>NpgsqlException</c>/<c>TimeoutException</c> and leaves the connection <c>Open</c>: the same
    /// backend pid comes back out of the pool and <c>hypopg_list_indexes</c> still returns the phantom. So a
    /// deadline that fires on a responsive-but-slow session CREATES the leak the reset exists to prevent —
    /// and a responsive session would have finished, because the reset's real cost is <b>0.24-0.93 ms</b>
    /// and is FLAT in the number of hypothetical indexes (100 of them reset faster than 1; it is a memory
    /// free, and this path ever creates one). <b>There is no legitimate reason for this statement to be
    /// slow, so any deadline short enough to fire can only fire on a session that would have succeeded.</b>
    /// Hence generous, not tight.</para>
    ///
    /// <para><b>Why it is nonetheless FINITE rather than <c>CommandTimeout = 0</c></b>, which is what
    /// "not running is worse than running long" argues for on its own. The other half of the same
    /// measurement: when the backend is UNRESPONSIVE (container paused mid-command) the timeout breaks the
    /// connection — <c>FullState=Broken</c>, a DIFFERENT backend pid on re-borrow, and
    /// <c>hypopg_list_indexes</c> back to 0. <b>Timing out against a dead session does not leak; it
    /// destroys the session that would have carried the leak.</b> So the harmful case is bounded away by
    /// making the value large, and the remaining case is one where firing is the correct outcome — while
    /// <c>0</c> would pin a command-plane worker and a pooled target connection on an unreachable server
    /// forever. 120 s is ~129,000x the measured cost, 6x the forward deadline (so the reset can never be
    /// the first statement in the experiment to time out), and leaves 40 s of the 5-minute claim lease.</para>
    ///
    /// <para><b>This NARROWS the leak rather than closing it</b>, following #2888's lock wait, #2901's
    /// command plane and #2928's COPY start phase. A reset that fails for any reason other than a broken
    /// connection — a permission error, a missing extension, a server-side cancel — still returns a usable,
    /// phantom-carrying session to the pool, and the <c>catch</c> below swallows it by design so the real
    /// failure reaches the caller. Bounding the statement cannot fix that; only refusing to return the
    /// connection could, and this experiment does not own the connection.</para>
    /// </summary>
    public const int ResetCommandTimeoutSeconds = ForwardCommandTimeoutSeconds * 6;

    /// <param name="PlannerWouldUseIt">The decisive answer. False is a real result.</param>
    /// <param name="CostBefore">Total estimated cost of the plan the server would use today.</param>
    /// <param name="CostAfter">Total estimated cost with the candidate visible. Equal to
    /// <paramref name="CostBefore"/> when the planner declined it.</param>
    /// <param name="HypotheticalIndexName">What hypopg called the candidate, so the name in the plan can be
    /// matched to it. Null when creation itself failed.</param>
    public readonly record struct Result(
        bool PlannerWouldUseIt,
        double CostBefore,
        double CostAfter,
        string? HypotheticalIndexName,
        string? PlanBeforeJson,
        string? PlanAfterJson,
        string Explanation);

    /// <summary>
    /// Runs the experiment on an OPEN connection to the monitored server.
    ///
    /// <para>The caller owns the connection because the caller owns the decision about which server this
    /// runs against, and that decision is the one thing about this feature that is not mine to make
    /// implicitly.</para>
    /// </summary>
    public static async Task<Result> RunAsync(
        NpgsqlConnection connection,
        string normalizedStatementText,
        string createIndexStatement,
        int postgresMajorVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(normalizedStatementText);
        ArgumentException.ThrowIfNullOrWhiteSpace(createIndexStatement);

        if (postgresMajorVersion < MinimumPostgresMajorForGenericPlan)
        {
            return new Result(
                false, 0, 0, null, null, null,
                $"This server is PostgreSQL {postgresMajorVersion}, and EXPLAIN (GENERIC_PLAN) arrived in "
                + $"{MinimumPostgresMajorForGenericPlan}. Stored statement text is normalized — literals "
                + "are $1, $2 — so without GENERIC_PLAN there is no way to plan it that does not involve "
                + "inventing parameter values, which would produce a plan for a query nobody ran. Refused "
                + "rather than guessed.");
        }

        /* ROLLED BACK unconditionally: the hypothetical index is session-local, but the session is pooled
           and would carry it into the next caller's work. */
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            await ExecuteAsync(connection, transaction,
                $"SET LOCAL statement_timeout = '{StatementTimeoutSeconds}s'", cancellationToken);

            var before = await ExplainAsync(connection, transaction, normalizedStatementText, cancellationToken);

            var indexName = await ScalarTextAsync(connection, transaction,
                "SELECT indexname FROM hypopg_create_index($1)", createIndexStatement, cancellationToken);

            var after = await ExplainAsync(connection, transaction, normalizedStatementText, cancellationToken);

            var costBefore = TotalCost(before);
            var costAfter = TotalCost(after);

            /* The index NAME appearing anywhere in the second plan is the decisive test, not the cost
               falling. Cost can move for reasons that have nothing to do with the candidate, and a cheaper
               plan that does not reference it is not evidence for building it. */
            var used = indexName is not null && after is not null
                && after.Contains(indexName, StringComparison.Ordinal);

            var saved = costBefore > 0 ? (costBefore - costAfter) / costBefore * 100 : 0;

            /* Formatted once, invariantly, then concatenated. An interpolated-string handler cannot span
               a concatenation, and a message this long has to wrap. */
            var beforeText = costBefore.ToString("N2", CultureInfo.InvariantCulture);
            var afterText = costAfter.ToString("N2", CultureInfo.InvariantCulture);
            var savedText = saved.ToString("N1", CultureInfo.InvariantCulture);

            return new Result(
                used, costBefore, costAfter, indexName, before, after,
                used
                    ? $"The planner WOULD use this index: estimated cost falls from {beforeText} to "
                      + $"{afterText}, a {savedText}% reduction. That is an ESTIMATE from the planner's own "
                      + "cost model on this server's current statistics, not a measured runtime — no "
                      + "statement was executed and no index was built."
                    : $"The planner would NOT use this index. The plan is unchanged at an estimated cost of "
                      + $"{beforeText}. This is a real answer rather than an inconclusive run: on this "
                      + "server's current statistics, building it would cost write throughput and disk and "
                      + "change nothing about this statement.");
        }
        finally
        {
            /* Explicit, and not left to disposal: a rollback that is skipped leaves the candidate visible
               to whoever gets this pooled session next, and every plan they read after that is wrong in a
               way nothing would report.

               Guarded, because a failed EXPLAIN can leave the transaction already aborted and disposed —
               and a finally that throws REPLACES the original exception with a meaningless one about
               transaction state. Measured: the first run against a real target hid an 08P01 behind an
               ObjectDisposedException from this very line. */
            try
            {
                await transaction.RollbackAsync(CancellationToken.None);
            }
            catch (Exception)
            {
                /* Nothing to add and nothing to save: the transaction is already gone, which is the
                   outcome this block wanted. Swallowed so the real failure reaches the caller. */
            }

            /* AND hypopg_reset(), because the rollback is NOT enough — the assumption that it was is the
               one this code originally shipped with, and the verification run disproved it: after two
               experiments and two rollbacks, hypopg_list_indexes still returned 2.

               Hypothetical indexes are SESSION-local, not transaction-local. They are held in the
               extension's own memory rather than in the catalog, so a transaction never owned them and
               rolling one back was never going to remove them. On a pooled connection that means every
               plan the next caller reads is planned against phantom indexes — wrong in a way nothing
               anywhere would report, which is the worst shape a defect can have in a monitoring tool.

               Its own try: reset failing must not replace a real failure either, and a connection too
               broken to run it is a connection the pool will discard. */
            try
            {
                await using var reset = new NpgsqlCommand("SELECT hypopg_reset()", connection)
                {
                    CommandTimeout = ResetCommandTimeoutSeconds,
                };
                await reset.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception)
            {
                /* Same reasoning as above. */
            }
        }
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection, DbTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction)
        {
            CommandTimeout = ForwardCommandTimeoutSeconds,
        };

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> ScalarTextAsync(
        NpgsqlConnection connection, DbTransaction transaction, string sql, string argument, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, (NpgsqlTransaction)transaction)
        {
            CommandTimeout = ForwardCommandTimeoutSeconds,
        };

        command.Parameters.AddWithValue(argument);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value as string;
    }

    /// <summary>
    /// The statement text is BOUND, never interpolated — and getting there took two failed designs worth
    /// recording, because both look correct.
    ///
    /// <para>
    /// The obvious one is <c>EXPLAIN (GENERIC_PLAN, FORMAT JSON) {text}</c> on an ordinary command. It
    /// fails: normalized text carries <c>$1</c>, PostgreSQL's extended protocol parses that as a required
    /// parameter, and the bind step supplies none — <c>08P01: bind message supplies 0 parameters, but
    /// prepared statement "" requires 1</c>. Adding a NULL parameter makes the error go away and produces
    /// a WRONG answer: <c>amount &gt; NULL</c> is provably NULL, so the planner returns a degenerate
    /// <c>Result</c> node instead of the generic plan. The whole point of <c>GENERIC_PLAN</c> is to plan
    /// without values, and binding one defeats it silently.
    /// </para>
    ///
    /// <para>
    /// So the statement travels as a VALUE into a transaction-local GUC, a <c>DO</c> block runs the EXPLAIN
    /// through <c>EXECUTE</c> where <c>$1</c> is just text, and the plan comes back through a second GUC.
    /// The bind step never sees a placeholder because the SQL never contains one. That the statement text
    /// is a bound parameter for its whole journey is not incidental — it is the property that makes this
    /// safe, and the first design did not have it.
    /// </para>
    /// </summary>
    private const string ExplainThroughGucSql = """
        DO $pm$
        DECLARE line text; acc text := '';
        BEGIN
          FOR line IN EXECUTE 'EXPLAIN (GENERIC_PLAN, FORMAT JSON) ' || current_setting('pm.stmt') LOOP
            acc := acc || line;
          END LOOP;
          PERFORM set_config('pm.plan', acc, true);
        END
        $pm$;
        """;

    private static async Task<string?> ExplainAsync(
        NpgsqlConnection connection, DbTransaction transaction, string statementText, CancellationToken cancellationToken)
    {
        /* is_local = true on both: the settings die with the transaction that is rolled back below, so
           nothing survives into the next caller of this pooled session. */
        await using (var stage = new NpgsqlCommand("SELECT set_config('pm.stmt', $1, true)", connection, (NpgsqlTransaction)transaction)
               { CommandTimeout = ForwardCommandTimeoutSeconds })
        {
            stage.Parameters.AddWithValue(statementText);
            await stage.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var explain = new NpgsqlCommand(ExplainThroughGucSql, connection, (NpgsqlTransaction)transaction)
               { CommandTimeout = ForwardCommandTimeoutSeconds })
        {
            await explain.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var read = new NpgsqlCommand("SELECT current_setting('pm.plan', true)", connection, (NpgsqlTransaction)transaction)
        {
            CommandTimeout = ForwardCommandTimeoutSeconds,
        };

        return (await read.ExecuteScalarAsync(cancellationToken))?.ToString();
    }

    /// <summary>
    /// The root node's <c>Total Cost</c>. Zero when the plan cannot be parsed, which the caller reads
    /// alongside <c>PlannerWouldUseIt</c> — a zero cost with a false verdict is the shape of a plan that
    /// could not be read, and neither number is quoted on its own.
    /// </summary>
    public static double TotalCost(string? planJson)
    {
        if (string.IsNullOrWhiteSpace(planJson))
        {
            return 0;
        }

        try
        {
            return JsonNode.Parse(planJson) is JsonArray { Count: > 0 } array
                   && array[0] is JsonObject root
                   && root["Plan"] is JsonObject plan
                   && plan["Total Cost"] is JsonValue cost
                ? cost.GetValue<double>()
                : 0;
        }
        catch (JsonException)
        {
            return 0;
        }
    }
}
