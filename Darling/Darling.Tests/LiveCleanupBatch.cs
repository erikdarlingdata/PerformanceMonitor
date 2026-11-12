/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Removal of shared-store objects that either SUCCEEDS or is heard about (#1873).
///
/// <para><b>The failure it replaces.</b> The live classes' cleanups ran each statement through a
/// <c>TryExecAsync</c> whose catch was empty, or wrote one <c>Console.WriteLine</c> that xUnit attaches to the
/// test — and on a PASSING test nobody reads it. So a <c>DROP MATERIALIZED VIEW</c> that lost a race reported
/// success, the aggregate survived into the shared store, and every later test (and on a reused database,
/// every later RUN) inherited it. A stranded aggregate is not inert: it changes compose's tier routing, feeds
/// the #1784 coverage gate, and makes <c>EnsureBaselineFallbackViewsAsync</c> a no-op for whatever it
/// shadows — which is how <c>query_stats_db_hourly</c> and <c>query_stats_db_daily</c> came to persist.</para>
///
/// <para><b>The collision is real, and it was reproduced.</b> Against PostgreSQL 18.4 + TimescaleDB 2.28.1, a
/// <c>refresh_continuous_aggregate</c> running concurrently with a <c>DROP MATERIALIZED VIEW</c> of the same
/// aggregate fails the DROP and LEAVES THE AGGREGATE STANDING, every time it collides. Two different
/// SQLSTATEs came out of the identical setup, depending on which phase of the refresh was in flight:
/// <list type="bullet">
/// <item><c>40P01</c> deadlock detected — the DROP is chosen as the victim while deleting from
/// <c>continuous_aggs_materialization_invalidation_log</c>, which the refresh wants a lock on.</item>
/// <item><c>XX000</c> tuple concurrently deleted — the refresh's own catalog maintenance beat it to a row.</item>
/// </list></para>
///
/// <para><b>Which is why this retries on ANY failure and lets a PROBE be the judge.</b> A SQLSTATE allow-list
/// is the natural shape and it is the wrong one here: <c>XX000</c> is <c>internal_error</c>, the class you
/// would never put on a retry list, yet it is what a perfectly ordinary refresh collision produces. The
/// postcondition — "the object is gone" — is the only thing that actually needs to be true, so that is what
/// is asserted, and the SQLSTATE never has to be classified at all. It also makes the "it was already gone"
/// case free: <c>DROP OWNED BY</c> on a role that never existed throws <c>42704</c>, the probe says gone, and
/// that is a success rather than a special case.</para>
///
/// <para><b>It never throws, deliberately.</b> These calls live in <c>finally</c> blocks, and a throw from a
/// finally REPLACES the body's in-flight exception — the exact masking #1794 was filed for and
/// <see cref="LiveStoreCleanup"/> exists to prevent. Residue is recorded to <see cref="Ledger"/> instead, and
/// <see cref="LivePostgresStoreFixture"/> fails the RUN with it once every test in the collection has
/// finished, where there is no test result left to poison — the loudness is added at the one place that can
/// afford it.</para>
///
/// <para>Since #1896 the callers reach this through <see cref="LiveStoreCleanup"/> rather than on the body's
/// own connection, which closes the other half of the same problem: a batch is only as good as the session it
/// runs on, and the session a failing test leaves behind is exactly the one that cannot answer a probe.</para>
/// </summary>
internal sealed class LiveCleanupBatch
{
    /// <summary>
    /// <c>DROP OWNED BY</c> for each of the four product roles (admin, viewer, web, mcp) that exists, and no error for one that
    /// does not: the statement has no <c>IF EXISTS</c> form, so a cleanup that names a role its class never created (the body
    /// failed before the batch ran, or the class makes only some of them) would throw 42704 and hide the real failure.
    /// A statement to put in front of <c>DROP ROLE IF EXISTS</c> for each role.
    /// </summary>
    public const string DropOwnedByExistingProductRolesSql =
        "DO $do$ DECLARE r text; BEGIN FOREACH r IN ARRAY ARRAY['admin', 'viewer', 'web', 'mcp'] LOOP "
        + "IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = r) THEN EXECUTE pg_catalog.format('DROP OWNED BY %I', r); END IF; "
        + "END LOOP; END $do$;";

    /// <summary>
    /// Attempts per object. The collision clears as soon as the in-flight refresh finishes, which in the test
    /// suite's data volumes is sub-second; the repro above needed one retry with a 20-million-row refresh
    /// running. Seven attempts spend at most ~9s of backoff before giving up, and only ever on the path that
    /// is already broken.
    /// </summary>
    private const int DefaultMaxAttempts = 7;

    private static readonly ConcurrentQueue<string> Recorded = new();

    private readonly NpgsqlConnection _connection;
    private readonly string _scope;
    private readonly bool _publishResidue;
    private readonly int _maxAttempts;
    private readonly List<string> _residue = [];

    /// <param name="connection">
    /// The cleanup connection. Prefer the fresh one <see cref="LiveStoreCleanup"/> hands out over the body's,
    /// which the failure being cleaned up after may already have destroyed.
    /// </param>
    /// <param name="publishResidue">
    /// Whether give-ups reach the run-wide <see cref="Ledger"/>, and so fail the run at collection teardown.
    /// Only <see cref="LiveCleanupBatchTests"/> passes false: its negative cases fail a removal ON PURPOSE, and
    /// a deliberate failure filed as a real accusation would turn every run red for the coverage of the thing
    /// that catches real ones. Those tests assert on <see cref="Residue"/> instead.
    /// </param>
    /// <param name="maxAttempts">
    /// Overridable only so the seam tests, whose removals fail BY DESIGN, do not each spend the full backoff
    /// budget proving a foregone conclusion. Every real caller takes the default.
    /// </param>
    public LiveCleanupBatch(NpgsqlConnection connection, bool publishResidue = true, int maxAttempts = DefaultMaxAttempts)
    {
        _connection = connection;
        _publishResidue = publishResidue;
        _maxAttempts = maxAttempts;
        _scope = TestContext.Current?.Test?.TestDisplayName ?? "(outside a test)";
    }

    /// <summary>What THIS batch could not remove. The run-wide view is <see cref="Ledger"/>.</summary>
    public IReadOnlyList<string> Residue => _residue;

    /// <summary>
    /// Everything that could not be removed, across the whole run, newest last. Read by
    /// <see cref="LivePostgresStoreFixture"/> at collection teardown so the residue it observes in the catalog
    /// arrives with the name of the test that failed to clean it up, rather than as an anonymous leftover.
    /// </summary>
    public static IReadOnlyList<string> Ledger => Recorded.ToList();

    /// <summary>
    /// Runs <paramref name="removeSql"/> until <paramref name="survivesSql"/> reports the object gone.
    /// </summary>
    /// <param name="what">How the object is named in the residue report, e.g. <c>continuous aggregate collect.query_stats_hourly</c>.</param>
    /// <param name="removeSql">The removal statement. Runs up to <see cref="DefaultMaxAttempts"/> times, so it must be idempotent — every caller uses an <c>IF EXISTS</c> / <c>if_exists =&gt; true</c> form.</param>
    /// <param name="survivesSql">A scalar query returning <c>boolean</c>: true while the object is still there.</param>
    public async Task RemoveAsync(string what, string removeSql, string survivesSql, CancellationToken ct)
    {
        Exception? lastFailure = null;

        for (var attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            try
            {
                await ExecuteAsync(removeSql, ct);
                lastFailure = null;
            }
            catch (Exception ex)
            {
                lastFailure = ex;
            }

            bool survives;
            try
            {
                survives = await SurvivesAsync(survivesSql, ct);
            }
            catch (Exception ex)
            {
                /* The probe itself could not answer — a broken session, or a catalog the extension was
                   supposed to bring. Unknowable is not gone, so treat it as residue and say why. */
                lastFailure = ex;
                survives = true;
            }

            if (!survives)
            {
                return;
            }

            if (attempt < _maxAttempts)
            {
                await Task.Delay(BackoffFor(attempt), ct);
            }
        }

        Record(what, removeSql, lastFailure);
    }

    /// <summary>200ms doubling to a 2s ceiling — long enough to outlast a test-sized refresh, short enough
    /// that a genuinely stuck object is reported inside ten seconds instead of hanging the suite.</summary>
    private static TimeSpan BackoffFor(int attempt)
        => TimeSpan.FromMilliseconds(Math.Min(200 * Math.Pow(2, attempt - 1), 2000));

    /* ---- the shared store's removable shapes, each with its probe in exactly one place ---- */

    /// <summary>
    /// Drops a SET of continuous aggregates, retrying the whole set rather than each one to exhaustion.
    ///
    /// <para><b>Taking the set is what makes it correct, not merely convenient.</b> The dailies are
    /// HIERARCHICAL aggregates — built over their hourly, not over raw — so dropping an hourly while its daily
    /// still stands fails <c>2BP01 cannot drop ... because other objects depend on it</c>, and
    /// <c>CASCADE</c> does not save it: TimescaleDB refuses to cascade through a dependent aggregate that has
    /// dependents of its own. Retrying that one aggregate cannot ever succeed, because what has to change is a
    /// DIFFERENT aggregate. Sweeping the whole set per round lets the daily go in the round that the hourly
    /// fails, and the hourly go in the next — to any depth, without this helper having to know the hierarchy.
    /// Found by the residue check itself on the second run against a reused database, where the old swallow had
    /// been stranding <c>query_store_stats_interval_hourly</c> silently.</para>
    ///
    /// <para><b>The refresh policy comes off first, every round.</b> That is the half that stops the race
    /// rather than surviving it: <c>EnsureContinuousAggregatesAsync</c> attaches policies whose jobs fire
    /// IMMEDIATELY (#1788), so a caller that did not go through <c>EnsureAggregatesWithoutPoliciesAsync</c> is
    /// dropping an aggregate the scheduler is still launching refreshes for, and a retry loop against a policy
    /// that keeps starting new ones can lose every round. Removing it bounds the collision to the ONE run
    /// already executing, which is what the retry then outlasts. It is best-effort by design — if the policy is
    /// already gone, or was never attached, the drop that follows is the thing that has to be true, and that
    /// one is verified.</para>
    /// </summary>
    public async Task DropContinuousAggregatesAsync(IEnumerable<string> views, CancellationToken ct)
    {
        var remaining = views.ToList();
        var lastFailure = new Dictionary<string, Exception?>(StringComparer.Ordinal);

        for (var round = 1; round <= _maxAttempts && remaining.Count > 0; round++)
        {
            foreach (var view in remaining.ToList())
            {
                try
                {
                    await ExecuteAsync($"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)", ct);
                }
                catch (Exception ex)
                {
                    _ = ex;
                }

                try
                {
                    await ExecuteAsync($"DROP MATERIALIZED VIEW IF EXISTS collect.{view} CASCADE", ct);
                    lastFailure[view] = null;
                }
                catch (Exception ex)
                {
                    lastFailure[view] = ex;
                }

                if (!await StillThereAsync(AggregateProbe(view), view, lastFailure, ct))
                {
                    remaining.Remove(view);
                }
            }

            if (remaining.Count > 0 && round < _maxAttempts)
            {
                await Task.Delay(BackoffFor(round), ct);
            }
        }

        foreach (var view in remaining)
        {
            Record($"continuous aggregate collect.{view}",
                $"DROP MATERIALIZED VIEW IF EXISTS collect.{view} CASCADE",
                lastFailure.GetValueOrDefault(view));
        }
    }

    private static string AggregateProbe(string view)
        => "SELECT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates "
           + $"WHERE view_schema = 'collect' AND view_name = '{view}')";

    /// <summary>The probe, with an unanswerable probe counted as "still there" — unknowable is not gone.</summary>
    private async Task<bool> StillThereAsync(
        string survivesSql, string key, Dictionary<string, Exception?> lastFailure, CancellationToken ct)
    {
        try
        {
            return await SurvivesAsync(survivesSql, ct);
        }
        catch (Exception ex)
        {
            lastFailure[key] = ex;
            return true;
        }
    }

    /// <summary>
    /// Removes a continuous aggregate's refresh policy and CONFIRMS it is gone, for the callers that keep the
    /// aggregate and only want the scheduler off it. The unverified form inside
    /// <see cref="DropContinuousAggregatesAsync"/> can afford to be best-effort because the drop that follows
    /// is checked; a caller that leaves the aggregate standing has no such backstop, and a policy it believes
    /// it removed is precisely the standing race (#1788) that strands aggregates in the first place.
    /// </summary>
    /// <remarks>
    /// The probe keys on the aggregate's OWN schema and name. Measured on TimescaleDB 2.28.1, not assumed:
    /// <c>timescaledb_information.jobs</c> reports a refresh policy's <c>hypertable_schema</c> /
    /// <c>hypertable_name</c> as the user-facing VIEW (<c>collect.query_stats_hourly</c>), not the
    /// materialization hypertable the job actually writes (<c>_timescaledb_internal._materialized_hypertable_10</c>).
    /// Joining <c>continuous_aggregates.materialization_hypertable_name</c> to it — the reading the column
    /// names invite — matches nothing, so the probe answers "gone" while the policy is still attached, which
    /// is a verification that always passes.
    /// </remarks>
    public async Task RemoveRefreshPolicyAsync(string view, CancellationToken ct)
        => await RemoveAsync(
            $"refresh policy on collect.{view}",
            $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)",
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.jobs "
            + "WHERE proc_name = 'policy_refresh_continuous_aggregate' "
            + $"AND hypertable_schema = 'collect' AND hypertable_name = '{view}')",
            ct);

    /// <summary>
    /// Drops a table (and, for a hypertable, its chunks and policies) created by a test.
    /// </summary>
    /// <param name="schema">
    /// Defaults to <c>collect</c>, which is where all but one caller creates. The exception earns the
    /// parameter rather than a second method: <c>DarlingSecuritySplitLiveTests</c> builds its compressed
    /// hypertable in <c>public</c> and MOVES it to <c>collect</c>, so depending on how far the body got it can
    /// be standing in either, and the teardown has to name both.
    /// </param>
    public async Task DropTableAsync(string table, CancellationToken ct, string schema = "collect")
        => await RemoveAsync(
            $"table {schema}.{table}",
            $"DROP TABLE IF EXISTS {schema}.{table} CASCADE",
            "SELECT EXISTS (SELECT 1 FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            + $"WHERE n.nspname = '{schema}' AND c.relname = '{table}')",
            ct);

    /// <summary>
    /// Removes a retention policy. An ARMED policy left on a shared relation is the most destructive residue
    /// of the lot — it drops chunks another live test planted, on ITS schedule, long after this test is over.
    /// </summary>
    public async Task RemoveRetentionPolicyAsync(string relation, CancellationToken ct)
        => await RemoveAsync(
            $"retention policy on collect.{relation}",
            $"SELECT remove_retention_policy('collect.{relation}', if_exists => true)",
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention' "
            + $"AND hypertable_schema = 'collect' AND hypertable_name = '{relation}')",
            ct);

    /// <summary>
    /// Runs a role-removal statement until no named role survives. Roles are CLUSTER-wide, so a leaked one
    /// outlives even a <c>DROP DATABASE</c> and greets the next run on the same cluster.
    /// </summary>
    public async Task DropRolesAsync(string removeSql, IReadOnlyList<string> roles, CancellationToken ct)
        => await RemoveAsync(
            $"roles {string.Join(", ", roles)}",
            removeSql,
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ("
            + string.Join(", ", roles.Select(r => $"'{r}'")) + "))",
            ct);

    /// <summary>
    /// Removes the roles a live test made, one statement at a time, and proves none is left (#5654).
    ///
    /// <para>The shape this replaces ran <c>DROP OWNED BY</c> and <c>DROP ROLE</c> for every role inside one <c>DO</c> block,
    /// that is one transaction. When a bring-your-own owner had granted the source login its privileges, the login's
    /// <c>DROP ROLE</c> failed with 2BP01 (privileges for the database, the schema and the submit function remained) and the
    /// whole block rolled back, so every role stayed; the same statements run one at a time did not fail. Roles are
    /// cluster-wide, so the next test of the class met <c>42710 role ... already exists</c> instead of the cause.
    /// Here, as the connected superuser and in the store's own database, each existing role first gives its objects to the
    /// superuser (<c>REASSIGN OWNED</c>) and loses its privileges (<c>DROP OWNED</c>) in its own statement; only then are the
    /// roles dropped. A role that still exists afterwards is reported by name, in the ledger and in the thrown exception.</para>
    /// </summary>
    /// <param name="roles">Every role the test can have made, including the fixed-name ones.</param>
    /// <param name="runSuffix">The per-test suffix every role of this test's own ends with; no role ending in it may survive.</param>
    public async Task RemoveRolesAsync(IReadOnlyList<string> roles, string runSuffix, CancellationToken ct)
    {
        Exception? lastFailure = null;
        var left = new List<string>();

        for (var attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            lastFailure = null;
            try
            {
                await EnsureOpenAsync(ct);
                var present = await RolesPresentAsync(roles, runSuffix, ct);
                var paramAcl = _connection.PostgreSqlVersion.Major >= 15;

                foreach (var role in present)
                {
                    var quoted = "\"" + role.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
                    try
                    {
                        if (paramAcl)
                        {
                            /* A SET grant on a parameter is cluster-wide; DROP OWNED in this database does not take it. */
                            await ExecuteAsync("REVOKE ALL ON PARAMETER pg_stat_statements.track_utility FROM " + quoted, ct);
                        }

                        await ExecuteAsync("REASSIGN OWNED BY " + quoted + " TO CURRENT_USER", ct);
                        await ExecuteAsync("DROP OWNED BY " + quoted, ct);
                    }
                    catch (Exception ex)
                    {
                        lastFailure = new InvalidOperationException($"role {role}: {ex.Message}", ex);
                    }
                }

                foreach (var role in present)
                {
                    try
                    {
                        await ExecuteAsync("DROP ROLE IF EXISTS \"" + role.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"", ct);
                    }
                    catch (Exception ex)
                    {
                        lastFailure = new InvalidOperationException($"role {role}: {ex.Message}", ex);
                    }
                }

                left = await RolesPresentAsync(roles, runSuffix, ct);
            }
            catch (Exception ex)
            {
                lastFailure = ex;
                left = [.. roles];
            }

            if (left.Count == 0)
            {
                return;
            }

            if (attempt < _maxAttempts)
            {
                await Task.Delay(BackoffFor(attempt), ct);
            }
        }

        var named = string.Join(", ", left);
        Record($"roles {named}", "REASSIGN OWNED BY <role> TO CURRENT_USER; DROP OWNED BY <role>; DROP ROLE IF EXISTS <role>", lastFailure);
        throw new InvalidOperationException(
            $"The live test left role(s) {named} behind; roles are cluster-wide, so the next test would meet them. Last failure: {lastFailure?.Message ?? "none reported"}",
            lastFailure);
    }

    /// <summary>The named roles, plus every role ending in this test's suffix, that exist now.</summary>
    private async Task<List<string>> RolesPresentAsync(IReadOnlyList<string> roles, string runSuffix, CancellationToken ct)
    {
        await EnsureOpenAsync(ct);
        using var command = new NpgsqlCommand(
            "SELECT rolname::text FROM pg_roles WHERE rolname = ANY($1) OR rolname LIKE $2 ORDER BY 1", _connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = roles.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = "%\\_" + runSuffix });
        var found = new List<string>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found.Add(reader.GetString(0));
        }

        return found;
    }

    private void Record(string what, string removeSql, Exception? failure)
    {
        var reason = failure is null
            ? "the removal statement reported success but the object is still there"
            : $"{failure.GetType().Name}"
              + (failure is PostgresException pg ? $" ({pg.SqlState})" : string.Empty)
              + $": {failure.Message}";

        var entry = string.Format(
            CultureInfo.InvariantCulture,
            "{0} survived {1} removal attempts — {2}{3}    statement: {4}{3}    test: {5}",
            what, _maxAttempts, reason, Environment.NewLine, removeSql, _scope);

        _residue.Add(entry);

        if (_publishResidue)
        {
            Recorded.Enqueue(entry);
        }

        /* Kept from the shape this replaces: xUnit attaches console output to the test, which is where a
           human already looking at THIS test will find it. It is no longer the only report — the run now
           fails at collection teardown — but it is still the fastest one to reach. */
        Console.WriteLine($"[cleanup residue] {entry}");
    }

    private async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        await EnsureOpenAsync(ct);
        using var command = new NpgsqlCommand(sql, _connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<bool> SurvivesAsync(string sql, CancellationToken ct)
    {
        await EnsureOpenAsync(ct);
        using var command = new NpgsqlCommand(sql, _connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// A prior statement may have BROKEN this session (proven on CI: a cagg drop died, the swallow hid it, and
    /// the next helper threw "Connection is not open" out of an otherwise-green test). Reopening the same
    /// <see cref="NpgsqlConnection"/> checks out a fresh pooled session, so one failed statement cannot
    /// cascade into every statement after it — and, here, cannot turn one object's residue into six.
    /// </summary>
    private async Task EnsureOpenAsync(CancellationToken ct)
    {
        if (_connection.State != System.Data.ConnectionState.Open)
        {
            await _connection.OpenAsync(ct);
        }
    }
}
