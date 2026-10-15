/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 Phase A (review round 2, L7): the PostgreSQL leg of the keyed <c>mcp.dataDetail</c> census. The SQL Server legs
/// (<c>StatementFilter*ReadsLiveTests</c>) plant canaries only in SQL Server reads, so a future <c>get_pg_*</c> key that
/// carries a literal, a login or a client address failed no test. This class plants
/// <see cref="StatementFilterCensus.MaskStatement"/> (both literal canaries), <see cref="StatementFilterCensus.LoginCanary"/>
/// and <see cref="StatementFilterCensus.HostCanary"/> straight into the store's activity, blocking, log-event, deadlock,
/// statement-text, replication and role-override rows, so the census sees the read-time masker alone, past the
/// collection-time redaction (<c>PgLogTextRedactor</c>) that would normally have stripped a literal first. Each read's raw
/// answer then goes through <see cref="StatementFilterCensus.AssertKeyedMaskingAsync"/> under <c>maskLiterals</c> and
/// <c>metadataOnly</c>.
///
/// <para>The reads, with the key each one carries: <c>get_pg_blocking</c> (<c>root_query</c>, <c>worst_victim_query</c>,
/// <c>root_username</c>), <c>get_pg_session_states</c> (<c>username</c>, <c>client_addr</c>), <c>get_pg_log_events</c>
/// (<c>message</c>, <c>user_name</c>), <c>get_pg_deadlocks</c> (<c>victim_statement</c>) and <c>get_pg_deadlock_detail</c>
/// (<c>graph</c>), which normalize the stored text themselves and so must answer its shape without its literals,
/// <c>get_pg_top_queries</c>
/// (<c>query_text</c>), <c>get_pg_replication_stats</c> (<c>client_addr</c>), <c>get_pg_server_config</c> and
/// <c>get_pg_server_config_changes</c> (<c>role_name</c>). Not here, by design: <c>get_pg_xmin_horizon</c> (its holder is a
/// pid and its detail names state, application and times, never a user, address or statement), <c>get_pg_plans</c>
/// (collection drops <c>Query Text</c> and redacts a plan's constants), and every other <c>get_pg_*</c> read, which
/// returns no statement, user or client address.</para>
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>), never the shared
/// <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StatementFilterPgReadsLiveTests
{
    private const string ServerName = "SsfPgCensusSrv";

    private static readonly string Literal = StatementFilterCensus.MaskStatement;
    private static readonly string Login = StatementFilterCensus.LoginCanary;
    private static readonly string Host = StatementFilterCensus.HostCanary;

    [Fact]
    public async Task EveryPgReadThatQuotesAStatementAUserOrAClientAddress_MasksItUnderBothLevels()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the PostgreSQL keyed masking census.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        using var host = await StatementFilterCensus.BuildHostAsync();

        /* Every read is checked before the test fails, so one run names each read and key that leaks. */
        var failures = new List<string>();
        var found = new List<string>();
        async Task Check(string tool, Func<Task<string>> read, params string[] expected)
        {
            try
            {
                string raw = await read();
                var planted = await StatementFilterCensus.AssertKeyedMaskingAsync(host, tool, raw);
                foreach (string canary in expected)
                {
                    Assert.True(planted.Contains(canary), $"{tool}: the raw answer did not hold {canary}, so the plant never reached it. Raw: {Head(raw)}");
                }

                found.AddRange(planted);
            }
            catch (Exception ex)
            {
                failures.Add(tool + ": " + ex.Message.Split('\n')[0]);
            }
        }

        await Check("get_pg_blocking",
            () => DarlingMcpPgBlockingTools.GetPgBlocking(postgres, ServerName, cancellationToken: ct),
            StatementFilterCensus.LiteralCanary, Login);
        await Check("get_pg_session_states",
            () => DarlingMcpPgSessionStatesTools.GetPgSessionStates(postgres, ServerName, cancellationToken: ct),
            Login, Host);
        await Check("get_pg_log_events",
            () => DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, ServerName, cancellationToken: ct),
            StatementFilterCensus.LiteralCanary, Login);
        /* The deadlock reads normalize a stored statement and graph before they answer (#4005, PgDeadlockLogParser), so
           their raw answer holds the statement's shape but none of its literals: the reader is the guard, and the census
           of the same answer stays in case that changes. */
        foreach (var (tool, read) in new (string, Func<Task<string>>)[]
        {
            ("get_pg_deadlocks", () => DarlingMcpPgDeadlockTools.GetPgDeadlocks(postgres, ServerName, cancellationToken: ct)),
            ("get_pg_deadlock_detail", () => DarlingMcpPgDeadlockTools.GetPgDeadlockDetail(postgres, ServerName, cancellationToken: ct)),
        })
        {
            await Check(tool, async () =>
            {
                string raw = await read();
                Assert.True(raw.Contains("zq_census_col", StringComparison.Ordinal), $"{tool}: the raw answer lost the planted statement. Raw: {Head(raw)}");
                foreach (string literal in StatementFilterCensus.LiteralCanaries)
                {
                    Assert.False(raw.Contains(literal, StringComparison.Ordinal), $"{tool}: the reader no longer normalizes the statement; its raw answer holds '{literal}'.");
                }

                return raw;
            });
        }

        await Check("get_pg_top_queries",
            () => DarlingMcpPgStatementTools.GetPgTopQueries(postgres, ServerName, cancellationToken: ct),
            StatementFilterCensus.LiteralCanary);
        await Check("get_pg_replication_stats",
            () => DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(postgres, ServerName, cancellationToken: ct),
            Host);
        await Check("get_pg_server_config",
            () => DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, cancellationToken: ct),
            Login);
        await Check("get_pg_server_config_changes",
            () => DarlingMcpPgServerStateTools.GetPgServerConfigChanges(postgres, ServerName, cancellationToken: ct),
            Login);

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
        StatementFilterCensus.AssertPlanted(found,
            StatementFilterCensus.LiteralCanary, StatementFilterCensus.NumericCanary, Login, Host);
    }

    private static string Head(string text) => text.Length <= 400 ? text : text[..400];

    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        int serverId = ServerIdHelper.GetDeterministicHashCode(ServerName);
        await DarlingMcpTestData.RegisterServerAsync(c, serverId, ServerName, ct);
        var now = DateTime.UtcNow;
        var at = DarlingMcpTestData.Naive(now.AddMinutes(-10));

        /* Blocking: the root (blocking side) and its victim both carry the statement, a login and a client address. */
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO pg_blocking_edges (collection_id, collection_time, server_id, server_name, blocked_backend_id, blocked_pid, blocking_backend_id, blocking_pid, database_name, blocked_username, blocked_application_name, blocked_client_addr, blocked_state, blocked_wait_event_type, blocked_wait_event, blocked_query, blocked_xact_duration_ms, blocked_query_duration_ms, blocking_username, blocking_application_name, blocking_client_addr, blocking_state, blocking_wait_event_type, blocking_wait_event, blocking_query, blocking_xact_duration_ms, blocking_query_duration_ms, blocked_pid_count, blocking_is_idle_in_transaction, query_text_may_be_truncated) VALUES ($1, $2, $3, $4, 1, 100, 2, 200, 'appdb', $5, 'app', $6, 'active', 'Lock', 'relation', $7, 100, 100, $5, 'app', $6, 'idle in transaction', NULL, NULL, $7, 200, 200, 1, TRUE, FALSE)",
            CollectionIdGenerator.Next(), at, serverId, ServerName, Login, Host, Literal);

        /* Activity: an idle-in-transaction horizon holder, the session the read reports. */
        await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name, client_addr,
     backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, 5, 500, 'appdb', $5, 'app', $6,
        'client backend', 'idle in transaction', NULL, NULL, 'SELECT', NULL,
        900000, 900000, 900000, 900000, 100, 100, 100,
        true, true, false,
        3, 1, 1, 3)",
            CollectionIdGenerator.Next(), at, serverId, ServerName, Login, Host);

        /* Log events: the statement in the message, the login in user_name. */
        await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_log_events (collection_id, collection_time, server_id, server_name, occurred_at, family, severity, database_name, user_name, application_name, pid, message, detail, context, raw_line_hash)
VALUES ($1, $2, $3, $4, $2, 'error', 'ERROR', 'appdb', $5, 'app', 500, $6, $7, $8, 'ssf-pg-census-line')",
            CollectionIdGenerator.Next(), at, serverId, ServerName, Login, "syntax error in: " + Literal,
            "Failing row contains (" + StatementFilterCensus.LiteralCanary + ").", "SQL statement \"" + Literal + "\"");

        /* Deadlocks: the victim's statement. */
        await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_deadlocks (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $2, 4242, 2, 'ssf-pg-census-hash', 'ShareLock', 'relation', $5, $6)",
            CollectionIdGenerator.Next(), at, serverId, ServerName, Literal,
            "Process 4242 waits for ShareLock on transaction 9; blocked by process 4243.\nProcess 4242: " + Literal);

        /* Top queries: one statement's counters and its text. */
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, min_exec_time_ms, max_exec_time_ms, mean_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms, wal_records, wal_fpi, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) VALUES ($1, $2, $3, $4, 1, 1, 1, TRUE, 10, 100.0, 1.0, 20.0, 10.0, 100, 100, 10, 0, 0, 0, 0, 1.0, 0.0, 0, 0, 0, 5, 50.0, 50, 60)",
            CollectionIdGenerator.Next(), at, serverId, ServerName);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen) VALUES ($1, 1, $2, $3, $3)",
            serverId, Literal, at);

        /* Replication: a standby connected from the client address. */
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO pg_replication_stats (collection_id, collection_time, server_id, server_name, application_name, client_addr, state, sync_state, sync_priority, sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, write_lag_ms, flush_lag_ms, replay_lag_ms, backend_start) VALUES ($1, $2, $3, $4, 'replica1', $5, 'streaming', 'async', 0, 0, 0, 0, 0, 1.0, 1.0, 1.0, $2)",
            CollectionIdGenerator.Next(), at, serverId, ServerName, Host);

        /* Role override (ALTER ROLE ... SET): two snapshots, each with a server-wide row beside it (a snapshot is its
           server-wide rows), so the override both reads and changes. */
        foreach (var (when, setting) in new[] { (now.AddHours(-2), "0"), (now.AddMinutes(-10), "30000") })
        {
            await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collect.pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, source, boot_val, reset_val, sourceline, pending_restart, database_name, role_name)
VALUES ($1, $2, $3, $4, 'work_mem', '4096', 'configuration file', '4096', '4096', 0, false, NULL, NULL)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), serverId, ServerName);
            await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collect.pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, source, boot_val, reset_val, sourceline, pending_restart, database_name, role_name)
VALUES ($1, $2, $3, $4, 'statement_timeout', $5, 'role', NULL, $5, 0, false, NULL, $6)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), serverId, ServerName, setting, Login);
        }
    }
}
