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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, end to end, against a REAL catalog: the monitoring role is granted only <c>pg_monitor</c> — never
/// superuser — and <c>pg_monitor</c> includes <c>pg_read_all_settings</c>, so that role sees
/// <c>GUC_SUPERUSER_ONLY</c> settings such as <c>primary_conninfo</c>. Its raw text on a standby can carry a
/// replication password in plain text. This plants that value with <c>ALTER SYSTEM SET</c>, reads it back
/// through the REAL <see cref="PgServerConfigCollector"/> (not a hand-rolled column read, so a revert of the
/// collector's redaction call sites fails this test), and asserts the secret never reaches a <c>Row</c> while
/// host, port, user and application_name survive.
///
/// <para>Cluster-wide state (<c>ALTER SYSTEM</c> is not per-database), so this is serialized with the rest of
/// the live-postgres collection and restores the setting in a <c>finally</c> that runs even on failure.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgSettingRedactionLivePostgresTests
{
    private const string RoleName = "s1a_4348_redact_probe";
    private const string RolePassword = "S1aRedactProbe4348Only";
    private const string SecretConninfo = "host=127.0.0.1 port=1 user=replicator password=hunter2 application_name=s1a";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task PrimaryConninfosPassword_IsRedactedByTheRealCollector_UnderAPgMonitorOnlyRole()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4348 redaction round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var owner = new NpgsqlConnection(cs);
        await owner.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"ALTER SYSTEM SET primary_conninfo = '{SecretConninfo}'", ct);
            await ExecAsync(owner, "SELECT pg_reload_conf()", ct);

            await ExecAsync(owner, $"DROP ROLE IF EXISTS {RoleName}", ct);
            await ExecAsync(owner, $"CREATE ROLE {RoleName} LOGIN PASSWORD '{RolePassword}'", ct);
            await ExecAsync(owner, $"GRANT pg_monitor TO {RoleName}", ct);

            var probeConnectionString = new NpgsqlConnectionStringBuilder(cs)
            {
                Username = RoleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString;

            await using var probe = new NpgsqlConnection(probeConnectionString);
            await probe.OpenAsync(ct);

            /* The vulnerability itself: a pg_monitor-only login, no superuser, reads the raw secret straight
               off pg_settings. Nothing in PostgreSQL's own permission model stops it — that is why the
               collector has to. */
            await using (var raw = new NpgsqlCommand("SELECT setting FROM pg_settings WHERE name = 'primary_conninfo'", probe))
            {
                var rawValue = (string?)await raw.ExecuteScalarAsync(ct);
                Assert.Contains("hunter2", rawValue, StringComparison.Ordinal);
            }

            var context = new CollectorContext
            {
                ServerId = -4348,
                ServerName = "pg-4348-redaction-probe",
                CollectionTime = DateTime.UtcNow,
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 },
                ExcludedDatabases = Array.Empty<string>(),
                CurrentDatabaseName = probe.Database,
            };

            var sql = PgServerConfigCollector.Instance.BuildQuery(context).Text;
            List<PgServerConfigCollector.Row> rows;
            await using (var read = new NpgsqlCommand(sql, probe))
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                rows = await PgServerConfigCollector.Instance.ReadAsync(reader, context, ct);
            }

            PgServerConfigCollector.Row? conninfoRow = null;
            foreach (var row in rows)
            {
                if (row.Name == "primary_conninfo" && row.DatabaseName is null && row.RoleName is null)
                {
                    conninfoRow = row;
                }

                /* NO column of ANY row — not just primary_conninfo's — carries the secret. boot_val and
                   reset_val go through the same redactor as setting, so this also covers them. */
                AssertNoSecret(row.Name);
                AssertNoSecret(row.Setting);
                AssertNoSecret(row.Unit);
                AssertNoSecret(row.Category);
                AssertNoSecret(row.Context);
                AssertNoSecret(row.VarType);
                AssertNoSecret(row.Source);
                AssertNoSecret(row.BootValue);
                AssertNoSecret(row.ResetValue);
                AssertNoSecret(row.SourceFile);
                AssertNoSecret(row.ShortDescription);
                AssertNoSecret(row.DatabaseName);
                AssertNoSecret(row.RoleName);
            }

            Assert.NotNull(conninfoRow);
            Assert.NotNull(conninfoRow!.Value.Setting);
            var setting = conninfoRow.Value.Setting!;
            Assert.Contains("password=********", setting, StringComparison.Ordinal);
            Assert.Contains("host=127.0.0.1", setting, StringComparison.Ordinal);
            Assert.Contains("port=1", setting, StringComparison.Ordinal);
            Assert.Contains("user=replicator", setting, StringComparison.Ordinal);
            Assert.Contains("application_name=s1a", setting, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            /* RunOwnedAsync, not RunAsync (#1902): the reset has to run on THIS session — primary_conninfo
               and the probe role are cluster-wide, not store rows a fresh connection could reach just as
               well, and there is nothing to reconnect to that a new connection would do differently. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await ExecAsync(owner, "ALTER SYSTEM SET primary_conninfo = ''", ct);
                await ExecAsync(owner, "SELECT pg_reload_conf()", ct);
                await ExecAsync(owner, $"DROP ROLE IF EXISTS {RoleName}", ct);
            });
        }
    }

    private static void AssertNoSecret(string? value)
    {
        if (value is null)
        {
            return;
        }

        Assert.DoesNotContain("hunter2", value, StringComparison.Ordinal);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
