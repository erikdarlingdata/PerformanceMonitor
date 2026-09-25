/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of #4214's store host profile. <see cref="DarlingStoreHostProfileTests"/> pins the pure
/// classification; this proves the whole path around it — a REAL <c>postgresql.conf</c>/<c>postgresql.auto.conf</c>
/// on a REAL managed data directory, a REAL running server's <c>pg_settings</c> — produces all four
/// <see cref="HostSettingVerdict"/> values end to end.
///
/// <para><b>Gated on DARLING_TEST_PGRUNTIME as well as DARLING_TEST_PG</b>, the same pair
/// <see cref="CiClusterWorkerSizingLiveTests"/> requires, for the same reason: file attribution needs the
/// rig's OWN data directory (<c>DARLING_TEST_PGRUNTIME\data</c>), not merely a store <c>DARLING_TEST_PG</c>
/// happens to be able to reach.</para>
///
/// <para><b>Why no restart is needed.</b> <c>shared_buffers</c>, <c>max_worker_processes</c> and
/// <c>timescaledb.max_background_workers</c> are all postmaster-context — this test never touches them, so it
/// never needs to bounce the shared rig other live classes are running against. It uses three settings whose
/// context is <c>sighup</c>/<c>user</c> (<c>effective_cache_size</c>, <c>maintenance_work_mem</c>,
/// <c>work_mem</c>) for <see cref="HostSettingVerdict.Matches"/>, applying them with <c>pg_reload_conf()</c>;
/// <c>max_connections</c> for <see cref="HostSettingVerdict.StaleAfterHardwareChange"/> needs no live change at
/// all, because <see cref="DarlingStoreHostProfile.ClassifyVerdict"/> compares the LIVE value against TODAY'S
/// derivation, not against whatever the managed block's own text says — PostgreSQL's untouched default (100)
/// already disagrees with <see cref="DarlingManagedPostgres.TargetMaxConnections"/> (200); and
/// <c>max_wal_size</c> for <see cref="HostSettingVerdict.OperatorOverride"/> goes through <c>ALTER SYSTEM</c>,
/// which file attribution alone resolves — no reload needed there either.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingStoreHostProfileLiveTests
{
    [Fact]
    public async Task GatherAsync_ProducesEveryVerdict_EndToEnd_Gated()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live store host profile test.");
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to the rig's runtime root (the folder holding pgsql\\ and data\\ side "
            + "by side) — this test edits the rig's OWN postgresql.conf/postgresql.auto.conf, which "
            + "DARLING_TEST_PG alone does not locate on disk.");

        var dataDirectory = Path.Combine(runtimeRoot!, "data");
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");
        Assert.SkipUnless(File.Exists(confPath),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} has no data\\postgresql.conf; point it at a rig's runtime "
            + "root (containing both pgsql\\ and an initialized data\\), not a bare pg-runtime.zip extraction.");

        var ct = TestContext.Current.CancellationToken;
        var originalConf = File.ReadAllText(confPath);
        var originalAutoConf = File.Exists(autoConfPath) ? File.ReadAllText(autoConfPath) : "";

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* Matches: a v8 (hardware-sizing) block re-stating THIS host's own current derivation for three
               sighup/user-context settings, then a reload so the LIVE value actually reflects it — the same
               "value derived for this host right now" ClassifyVerdict compares against. */
            var (_, _, _, hostMemory) = DarlingStoreHostProfile.GatherHostFacts();
            var memorySettings = DarlingManagedPostgres.DeriveMemorySettings(DarlingManagedPostgres.QuantizeRam(hostMemory.EffectiveBytes));
            File.AppendAllText(confPath, "\n" + DarlingManagedPostgres.ConfMarkerV8 + "\n"
                + $"effective_cache_size = {memorySettings.EffectiveCacheSizeMb}MB\n"
                + $"maintenance_work_mem = {memorySettings.MaintenanceWorkMemMb}MB\n"
                + $"work_mem = {memorySettings.WorkMemMb}MB\n");

            /* Stale-after-hardware-change: a v4 (write-throughput) block that set max_connections once, at
               the value PostgreSQL's own untouched default already carries (100) — no live change needed,
               because today's derivation (TargetMaxConnections = 200) has moved past it, and the verdict
               compares the LIVE value against TODAY's derivation, not the block's own historical text. */
            File.AppendAllText(confPath, "\n" + DarlingManagedPostgres.ConfMarkerV4 + "\n"
                + "max_connections = 100\n");

            await using (var reloadCmd = new NpgsqlCommand("SELECT pg_reload_conf()", connection))
            {
                await reloadCmd.ExecuteNonQueryAsync(ct);
            }

            await WaitForLiveSettingMbAsync(connection, "work_mem", memorySettings.WorkMemMb, ct);

            /* Operator-override: ALTER SYSTEM, on a setting NEITHER managed block above touches, so its only
               file-based assignment is the one postgresql.auto.conf ALWAYS wins with — AttributeManagedSetting
               checks postgresql.auto.conf before postgresql.conf at all. No reload needed: file attribution
               never reads pg_settings. */
            await using (var alterCmd = new NpgsqlCommand("ALTER SYSTEM SET max_wal_size = '2GB'", connection))
            {
                await alterCmd.ExecuteNonQueryAsync(ct);
            }

            var postgres = new PostgresConfig { Managed = true, DataDirectory = dataDirectory };
            var profile = await DarlingStoreHostProfile.GatherAsync(postgres, connection, ct);
            var byName = profile.Settings.ToDictionary(s => s.Name, s => s.Verdict, StringComparer.Ordinal);

            Assert.Equal(HostSettingVerdict.Matches, byName["effective_cache_size"]);
            Assert.Equal(HostSettingVerdict.Matches, byName["maintenance_work_mem"]);
            Assert.Equal(HostSettingVerdict.Matches, byName["work_mem"]);
            Assert.Equal(HostSettingVerdict.StaleAfterHardwareChange, byName["max_connections"]);
            Assert.Equal(HostSettingVerdict.OperatorOverride, byName["max_wal_size"]);

            /* Not-managed: the SAME live connection, a bring-your-own config this time — every setting reads
               not-managed regardless of what pg_settings.source says (ruling: a BYO store gets not-managed
               for every setting, because this class never wrote any block for it to compare against). */
            var byoPostgres = new PostgresConfig { Managed = false };
            var byoProfile = await DarlingStoreHostProfile.GatherAsync(byoPostgres, connection, ct);
            Assert.All(byoProfile.Settings, s => Assert.Equal(HostSettingVerdict.NotManaged, s.Verdict));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                File.WriteAllText(confPath, originalConf);
                File.WriteAllText(autoConfPath, originalAutoConf);
                await using var reloadCmd = new NpgsqlCommand("SELECT pg_reload_conf()", cleanup);
                await reloadCmd.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// Polls <c>pg_settings</c> on the SAME session for up to five seconds until <paramref name="settingName"/>
    /// normalizes (<see cref="DarlingStoreHostProfile.NormalizePgSetting"/> — the identical normalization the
    /// profile itself applies) to <paramref name="expectedMb"/>. A backend picks up a pending
    /// <c>pg_reload_conf()</c> at its next query boundary, which is almost always before this ever loops once;
    /// the poll is cheap insurance against a slower CI runner, not the thing under test — the real assertion is
    /// the verdict <see cref="DarlingStoreHostProfile.GatherAsync"/> reports afterward.
    /// </summary>
    private static async Task WaitForLiveSettingMbAsync(NpgsqlConnection connection, string settingName, long expectedMb, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (true)
        {
            long? normalized;
            await using (var cmd = new NpgsqlCommand("SELECT setting, unit FROM pg_settings WHERE name = @name", connection))
            {
                cmd.Parameters.AddWithValue("name", settingName);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                normalized = await reader.ReadAsync(ct)
                    ? DarlingStoreHostProfile.NormalizePgSetting(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1))
                    : null;
            }

            if (normalized == expectedMb || DateTime.UtcNow >= deadline)
            {
                return;
            }

            await Task.Delay(200, ct);
        }
    }
}
