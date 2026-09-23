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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The control-plane Stage 1 store provider. The pure tests pin the reload/reconcile merge logic with
/// no database: <see cref="StoreConfigProvider.ApplyToConfig"/> swaps the held config's sections in place
/// (so the by-reference <see cref="DarlingAlertSettings"/> seam reflects a store reload immediately), and
/// the schedule resolvers layer <c>config_collector_schedules</c> overrides on
/// <see cref="CollectorScheduleDefaults"/> (per-server &gt; fleet &gt; default, per column). The live tests
/// (gated on DARLING_TEST_PG) prove the V17 bump trigger fires and are transaction-rolled-back so they never
/// clobber a shared dev store's singleton config rows.
/// </summary>
/* #1776: rolling the writes back protects the DATA, not the CONCURRENCY — an uncommitted write still holds its
   row locks and still fires the config_version bump trigger, so running unserialized against the shared store
   raced the 63 classes that do carry this attribute. Measured over three consecutive full-suite runs against one
   long-lived database: this class failed in run 1, and the failures landed on a DIFFERENT unserialized class each
   run, which is what makes the class of bug so expensive — it looks like the change under test broke something it
   never touched. CI never sees it because it creates a throwaway cluster per run. */
[Collection("live-postgres")]
public sealed class StoreConfigProviderTests
{
    /* ---------------- pure: apply (view -> held config, by-reference seam) ---------------- */

    [Fact]
    public void ApplyToConfig_SwapsEverySection_ByReferenceSeamReflectsItWithoutReconstruct()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config); // holds config by reference

        /* Sanity — the defaults before the swap. */
        Assert.Equal(80, settings.CpuThresholdPercent);
        Assert.True(config.CapturePlans);
        Assert.Equal(1.5, settings.AnalysisNotifySeverity);
        Assert.False(settings.SmtpEnabled);
        Assert.False(settings.TeamsWebhookEnabled);

        var view = new StoreConfigView
        {
            CapturePlans = false,
            McpEnabled = true,
            McpPort = 6000,
            WebEnabled = true,
            WebPort = 6001,
            Alerts = new AlertsConfig { CpuThresholdPercent = 42, ExcludedDatabases = { "tempdb" } },
            Analysis = new AnalysisConfig { Enabled = false, IntervalMinutes = 90, NotificationsEnabled = false, NotifySeverity = 0.75 },
            Smtp = new SmtpConfig { Host = "smtp.example.com", From = "a@b.com", To = "c@d.com" },
            Webhooks = new WebhooksConfig { TeamsUrl = "https://teams.example.com/hook" },
        };

        StoreConfigProvider.ApplyToConfig(config, view);

        /* The held config is mutated in place. */
        Assert.False(config.CapturePlans);
        Assert.Equal(42, config.Alerts.CpuThresholdPercent);
        Assert.True(config.Mcp.Enabled);
        Assert.Equal(6000, config.Mcp.Port);
        Assert.True(config.Web.Enabled);
        Assert.Equal(6001, config.Web.Port);
        Assert.Equal(90, config.Analysis.IntervalMinutes);
        Assert.False(config.Analysis.Enabled);

        /* The SAME settings adapter instance reflects the swap — the "future config-reload" seam. */
        Assert.Equal(42, settings.CpuThresholdPercent);
        Assert.Equal(0.75, settings.AnalysisNotifySeverity);
        Assert.True(settings.SmtpEnabled);
        Assert.Equal("smtp.example.com", settings.SmtpServer);
        Assert.Equal("c@d.com", settings.SmtpRecipients);
        Assert.True(settings.TeamsWebhookEnabled);
        Assert.Contains("tempdb", settings.ExcludedDatabases);
    }

    [Fact]
    public void ApplyToConfig_NotifySeverity_ClampsThroughTheSettingsSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView { Analysis = new AnalysisConfig { NotifySeverity = 9.9 } });
        Assert.Equal(2.0, settings.AnalysisNotifySeverity); // above max -> 2.0

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView { Analysis = new AnalysisConfig { NotifySeverity = -1.0 } });
        Assert.Equal(0.0, settings.AnalysisNotifySeverity); // below min -> 0.0
    }

    /* ---------------- pure: schedule resolution (override layering) ---------------- */

    [Fact]
    public void ResolveSchedule_EmptyOverrides_ReturnsCollectorScheduleDefaults()
    {
        var def = CollectorScheduleDefaults.All["wait_stats"];
        var eff = StoreConfigProvider.ResolveSchedule("wait_stats", 123, Array.Empty<ScheduleOverride>());

        Assert.Equal(def.FrequencyMinutes, eff.FrequencyMinutes);
        Assert.Equal(def.RetentionDays, eff.RetentionDays);
        Assert.True(eff.Enabled);
    }

    [Fact]
    public void ResolveSchedule_FleetOverride_AppliesWhenNoPerServerRow()
    {
        var overrides = new[] { new ScheduleOverride(null, "wait_stats", 15, 60, true) };
        var eff = StoreConfigProvider.ResolveSchedule("wait_stats", 123, overrides);

        Assert.Equal(15, eff.FrequencyMinutes);
        Assert.Equal(60, eff.RetentionDays);
        Assert.True(eff.Enabled);
    }

    [Fact]
    public void ResolveSchedule_PerServerWinsOverFleet_PerColumnNullFallsThrough()
    {
        var overrides = new[]
        {
            new ScheduleOverride(null, "wait_stats", 15, 60, true),   // fleet
            new ScheduleOverride(123, "wait_stats", 2, null, true),   // per-server: freq 2, retention NULL
        };
        var eff = StoreConfigProvider.ResolveSchedule("wait_stats", 123, overrides);

        Assert.Equal(2, eff.FrequencyMinutes);   // per-server frequency wins
        Assert.Equal(60, eff.RetentionDays);     // per-server retention NULL -> fleet 60
    }

    [Fact]
    public void ResolveSchedule_DisabledOverride_ReportsDisabled_FrequencyFallsToDefault()
    {
        var overrides = new[] { new ScheduleOverride(123, "wait_stats", null, null, false) };
        var eff = StoreConfigProvider.ResolveSchedule("wait_stats", 123, overrides);

        Assert.False(eff.Enabled);
        Assert.Equal(CollectorScheduleDefaults.All["wait_stats"].FrequencyMinutes, eff.FrequencyMinutes);
    }

    [Fact]
    public void ResolveSchedule_OtherServersRow_DoesNotLeakToThisServer()
    {
        var overrides = new[] { new ScheduleOverride(999, "wait_stats", 99, 99, false) }; // a different server
        var eff = StoreConfigProvider.ResolveSchedule("wait_stats", 123, overrides);

        var def = CollectorScheduleDefaults.All["wait_stats"];
        Assert.Equal(def.FrequencyMinutes, eff.FrequencyMinutes);
        Assert.True(eff.Enabled);
    }

    /* ---------------- #3929/#3930: on-load collectors also get a daily recapture cadence ---------------- */

    /// <summary>
    /// Pin 1 of #3929/#3930: the on-load set (frequency 0 - server_config, database_config,
    /// database_scoped_config, trace_flags, server_properties) now resolves to a daily
    /// (<see cref="CollectorScheduleDefaults.OnLoadRecaptureMinutes"/>) recurring interval through
    /// <see cref="CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes"/> - the one substitution both
    /// Darling's worker and Lite's ScheduleManager make instead of special-casing these five collectors by
    /// name. Every OTHER (already-scheduled) default is untouched - the substitution only fires on 0.
    /// </summary>
    [Fact]
    public void EffectiveRecurringIntervalMinutes_OnLoadSet_ResolvesToDailyRecapture_OthersUnchanged()
    {
        var onLoadCount = 0;
        foreach (var (name, entry) in CollectorScheduleDefaults.All)
        {
            var effective = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(entry.FrequencyMinutes);
            if (entry.FrequencyMinutes == 0)
            {
                onLoadCount++;
                Assert.Equal(CollectorScheduleDefaults.OnLoadRecaptureMinutes, effective);
            }
            else
            {
                Assert.Equal(entry.FrequencyMinutes, effective);
            }
        }

        /* The five named in #3929/#3930: server_config, database_config, database_scoped_config, trace_flags,
           server_properties. A floor, not an exact count pinned by name, so a future sixth on-load collector
           does not need this test edited - it would just also get the substitution. */
        Assert.True(onLoadCount >= 5, $"expected at least 5 on-load (frequency 0) collectors, found {onLoadCount}");
    }

    /// <summary>
    /// An operator's own override still wins (#3929/#3930's "ruling"): a non-zero override for one of the
    /// on-load collectors resolves and schedules exactly like any other collector's override, untouched by the
    /// on-load substitution, because that substitution only ever fires when the EFFECTIVE (post-override)
    /// frequency is 0.
    /// </summary>
    [Fact]
    public void ResolveSchedule_OperatorOverrideOnAnOnLoadCollector_StillWins_OverTheDailySubstitution()
    {
        var overrides = new[] { new ScheduleOverride(123, "trace_flags", 10, null, true) };
        var eff = StoreConfigProvider.ResolveSchedule("trace_flags", 123, overrides);

        Assert.Equal(10, eff.FrequencyMinutes);
        Assert.Equal(10, CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(eff.FrequencyMinutes));
    }

    /// <summary>
    /// Pin 3 of #3930: simulating <see cref="DarlingWorker.ComputeSeededNextDue"/> called repeatedly with the
    /// on-load recapture interval - exactly what the worker's connect loop, reload recompute and due-collector
    /// sweep now do for a frequency-0 collector - never lets the gap between two captures exceed
    /// <see cref="CollectorScheduleDefaults.OnLoadRecaptureMinutes"/> (a day), which is far inside every
    /// on-load collector's 30-day (or server_properties's 365-day) retention. A server connected for 45 days
    /// straight (one connect, then 44 simulated daily due cycles with no reconnect) therefore never goes more
    /// than a day without a fresh database_config/trace_flags/etc. capture, so its newest snapshot always
    /// stays inside retention - the #3930 field defect (30+ days connected loses the on-load config entirely)
    /// cannot recur.
    /// </summary>
    [Fact]
    public void ComputeSeededNextDue_RepeatedOnLoadCycle_NeverGapsPastRetention_Over45SimulatedDays()
    {
        var interval = CollectorScheduleDefaults.OnLoadRecaptureMinutes;
        var retentionDays = CollectorScheduleDefaults.All["trace_flags"].RetentionDays;

        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        DateTime? lastRun = now; // the initial on-connect capture

        var maxGap = TimeSpan.Zero;
        for (var day = 0; day < 44; day++)
        {
            var jitter = DarlingWorker.SeedJitter(serverId: 42, interval * 60);
            var due = DarlingWorker.ComputeSeededNextDue(lastRun, interval, now, jitter);

            /* The worker only dispatches once "now" reaches "due" (RunDueCollectorsAsync's now < due gate);
               simulate the sweep catching it right at that instant. */
            now = due;
            var gap = now - lastRun!.Value;
            if (gap > maxGap) maxGap = gap;

            lastRun = now; // this cycle's capture becomes the watermark for the next
        }

        Assert.True(maxGap <= TimeSpan.FromMinutes(interval) + TimeSpan.FromMinutes(2.5),
            $"a gap of {maxGap} between on-load recaptures exceeds the daily interval (+ the small seed jitter cap)");
        Assert.True(maxGap.TotalDays < retentionDays,
            $"a {maxGap.TotalDays:F1}-day gap would still be well inside the {retentionDays}-day retention window, but the assertion above is the real guarantee");
    }

    [Fact]
    public void ResolveFleetRetentionDays_FleetOverrideWins_PerServerIgnored_ElseDefault()
    {
        var def = CollectorScheduleDefaults.All["query_stats"].RetentionDays;

        Assert.Equal(def, StoreConfigProvider.ResolveFleetRetentionDays("query_stats", Array.Empty<ScheduleOverride>()));

        /* A per-server retention override cannot apply to a shared-table purge — ignored. */
        var perServerOnly = new[] { new ScheduleOverride(9, "query_stats", null, 5, true) };
        Assert.Equal(def, StoreConfigProvider.ResolveFleetRetentionDays("query_stats", perServerOnly));

        var fleet = new[] { new ScheduleOverride(null, "query_stats", null, 45, true) };
        Assert.Equal(45, StoreConfigProvider.ResolveFleetRetentionDays("query_stats", fleet));
    }

    [Fact]
    public void Resolve_RejectsDestructiveRetention_AndNegativeFrequency_FallingBackToDefaults()
    {
        var def = CollectorScheduleDefaults.All["query_stats"];

        /* Retention 0/negative would invert the purge cutoff and delete everything — it must NOT be
           honored; the resolvers fall through to the safe code default. */
        foreach (var bad in new[] { 0, -1, -3650 })
        {
            var fleet = new[] { new ScheduleOverride(null, "query_stats", null, bad, true) };
            Assert.Equal(def.RetentionDays, StoreConfigProvider.ResolveFleetRetentionDays("query_stats", fleet));
            Assert.Equal(def.RetentionDays, StoreConfigProvider.ResolveSchedule("query_stats", 1, fleet).RetentionDays);
        }

        /* A negative frequency must not be honored (would make the collector run every sweep). */
        var badFreq = new[] { new ScheduleOverride(null, "query_stats", -5, null, true) };
        Assert.Equal(def.FrequencyMinutes, StoreConfigProvider.ResolveSchedule("query_stats", 1, badFreq).FrequencyMinutes);

        /* A valid retention (>= 1) and frequency 0 (on-load-only) are still honored. */
        var ok = new[] { new ScheduleOverride(null, "query_stats", 0, 1, true) };
        var eff = StoreConfigProvider.ResolveSchedule("query_stats", 1, ok);
        Assert.Equal(0, eff.FrequencyMinutes);
        Assert.Equal(1, eff.RetentionDays);
    }

    /// <summary>
    /// #3532: a delta-family cadence past <see cref="CollectorDeltaCalculator.MaxDeltaFrequencyMinutes"/>
    /// would exceed the shared delta gap policy every cycle — the collector re-baselines each run and
    /// stores (0, 0) forever, fabricating permanent quiet. The viewer's editor refuses to write such a
    /// row, but a hand-written or pre-fix row can still exist, so the resolver treats it as "no override"
    /// and falls through to the next level, exactly like a negative frequency.
    /// </summary>
    [Fact]
    public void Resolve_RejectsADeltaFamilyCadencePastTheGapPolicyCap_FallingThrough()
    {
        var def = CollectorScheduleDefaults.All["wait_stats"];
        var cap = CollectorDeltaCalculator.MaxDeltaFrequencyMinutes;

        /* A poisoned fleet row falls all the way through to the code default. */
        var fleetBad = new[] { new ScheduleOverride(null, "wait_stats", cap + 60, null, true) };
        Assert.Equal(def.FrequencyMinutes, StoreConfigProvider.ResolveSchedule("wait_stats", 1, fleetBad).FrequencyMinutes);

        /* A poisoned per-server row falls through to a VALID fleet row, per-column. */
        var layered = new[]
        {
            new ScheduleOverride(null, "wait_stats", 15, null, true),
            new ScheduleOverride(1, "wait_stats", cap + 1, null, true),
        };
        Assert.Equal(15, StoreConfigProvider.ResolveSchedule("wait_stats", 1, layered).FrequencyMinutes);

        /* The cap itself is honored, the PostgreSQL delta family is covered, and a snapshot collector
           keeps its long cadence — the bound is per-collector-kind, not blanket. */
        var atCap = new[] { new ScheduleOverride(null, "wait_stats", cap, null, true) };
        Assert.Equal(cap, StoreConfigProvider.ResolveSchedule("wait_stats", 1, atCap).FrequencyMinutes);

        var pgBad = new[] { new ScheduleOverride(null, "pg_wait_stats", cap + 60, null, true) };
        Assert.Equal(CollectorScheduleDefaults.All["pg_wait_stats"].FrequencyMinutes,
            StoreConfigProvider.ResolveSchedule("pg_wait_stats", 1, pgBad).FrequencyMinutes);

        var snapshot = new[] { new ScheduleOverride(null, "database_size_stats", cap + 60, null, true) };
        Assert.Equal(cap + 60, StoreConfigProvider.ResolveSchedule("database_size_stats", 1, snapshot).FrequencyMinutes);
    }

    /* ---------------- live (DARLING_TEST_PG): the V17 bump trigger, rolled back ---------------- */

    [Fact]
    public async Task ConfigVersion_BumpTriggers_FireOnConfigWrites_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the config_version bump-trigger test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Everything inside a transaction that ROLLS BACK — the shared dev store's real singleton config
           rows are never mutated. Names are schema-qualified so resolution is search_path-independent. */
        await using var tx = await connection.BeginTransactionAsync(ct);

        await ExecAsync(connection, tx,
            "INSERT INTO config.config_service (id, updated_at) VALUES (1, now() AT TIME ZONE 'UTC') ON CONFLICT (id) DO NOTHING", ct);
        var before = Convert.ToInt64(await ScalarAsync(connection, tx, "SELECT config_version FROM config.config_service WHERE id = 1", ct));

        /* A desired-state write bumps the beacon via the statement-level trigger. */
        await ExecAsync(connection, tx,
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, enabled) VALUES (424242, 'wait_stats', 7, true)", ct);
        var afterSchedule = Convert.ToInt64(await ScalarAsync(connection, tx, "SELECT config_version FROM config.config_service WHERE id = 1", ct));
        Assert.True(afterSchedule > before, "config_version should bump on a config_collector_schedules write");

        /* A DIRECT config_service write self-bumps without the writer touching config_version. */
        await ExecAsync(connection, tx, "UPDATE config.config_service SET paused = NOT paused WHERE id = 1", ct);
        var afterService = Convert.ToInt64(await ScalarAsync(connection, tx, "SELECT config_version FROM config.config_service WHERE id = 1", ct));
        Assert.True(afterService > afterSchedule, "config_version should self-bump on a direct config_service write");

        await tx.RollbackAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, NpgsqlTransaction tx, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection, tx);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, NpgsqlTransaction tx, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection, tx);
        return await command.ExecuteScalarAsync(ct);
    }
}
