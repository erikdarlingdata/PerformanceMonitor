/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The bundled-Postgres bootstrap (the shipped zero-admin default). Ungated: the derived
/// connection string, the postgresql.conf append pins (timescaledb preload, port, loopback
/// only), the generated password's shape, the DPAPI credential round-trip through the stored
/// file, and the data-directory/credential path conventions. Gated on DARLING_TEST_PGRUNTIME
/// (the path of an assembled pg-runtime directory — the folder containing
/// pgsql\bin\pg_ctl.exe; <c>fetch-pg-runtime.ps1 -KeepWork</c> leaves one at
/// artifacts\pg-runtime-work\assemble\pg-runtime): the full first-run story into a temp data
/// directory on a scratch port — initdb, start, create database, authenticate, then an
/// idempotent second EnsureRunning and an ownership-respecting stop — never touching a real
/// Postgres and never downloading anything.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], and NOT for the reason a sweep might assume.
   This class never reads DARLING_TEST_PG at all — it reads DARLING_TEST_PGRUNTIME, which merely shares that
   prefix, and it stands up its OWN throwaway cluster from the bundled runtime. A substring search for
   "DARLING_TEST_PG" matches it anyway (that is how #1776's original sweep came to list it), so this note is here to
   stop the next one serializing a class that touches no shared store. */
public sealed class DarlingManagedPostgresTests
{
    [Fact]
    public void DerivedConnectionString_LocalhostPortDarlingDarling()
    {
        var parsed = new NpgsqlConnectionStringBuilder(DarlingManagedPostgres.BuildConnectionString(5641, "pw123"));

        /* Explicit IPv4 loopback (not the name "localhost"): listen_addresses binds 127.0.0.1 (plus the
           optional network IP when exposed), NOT ::1, so a host resolving "localhost" to IPv6 first could
           otherwise miss the listener (darling-network-endpoints). */
        Assert.Equal("127.0.0.1", parsed.Host);
        Assert.Equal(5641, parsed.Port);
        Assert.Equal("darling", parsed.Username);
        Assert.Equal("pw123", parsed.Password);
        Assert.Equal("darling", parsed.Database);

        /* V8 split: the owner connection string carries the collect/config search path so the
           service's bare-name COPY writes and reads resolve to the new schemas on every pooled
           connection, regardless of the database default. Same schemas, same order as the SQL-side
           PgSchemaGenerator.SearchPath. */
        Assert.Equal("collect,config,public", parsed.SearchPath);
        Assert.Equal(
            PerformanceMonitor.Darling.Storage.PgSchemaGenerator.SearchPath.Replace(" ", "", StringComparison.Ordinal),
            parsed.SearchPath);
    }

    [Fact]
    public void RoleCredentialPaths_BesideTheDataDirectory()
    {
        /* The admin/viewer role credentials live beside the data directory, same posture as the
           owner's pg-credential.dpapi (trailing separator tolerated). */
        Assert.Equal(@"D:\darling\pg-admin-credential.dpapi", DarlingManagedPostgres.AdminCredentialPathFor(@"D:\darling\pg"));
        Assert.Equal(@"D:\darling\pg-admin-credential.dpapi", DarlingManagedPostgres.AdminCredentialPathFor(@"D:\darling\pg\"));
        Assert.Equal(@"D:\darling\pg-viewer-credential.dpapi", DarlingManagedPostgres.ViewerCredentialPathFor(@"D:\darling\pg"));

        /* Three distinct files: owner, admin, viewer. */
        Assert.Equal("pg-credential.dpapi", DarlingManagedPostgres.CredentialFileName);
        Assert.Equal("pg-admin-credential.dpapi", DarlingManagedPostgres.AdminCredentialFileName);
        Assert.Equal("pg-viewer-credential.dpapi", DarlingManagedPostgres.ViewerCredentialFileName);
    }

    [Fact]
    public void ConfAppend_PinsPreloadPortAndLoopbackOnly()
    {
        var block = DarlingManagedPostgres.BuildConfAppend(5641);

        Assert.Contains(DarlingManagedPostgres.ConfMarker, block, StringComparison.Ordinal);
        Assert.Contains("shared_preload_libraries = 'timescaledb'", block, StringComparison.Ordinal);
        Assert.Contains("port = 5641", block, StringComparison.Ordinal);
        Assert.Contains("listen_addresses = '127.0.0.1'", block, StringComparison.Ordinal);

        /* Worker sizing lives in the v2 block and memory sizing in the v3 block, never in v1 — pre-v2/v3
           clusters heal by gaining the LATER blocks, so v1's content must stay stable. */
        Assert.DoesNotContain("max_worker_processes", block, StringComparison.Ordinal);
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// PostgreSQL's default max_worker_processes = 8 cannot launch the per-hypertable compression
    /// policy jobs (live smoke: "failed to start a background worker" storms). Pins the
    /// TimescaleDB-guidance sizing, DERIVED from the hypertable count so it never goes stale as
    /// collectors are added: background workers = hypertables + 2; max_worker_processes = 3 + that + 8.
    /// The count is <see cref="TimescaleSupport.HypertableCount"/> = the collector catalog PLUS collection_log
    /// (the V23 hypertable outside the catalog), so the collection_log compression policy is not under-provisioned.
    /// </summary>
    [Fact]
    public void WorkerSizingConfAppend_PinsV2MarkerAndSizing()
    {
        /* collection_log is a hypertable but lives OUTSIDE the collector catalog, so the true count is
           collectors + 1 — the worker sizing must derive from that, not HypertableTables.Count alone. */
        Assert.Equal(TimescaleSupport.HypertableTables.Count + 1, TimescaleSupport.HypertableCount);

        var block = DarlingManagedPostgres.BuildWorkerSizingConfAppend();
        var expectedBackgroundWorkers = TimescaleSupport.HypertableCount + 2;
        var expectedWorkerProcesses = 3 + expectedBackgroundWorkers + 8;

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV2, block, StringComparison.Ordinal);
        Assert.Contains($"timescaledb.max_background_workers = {expectedBackgroundWorkers}", block, StringComparison.Ordinal);
        Assert.Contains($"max_worker_processes = {expectedWorkerProcesses}", block, StringComparison.Ordinal);

        /* v2 must not restate v1 settings — the blocks compose, they don't compete. */
        Assert.DoesNotContain("shared_preload_libraries", block, StringComparison.Ordinal);
        Assert.DoesNotContain("listen_addresses", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// SCALE-READINESS memory tuning (mirrors the worker sizing): the v3 block derives shared_buffers /
    /// effective_cache_size / maintenance_work_mem / work_mem from the host's physical RAM, injected here so
    /// the derivation is deterministic and unit-testable. 8 GB is the current DARLING01 box — it pins
    /// work_mem at its 16 MB floor, shared_buffers at the 1 GB co-located cap, and maintenance_work_mem at
    /// the #1777 compression floor (5% of 8 GB = 409 MB is well under it, and 25% = 2 GB does not bite).
    /// </summary>
    [Fact]
    public void MemorySizingConfAppend_PinsV3Marker_AndDerivesFrom8GbRam()
    {
        const long eightGb = 8L * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildMemorySizingConfAppend(eightGb);

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV3, block, StringComparison.Ordinal);
        Assert.Contains("shared_buffers = 1024MB", block, StringComparison.Ordinal);        /* 25% of 8 GB, capped at the 1 GB co-located ceiling (#1559) */
        Assert.Contains("effective_cache_size = 6144MB", block, StringComparison.Ordinal);  /* 75% of 8 GB */
        Assert.Contains("maintenance_work_mem = 1536MB", block, StringComparison.Ordinal);  /* the #1777 measured floor; 5% of 8 GB = 409 MB is far under it */
        Assert.Contains("work_mem = 16MB", block, StringComparison.Ordinal);                /* RAM/512, at the 16 MB floor */

        /* The blocks compose, they don't compete — v3 must not restate v1/v2 settings. */
        Assert.DoesNotContain("shared_preload_libraries", block, StringComparison.Ordinal);
        Assert.DoesNotContain("max_worker_processes", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v4 write-throughput block (24-server field incident): PG's default max_connections = 100
    /// and max_wal_size = 1GB are toy-sized — a fleet bootstrap's write burst forced back-to-back
    /// spread checkpoints while backend spawn churn surfaced as transient store write failures.
    /// Fixed values, deliberately not derived (WAL space is a ceiling, idle connections are cheap).
    /// </summary>
    [Fact]
    public void WriteThroughputConfAppend_PinsV4Marker_ConnectionsAndWalCeiling()
    {
        var block = DarlingManagedPostgres.BuildWriteThroughputConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV4, block, StringComparison.Ordinal);
        Assert.Contains("max_connections = 200", block, StringComparison.Ordinal);
        Assert.Contains("max_wal_size = 4GB", block, StringComparison.Ordinal);

        /* The blocks compose, they don't compete — v4 must not restate v1/v2/v3 settings. */
        Assert.DoesNotContain("shared_preload_libraries", block, StringComparison.Ordinal);
        Assert.DoesNotContain("max_worker_processes", block, StringComparison.Ordinal);
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v5 co-located-sizing override (#1559): re-states shared_buffers at the CAPPED derivation so an
    /// existing cluster provisioned under the old min(25%, 8 GB) rule heals DOWN via conf
    /// last-occurrence-wins. Pins the marker, the capped value at two RAM tiers, and that the block
    /// restates NOTHING else (compose, don't compete).
    /// </summary>
    [Fact]
    public void ColocatedSizingConfAppend_PinsV5Marker_AndCappedSharedBuffers()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildColocatedSizingConfAppend(sixteenGb);
        Assert.Contains(DarlingManagedPostgres.ConfMarkerV5, block, StringComparison.Ordinal);
        Assert.Contains("shared_buffers = 1024MB", block, StringComparison.Ordinal); /* 25% of 16 GB, capped */

        const long twoGb = 2L * 1024 * 1024 * 1024;
        var small = DarlingManagedPostgres.BuildColocatedSizingConfAppend(twoGb);
        Assert.Contains("shared_buffers = 512MB", small, StringComparison.Ordinal);  /* under the cap: restated as-is */

        /* The blocks compose, they don't compete — v5 restates ONLY shared_buffers. */
        Assert.DoesNotContain("max_connections", block, StringComparison.Ordinal);
        Assert.DoesNotContain("work_mem", block, StringComparison.Ordinal);
        Assert.DoesNotContain("effective_cache_size", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v6 log-rotation block (#1652): the logging collector as a SELF-CAPPING weekday ring. The three
    /// settings that make the ring bounded are load-bearing together — %a weekday naming caps the set at
    /// seven files, truncate-on-rotation stops a weekday file from growing week over week, and
    /// rotation_size 0 keeps size rolls (which append rather than truncate) from defeating the ring.
    /// </summary>
    [Fact]
    public void LogRotationConfAppend_PinsV6Marker_AndTheSelfCappingWeekdayRing()
    {
        var block = DarlingManagedPostgres.BuildLogRotationConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV6, block, StringComparison.Ordinal);
        Assert.Contains("logging_collector = on", block, StringComparison.Ordinal);
        Assert.Contains("log_directory = 'log'", block, StringComparison.Ordinal);
        Assert.Contains("log_filename = 'postgresql-%a.log'", block, StringComparison.Ordinal);
        Assert.Contains("log_rotation_age = 1d", block, StringComparison.Ordinal);
        Assert.Contains("log_rotation_size = 0", block, StringComparison.Ordinal);
        Assert.Contains("log_truncate_on_rotation = on", block, StringComparison.Ordinal);

        /* The blocks compose, they don't compete. */
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
        Assert.DoesNotContain("max_connections", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v7 compression-memory override (#1777) — the PROPAGATION half of the raised floor, and the only
    /// reason an EXISTING store adopts it. A store provisioned before #1777 carries a v3 block whose
    /// maintenance_work_mem was written under the old min(5% RAM, 1 GB) rule: on a 16 GB host that is the
    /// 819 MB line simulated here. Appending v7 must make the LAST occurrence the new 1536 MB value, which
    /// is the one PostgreSQL honors — the v3 block is never rewritten in place.
    /// </summary>
    [Fact]
    public void CompressionMemoryConfAppend_PinsV7Marker_AndOverridesAnOlderV3Line()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildCompressionMemoryConfAppend(sixteenGb);

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV7, block, StringComparison.Ordinal);
        Assert.Contains("maintenance_work_mem = 1536MB", block, StringComparison.Ordinal);

        /* The pre-#1777 conf shape: a v3 block carrying the OLD landing value. Appending v7 is what an
           existing store's next service-owned start does, and last-occurrence-wins is what makes it real. */
        var legacyConf =
            DarlingManagedPostgres.ConfMarkerV3 + "\n" +
            "shared_buffers = 1024MB\n" +
            "effective_cache_size = 12288MB\n" +
            "maintenance_work_mem = 819MB\n" +
            "work_mem = 32MB\n";
        Assert.Equal("819MB", LastSettingValue(legacyConf, "maintenance_work_mem"));
        Assert.Equal("1536MB", LastSettingValue(legacyConf + block, "maintenance_work_mem"));

        /* The older block is preserved, not edited — the heal path only ever appends. */
        Assert.Contains("maintenance_work_mem = 819MB", legacyConf + block, StringComparison.Ordinal);

        /* The blocks compose, they don't compete — v7 restates ONLY maintenance_work_mem. (The work_mem
           probe is anchored to a line start: "maintenance_work_mem = " trivially contains "work_mem = ".) */
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
        Assert.DoesNotContain("\nwork_mem = ", block, StringComparison.Ordinal);
        Assert.DoesNotContain("effective_cache_size", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The value PostgreSQL would honor for <paramref name="setting"/>: the LAST assignment in the file,
    /// which is the whole mechanism behind the versioned override blocks (v5 shared_buffers, v7
    /// maintenance_work_mem). Ignores comment lines so a marker can never be read as an assignment.
    /// </summary>
    private static string? LastSettingValue(string conf, string setting)
    {
        string? value = null;
        foreach (var raw in conf.Split('\n'))
        {
            var line = raw.Trim();
            if (line.StartsWith('#'))
            {
                continue;
            }

            var separator = line.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0 && line[..separator].Trim().Equals(setting, StringComparison.Ordinal))
            {
                /* The stock conf trails inline comments after the value ("100  # (change requires
                   restart)"); ours never do, but the parser must not depend on that. */
                var assignment = line[(separator + 1)..];
                var comment = assignment.IndexOf('#', StringComparison.Ordinal);
                value = (comment >= 0 ? assignment[..comment] : assignment).Trim();
            }
        }

        return value;
    }

    /// <summary>
    /// #1652: the diagnostics tail must follow the log wherever Postgres last wrote it — pg.log for
    /// pre-collector/startup failures, the v6 ring for a server that came up and then complained.
    /// </summary>
    [Fact]
    public void PickNewestServerLog_ChoosesTheNewestOfPgLogAndTheRing()
    {
        var root = Directory.CreateTempSubdirectory("darling-pglogpick-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var pgLog = Path.Combine(root.FullName, "pg.log");

            /* Nothing exists yet. */
            Assert.Null(DarlingManagedPostgres.PickNewestServerLog(pgLog, dataDirectory));

            /* Only pg.log — the pre-collector failure shape. */
            File.WriteAllText(pgLog, "FATAL: could not start");
            Assert.Equal(pgLog, DarlingManagedPostgres.PickNewestServerLog(pgLog, dataDirectory));

            /* The ring exists and is newer — the started-then-complained shape. */
            var ringDirectory = Path.Combine(dataDirectory, "log");
            Directory.CreateDirectory(ringDirectory);
            var ringFile = Path.Combine(ringDirectory, "postgresql-Mon.log");
            File.WriteAllText(ringFile, "ERROR: something after startup");
            File.SetLastWriteTimeUtc(pgLog, DateTime.UtcNow.AddMinutes(-10));
            File.SetLastWriteTimeUtc(ringFile, DateTime.UtcNow);
            Assert.Equal(ringFile, DarlingManagedPostgres.PickNewestServerLog(pgLog, dataDirectory));

            /* pg.log newer again (a fresh failed restart after the server had been up). */
            File.SetLastWriteTimeUtc(pgLog, DateTime.UtcNow.AddMinutes(5));
            Assert.Equal(pgLog, DarlingManagedPostgres.PickNewestServerLog(pgLog, dataDirectory));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// #1652: the one-time legacy cap — an oversized pre-rotation pg.log rolls to pg.log.old (replacing any
    /// prior roll, so the pair is bounded forever); a small file is left alone; a missing file is a no-op.
    /// </summary>
    [Fact]
    public void CapLegacyServerLog_RollsOnlyOversizedFiles()
    {
        var root = Directory.CreateTempSubdirectory("darling-pglogcap-");
        try
        {
            var pgLog = Path.Combine(root.FullName, "pg.log");
            var rolled = pgLog + ".old";

            /* Missing file: no-op, no throw. */
            DarlingManagedPostgres.CapLegacyServerLog(pgLog, capBytes: 10, logger: null);
            Assert.False(File.Exists(rolled));

            /* Under the cap: untouched. */
            File.WriteAllText(pgLog, "small");
            DarlingManagedPostgres.CapLegacyServerLog(pgLog, capBytes: 1024, logger: null);
            Assert.True(File.Exists(pgLog));
            Assert.False(File.Exists(rolled));

            /* Over the cap: rolled aside; a second oversized roll REPLACES the first (two files, ever). */
            File.WriteAllText(pgLog, new string('x', 2048));
            DarlingManagedPostgres.CapLegacyServerLog(pgLog, capBytes: 1024, logger: null);
            Assert.False(File.Exists(pgLog));
            Assert.True(File.Exists(rolled));

            File.WriteAllText(pgLog, new string('y', 4096));
            DarlingManagedPostgres.CapLegacyServerLog(pgLog, capBytes: 1024, logger: null);
            Assert.Equal(4096, new FileInfo(rolled).Length);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// On a big box every cap/ceiling engages: shared_buffers pins at the 1 GB co-located cap (not 25% =
    /// 16 GB), maintenance_work_mem at the #1777 2 GB cap (not 5% = 3.2 GB), work_mem at 64 MB (not
    /// RAM/512 = 128 MB); effective_cache_size stays the uncapped 75% planner hint.
    /// </summary>
    [Fact]
    public void MemorySizingConfAppend_EngagesCapsOnLargeRam()
    {
        const long sixtyFourGb = 64L * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildMemorySizingConfAppend(sixtyFourGb);

        Assert.Contains("shared_buffers = 1024MB", block, StringComparison.Ordinal);          /* capped at the 1 GB co-located ceiling (#1559) */
        Assert.Contains("effective_cache_size = 49152MB", block, StringComparison.Ordinal);   /* 75% of 64 GB, uncapped */
        Assert.Contains("maintenance_work_mem = 2048MB", block, StringComparison.Ordinal);    /* capped at 2 GB (#1777) */
        Assert.Contains("work_mem = 64MB", block, StringComparison.Ordinal);                  /* capped at 64 MB */
    }

    /// <summary>
    /// The pure derivation across RAM tiers, pinning each formula and its cap/clamp. work_mem is the
    /// flagged per-connection setting: it scales RAM/512 and reaches the 64 MB ceiling at 32 GB, so the
    /// pathological max_connections × sorts × work_mem never grows past the ceiling on a bigger box.
    ///
    /// <para>The maintenance_work_mem column is the #1777 LANDING TABLE, and each row exercises a different
    /// one of the three terms so the interaction cannot silently change: at 2 GB and 4 GB the 25%-of-RAM
    /// SMALL-HOST GUARD wins (512 / 1024 MB — the floor is held back rather than overcommitting the box);
    /// at 8 GB and 16 GB the measured 1536 MB FLOOR wins (16 GB is the RAM class the field measurement came
    /// from, and it must land exactly on the 1536 MB capture point); at 32 GB the raw 5%-of-RAM term has
    /// finally overtaken the floor and wins on its own (1638 MB); at 64 GB the 2 GB CAP wins (5% would be
    /// 3276 MB, and the field data showed nothing to gain past 1536).</para>
    /// </summary>
    [Theory]
    [InlineData(2, 512, 1536, 512, 16)]      /* 2 GB: maintenance held to 25% of RAM by the small-host guard; work_mem at the 16 MB floor */
    [InlineData(4, 1024, 3072, 1024, 16)]    /* 4 GB: the smallest host — the 25% guard holds the 1536 floor down to 1 GB (#1777) */
    [InlineData(8, 1024, 6144, 1536, 16)]    /* 8 GB: shared_buffers hits the 1 GB co-located cap (#1559); maintenance at the measured floor; work_mem at the floor */
    [InlineData(16, 1024, 12288, 1536, 32)]  /* 16 GB: the field-measured class — maintenance lands exactly on the 1536 MB capture point; work_mem RAM/512 = 32 MB */
    [InlineData(32, 1024, 24576, 1638, 64)]  /* 32 GB: 5% of RAM has overtaken the floor and wins outright; work_mem hits the 64 MB ceiling */
    [InlineData(64, 1024, 49152, 2048, 64)]  /* 64 GB: maintenance at the 2 GB cap; everything but effective_cache_size capped */
    public void DeriveMemorySettings_PerTier(long ramGb, int sharedBuffersMb, int effectiveCacheMb, int maintenanceMb, int workMemMb)
    {
        var settings = DarlingManagedPostgres.DeriveMemorySettings(ramGb * 1024 * 1024 * 1024);

        Assert.Equal(sharedBuffersMb, settings.SharedBuffersMb);
        Assert.Equal(effectiveCacheMb, settings.EffectiveCacheSizeMb);
        Assert.Equal(maintenanceMb, settings.MaintenanceWorkMemMb);
        Assert.Equal(workMemMb, settings.WorkMemMb);
    }

    /// <summary>A zero/garbage RAM reading (the GlobalMemoryStatusEx failure path) falls back to a
    /// conservative 4 GB derivation rather than emitting a 0 MB / divide-by-nothing setting.</summary>
    [Fact]
    public void DeriveMemorySettings_NonPositiveRam_FallsBackToConservative4Gb()
    {
        var settings = DarlingManagedPostgres.DeriveMemorySettings(0);

        Assert.Equal(1024, settings.SharedBuffersMb);       /* 25% of the 4 GB fallback */
        Assert.Equal(3072, settings.EffectiveCacheSizeMb);  /* 75% of 4 GB */
        Assert.Equal(1024, settings.MaintenanceWorkMemMb);  /* the #1777 1536 MB floor, held to 25% of the 4 GB fallback */
        Assert.Equal(16, settings.WorkMemMb);               /* RAM/512 = 8 MB, lifted to the 16 MB floor */
    }

    /* ===================== #2845 v8 hardware re-derivation ===================== */

    /// <summary>
    /// Reporting jitter is not a hardware change (#2845 review). The fingerprint is an exact comparison and
    /// v8 runs on EVERY start, so without quantization a host whose reported total wobbles by a few MB
    /// between reboots would append a fresh seven-line block on every restart, forever — defeating the
    /// "converges immediately" invariant the design rests on. <c>ullTotalPhys</c> is not guaranteed
    /// bit-identical across reboots (balloon / Dynamic-Memory guests especially), and the fleet's own
    /// readings are already non-round: 31.5 GB on a nominally 32 GB host.
    ///
    /// <para>A GB of granularity sits far above any plausible jitter and far below the smallest real resize
    /// this class sees (4 -> 8 GB), so it cannot mask a genuine change — the last case asserts exactly
    /// that.</para>
    /// </summary>
    [Fact]
    public void HardwareFingerprint_RamJitterWithinAGb_IsNotAHardwareChange()
    {
        const long oneGb = 1024L * 1024 * 1024;
        const int hypertables = 40;

        /* A nominally 32 GB host, as three plausible readings of the same machine. */
        var nominal = 32 * oneGb;
        var short31Point5 = 31L * oneGb + 512L * 1024 * 1024;  /* what the fleet actually reports */
        var wobble = 32 * oneGb - 7L * 1024 * 1024;            /* a few MB less on the next boot */

        var conf = DarlingManagedPostgres.BuildHardwareSizingConfAppend(short31Point5, hypertables);

        foreach (var reading in new[] { nominal, short31Point5, wobble })
        {
            Assert.True(
                DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
                    conf, DarlingManagedPostgres.BuildHardwareFingerprint(reading, hypertables)),
                $"reading {reading} should be the same machine, not a hardware change");
        }

        /* And the block is exactly reproducible from its own fingerprint - same quantized value both. */
        Assert.Equal(
            DarlingManagedPostgres.BuildHardwareSizingConfAppend(nominal, hypertables),
            DarlingManagedPostgres.BuildHardwareSizingConfAppend(short31Point5, hypertables));

        /* A REAL resize still reads as one - quantization cannot mask a genuine change. */
        Assert.False(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            conf, DarlingManagedPostgres.BuildHardwareFingerprint(64 * oneGb, hypertables)));
    }


    /// <summary>
    /// A NON-AUTHORITATIVE RAM reading must append nothing, whatever the conf says (#2845 review).
    ///
    /// <para>The subtlety this pins: <c>GetTotalPhysicalMemoryBytes</c> falls back to
    /// <c>GC.GetGCMemoryInfo().TotalAvailableMemoryBytes</c> before it reaches the fixed 4 GB sentinel, and
    /// that middle tier is a LIVE value — it varies between calls and sits below true physical RAM. So
    /// normalising only the "&lt;= 0" case does not make the fingerprint stable: an intermittently failing
    /// Win32 call would mint a NOVEL fingerprint on each blip, append a block every time (this check runs
    /// on every start, unlike the marker-gated v1-v7), and derive the planner's cache estimate from the low
    /// guess with immediate effect. Guarding on the VALUE cannot fix that; guarding on whether the reading
    /// is trustworthy at all can. Absence of a reading is not evidence the hardware is unchanged, so the
    /// answer is to do nothing and leave the last known-good block in force.</para>
    /// </summary>
    [Fact]
    public void ShouldAppendHardwareSizing_NonAuthoritativeRamReading_AppendsNothing()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        const int hypertables = 40;

        var conf = DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, hypertables);

        /* A genuine 16 -> 32 GB resize DOES append, but only with an authoritative reading behind it. */
        Assert.True(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            conf, ramReadingIsAuthoritative: true,
            DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables)));

        /* The same apparent change, from a reading we could not trust, must do nothing at all. */
        Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            conf, ramReadingIsAuthoritative: false,
            DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables)));

        /* And it stays inert for ANY value the GC fallback might invent, which is the append-loop case. */
        foreach (var guessGb in new long[] { 3, 7, 12, 29 })
        {
            Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
                conf, ramReadingIsAuthoritative: false,
                DarlingManagedPostgres.BuildHardwareFingerprint(guessGb * 1024 * 1024 * 1024, hypertables)));
        }
    }


    /// <summary>
    /// THE PROPERTY THIS ISSUE IS ABOUT: a RAM change makes the conf stale, and staleness is what triggers
    /// re-derivation. Asserted on the decision function rather than on a code shape, so a refactor that
    /// keeps the behaviour keeps the pin green and one that loses it goes red.
    ///
    /// <para>The 16 -> 31.5 GB pair is the live case from #2845: three boxes resized under a marker-keyed
    /// scheme kept effective_cache_size at 75% of the RAM they no longer had.</para>
    /// </summary>
    [Fact]
    public void HardwareFingerprint_RamChange_TriggersRederivation()
        {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        const int hypertables = 40;

        var conf = "shared_buffers = 1024MB\n" +
            DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, hypertables);

        /* Same host: already derived here, nothing to do. */
        Assert.True(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            conf, DarlingManagedPostgres.BuildHardwareFingerprint(sixteenGb, hypertables)));

        /* Resized: the sizing in the file was derived under RAM this host no longer has. */
        Assert.False(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            conf, DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables)));

        /* Collector added: the worker counts in the file are undersized for the new hypertable count. */
        Assert.False(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            conf, DarlingManagedPostgres.BuildHardwareFingerprint(sixteenGb, hypertables + 1)));
    }

    /// <summary>
    /// The LAST fingerprint decides, not any fingerprint — the case a <c>conf.Contains(fingerprint)</c>
    /// test gets wrong and the reason the helper exists at all.
    ///
    /// <para>A host resized 16 -> 32 -> back to 16 GB has BOTH fingerprints in its conf. Contains would find
    /// the original 16 GB line still present and skip the append, leaving the 32 GB block as the last
    /// occurrence of effective_cache_size and therefore still in force — a box sized for RAM it does not
    /// have, latched permanently. postgresql.conf resolves duplicates by last-occurrence-wins, so the
    /// staleness test has to ask the same question the file answers.</para>
    /// </summary>
    [Fact]
    public void HardwareFingerprint_ResizeBackToPreviousSize_StillRederives()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        const int hypertables = 40;

        var conf =
            DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, hypertables) +
            DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, hypertables);

        var sixteenGbFingerprint = DarlingManagedPostgres.BuildHardwareFingerprint(sixteenGb, hypertables);

        /* The 16 GB fingerprint IS present — a Contains test would return true here and skip. */
        Assert.Contains(sixteenGbFingerprint, conf, StringComparison.Ordinal);

        /* But it is not the LAST one, so the box is running 32 GB sizing and must re-derive. */
        Assert.False(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(conf, sixteenGbFingerprint));

        /* And the 32 GB block, being last, correctly reports itself as current. */
        Assert.True(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            conf, DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables)));
    }

    /// <summary>
    /// CONSTRAINT PIN 1 (#1559 / #2845): the hardware block must never emit <c>shared_buffers</c>, at ANY
    /// host size. The 1 GB cap is the Windows error-487 mitigation and the condition is live on the fleet
    /// (measured 2026-09-03: 4-205 occurrences/day across three boxes, zero could-not-fork — the retry path
    /// holding is exactly the margin a larger segment would spend).
    ///
    /// <para>Excluded STRUCTURALLY rather than by trusting min(25% RAM, 1 GB) to keep returning 1 GB: this
    /// pin holds even if someone later raises the cap in <see cref="DarlingManagedPostgres.DeriveMemorySettings"/>,
    /// which is the point. Raising it is a formula decision that belongs in a reviewed version-keyed block,
    /// not something a host resize propagates to production on its own.</para>
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(512)]
    public void HardwareSizingConfAppend_NeverEmitsSharedBuffers(long ramGb)
    {
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(ramGb * 1024 * 1024 * 1024, 40);

        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// CONSTRAINT PIN 2 (#2845): the hardware block must never emit <c>work_mem</c>, at ANY host size.
    /// The formula would take it 31 -> 63 MB on the resized boxes, and the only measurements above 31 MB on
    /// this store's heaviest read are worse (PlanRegressionSql: default 26,565 ms, 31 MB 25,617 ms,
    /// 512 MB 59,323 ms). It is also the wrong KIND of setting for this block — a per-sort, per-connection
    /// ceiling that follows from the query mix, not from the machine.
    ///
    /// <para>Note the theory covers 32 GB and above, where the formula clamps to the 64 MB ceiling: those
    /// are precisely the sizes where a naive "apply the formula to the new RAM" change would have doubled
    /// it.</para>
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(512)]
    public void HardwareSizingConfAppend_NeverEmitsWorkMem(long ramGb)
    {
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(ramGb * 1024 * 1024 * 1024, 40);

        /* Anchored on the newline that starts every setting line. A bare "work_mem = " is a SUBSTRING of
           "maintenance_work_mem = ", so the unanchored form fails against a correct block — caught by the
           harness before this shipped, and the reason the positive assertion below is here as a guard. */
        Assert.DoesNotContain("\nwork_mem = ", block, StringComparison.Ordinal);
        Assert.Contains("\nmaintenance_work_mem = ", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The block emits what it is for, at the values the resized fleet should have had. 31.5 GB is the
    /// m7i.2xlarge reading; 32 GB is used here for a round assertion. effective_cache_size 24576MB is the
    /// number #2845 was filed over — the boxes were sitting at 11.86 GB, which is 75% of the 16 GB they had
    /// before the resize.
    /// </summary>
    [Fact]
    public void HardwareSizingConfAppend_EmitsHostDerivedSettings()
    {
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, 40);

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV8, block, StringComparison.Ordinal);
        Assert.Contains("effective_cache_size = 24576MB", block, StringComparison.Ordinal);  /* 75% of 32 GB (was 11.86 GB = 75% of 16 GB) */
        Assert.Contains("maintenance_work_mem = 1638MB", block, StringComparison.Ordinal);   /* 5% of 32 GB, past the 1536 floor, under the 2 GB cap */
        Assert.Contains("timescaledb.max_background_workers = 42", block, StringComparison.Ordinal);  /* 40 hypertables + 2 */
        Assert.Contains("max_worker_processes = 53", block, StringComparison.Ordinal);       /* 3 + 42 + 8 */
    }

    /// <summary>
    /// The v2 block and the v8 re-derivation share ONE worker formula (#2845), so the two writers of
    /// max_worker_processes cannot drift apart and produce a conf whose last occurrence disagrees with the
    /// block that established it.
    /// </summary>
    [Fact]
    public void HardwareSizingConfAppend_WorkerCountsMatchV2Formula()
    {
        var v2 = DarlingManagedPostgres.BuildWorkerSizingConfAppend();
        var v8 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(
            32L * 1024 * 1024 * 1024, TimescaleSupport.HypertableCount);

        foreach (var setting in new[] { "timescaledb.max_background_workers = ", "max_worker_processes = " })
        {
            var fromV2 = ExtractSettingLine(v2, setting);
            var fromV8 = ExtractSettingLine(v8, setting);
            Assert.Equal(fromV2, fromV8);
        }

        static string ExtractSettingLine(string block, string setting)
        {
            var start = block.IndexOf(setting, StringComparison.Ordinal);
            Assert.True(start >= 0, $"block did not contain '{setting}'");
            var end = block.IndexOf('\n', start);
            return (end < 0 ? block[start..] : block[start..end]).TrimEnd('\r');
        }
    }

    /// <summary>
    /// A failed RAM reading fingerprints as the 4 GB fallback it actually derived under, not as "0". If it
    /// recorded zero, the next successful reading would look like a hardware change and append a block on
    /// every alternating start — an append loop rather than a converging heal.
    /// </summary>
    [Fact]
    public void HardwareFingerprint_NonPositiveRam_MatchesTheFallbackItDerivedUnder()
    {
        const long fourGb = 4L * 1024 * 1024 * 1024;

        Assert.Equal(
            DarlingManagedPostgres.BuildHardwareFingerprint(fourGb, 40),
            DarlingManagedPostgres.BuildHardwareFingerprint(0, 40));
    }

    [Fact]
    public void GeneratePassword_32AlphanumericCryptoRandom()
    {
        var first = DarlingManagedPostgres.GeneratePassword();
        var second = DarlingManagedPostgres.GeneratePassword();

        /* Alphanumeric-only by design (survives --pwfile and connection strings without
           escaping); the 32-char length carries the strength (~190 bits over a 62 charset). */
        Assert.Equal(32, first.Length);
        Assert.All(first, c => Assert.True(char.IsAsciiLetterOrDigit(c), $"unexpected password character '{c}'"));
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void PathConventions_DefaultDataDirectory_AndCredentialBesideIt()
    {
        Assert.Equal(
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "PerformanceMonitorDarling", "pg"),
            DarlingManagedPostgres.ResolveDataDirectory(new PostgresConfig()));

        /* The credential lives BESIDE the data directory (not inside it — initdb wants the
           directory empty), trailing separator tolerated. */
        Assert.Equal(@"D:\darling\pg-credential.dpapi", DarlingManagedPostgres.CredentialPathFor(@"D:\darling\pg"));
        Assert.Equal(@"D:\darling\pg-credential.dpapi", DarlingManagedPostgres.CredentialPathFor(@"D:\darling\pg\"));
    }

    [Fact]
    public void StoredCredential_DpapiRoundTrip_DerivesTheConnectionString()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-pgcred-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var config = new PostgresConfig { Managed = true, Port = 5991, DataDirectory = dataDirectory };

            /* No credential yet → null (the MCP host's first-boot wait relies on this). */
            Assert.Null(DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(config));

            var password = DarlingManagedPostgres.GeneratePassword();
            var credentialPath = DarlingManagedPostgres.CredentialPathFor(dataDirectory);
            File.WriteAllText(credentialPath, DarlingSecrets.Protect(password));

            /* The blob on disk is never the plaintext. */
            Assert.DoesNotContain(password, File.ReadAllText(credentialPath), StringComparison.Ordinal);

            var derived = DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(config);
            Assert.NotNull(derived);
            var parsed = new NpgsqlConnectionStringBuilder(derived);
            Assert.Equal(password, parsed.Password);
            Assert.Equal(5991, parsed.Port);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void McpCredentialPath_BesideTheDataDirectory_AndDistinctFile()
    {
        /* The mcp role credential lives beside the data directory, same posture as owner/admin/viewer
           (trailing separator tolerated) — a fourth distinct file (darling-network-endpoints, D3-role). */
        Assert.Equal(@"D:\darling\pg-mcp-credential.dpapi", DarlingManagedPostgres.McpCredentialPathFor(@"D:\darling\pg"));
        Assert.Equal(@"D:\darling\pg-mcp-credential.dpapi", DarlingManagedPostgres.McpCredentialPathFor(@"D:\darling\pg\"));
        Assert.Equal("pg-mcp-credential.dpapi", DarlingManagedPostgres.McpCredentialFileName);
        Assert.Equal("mcp", DarlingManagedPostgres.McpRoleName);
    }

    [Fact]
    public void McpStoredCredential_DpapiRoundTrip_DerivesMcpConnectionString()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var root = Directory.CreateTempSubdirectory("darling-mcpcred-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var config = new PostgresConfig { Managed = true, Port = 5992, DataDirectory = dataDirectory };

            /* No mcp credential yet → null (the MCP host polls for it after the worker provisions it). */
            Assert.Null(DarlingManagedPostgres.TryBuildMcpConnectionStringFromStoredCredential(config));

            var password = DarlingManagedPostgres.GeneratePassword();
            File.WriteAllText(DarlingManagedPostgres.McpCredentialPathFor(dataDirectory), DarlingSecrets.Protect(password));

            var derived = DarlingManagedPostgres.TryBuildMcpConnectionStringFromStoredCredential(config);
            Assert.NotNull(derived);
            var parsed = new NpgsqlConnectionStringBuilder(derived);

            /* The mcp pool connects as the mcp role over the explicit IPv4 loopback, same search path. */
            Assert.Equal("127.0.0.1", parsed.Host);
            Assert.Equal(5992, parsed.Port);
            Assert.Equal("mcp", parsed.Username);
            Assert.Equal(password, parsed.Password);
            Assert.Equal("darling", parsed.Database);
            Assert.Equal("collect,config,public", parsed.SearchPath);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void CertificateSanCoversIp_TrueForItsIpSan_FalseForOthers()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Parse("192.168.1.205"));
        san.AddDnsName("test-host");
        request.CertificateExtensions.Add(san.Build());
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));

        Assert.True(DarlingManagedPostgres.CertificateSanCoversIp(cert, IPAddress.Parse("192.168.1.205")));
        Assert.False(DarlingManagedPostgres.CertificateSanCoversIp(cert, IPAddress.Parse("192.168.1.206")));
        Assert.False(DarlingManagedPostgres.CertificateSanCoversIp(cert, IPAddress.Parse("10.0.0.1")));
    }

    [Fact]
    public void EnsureServerCertificate_ReusesWhenSanCoversIp_RegeneratesOnIpChange()
    {
        /* The key hardening (DarlingFileSecurity.HardenFile) is Windows-only. */
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Cert key hardening is Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-cert-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var config = new PostgresConfig { Managed = true, Port = 5993, DataDirectory = dataDirectory };
            var pg = new DarlingManagedPostgres(config, NullLogger.Instance);

            /* The cert/key live beside the data directory, same convention as the credential files. */
            var certPath = Path.Combine(root.FullName, DarlingManagedPostgres.ServerCertFileName);
            var keyPath = Path.Combine(root.FullName, DarlingManagedPostgres.ServerKeyFileName);

            /* First generation for IP A. */
            pg.EnsureServerCertificate(IPAddress.Parse("192.168.1.205"), certPath, keyPath);
            Assert.True(File.Exists(certPath) && File.Exists(keyPath));
            var certA = File.ReadAllBytes(certPath);

            /* Same IP -> reuse (SAN covers it), bytes unchanged. */
            pg.EnsureServerCertificate(IPAddress.Parse("192.168.1.205"), certPath, keyPath);
            Assert.Equal(certA, File.ReadAllBytes(certPath));

            /* Different IP -> regenerate (stale SAN would break verify-full); the new cert covers B. */
            pg.EnsureServerCertificate(IPAddress.Parse("192.168.1.206"), certPath, keyPath);
            var certB = File.ReadAllBytes(certPath);
            Assert.NotEqual(certA, certB);
            using var reloaded = X509Certificate2.CreateFromPem(File.ReadAllText(certPath));
            Assert.True(DarlingManagedPostgres.CertificateSanCoversIp(reloaded, IPAddress.Parse("192.168.1.206")));
            Assert.False(DarlingManagedPostgres.CertificateSanCoversIp(reloaded, IPAddress.Parse("192.168.1.205")));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Bootstrap_EndToEnd_FirstRunThenIdempotentSecondRun_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe; " +
            "Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under artifacts\\pg-runtime-work\\assemble\\pg-runtime) " +
            "to run the managed-Postgres bootstrap E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pgboot-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };

        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            /* First run: initdb (scram + generated credential) + conf append + start + create db. */
            var connectionString = await owner.EnsureRunningAsync(timeout.Token);
            Assert.True(owner.StartedByThisProcess);
            Assert.True(File.Exists(Path.Combine(dataDirectory, "PG_VERSION")));

            var credentialPath = DarlingManagedPostgres.CredentialPathFor(dataDirectory);
            Assert.True(File.Exists(credentialPath));
            var credentialBytes = File.ReadAllBytes(credentialPath);

            var conf = File.ReadAllText(Path.Combine(dataDirectory, "postgresql.conf"));
            Assert.Contains("shared_preload_libraries = 'timescaledb'", conf, StringComparison.Ordinal);
            Assert.Contains("listen_addresses = '127.0.0.1'", conf, StringComparison.Ordinal);
            /* HypertableCount, not HypertableTables.Count: the product sizes workers from the TRUE
               hypertable count (catalog + collection_log, the V23 non-catalog hypertable). */
            Assert.Contains($"max_worker_processes = {3 + (TimescaleSupport.HypertableCount + 2) + 8}", conf, StringComparison.Ordinal);

            /* v3 memory sizing rode the SAME append path on first run, derived from THIS host's physical RAM
               (the exact MB depend on the runner, so pin the marker + that the settings are present). */
            Assert.Contains(DarlingManagedPostgres.ConfMarkerV3, conf, StringComparison.Ordinal);
            Assert.Contains("shared_buffers = ", conf, StringComparison.Ordinal);
            Assert.Contains("work_mem = ", conf, StringComparison.Ordinal);

            /* v4 write throughput rode the same first-run append: connection headroom + WAL ceiling. */
            Assert.Contains(DarlingManagedPostgres.ConfMarkerV4, conf, StringComparison.Ordinal);

            /* v5 co-located sizing rode the same first-run append (its shared_buffers override equals the
               v3 value on a fresh cluster, since both now derive through the same 1 GB cap). */
            Assert.Contains(DarlingManagedPostgres.ConfMarkerV5, conf, StringComparison.Ordinal);
            Assert.Contains("max_connections = 200", conf, StringComparison.Ordinal);
            Assert.Contains("max_wal_size = 4GB", conf, StringComparison.Ordinal);

            /* v6 log rotation rode the same first-run append, and the server ACCEPTED it (a bad line here
               fails pg_ctl start outright) — the logging collector is live, proven by the weekday ring file
               it creates under <data>\log the moment it starts (#1652). */
            Assert.Contains(DarlingManagedPostgres.ConfMarkerV6, conf, StringComparison.Ordinal);
            var ringFiles = Directory.GetFiles(Path.Combine(dataDirectory, "log"), "postgresql-*.log");
            Assert.NotEmpty(ringFiles);

            /* v7 compression memory (#1777) rode the same first-run append. Its value derives from THIS
               host's RAM, so pin the marker and capture the conf's EFFECTIVE value (the last assignment,
               which is the one the server honors) to compare against the live setting below. */
            Assert.Contains(DarlingManagedPostgres.ConfMarkerV7, conf, StringComparison.Ordinal);
            var confMaintenanceWorkMem = LastSettingValue(conf, "maintenance_work_mem");
            Assert.NotNull(confMaintenanceWorkMem);

            /* The derived credential really authenticates (scram, not trust) into the darling
               database — and the server started with our appended conf, so the timescaledb
               preload line was accepted; the v2 worker sizing was accepted too (the setting is
               live, not just written). */
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(timeout.Token);
                using var current = new NpgsqlCommand(
                    "SELECT current_database(), current_user, current_setting('max_worker_processes'), current_setting('work_mem'), current_setting('shared_buffers'), " +
                    "pg_size_bytes(current_setting('maintenance_work_mem')), pg_size_bytes(@confMaintenance)",
                    connection);
                current.Parameters.AddWithValue("confMaintenance", confMaintenanceWorkMem);
                using var reader = await current.ExecuteReaderAsync(timeout.Token);
                Assert.True(await reader.ReadAsync(timeout.Token));
                Assert.Equal("darling", reader.GetString(0));
                Assert.Equal("darling", reader.GetString(1));
                /* Derived, not hard-pinned (a "40" pin from the 27-hypertable era went stale when
                   collectors were added): the same HypertableCount formula BuildWorkerSizingConfAppend
                   writes into the conf, proven LIVE here. */
                Assert.Equal((3 + (TimescaleSupport.HypertableCount + 2) + 8).ToString(CultureInfo.InvariantCulture), reader.GetString(2));
                /* The v3 memory block is LIVE, not merely written: work_mem and shared_buffers hold our
                   derived values (>= the 16 MB work_mem floor / 25%-of-RAM shared_buffers on any real host),
                   never the stock 4 MB / 128 MB defaults. */
                Assert.NotEqual("4MB", reader.GetString(3));
                Assert.NotEqual("128MB", reader.GetString(4));

                /* #1777: the v7 override is LIVE, not merely written — the server holds exactly the conf's
                   effective (last) assignment, never the stock 64 MB default. Compared in BYTES because
                   PostgreSQL normalizes units on the way out: a conf line of "2048MB" reads back as "2GB",
                   the same setting and a failed string compare (seen live on a large-RAM runner). */
                Assert.Equal(reader.GetInt64(6), reader.GetInt64(5));
                Assert.NotEqual(64L * 1024 * 1024, reader.GetInt64(5));
            }

            /* Second EnsureRunning against the live server: idempotent — no re-init (credential
               bytes untouched), no ownership grab (this instance did not start the server),
               the same derived connection string, and no duplicate conf blocks (one v1 marker,
               one v2 marker). */
            var second = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var secondConnectionString = await second.EnsureRunningAsync(timeout.Token);
            Assert.False(second.StartedByThisProcess);
            Assert.Equal(credentialBytes, File.ReadAllBytes(credentialPath));
            Assert.Equal(connectionString, secondConnectionString);

            var confAfterSecond = File.ReadAllText(Path.Combine(dataDirectory, "postgresql.conf"));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarker));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarkerV2));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarkerV3));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarkerV4));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarkerV5));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarkerV6));
            Assert.Equal(1, CountOccurrences(confAfterSecond, DarlingManagedPostgres.ConfMarkerV7));

            /* Both up/down probes below must bypass Npgsql's pool: OpenAsync on a pooled string
               can hand back an idle socket with no I/O at all, which "succeeds" against a stopped
               server — the refused-connection assert below failed exactly that way live. */
            var unpooled = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;

            /* A non-owner's stop must be a no-op — the server keeps accepting connections. */
            await second.StopIfStartedByThisProcessAsync();
            await using (var stillUp = new NpgsqlConnection(unpooled))
            {
                await stillUp.OpenAsync(timeout.Token);
            }

            /* The owner's stop is real: fast shutdown, then connections are refused. */
            await owner.StopIfStartedByThisProcessAsync();
            Assert.False(owner.StartedByThisProcess);
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using var refused = new NpgsqlConnection(unpooled);
                await refused.OpenAsync(timeout.Token);
            });
        }
        finally
        {
            /* Idempotent when the happy path already stopped it; the safety net when an assert
               threw mid-flight. */
            await owner.StopIfStartedByThisProcessAsync();
            TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// #1777 PROPAGATION, proven against a real server: an EXISTING store — one whose conf carries the v3
    /// block written under the old <c>min(5% RAM, 1 GB)</c> rule and no v7 marker — must adopt the raised
    /// maintenance_work_mem on its next service-owned start. This is the half that actually reaches the
    /// field; a formula change alone would only ever have applied to a fresh initdb, and the boxes that
    /// need it are already collecting.
    ///
    /// <para>The pre-#1777 conf is reconstructed exactly, not approximated: the v7 block is removed (it is
    /// the last thing appended, so truncating at its marker restores the old file byte-for-byte) and the v3
    /// block's value is rewritten to 819 MB, which is what the old formula produced on a 16 GB host — the
    /// RAM class the field measurement came from. The BEFORE reading is taken from the live server, so the
    /// old value is proven in effect before the new one is proven to replace it.</para>
    /// </summary>
    [Fact]
    public async Task ExistingStore_AdoptsRaisedMaintenanceWorkMem_OnNextStart_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe; " +
            "Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under artifacts\\pg-runtime-work\\assemble\\pg-runtime) " +
            "to run the #1777 conf-propagation E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pgv7-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        const string legacyValue = "819MB";

        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));

            /* A real store, provisioned the normal way. */
            await owner.EnsureRunningAsync(timeout.Token);
            await owner.StopIfStartedByThisProcessAsync();

            /* Rewind the conf to its pre-#1777 shape: drop the v7 block (appended last, so the marker is a
               clean truncation point) and put the OLD formula's 16 GB landing value in the v3 block. */
            var fresh = await File.ReadAllTextAsync(confPath, timeout.Token);
            var v7Index = fresh.IndexOf(DarlingManagedPostgres.ConfMarkerV7, StringComparison.Ordinal);
            Assert.True(v7Index > 0, "The fresh conf should carry the v7 block before it is rewound.");
            var derivedValue = LastSettingValue(fresh, "maintenance_work_mem");
            Assert.NotNull(derivedValue);
            /* The whole test turns on before != after. A host with ~3.2 GB RAM would derive exactly 819MB
               through the 25% guard and make the comparison vacuous — that is a property of the RUNNER,
               not a product failure, so skip rather than pass emptily. */
            Assert.SkipWhen(string.Equals(derivedValue, legacyValue, StringComparison.Ordinal),
                $"This host derives maintenance_work_mem = {derivedValue}, the same value the test uses as the legacy reading.");

            var legacyConf = fresh[..v7Index]
                .Replace($"maintenance_work_mem = {derivedValue}", $"maintenance_work_mem = {legacyValue}", StringComparison.Ordinal);
            await File.WriteAllTextAsync(confPath, legacyConf, timeout.Token);

            Assert.DoesNotContain(DarlingManagedPostgres.ConfMarkerV7, legacyConf, StringComparison.Ordinal);
            Assert.Equal(legacyValue, LastSettingValue(legacyConf, "maintenance_work_mem"));

            /* The service-owned start: EnsureConfAppended heals BEFORE pg_ctl start, so the raised value is
               live on this very start rather than one restart later. That ordering is what makes the live
               assertion below decisive — without the v7 append this server would have come up on the
               rewound conf and reported the legacy 819MB. */
            var healedOwner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var healedConnectionString = await healedOwner.EnsureRunningAsync(timeout.Token);
            try
            {
                var healedConf = await File.ReadAllTextAsync(confPath, timeout.Token);
                Assert.Equal(1, CountOccurrences(healedConf, DarlingManagedPostgres.ConfMarkerV7));
                Assert.Equal(derivedValue, LastSettingValue(healedConf, "maintenance_work_mem"));
                /* Appended, never rewritten in place — the legacy line is still there, just outvoted. */
                Assert.Contains($"maintenance_work_mem = {legacyValue}", healedConf, StringComparison.Ordinal);

                var (live, expected) = await ReadSettingAndLiteralBytesAsync(
                    healedConnectionString, "maintenance_work_mem", derivedValue, timeout.Token);
                Assert.Equal(expected, live);
                Assert.NotEqual(819L * 1024 * 1024, live);
            }
            finally
            {
                await healedOwner.StopIfStartedByThisProcessAsync();
            }

            /* A third start must not append a second v7 block. */
            Assert.Equal(1, CountOccurrences(await File.ReadAllTextAsync(confPath, timeout.Token), DarlingManagedPostgres.ConfMarkerV7));
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// Reads one live GUC and one postgresql.conf size literal, both as BYTES, through an UNPOOLED
    /// connection — so the reading always costs real I/O against the server running right now rather than a
    /// recycled idle socket.
    ///
    /// <para>Bytes rather than the raw strings, and measured BY THE SERVER rather than by reimplementing
    /// PostgreSQL's unit parsing here: the server normalizes memory units on the way out, so a conf line of
    /// <c>2048MB</c> reads back as <c>2GB</c> — the same setting, and a string compare that fails. That is
    /// not hypothetical; it is what a large-RAM runner did to the first version of this test.</para>
    /// </summary>
    private static async Task<(long Live, long Expected)> ReadSettingAndLiteralBytesAsync(
        string connectionString, string setting, string literal, CancellationToken cancellationToken)
    {
        var unpooled = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;
        await using var connection = new NpgsqlConnection(unpooled);
        await connection.OpenAsync(cancellationToken);
        using var command = new NpgsqlCommand(
            "SELECT pg_size_bytes(current_setting(@setting)), pg_size_bytes(@literal)", connection);
        command.Parameters.AddWithValue("setting", setting);
        command.Parameters.AddWithValue("literal", literal);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        Assert.True(await reader.ReadAsync(cancellationToken));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static int CountOccurrences(string text, string value)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(value, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += value.Length;
        }

        return count;
    }

    private static int FindFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>Postgres releases its files a beat after fast shutdown — retry the temp-dir delete.</summary>
    private static void TryDeleteRecursive(string path)
    {
        for (var attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                Directory.Delete(path, recursive: true);
                return;
            }
            catch (IOException)
            {
                Thread.Sleep(500);
            }
            catch (UnauthorizedAccessException)
            {
                Thread.Sleep(500);
            }
        }
    }

    /* ============ #2186: the bootstrap's failure messages, in the operator's words ============
       These pin the SHIPPED strings, not the decoder — DarlingToolExitCodeTests owns the decode.
       The distinction is the whole point: #1738 was a correct check that nothing invoked, and a
       correct decoder no message calls would be the same defect wearing a new hat. */

    private const int StatusDllNotFound = unchecked((int)0xC0000135);
    private const string FieldBinDirectory = @"C:\PerformanceMonitorDarling\pg-runtime\pgsql\bin";
    private const string FieldDataDirectory = @"C:\ProgramData\PerformanceMonitorDarling\pg";

    /// <summary>
    /// The reported failure, rebuilt from the field's own numbers: exit -1073741515 and an empty capture.
    /// Every clause the report was missing has to be present, and the clause it HAD has to survive — field
    /// reports and the issue tracker are searchable by "initdb failed (exit code", so the fix must not
    /// rename the thing operators paste into search.
    /// </summary>
    [Fact]
    public void InitDbFailureMessage_TurnsTheFieldReportIntoADiagnosis()
    {
        var message = DarlingManagedPostgres.BuildInitDbFailureMessage(
            -1073741515, Path.Combine(FieldBinDirectory, "initdb.exe"), FieldDataDirectory, string.Empty);

        Assert.StartsWith("initdb failed (exit code -1073741515", message, StringComparison.Ordinal);
        Assert.Contains(FieldDataDirectory, message, StringComparison.Ordinal);

        /* What the number means, and that Windows rather than PostgreSQL set it. */
        Assert.Contains("0xC0000135", message, StringComparison.Ordinal);
        Assert.Contains("STATUS_DLL_NOT_FOUND", message, StringComparison.Ordinal);

        /* The empty field is stated as expected. The report's "Output:" trailing a blank line is what made
           the whole thing read as missing data. */
        Assert.Contains("Output:", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Output:\n\n", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Output:\n(none)", message, StringComparison.Ordinal);
        Assert.Contains("expected", message, StringComparison.OrdinalIgnoreCase);

        /* Both causes, and the directory the DLLs are supposed to be in. */
        Assert.Contains(FieldBinDirectory, message, StringComparison.Ordinal);
        Assert.Contains("vcruntime140_1.dll", message, StringComparison.Ordinal);
        Assert.Contains("NT SERVICE", message, StringComparison.Ordinal);
    }

    /// <summary>An ordinary initdb failure is left to speak for itself: its own stderr is the diagnosis,
    /// and it must not be pushed below a screen of loader boilerplate that does not apply.</summary>
    [Fact]
    public void InitDbFailureMessage_LeavesARealInitDbErrorAlone()
    {
        const string stderr = "initdb: error: directory \"C:\\pg\" exists but is not empty";

        var message = DarlingManagedPostgres.BuildInitDbFailureMessage(
            1, Path.Combine(FieldBinDirectory, "initdb.exe"), FieldDataDirectory, stderr);

        Assert.Equal($"initdb failed (exit code 1) for {FieldDataDirectory}.\nOutput:\n{stderr}", message);
    }

    /// <summary>
    /// pg_ctl status's own exit 4 really does mean the data directory is unusable, and the message keeps
    /// saying so. A Windows status means pg_ctl never ran — keeping the verdict there would point an
    /// operator at deleting a healthy store to fix a missing DLL.
    /// </summary>
    [Fact]
    public void StatusFailureMessage_BlamesTheDataDirectoryOnlyWhenPgCtlActuallySaidSo()
    {
        var pgCtl = Path.Combine(FieldBinDirectory, "pg_ctl.exe");

        var pgCtlVerdict = DarlingManagedPostgres.BuildStatusFailureMessage(4, pgCtl, FieldDataDirectory, "pg_ctl: could not open ...");
        Assert.Contains("the data directory is not usable", pgCtlVerdict, StringComparison.Ordinal);

        var loaderFailure = DarlingManagedPostgres.BuildStatusFailureMessage(StatusDllNotFound, pgCtl, FieldDataDirectory, string.Empty);
        Assert.DoesNotContain("the data directory is not usable", loaderFailure, StringComparison.Ordinal);
        Assert.Contains("STATUS_DLL_NOT_FOUND", loaderFailure, StringComparison.Ordinal);
    }

    /// <summary>
    /// The start failure's log tail has the initdb message's trap in another costume: on a loader status
    /// pg_ctl never started a postmaster, so "(no server log written)" is true and useless. The diagnosis
    /// has to arrive BEFORE the tail invites an operator to go read a log that was never going to exist.
    /// </summary>
    [Fact]
    public void StartFailureMessage_DiagnosesBeforeItPointsAtAnEmptyServerLog()
    {
        var message = DarlingManagedPostgres.BuildStartFailureMessage(
            StatusDllNotFound, Path.Combine(FieldBinDirectory, "pg_ctl.exe"), FieldDataDirectory, "(no server log written)");

        var diagnosis = message.IndexOf("STATUS_DLL_NOT_FOUND", StringComparison.Ordinal);
        var tail = message.IndexOf("Server log tail:", StringComparison.Ordinal);

        Assert.True(diagnosis >= 0, "the start failure must decode a Windows status");
        Assert.True(tail > diagnosis, "the loader diagnosis has to precede the server-log tail it explains");
    }

    /// <summary>
    /// The wiring, pinned at the source: three correct builders that no throw site calls would leave the
    /// shipped message exactly as it was reported. Behavioral coverage cannot reach these — reproducing
    /// them needs a bundled Postgres that dies in the Windows loader, which is not something a CI runner
    /// can be asked to arrange.
    /// </summary>
    [Fact]
    public void TheBootstrapThrowSitesActuallyUseTheseMessages()
    {
        var source = ReadManagedPostgresSource();

        Assert.Contains("BuildInitDbFailureMessage(exitCode, initDb, _dataDirectory, output, runtimeProbe));", source, StringComparison.Ordinal);
        Assert.Contains("throw new InvalidOperationException(BuildStatusFailureMessage(exitCode, pgCtl, _dataDirectory, output)),", source, StringComparison.Ordinal);
        Assert.Contains("BuildStartFailureMessage(exitCode, pgCtl, _dataDirectory, ReadServerLogTail())", source, StringComparison.Ordinal);

        /* And that no bootstrap failure went back to interpolating the bare code. */
        Assert.DoesNotContain("exit code {exitCode}", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2185: the runtime probe is GATED on a loader status, pinned at the source because the gate is the
    /// whole design and behavioral coverage cannot reach it.
    ///
    /// <para>Two things would go wrong ungated. Every ordinary initdb failure — a non-empty data directory,
    /// a bad locale, a permissions refusal — would launch two extra processes and then append a paragraph
    /// about DLL loading to an error that has nothing to do with loading, which is worse than silence
    /// because it sends the operator down the wrong path. And the probe only MEANS anything against a
    /// loader status: "both binaries load fine" is a useful finding when Windows just refused to load one,
    /// and noise otherwise.</para>
    /// </summary>
    [Fact]
    public void TheRuntimeProbeOnlyRunsForALoaderStatus()
    {
        var source = ReadManagedPostgresSource();

        Assert.Contains("DarlingToolExitCode.IsLoaderStatus(exitCode)", source, StringComparison.Ordinal);
        Assert.Contains("? await ProbeRuntimeBinariesAsync(binDirectory, cancellationToken)", source, StringComparison.Ordinal);
        Assert.Contains(": string.Empty;", source, StringComparison.Ordinal);
    }

    private static string ReadManagedPostgresSource([CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
