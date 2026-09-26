/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
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
    /// The v9 session-time-zone block: the belt behind
    /// <c>StoreSqlClockDisciplineTests</c>. The store's timestamp columns hold naive UTC while initdb takes
    /// <c>timezone</c> from the host OS, so on a Windows box outside UTC every comparison of a naive column
    /// against <c>now()</c> has the naive side converted at the machine's local zone. Pinning UTC makes that
    /// conversion the identity — for MANAGED stores only, which is why it is a backstop and not the fix.
    ///
    /// <para>Nothing else may ride in this block: it is appended after the v8 hardware check, whose
    /// staleness test keys on the last fingerprint line in the text read before any of these appends. A
    /// sizing line here would be both invisible to that check and able to override it.</para>
    /// </summary>
    [Fact]
    public void TimeZoneConfAppend_PinsV9Marker_AndCarriesNothingButTheZone()
    {
        var block = DarlingManagedPostgres.BuildTimeZoneConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV9, block, StringComparison.Ordinal);
        Assert.Contains("timezone = 'UTC'", block, StringComparison.Ordinal);

        /* No fingerprint line, or the v8 staleness check silently stops checking. */
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, block, StringComparison.Ordinal);

        /* The blocks compose, they don't compete. */
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
        Assert.DoesNotContain("maintenance_work_mem", block, StringComparison.Ordinal);
        Assert.DoesNotContain("max_worker_processes", block, StringComparison.Ordinal);

        /* `timezone`, not `log_timezone`: the session zone is what resolves a mixed timestamp/timestamptz
           comparison. Setting only the log zone would change what the log says and nothing about the data. */
        Assert.DoesNotContain("log_timezone", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The v11 job-execution-logging block (#3175). This block exists for a reason no other one does: the
    /// setting was written into the <b>v1</b> block by #1681, and v1's marker is present on every
    /// pre-existing cluster, so the append that carried it was skipped and the GUC reached fresh initdbs
    /// only. It is asserted as a LAST-OCCURRENCE value rather than a substring, because that is what
    /// PostgreSQL honours and what makes the block an override rather than a hope.
    /// </summary>
    [Fact]
    public void JobExecutionLoggingConfAppend_PinsV11Marker_AndTurnsTheGucOn()
    {
        var block = DarlingManagedPostgres.BuildJobExecutionLoggingConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV11, block, StringComparison.Ordinal);
        Assert.Equal("on", LastSettingValue(block, StoreSelfMetrics.JobExecutionLoggingSetting));

        /* No fingerprint line, or the v8 staleness check silently stops checking (it reads the conf as it
           stood before any of these appends, and only holds while no later block writes one). */
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, block, StringComparison.Ordinal);

        /* The blocks compose, they don't compete: this one restates exactly one setting. Every v1 name is
           taken from the v1 builder rather than retyped, so a v1 that gained a setting is covered here too. */
        foreach (var name in SettingNames(DarlingManagedPostgres.BuildConfAppend(5641)))
        {
            Assert.Null(LastSettingValue(block, name));
        }
    }

    /// <summary>
    /// The MOVE, pinned in both directions (#3175): the GUC is in the v11 block and is NOT in the v1 block.
    ///
    /// <para>Both halves matter and neither alone is the claim. Present-in-v11 alone would pass with a
    /// duplicate left behind in v1 — harmless at runtime, and exactly the reading that produced the defect:
    /// the repository would still assert this setting in the one block that provably cannot deliver it.
    /// Absent-from-v1 alone would pass if the setting were dropped entirely.</para>
    /// </summary>
    [Fact]
    public void TheJobExecutionLoggingGuc_IsInV11AndNotInTheUnhealableV1Block()
    {
        var v1 = DarlingManagedPostgres.BuildConfAppend(5641);
        var v11 = DarlingManagedPostgres.BuildJobExecutionLoggingConfAppend();

        Assert.Null(LastSettingValue(v1, StoreSelfMetrics.JobExecutionLoggingSetting));
        Assert.DoesNotContain(StoreSelfMetrics.JobExecutionLoggingSetting, v1, StringComparison.Ordinal);
        Assert.Equal("on", LastSettingValue(v11, StoreSelfMetrics.JobExecutionLoggingSetting));
    }

    /// <summary>
    /// The v1 block's CONTENT IS FROZEN, and this is the pin whose absence let #3175 happen (#1681 added a
    /// setting here and nothing said anything).
    ///
    /// <para><b>Why a freeze rather than a minimum.</b> v1's marker is present on every cluster that
    /// already exists, and <c>EnsureConfAppended</c> skips a block whose marker it finds — so a setting
    /// added to this block can only ever reach a cluster that is initdb'd afterwards. That is not a
    /// property of any particular setting; it is a property of the BLOCK. A new setting therefore needs its
    /// own marker, and this test is the thing that says so at the moment someone types it into the wrong
    /// builder rather than a release later.</para>
    ///
    /// <para>The three names are load-bearing beyond the count: <c>shared_preload_libraries</c> is
    /// list-valued and a later assignment REPLACES the list rather than extending it, and
    /// <c>listen_addresses</c>/<c>port</c> govern who can reach the store — which is why this block must
    /// never be re-appended to a cluster that already has it, and why the fix for #3175 is a new marker
    /// rather than a looser match on this one.</para>
    /// </summary>
    [Fact]
    public void ConfV1Block_ContentIsFrozen_ANewSettingNeedsItsOwnMarker()
    {
        var names = SettingNames(DarlingManagedPostgres.BuildConfAppend(5641));

        Assert.Equal(
            new[] { "default_toast_compression", "listen_addresses", "port", "shared_preload_libraries" },
            names.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// Healing an EXISTING cluster's conf adds the v11 block and re-applies NO v1 content (#3175) — the
    /// structural half of "do not simply widen the v1 marker", stated as an invariant over the file rather
    /// than as prose on the marker.
    ///
    /// <para><b>What widening would have done, and why it is measured harm rather than tidiness.</b> Making
    /// v1's check ask "is the GUC line present?" would re-append the whole shared v1 block to every
    /// pre-existing cluster. Measured on TimescaleDB 2.30.0/PG17: a conf carrying an operator's
    /// <c>shared_preload_libraries = 'timescaledb,pg_stat_statements'</c> came back up serving
    /// <c>'timescaledb'</c> alone once the v1 block was appended behind it, because the GUC is list-valued
    /// and the last assignment replaces the list. This test fails the moment any v1 setting appears twice.
    /// </para>
    ///
    /// <para>The v1 names are DERIVED from the v1 builder rather than listed, so a v1 that gains a setting
    /// (which <see cref="ConfV1Block_ContentIsFrozen_ANewSettingNeedsItsOwnMarker"/> forbids) is covered
    /// here without editing this test.</para>
    ///
    /// <para><b>The fixture carries initdb's COMMENTED defaults, and that is load-bearing.</b> A real
    /// postgresql.conf documents every setting as a commented line before the product appends anything, so
    /// a fixture built only from the product's own blocks cannot see an instrument that counts a comment as
    /// an assignment — and the first version of this test used one. It passed here and failed against a
    /// live cluster, where <c>#shared_preload_libraries = ''</c> made a substring count read 2 on a
    /// perfectly healthy file. Counting through <see cref="CountAssignments"/> against a fixture that
    /// contains the decoys is what makes "exactly one assignment" a claim about the product rather than
    /// about the fixture.</para>
    /// </summary>
    [Fact]
    public void HealingAConfWithoutV11_AppendsOnlyThatBlock_AndReAppliesNoV1Setting()
    {
        /* initdb's generated preamble, in shape: every setting present as a COMMENTED line, including the
           ones the product also assigns. These are decoys for any instrument that counts substrings. */
        const string StockPreamble =
            "# -----------------------------\n" +
            "# PostgreSQL configuration file\n" +
            "# -----------------------------\n" +
            "#shared_preload_libraries = ''\t# (change requires restart)\n" +
            "#port = 5432\t\t\t\t# (change requires restart)\n" +
            "#listen_addresses = 'localhost'\t\t# (change requires restart)\n" +
            "#default_toast_compression = 'pglz'\t# 'pglz' or 'lz4'\n" +
            "#timescaledb.enable_job_execution_logging = off\n";

        /* The field shape: a cluster whose conf carries v1 (and every later marker) but no v11 block, so
           the GUC has no live assignment and nothing in the file says so. */
        var existing = StockPreamble
            + DarlingManagedPostgres.BuildConfAppend(5641)
            + DarlingManagedPostgres.BuildTimeZoneConfAppend()
            + DarlingManagedPostgres.BuildMessageLocaleConfAppend();

        /* The pre-heal reading, asserted rather than assumed: absent, not off. ZERO live assignments — the
           commented decoy above is not one — which is what made the effective value `off` with
           `source = default` in the field. */
        Assert.Equal(0, CountAssignments(existing, StoreSelfMetrics.JobExecutionLoggingSetting));
        Assert.Null(LastSettingValue(existing, StoreSelfMetrics.JobExecutionLoggingSetting));

        var healed = existing + DarlingManagedPostgres.BuildJobExecutionLoggingConfAppend();

        /* The heal: exactly one v11 block, and the GUC now has exactly one assignment, whose value is on. */
        Assert.Equal(1, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV11));
        Assert.Equal(1, CountAssignments(healed, StoreSelfMetrics.JobExecutionLoggingSetting));
        Assert.Equal("on", LastSettingValue(healed, StoreSelfMetrics.JobExecutionLoggingSetting));

        /* And nothing else moved: every v1 setting still has exactly ONE live assignment, so no part of the
           shared v1 block was re-applied. A widened v1 match would put every one of these at two. */
        foreach (var name in SettingNames(DarlingManagedPostgres.BuildConfAppend(5641)))
        {
            Assert.Equal(1, CountAssignments(healed, name));
        }

        /* The v1 marker itself is untouched, which is the direct statement that v1 did not re-fire. */
        Assert.Equal(1, CountOccurrences(healed, DarlingManagedPostgres.ConfMarker));
    }

    /// <summary>
    /// Every managed conf marker is distinct, and none is a SUBSTRING of another (#3175). Enumerated by
    /// reflection over the <c>ConfMarker*</c> constants, so a v12 is covered on the commit that adds it.
    ///
    /// <para><b>Why substring and not just equality.</b> <c>EnsureConfAppended</c> asks each question as
    /// <c>conf.Contains(marker)</c>. If one marker were a prefix or substring of another, a cluster
    /// carrying only the longer block would answer "present" for the shorter one and silently never gain
    /// it — the #3175 failure reproduced by a different route, and one that a rewording could introduce by
    /// accident. Note the v1 marker is nearly a prefix of every later one and is saved only by the
    /// parenthesised version segment sitting where v1 has <c> -- </c>; that is not obvious by eye, which is
    /// why it is asserted.</para>
    /// </summary>
    [Fact]
    public void EveryConfMarker_IsDistinct_AndNoneIsASubstringOfAnother()
    {
        /* Public AND NonPublic, matching StoreLogSeverityLocaleTests.DeclaredConfMarkers: a marker does not
           have to be public to be asked about by EnsureConfAppended, and the substring hazard is a property
           of the Contains check, not of the accessibility of the constant it reads. */
        var markers = typeof(DarlingManagedPostgres)
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.StartsWith("ConfMarker", StringComparison.Ordinal))
            .Select(f => (f.Name, Value: (string)f.GetRawConstantValue()!))
            .ToArray();

        /* A positive control on the enumeration itself: an empty or one-element set would make every
           assertion below vacuously true, and a reflection filter that stopped matching is exactly the
           silent failure this shape invites. Eleven blocks as of #3175; twelve as of #3802 (v12 WAL sizing);
           thirteen as of #3899 (v13 statement statistics); fourteen as of #3909 (v14 PostgreSQL 17
           maintenance_work_mem limit); fifteen as of #4246 (v15 WAL compression). */
        Assert.Equal(15, markers.Length);

        Assert.Equal(markers.Length, markers.Select(m => m.Value).Distinct(StringComparer.Ordinal).Count());

        foreach (var (name, value) in markers)
        {
            foreach (var (otherName, otherValue) in markers)
            {
                if (string.Equals(name, otherName, StringComparison.Ordinal))
                {
                    continue;
                }

                Assert.False(
                    otherValue.Contains(value, StringComparison.Ordinal),
                    $"{name} is a substring of {otherName}, so EnsureConfAppended's Contains check for {name} " +
                    "would be satisfied by a cluster that only ever gained the other block.");
            }
        }
    }

    /// <summary>
    /// The v13 block (#3899): two settings and nothing else — the preload list, MERGED so it keeps whatever the
    /// file already loaded, and utility tracking off. The second is a security setting: provisioning puts each
    /// role password in an ALTER ROLE literal every start, and with utility tracking on pg_stat_statements
    /// records that statement verbatim (measured on the bundled 18.4 / 1.12).
    /// </summary>
    [Fact]
    public void V13Block_MergesTheLibraryIntoTheEffectiveList_AndTurnsUtilityTrackingOff()
    {
        var block = DarlingManagedPostgres.BuildStatementStatisticsConfAppend("timescaledb,auto_explain");

        Assert.Equal(1, CountOccurrences(block, DarlingManagedPostgres.ConfMarkerV13));
        Assert.Equal(
            new[] { "pg_stat_statements.track_utility", "shared_preload_libraries" },
            SettingNames(block).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal("timescaledb,auto_explain,pg_stat_statements", LastConfAssignment(block, "shared_preload_libraries"));
        Assert.Equal("off", LastConfAssignment(block, "pg_stat_statements.track_utility"));
    }

    /// <summary>The merge adds the library once and keeps every other name, spelling and order; an empty or
    /// absent list merges from timescaledb, the one library a managed store must never lose (#3899).</summary>
    [Theory]
    [InlineData(null, "timescaledb,pg_stat_statements")]
    [InlineData("", "timescaledb,pg_stat_statements")]
    [InlineData("timescaledb", "timescaledb,pg_stat_statements")]
    [InlineData("timescaledb,auto_explain", "timescaledb,auto_explain,pg_stat_statements")]
    [InlineData("timescaledb, pg_stat_statements", "timescaledb,pg_stat_statements")]
    [InlineData("\"timescaledb\" , PG_STAT_STATEMENTS", "timescaledb,PG_STAT_STATEMENTS")]
    [InlineData("auto_explain", "auto_explain,pg_stat_statements")]
    [InlineData("\"timescaledb,pg_stat_statements\"", "\"timescaledb,pg_stat_statements\",pg_stat_statements")]
    public void MergePreloadLibraries_AddsTheLibraryOnce_AndKeepsEverythingElse(string? effective, string expected)
        => Assert.Equal(expected, DarlingManagedPostgres.MergePreloadLibraries(effective));

    /// <summary>The LAST live assignment is the one PostgreSQL honours; a commented default is not one, the
    /// <c>=</c> is optional, both quote escapes are read, and a longer setting name is not a match (#3899).</summary>
    [Theory]
    [InlineData("#shared_preload_libraries = ''\t# (change requires restart)\n", null)]
    [InlineData("shared_preload_libraries = 'timescaledb'\n", "timescaledb")]
    [InlineData("shared_preload_libraries = 'timescaledb'\nshared_preload_libraries = 'timescaledb,auto_explain'   # mine\n", "timescaledb,auto_explain")]
    [InlineData("shared_preload_libraries 'a,b'\n", "a,b")]
    [InlineData("shared_preload_libraries = timescaledb # a comment\n", "timescaledb")]
    [InlineData("shared_preload_libraries_extra = 'x'\n", null)]
    [InlineData("shared_preload_libraries = 'it''s'\n", "it's")]
    [InlineData("shared_preload_libraries = 'a\\'b'\n", "a'b")]
    [InlineData("  SHARED_PRELOAD_LIBRARIES='x'\r\n", "x")]
    [InlineData("", null)]
    public void ParseConfText_ReadsTheLastLiveAssignment(string conf, string? expected)
        => Assert.Equal(expected, LastConfAssignment(conf, "shared_preload_libraries"));

    /// <summary>
    /// THE fresh-cluster hazard (#3899), through the real heal. On a new cluster the only live preload assignment
    /// is written by the v1 append in the SAME heal, after the method read the file once; a v13 merge from that
    /// once-read text would see only initdb's commented default and write a list WITHOUT timescaledb. The heal
    /// re-reads the file for v13, and this proves it: the last live assignment is timescaledb plus the library,
    /// utility tracking is off, and a second heal appends no second v13 block.
    /// </summary>
    [Fact]
    public void FreshConfHeal_KeepsTimescaleInThePreloadList_AndASecondHealAppendsNoSecondV13()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            File.WriteAllText(confPath, "#shared_preload_libraries = ''\t# (change requires restart)\n#port = 5432\n");
            var pg = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 5995, DataDirectory = dataDirectory }, NullLogger.Instance);

            pg.EnsureConfAppended(dataDirectory);
            var first = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(first, DarlingManagedPostgres.ConfMarkerV13));
            Assert.Equal("timescaledb,pg_stat_statements", LastConfAssignment(first, "shared_preload_libraries"));
            Assert.Equal("off", LastConfAssignment(first, "pg_stat_statements.track_utility"));

            pg.EnsureConfAppended(dataDirectory);
            var second = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(second, DarlingManagedPostgres.ConfMarkerV13));
            Assert.Equal("timescaledb,pg_stat_statements", LastConfAssignment(second, "shared_preload_libraries"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// An EXISTING cluster whose operator extended the preload list keeps it (#3899): the heal appends v13 with
    /// the operator's list plus the library, not a fixed literal that would drop their addition — the
    /// list-replacement hazard <see cref="HealingAConfWithoutV11_AppendsOnlyThatBlock_AndReAppliesNoV1Setting"/>
    /// documents, applied to the one block that has to restate the list.
    /// </summary>
    [Fact]
    public void ExistingConfHeal_KeepsTheOperatorsPreloadLibraries()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13op-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            File.WriteAllText(confPath,
                DarlingManagedPostgres.BuildConfAppend(5641)
                + "shared_preload_libraries = 'timescaledb,auto_explain'   # the operator's own\n");
            var pg = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 5641, DataDirectory = dataDirectory }, NullLogger.Instance);

            pg.EnsureConfAppended(dataDirectory);

            var healed = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV13));
            Assert.Equal(
                "timescaledb,auto_explain,pg_stat_statements",
                LastConfAssignment(healed, "shared_preload_libraries"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The v13 ALTER SYSTEM pin (#3899), the v12 shape: an auto.conf preload list WITHOUT the library yields one
    /// warning naming the list as written, the precedence, and the exact statement that fixes it, written as
    /// ONE quoted literal PER library. #3904's review reproduced the first version's advice, the whole list in
    /// one literal, storing a single library name the store could not start with; this pins that the advice
    /// can never take that form again. A list that already carries the library, and no auto.conf at all, say
    /// nothing, and the file is never edited.
    /// </summary>
    [Fact]
    public void V13AlterSystemPreloadOverride_WithoutTheLibrary_IsLoggedWithAMultiLiteralFix_AndNothingIsEdited()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13auto-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var config = new PostgresConfig { Managed = true, Port = 5996, DataDirectory = dataDirectory };
            var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");

            var none = new CapturingTestLogger();
            new DarlingManagedPostgres(config, none).LogStatementStatisticsPreloadCoverage(dataDirectory);
            Assert.Equal("(no log lines captured)", none.Joined);

            File.WriteAllText(autoConfPath, "# Do not edit this file manually!\nshared_preload_libraries = 'timescaledb, pg_stat_statements'\n");
            var carried = new CapturingTestLogger();
            new DarlingManagedPostgres(config, carried).LogStatementStatisticsPreloadCoverage(dataDirectory);
            Assert.Equal("(no log lines captured)", carried.Joined);

            const string AutoConf = "# Do not edit this file manually!\n# It will be overwritten by the ALTER SYSTEM command.\nshared_preload_libraries = 'timescaledb'\n";
            File.WriteAllText(autoConfPath, AutoConf);
            var logger = new CapturingTestLogger();
            new DarlingManagedPostgres(config, logger).LogStatementStatisticsPreloadCoverage(dataDirectory);

            var line = Assert.Single(logger.Joined.Split(" | "));
            Assert.StartsWith("Warning: ", line, StringComparison.Ordinal);
            Assert.Contains("shared_preload_libraries = 'timescaledb'", line, StringComparison.Ordinal);
            Assert.Contains("AFTER postgresql.conf", line, StringComparison.Ordinal);
            Assert.Contains("ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_stat_statements'", line, StringComparison.Ordinal);
            Assert.DoesNotContain("'timescaledb,pg_stat_statements'", line, StringComparison.Ordinal);
            Assert.Equal(AutoConf, File.ReadAllText(autoConfPath));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The one-literal mistake's own signature (#3904's review): <c>ALTER SYSTEM SET shared_preload_libraries =
    /// 'timescaledb,pg_stat_statements'</c> is stored as ONE double-quoted library name, and the store will not
    /// start. The parser reads it as that one name (the first version split it on its commas, which is what
    /// hid it), and the coverage check says so as an Error with a fix that works on a store that is DOWN: the
    /// line comes out by hand, since ALTER SYSTEM needs a running server.
    /// </summary>
    [Fact]
    public void AnAlterSystemListStoredAsOneName_IsAnError_WithAFixThatWorksWhileTheStoreIsDown()
    {
        Assert.Equal(new[] { "timescaledb,pg_stat_statements" }, DarlingManagedPostgres.ParsePreloadList("\"timescaledb,pg_stat_statements\""));

        var root = Directory.CreateTempSubdirectory("darling-v13brick-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");
            const string Bricked = "# Do not edit this file manually!\nshared_preload_libraries = '\"timescaledb,pg_stat_statements\"'\n";
            File.WriteAllText(autoConfPath, Bricked);

            var logger = new CapturingTestLogger();
            new DarlingManagedPostgres(new PostgresConfig { Managed = true, Port = 5997, DataDirectory = dataDirectory }, logger)
                .LogStatementStatisticsPreloadCoverage(dataDirectory);

            var line = Assert.Single(logger.Joined.Split(" | "));
            Assert.StartsWith("Error: ", line, StringComparison.Ordinal);
            Assert.Contains("ONE library holding commas", line, StringComparison.Ordinal);
            Assert.Contains("delete that line from postgresql.auto.conf by hand", line, StringComparison.Ordinal);
            Assert.Contains("ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_stat_statements'", line, StringComparison.Ordinal);
            Assert.Equal(Bricked, File.ReadAllText(autoConfPath));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The "later edits" half of #3904's review: the v13 block restates the list ONCE, so a library an operator
    /// adds afterwards to an EARLIER assignment is overridden without a word, and an assignment appended AFTER
    /// the block without the library turns statement statistics off. Both are named, per start, with the line
    /// in force; a pristine healed conf says nothing.
    /// </summary>
    [Fact]
    public void AnEditTheV13BlockOverrides_AndALaterListWithoutTheLibrary_AreBothNamed()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13edit-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            var config = new PostgresConfig { Managed = true, Port = 5998, DataDirectory = dataDirectory };
            File.WriteAllText(confPath, "#shared_preload_libraries = ''\n");
            new DarlingManagedPostgres(config, NullLogger.Instance).EnsureConfAppended(dataDirectory);

            var pristine = new CapturingTestLogger();
            new DarlingManagedPostgres(config, pristine).LogStatementStatisticsPreloadCoverage(dataDirectory);
            Assert.Equal("(no log lines captured)", pristine.Joined);

            /* An operator adds auto_explain to v1's line, above the v13 block. */
            var healed = File.ReadAllText(confPath);
            File.WriteAllText(confPath, healed.Replace(
                "shared_preload_libraries = 'timescaledb'\n", "shared_preload_libraries = 'timescaledb,auto_explain'\n", StringComparison.Ordinal));
            var edited = new CapturingTestLogger();
            new DarlingManagedPostgres(config, edited).LogStatementStatisticsPreloadCoverage(dataDirectory);
            var overridden = Assert.Single(edited.Joined.Split(" | "));
            Assert.StartsWith("Warning: auto_explain is named by the shared_preload_libraries assignment at", overridden, StringComparison.Ordinal);
            Assert.Contains("replaces the whole list", overridden, StringComparison.Ordinal);

            /* An operator appends their own list after the block, without the library. */
            File.AppendAllText(confPath, "shared_preload_libraries = 'timescaledb,auto_explain'\n");
            var appended = new CapturingTestLogger();
            new DarlingManagedPostgres(config, appended).LogStatementStatisticsPreloadCoverage(dataDirectory);
            var later = Assert.Single(appended.Joined.Split(" | "));
            Assert.StartsWith("Warning: ", later, StringComparison.Ordinal);
            Assert.Contains("without pg_stat_statements, and it is the assignment in force", later, StringComparison.Ordinal);
            Assert.Contains("shared_preload_libraries = 'timescaledb,auto_explain,pg_stat_statements'", later, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The include half of #3904's review: the v13 block is appended at the END of postgresql.conf, after every
    /// include above it, so a list set in an included file is one the block replaces. The heal merges from the
    /// list PostgreSQL would actually read, includes followed (a relative include, then an include_dir's
    /// <c>*.conf</c> files in name order, a dot-file and a non-.conf file skipped), and keeps its libraries.
    /// </summary>
    [Fact]
    public void TheV13Merge_FollowsIncludes_SoAnIncludedPreloadListIsKept()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13incl-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var confDirectory = Path.Combine(dataDirectory, "conf.d");
            Directory.CreateDirectory(confDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            File.WriteAllText(Path.Combine(dataDirectory, "extra.conf"), "shared_preload_libraries = 'timescaledb,auto_explain'\n");
            File.WriteAllText(Path.Combine(confDirectory, "10-first.conf"), "shared_preload_libraries = 'timescaledb,auto_explain,pg_prewarm'\n");
            File.WriteAllText(Path.Combine(confDirectory, "20-second.conf"), "work_mem = '8MB'\n");
            File.WriteAllText(Path.Combine(confDirectory, ".hidden.conf"), "shared_preload_libraries = 'nope_hidden'\n");
            File.WriteAllText(Path.Combine(confDirectory, "30-notes.txt"), "shared_preload_libraries = 'nope_txt'\n");
            File.WriteAllText(confPath,
                DarlingManagedPostgres.BuildConfAppend(5999)
                + "include 'extra.conf'\n"
                + "include_dir 'conf.d'\n"
                + "include_if_exists 'missing.conf'\n");

            var assignments = DarlingManagedPostgres.ReadConfAssignments(confPath, "shared_preload_libraries");
            Assert.Equal(
                new[] { "timescaledb", "timescaledb,auto_explain", "timescaledb,auto_explain,pg_prewarm" },
                assignments.Select(a => a.Value).ToArray());
            Assert.EndsWith("10-first.conf", assignments[^1].File, StringComparison.Ordinal);

            var logger = new CapturingTestLogger();
            var pg = new DarlingManagedPostgres(new PostgresConfig { Managed = true, Port = 5999, DataDirectory = dataDirectory }, logger);
            pg.EnsureConfAppended(dataDirectory);

            Assert.Equal(
                "timescaledb,auto_explain,pg_prewarm,pg_stat_statements",
                DarlingManagedPostgres.ReadConfAssignments(confPath, "shared_preload_libraries")[^1].Value);
            Assert.DoesNotContain("is the assignment in force", logger.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("replaces the whole list", logger.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("ONE library holding commas", logger.Joined, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The two written forms of a list: the conf file's (one literal, a name double-quoted only when it
    /// must be, which the parser reads back whole) and ALTER SYSTEM's (one literal per library).</summary>
    [Fact]
    public void PreloadLists_AreWrittenInTheFormEachDestinationReads()
    {
        Assert.Equal("timescaledb,pg_stat_statements", DarlingManagedPostgres.FormatPreloadList(["timescaledb", "pg_stat_statements"]));
        Assert.Equal("timescaledb,\"odd,name\",\"q\"\"t\"", DarlingManagedPostgres.FormatPreloadList(["timescaledb", "odd,name", "q\"t"]));
        Assert.Equal(
            new[] { "timescaledb", "odd,name", "q\"t" },
            DarlingManagedPostgres.ParsePreloadList(DarlingManagedPostgres.FormatPreloadList(["timescaledb", "odd,name", "q\"t"])).ToArray());

        Assert.Equal("'timescaledb', 'pg_stat_statements'", DarlingManagedPostgres.FormatAlterSystemPreloadList(["timescaledb", "pg_stat_statements"]));
        Assert.Equal("'it''s'", DarlingManagedPostgres.FormatAlterSystemPreloadList(["it's"]));
    }

    /// <summary>
    /// A quoted conf value is de-escaped the way PostgreSQL's own <c>DeescapeQuotedString</c> does it (#3915's
    /// review): <c>\\</c> is one backslash, so an include directory ending in one is read whole instead of running
    /// on to the end of the line; <c>\t</c> and octal escapes are characters; and a value written through
    /// <see cref="DarlingManagedPostgres.EscapeConfValue"/> reads back exactly, a Windows library path included.
    /// </summary>
    [Fact]
    public void ConfValues_AreDeescapedThePostgresWay_AndEscapedValuesRoundTrip()
    {
        Assert.Equal(@"C:\pg\conf.d\", LastConfAssignment("include_dir = 'C:\\\\pg\\\\conf.d\\\\'   # the operator's\n", "include_dir"));
        Assert.Equal("tab\there", LastConfAssignment("x = 'tab\\there'\n", "x"));
        Assert.Equal("A", LastConfAssignment("x = '\\101'\n", "x"));
        Assert.Equal("it's", LastConfAssignment("x = 'it\\'s'\n", "x"));

        foreach (var value in new[] { @"C:\libs\x", "it's", @"a\'b", "plain" })
        {
            Assert.Equal(value, LastConfAssignment($"x = '{DarlingManagedPostgres.EscapeConfValue(value)}'\n", "x"));
        }

        /* The v13 block writes through the same escaping, so a library path survives the heal. */
        var block = DarlingManagedPostgres.BuildStatementStatisticsConfAppend(@"timescaledb,C:\libs\auto_explain");
        Assert.Equal(@"timescaledb,C:\libs\auto_explain,pg_stat_statements", LastConfAssignment(block, "shared_preload_libraries"));
    }

    /// <summary>
    /// A preload list is judged by PostgreSQL's own <c>SplitDirectoriesString</c> rules (#3915's review): a list
    /// the server rejects loads NOTHING, TimescaleDB included, so "it names pg_stat_statements" is not enough.
    /// </summary>
    [Theory]
    [InlineData(null, true)]
    [InlineData("", true)]
    [InlineData("   ", true)]
    [InlineData("timescaledb", true)]
    [InlineData(" timescaledb , pg_stat_statements ", true)]
    [InlineData("\"timescaledb,pg_stat_statements\"", true)]
    [InlineData("\"q\"\"t\", x", true)]
    [InlineData("timescaledb,pg_stat_statements,", false)]
    [InlineData(",timescaledb", false)]
    [InlineData("a,,b", false)]
    [InlineData("\"unclosed", false)]
    [InlineData("\"a\"b", false)]
    [InlineData("a, \"b\" c", false)]
    public void PreloadLists_AreValidExactlyWhenPostgresAcceptsThem(string? list, bool valid)
        => Assert.Equal(valid, DarlingManagedPostgres.IsValidPreloadList(list));

    /// <summary>An in-force list PostgreSQL rejects is reported as an Error with a repaired list, in the form the
    /// file it lives in takes; the file is not edited.</summary>
    [Fact]
    public void AnInvalidPreloadListInForce_IsAnError_WithTheRepairedList()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13invalid-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            const string AutoConf = "shared_preload_libraries = 'timescaledb, pg_stat_statements,'\n";
            File.WriteAllText(Path.Combine(dataDirectory, "postgresql.auto.conf"), AutoConf);

            var logger = new CapturingTestLogger();
            new DarlingManagedPostgres(new PostgresConfig { Managed = true, Port = 5993, DataDirectory = dataDirectory }, logger)
                .LogStatementStatisticsPreloadCoverage(dataDirectory);

            var line = Assert.Single(logger.Joined.Split(" | "));
            Assert.StartsWith("Error: ", line, StringComparison.Ordinal);
            Assert.Contains("not a list PostgreSQL accepts", line, StringComparison.Ordinal);
            Assert.Contains("ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_stat_statements'", line, StringComparison.Ordinal);
            Assert.Equal(AutoConf, File.ReadAllText(Path.Combine(dataDirectory, "postgresql.auto.conf")));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The v13 heal over an invalid list says so, and writes the list corrected, since the server has
    /// been loading nothing from it.</summary>
    [Fact]
    public void TheV13Heal_OverAnInvalidList_SaysSo_AndWritesItCorrected()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13heal-invalid-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            File.WriteAllText(confPath,
                DarlingManagedPostgres.BuildConfAppend(5992) + "shared_preload_libraries = 'timescaledb,auto_explain,'\n");

            var logger = new CapturingTestLogger();
            new DarlingManagedPostgres(new PostgresConfig { Managed = true, Port = 5992, DataDirectory = dataDirectory }, logger)
                .EnsureConfAppended(dataDirectory);

            Assert.Contains("which is not a list PostgreSQL accepts, so the store has been loading no library from it", logger.Joined, StringComparison.Ordinal);
            Assert.Equal("timescaledb,auto_explain,pg_stat_statements", LastConfAssignment(File.ReadAllText(confPath), "shared_preload_libraries"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// An include cycle that fans out (two files in a directory that each include the directory again) is
    /// refused by PostgreSQL once it nests past ten levels, but read naively it is 2^10 file reads on the start
    /// path before that; the walk stops at its file budget instead (#3915's review).
    /// </summary>
    [Fact]
    public void AFanningIncludeCycle_StopsAtTheFileBudget()
    {
        var root = Directory.CreateTempSubdirectory("darling-v13cycle-");
        try
        {
            var confDirectory = Path.Combine(root.FullName, "conf.d");
            Directory.CreateDirectory(confDirectory);
            File.WriteAllText(Path.Combine(confDirectory, "a.conf"), "include_dir '.'\nshared_preload_libraries = 'a'\n");
            File.WriteAllText(Path.Combine(confDirectory, "b.conf"), "include_dir '.'\nshared_preload_libraries = 'b'\n");
            var confPath = Path.Combine(root.FullName, "postgresql.conf");
            File.WriteAllText(confPath, "include_dir 'conf.d'\n");

            var assignments = DarlingManagedPostgres.ReadConfAssignments(confPath, "shared_preload_libraries");

            Assert.InRange(assignments.Count, 1, DarlingManagedPostgres.MaxConfFilesRead);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The value of the last assignment of <paramref name="name"/> in conf text, through the product's
    /// own parser.</summary>
    private static string? LastConfAssignment(string? conf, string name) =>
        DarlingManagedPostgres.ParseConfText(conf)
            .Where(a => a.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            .Select(a => a.Value)
            .LastOrDefault();

    /// <summary>
    /// The assignment names in a conf fragment — the left side of every non-comment <c>=</c> line. Derived
    /// from a builder's own output so a pin over "what this block writes" cannot drift from what it writes.
    /// </summary>
    private static string[] SettingNames(string conf)
    {
        var names = new List<string>();
        foreach (var raw in conf.Split('\n'))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var separator = line.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0)
            {
                var name = line[..separator].Trim();
                if (!names.Contains(name, StringComparer.Ordinal))
                {
                    names.Add(name);
                }
            }
        }

        return names.ToArray();
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
    /// How many LIVE assignments of <paramref name="setting"/> the conf carries — the instrument for "was
    /// this block re-applied", which a substring count cannot be.
    ///
    /// <para><b>Why not <c>CountOccurrences(conf, name + " = ")</c>.</b> initdb's generated
    /// postgresql.conf documents every setting as a COMMENTED line, so a real cluster's file already
    /// carries <c>#shared_preload_libraries = ''</c> before the product appends anything — a substring
    /// count reads 2 on a perfectly healthy conf and the assertion then fails for a reason that has
    /// nothing to do with the product. Not hypothetical: it is what the first version of
    /// <see cref="ExistingStore_GainsJobExecutionLogging_OnNextStart_Gated"/> did against a live cluster,
    /// while the synthetic fixture in
    /// <see cref="HealingAConfWithoutV11_AppendsOnlyThatBlock_AndReAppliesNoV1Setting"/> passed because it
    /// held only the product's own blocks and none of initdb's decoys. Same comment discipline as
    /// <see cref="LastSettingValue"/>, so the two agree about what an assignment is.</para>
    /// </summary>
    private static int CountAssignments(string conf, string setting)
    {
        var count = 0;
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
                count++;
            }
        }

        return count;
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
        Assert.Contains("maintenance_work_mem = 2047MB", block, StringComparison.Ordinal);    /* capped just under 2 GB (#1777; 2047 for PostgreSQL 17, #3909) */
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
    /// finally overtaken the floor and wins on its own (1638 MB); at 64 GB the CAP wins (5% would be 3276 MB,
    /// and the field data showed nothing to gain past 1536). The cap is 2047 MB, not 2048: 2048 is over
    /// PostgreSQL 17's Windows limit and stops a 17 store from starting (#3909).</para>
    /// </summary>
    [Theory]
    [InlineData(2, 512, 1536, 512, 16)]      /* 2 GB: maintenance held to 25% of RAM by the small-host guard; work_mem at the 16 MB floor */
    [InlineData(4, 1024, 3072, 1024, 16)]    /* 4 GB: the smallest host — the 25% guard holds the 1536 floor down to 1 GB (#1777) */
    [InlineData(8, 1024, 6144, 1536, 16)]    /* 8 GB: shared_buffers hits the 1 GB co-located cap (#1559); maintenance at the measured floor; work_mem at the floor */
    [InlineData(16, 1024, 12288, 1536, 32)]  /* 16 GB: the field-measured class — maintenance lands exactly on the 1536 MB capture point; work_mem RAM/512 = 32 MB */
    [InlineData(32, 1024, 24576, 1638, 64)]  /* 32 GB: 5% of RAM has overtaken the floor and wins outright; work_mem hits the 64 MB ceiling */
    [InlineData(64, 1024, 49152, 2047, 64)]  /* 64 GB: maintenance at the 2047 MB cap; everything but effective_cache_size capped */
    public void DeriveMemorySettings_PerTier(long ramGb, int sharedBuffersMb, int effectiveCacheMb, int maintenanceMb, int workMemMb)
    {
        var settings = DarlingManagedPostgres.DeriveMemorySettings(ramGb * 1024 * 1024 * 1024);

        Assert.Equal(sharedBuffersMb, settings.SharedBuffersMb);
        Assert.Equal(effectiveCacheMb, settings.EffectiveCacheSizeMb);
        Assert.Equal(maintenanceMb, settings.MaintenanceWorkMemMb);
        Assert.Equal(workMemMb, settings.WorkMemMb);
    }

    /// <summary>
    /// #3909: no host size can derive a <c>maintenance_work_mem</c> PostgreSQL 17 refuses. The old 2048 MB cap
    /// was 2097152 kB, one over 17's Windows limit, which is FATAL at startup.
    /// </summary>
    [Theory]
    [InlineData(40)]
    [InlineData(128)]
    [InlineData(1024)]
    public void DeriveMemorySettings_NeverExceedsPostgres17sLimit(long ramGb)
    {
        var settings = DarlingManagedPostgres.DeriveMemorySettings(ramGb * 1024 * 1024 * 1024);

        Assert.True(settings.MaintenanceWorkMemMb * 1024L <= DarlingManagedPostgres.LegacyMaintenanceWorkMemMaxKb,
            $"{ramGb} GB derives maintenance_work_mem = {settings.MaintenanceWorkMemMb}MB, over PostgreSQL 17's {DarlingManagedPostgres.LegacyMaintenanceWorkMemMaxKb} kB limit");
    }

    [Theory]
    [InlineData("2048MB", 2097152L)]
    [InlineData("2GB", 2097152L)]
    [InlineData("2047MB", 2096128L)]
    [InlineData("2047 MB", 2096128L)]
    [InlineData("65536", 65536L)]
    [InlineData("65536kB", 65536L)]
    [InlineData("1TB", 1073741824L)]
    [InlineData("2147483648B", 2097152L)]
    [InlineData("lots", null)]
    [InlineData("2048XB", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void ParseMaintenanceWorkMemKb_ReadsPostgresMemoryUnits(string? value, long? expectedKb)
        => Assert.Equal(expectedKb, DarlingManagedPostgres.ParseMaintenanceWorkMemKb(value));

    [Theory]
    [InlineData(17, "2048MB", true)]
    [InlineData(16, "3GB", true)]
    [InlineData(18, "2048MB", false)]
    [InlineData(17, "2047MB", false)]
    [InlineData(17, "lots", false)]
    [InlineData(null, "2048MB", false)]
    public void NeedsLegacyMaintenanceWorkMemCap_OnlyForAnOverLimitValueOnPostgres17OrEarlier(int? major, string value, bool expected)
        => Assert.Equal(expected, DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(major, value));

    [Fact]
    public void LegacyMaintenanceWorkMemCapBlock_WritesTheCapAndCarriesNoV8OrV12Line()
    {
        var block = DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV14, block, StringComparison.Ordinal);
        Assert.Equal($"{DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB", LastSettingValue(block, "maintenance_work_mem"));
        Assert.False(DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(17, LastSettingValue(block, "maintenance_work_mem")));

        /* The v8 and v12 heals key on the last line carrying their own prefixes; a block that carried either
           would change what they read. */
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, block, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedPostgres.ConfWalSizingStampPrefix, block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The heal on real files, without a server (#3909). It follows the value PostgreSQL would use: the last
    /// assignment in postgresql.conf and its includes, then postgresql.auto.conf. It appends only for a
    /// PostgreSQL 17 data directory whose value is over the limit, and only once. An over-limit ALTER SYSTEM
    /// value cannot be overridden from postgresql.conf, so that case is left untouched and logged instead.
    /// </summary>
    [Theory]
    [InlineData("17", "maintenance_work_mem = 2048MB\n", null, null, true)]
    [InlineData("18", "maintenance_work_mem = 2048MB\n", null, null, false)]
    [InlineData("17", "maintenance_work_mem = 1536MB\n", null, null, false)]
    [InlineData("17", "include 'sizing.conf'\n", "maintenance_work_mem = '2GB'\n", null, true)]
    [InlineData("17", "maintenance_work_mem = 1536MB\n", null, "maintenance_work_mem = '2048MB'\n", false)]
    [InlineData("17", "maintenance_work_mem = 2048MB\n", null, "maintenance_work_mem = '1GB'\n", false)]
    public void HealLegacyMaintenanceWorkMem_AppendsOnlyWhenTheValueInForceWouldStopPostgres17(
        string pgVersion, string conf, string? includedConf, string? autoConf, bool expectAppend)
    {
        var dataDirectory = Directory.CreateTempSubdirectory("darling-v14-").FullName;
        try
        {
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), pgVersion + "\n");
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            File.WriteAllText(confPath, conf);
            if (includedConf is not null)
            {
                File.WriteAllText(Path.Combine(dataDirectory, "sizing.conf"), includedConf);
            }

            if (autoConf is not null)
            {
                File.WriteAllText(Path.Combine(dataDirectory, "postgresql.auto.conf"), autoConf);
            }

            var managed = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, DataDirectory = dataDirectory }, NullLogger.Instance);
            managed.HealLegacyMaintenanceWorkMem(dataDirectory);

            var healed = File.ReadAllText(confPath);
            Assert.Equal(expectAppend ? 1 : 0, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV14));

            /* Converged: a second pass finds the block's own value in force and appends nothing. */
            managed.HealLegacyMaintenanceWorkMem(dataDirectory);
            Assert.Equal(healed, File.ReadAllText(confPath));
        }
        finally
        {
            Directory.Delete(dataDirectory, recursive: true);
        }
    }

    /// <summary>
    /// #3909: the heal runs BEFORE the store upgrade can start the old cluster. On a poisoned PostgreSQL 17 conf,
    /// that start is the first thing to fail. EnsureConfAppended also carries the check, but it runs after
    /// EnsureDataDirectoryMajorAsync, too late for the upgrade path. This is a wiring pin because no behavioral
    /// test here runs pg_upgrade on a poisoned store.
    /// </summary>
    [Fact]
    public void LegacyMaintenanceWorkMemHeal_RunsBeforeTheStoreUpgradeCanStartTheOldCluster()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");
        var method = source.IndexOf("public async Task<string> EnsureRunningAsync(", StringComparison.Ordinal);
        Assert.True(method >= 0, "EnsureRunningAsync's signature moved, so this pin can no longer find it.");

        var heal = source.IndexOf("HealLegacyMaintenanceWorkMem(_dataDirectory);", method, StringComparison.Ordinal);
        var upgrade = source.IndexOf("await EnsureDataDirectoryMajorAsync(binDirectory, networkPlan, cancellationToken);", method, StringComparison.Ordinal);
        Assert.True(heal > method, "EnsureRunningAsync no longer heals maintenance_work_mem before anything starts a PostgreSQL 17 cluster (#3909).");
        Assert.True(upgrade > heal,
            "The heal must run before EnsureDataDirectoryMajorAsync: the store upgrade's first step starts the old cluster on the data directory's own conf.");
    }

    /// <summary>
    /// #3909 through the product's own bootstrap. A store on PostgreSQL 17 whose conf ends with the old 2048 MB
    /// value (the shape a reverted upgrade left on a 40 GB+ host) comes back up through
    /// <see cref="DarlingManagedPostgres.EnsureRunningAsync"/> with the capped value in force. The 17 runtime is
    /// the runtime here, with no package beside it, so nothing is upgraded: this is the store that stays on 17.
    /// Gated on DARLING_TEST_PGRUNTIME_OLD.
    /// </summary>
    [Fact]
    public async Task Postgres17Store_WithTheOldCapInItsConf_StartsThroughTheBootstrap_Gated()
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_OLD");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime),
            "Set DARLING_TEST_PGRUNTIME_OLD to an assembled PostgreSQL 17 pg-runtime (new-upgraded-store-fixture.ps1 builds one).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(oldRuntime!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_OLD={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pg17boot-");
        var runtimeRoot = Path.Combine(root.FullName, "deploy", "pg-runtime");
        var source = Path.Combine(oldRuntime!, "pgsql");
        foreach (var file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
        {
            var target = Path.Combine(runtimeRoot, "pgsql", Path.GetRelativePath(source, file));
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            File.Copy(file, target);
        }

        var dataDirectory = Path.Combine(root.FullName, "store", "pg");
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        var config = new PostgresConfig { Managed = true, Port = FindFreeTcpPort(), DataDirectory = dataDirectory };
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));
        DarlingManagedPostgres? first = null;
        DarlingManagedPostgres? second = null;
        try
        {
            first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await first.EnsureRunningAsync(timeout.Token);
            await first.StopIfStartedByThisProcessAsync();
            Assert.Equal(17, DarlingStoreUpgrade.TryReadDataDirectoryMajor(dataDirectory));

            await File.AppendAllTextAsync(confPath, "\n# written by a build before #3909\nmaintenance_work_mem = 2048MB\n", timeout.Token);

            second = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var connectionString = await second.EnsureRunningAsync(timeout.Token);

            var (live, expected) = await ReadSettingAndLiteralBytesAsync(
                connectionString, "maintenance_work_mem", $"{DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB", timeout.Token);
            Assert.Equal(expected, live);
            Assert.Equal(1, CountOccurrences(await File.ReadAllTextAsync(confPath, timeout.Token), DarlingManagedPostgres.ConfMarkerV14));
        }
        finally
        {
            if (first is not null)
            {
                await first.StopIfStartedByThisProcessAsync();
            }

            if (second is not null)
            {
                await second.StopIfStartedByThisProcessAsync();
            }

            try
            {
                root.Delete(recursive: true);
            }
            catch (IOException)
            {
                /* A temp directory the OS still holds is not this test's failure. */
            }
        }
    }

    /// <summary>
    /// #3909 on a real PostgreSQL 17 server. A 17 data directory whose conf carries the old 2048 MB value
    /// refuses to start. After <see cref="DarlingManagedPostgres.HealLegacyMaintenanceWorkMem"/> it starts with
    /// the capped value, and a second heal appends nothing. Gated on DARLING_TEST_PGRUNTIME_OLD, the
    /// previous-major runtime new-upgraded-store-fixture.ps1 builds (the nightly sets it).
    /// </summary>
    [Fact]
    public async Task Postgres17StoreWithTheOldCap_StartsAfterTheHeal_Gated()
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_OLD");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime),
            "Set DARLING_TEST_PGRUNTIME_OLD to an assembled PostgreSQL 17 pg-runtime (new-upgraded-store-fixture.ps1 builds one).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        var bin = Path.Combine(oldRuntime!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_OLD={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pg17mwm-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var port = FindFreeTcpPort();
        var pgCtl = Path.Combine(bin, "pg_ctl.exe");
        var startArguments = $"-D \"{dataDirectory}\" -o \"-p {port} -c listen_addresses=127.0.0.1\" -w -t 60 start";
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var started = false;
        try
        {
            var (initExit, initOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"), $"-D \"{dataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(initExit == 0, $"initdb failed: {initOutput}");
            Assert.Equal(17, DarlingStoreUpgrade.TryReadDataDirectoryMajor(dataDirectory));

            /* The shape a reverted upgrade left on a 40 GB+ host: a v7 block at the old 2048 MB cap. */
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            await File.AppendAllTextAsync(confPath,
                "\n" + DarlingManagedPostgres.ConfMarkerV7 + "\nmaintenance_work_mem = 2048MB\n", timeout.Token);

            /* Control: PostgreSQL 17 refuses to start on it, which is the outage. */
            var refused = await DarlingManagedPostgres.RunDetachingToolAsync(pgCtl, startArguments, TimeSpan.FromMinutes(2), timeout.Token);
            Assert.NotEqual(0, refused);

            var managed = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = port, DataDirectory = dataDirectory }, NullLogger.Instance, oldRuntime);
            managed.HealLegacyMaintenanceWorkMem(dataDirectory);
            var healed = await File.ReadAllTextAsync(confPath, timeout.Token);
            Assert.Equal(1, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV14));

            var exit = await DarlingManagedPostgres.RunDetachingToolAsync(pgCtl, startArguments, TimeSpan.FromMinutes(2), timeout.Token);
            Assert.True(exit == 0, "PostgreSQL 17 should start once the v14 block is the last maintenance_work_mem assignment");
            started = true;

            await using (var connection = new NpgsqlConnection($"Host=127.0.0.1;Port={port};Username=darling;Database=postgres;Pooling=false"))
            {
                await connection.OpenAsync(timeout.Token);
                await using var show = new NpgsqlCommand("SELECT pg_size_bytes(current_setting('maintenance_work_mem'))", connection);
                Assert.Equal(DarlingManagedPostgres.MaintenanceWorkMemCapMb * 1024L * 1024L,
                    Convert.ToInt64(await show.ExecuteScalarAsync(timeout.Token), CultureInfo.InvariantCulture));
            }

            /* Converged: a second heal finds its own line in force and appends nothing. */
            managed.HealLegacyMaintenanceWorkMem(dataDirectory);
            Assert.Equal(healed, await File.ReadAllTextAsync(confPath, timeout.Token));
        }
        finally
        {
            if (started)
            {
                await DarlingManagedPostgres.RunToolAsync(pgCtl, $"stop -D \"{dataDirectory}\" -m fast -w -t 60", TimeSpan.FromMinutes(2), CancellationToken.None);
            }

            try
            {
                root.Delete(recursive: true);
            }
            catch (IOException)
            {
                /* A temp directory the OS still holds is not this test's failure. */
            }
        }
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
        var thirtyTwoGbBlock = DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, hypertables);

        /* A genuine 16 -> 32 GB resize DOES append, but only with an authoritative reading behind it. */
        Assert.True(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            conf, ramReadingIsAuthoritative: true,
            DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables), thirtyTwoGbBlock));

        /* The same apparent change, from a reading we could not trust, must do nothing at all. */
        Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            conf, ramReadingIsAuthoritative: false,
            DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables), thirtyTwoGbBlock));

        /* And it stays inert for ANY value the GC fallback might invent, which is the append-loop case. */
        foreach (var guessGb in new long[] { 3, 7, 12, 29 })
        {
            var guessBlock = DarlingManagedPostgres.BuildHardwareSizingConfAppend(guessGb * 1024 * 1024 * 1024, hypertables);
            Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
                conf, ramReadingIsAuthoritative: false,
                DarlingManagedPostgres.BuildHardwareFingerprint(guessGb * 1024 * 1024 * 1024, hypertables), guessBlock));
        }

        /* #4207: a non-authoritative reading must heal nothing even when BOTH of the new conditions are
           also true — duplicate blocks present AND the newest one's content stale. Authoritativeness gates
           the whole decision, not just the original fingerprint leg of it. */
        var duplicatesConf = conf + thirtyTwoGbBlock;
        Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            duplicatesConf, ramReadingIsAuthoritative: false,
            DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, hypertables), thirtyTwoGbBlock));
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
    /// PIN (#4207): the hardware block emits ALL FOUR <see cref="DarlingManagedPostgres.MemorySettings"/>
    /// values, <c>work_mem</c> included, from the SAME <see cref="DarlingManagedPostgres.DeriveMemorySettings"/>
    /// call the block already uses for the other three. Before #4207 this was the opposite pin
    /// (<c>HardwareSizingConfAppend_NeverEmitsWorkMem</c>): #2845 measured a regression at 512 MB — 8x this
    /// formula's own 64 MB ceiling — and concluded to exclude work_mem altogether, so the actual clamped
    /// value the formula derives was never measured. #4207 measured what excluding it cost instead: three
    /// field stores stuck at the v3 block's 31 MB after a resize to 31.5 GB, one of them having spilled 33 TB
    /// to temp files since creation. See <see cref="DarlingManagedPostgres.BuildHardwareSizingConfAppend"/>
    /// for the full reversal.
    ///
    /// <para>Covers 32 GB and above, where the formula clamps to the 64 MB ceiling, on purpose: those are the
    /// sizes where a resize can no longer move the value further, so a regression there would be the
    /// permanent state of every sufficiently large store, not a transient one.</para>
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(512)]
    public void HardwareSizingConfAppend_EmitsAllFourMemorySettings(long ramGb)
    {
        var ramBytes = ramGb * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(ramBytes, 40);
        var expected = DarlingManagedPostgres.DeriveMemorySettings(DarlingManagedPostgres.QuantizeRam(ramBytes));

        /* Anchored on the newline that starts every setting line: a bare "work_mem = " is a SUBSTRING of
           "maintenance_work_mem = ", so the unanchored form would pass even if this read the wrong line. */
        Assert.Contains($"\neffective_cache_size = {expected.EffectiveCacheSizeMb}MB\n", block, StringComparison.Ordinal);
        Assert.Contains($"\nmaintenance_work_mem = {expected.MaintenanceWorkMemMb}MB\n", block, StringComparison.Ordinal);
        Assert.Contains($"\nwork_mem = {expected.WorkMemMb}MB\n", block, StringComparison.Ordinal);

        /* shared_buffers stays excluded (CONSTRAINT PIN 1, #1559/#2845) — this pin is about the other three,
           not a licence to re-derive every MemorySettings field. */
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The exact field measurement from #4207: the reported host RAM, 33,788,809,216 bytes (the issue's
    /// "31.5 GiB" — actually 31.47 GiB, which <see cref="DarlingManagedPostgres.QuantizeRam"/> rounds DOWN to
    /// 31 GB, half a GB short of the 31.5 GB midpoint that would round up). At 31 GB, <c>work_mem</c> is
    /// <b>62 MB</b>, not the issue's rough "63 MB" (63 is 31.5 GiB's OWN raw RAM/512, i.e. what you get by
    /// skipping the quantization step) — and not the 31 MB the stale v3 block left in force after the resize
    /// either way. Either figure is roughly double the stale value, which is the point; this pins the one the
    /// code actually derives from the reported bytes.
    /// </summary>
    [Fact]
    public void HardwareSizingConfAppend_MeasuredFieldRam_EmitsWorkMem62Mb()
    {
        const long measuredFieldRamBytes = 33_788_809_216L;
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(measuredFieldRamBytes, 40);

        Assert.Contains("\nwork_mem = 62MB\n", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The block emits what it is for, at the values the resized fleet should have had. 31.5 GB is the
    /// m7i.2xlarge reading; 32 GB is used here for a round assertion. effective_cache_size 24576MB is the
    /// number #2845 was filed over — the boxes were sitting at 11.86 GB, which is 75% of the 16 GB they had
    /// before the resize. work_mem 64MB is the #4207 addition — the 32 GB round number lands exactly on the
    /// formula's ceiling; <see cref="HardwareSizingConfAppend_MeasuredFieldRam_EmitsWorkMem62Mb"/> covers the
    /// field's actual reading, just under it.
    /// </summary>
    [Fact]
    public void HardwareSizingConfAppend_EmitsHostDerivedSettings()
    {
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, 40);

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV8, block, StringComparison.Ordinal);
        Assert.Contains("effective_cache_size = 24576MB", block, StringComparison.Ordinal);  /* 75% of 32 GB (was 11.86 GB = 75% of 16 GB) */
        Assert.Contains("maintenance_work_mem = 1638MB", block, StringComparison.Ordinal);   /* 5% of 32 GB, past the 1536 floor, under the 2 GB cap */
        Assert.Contains("work_mem = 64MB", block, StringComparison.Ordinal);                 /* RAM/512 = 64 MB, exactly the ceiling */
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

    /* ===================== #4207 v8 replace-in-place (was append-only) ===================== */

    /// <summary>
    /// PIN (#4207): two successive fingerprint changes — the field's actual pattern, RAM then hypertable
    /// count — leave exactly ONE v8 block, not a growing pile. Before this fix each call to the equivalent
    /// append appended a fresh block, which is how the field stores reached three copies from three
    /// fingerprint changes since creation.
    /// </summary>
    [Fact]
    public void ReplaceOrAppendHardwareSizingBlock_TwoSuccessiveFingerprintChanges_LeaveExactlyOneBlock()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;

        var conf = "shared_buffers = 1024MB\n";
        conf = DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(
            conf, DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, 40));
        Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV8));

        /* First change: a resize, 16 -> 32 GB. */
        conf = DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(
            conf, DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, 40));
        Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV8));

        /* Second change: a hypertable count change with no resize, 32 GB stays but 40 -> 41. This is the
           axis #2845 considered splitting into its own fingerprint; #4207 keeps it joined (see the
           EnsureConfAppended v8 comment) because an in-place rewrite makes it exactly as cheap as a resize. */
        conf = DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(
            conf, DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, 41));
        Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV8));
        Assert.True(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            conf, DarlingManagedPostgres.BuildHardwareFingerprint(thirtyTwoGb, 41)));

        /* The unrelated line outside every block survived all three rewrites untouched. */
        Assert.StartsWith("shared_buffers = 1024MB\n", conf, StringComparison.Ordinal);
    }

    /// <summary>
    /// PIN (#4207): a conf carrying three v8 blocks — the exact shape #4207 measured on all three production
    /// stores (72 -&gt; 73 -&gt; 74 background workers) — collapses to ONE on the next rewrite, at the
    /// FIRST block's position. Rewriting the first position rather than the last is what keeps a manual
    /// override positioned after the old last block still winning (see
    /// <see cref="DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock"/>): collapsing can only move the
    /// v8 lines EARLIER in the file, never later, so nothing that used to lose to the last block can start
    /// winning, and nothing that used to beat it can start losing.
    /// </summary>
    [Fact]
    public void ReplaceOrAppendHardwareSizingBlock_ThreeExistingBlocks_CollapseToOneAtTheFirstPosition()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        const long sixtyFourGb = 64L * 1024 * 1024 * 1024;

        var block1 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, 40);
        var block2 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, 41);
        var block3 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixtyFourGb, 42);
        var conf = "port = 5432\n" + block1 + block2 + block3;
        var firstMarkerPosition = conf.IndexOf(DarlingManagedPostgres.ConfMarkerV8, StringComparison.Ordinal);
        Assert.Equal(3, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV8));  /* the broken shape, confirmed */

        var newBlock = DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixtyFourGb, 43);
        var rewritten = DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(conf, newBlock);

        Assert.Equal(1, CountOccurrences(rewritten, DarlingManagedPostgres.ConfMarkerV8));
        Assert.Equal(
            firstMarkerPosition,
            rewritten.IndexOf(DarlingManagedPostgres.ConfMarkerV8, StringComparison.Ordinal));
        Assert.True(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(
            rewritten, DarlingManagedPostgres.BuildHardwareFingerprint(sixtyFourGb, 43)));
    }

    /// <summary>
    /// PIN (#4207): every byte outside a v8 span is byte-identical after a rewrite that both updates the
    /// first block and removes a second one — INCLUDING an operator's own line sitting BETWEEN the two
    /// blocks, which a naive "keep the first block's text, drop everything from the second marker on" splice
    /// would lose even though it is not part of either block.
    /// </summary>
    [Fact]
    public void ReplaceOrAppendHardwareSizingBlock_LinesOutsideBlocks_AreByteIdenticalAfterRewrite()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        const long thirtyTwoGb = 32L * 1024 * 1024 * 1024;
        const long sixtyFourGb = 64L * 1024 * 1024 * 1024;

        const string before = "# operator header\nport = 5432\n";
        /* Blank-line-led, like every block this file writes - see FindHardwareSizingBlockEnd's doc for why
           an operator line with NO leading blank line is a known edge case this rule does not cover. */
        const string between = "\nwork_mem = 999MB   # an operator override sitting between two v8 blocks\n";
        const string after = "\n# trailing operator block\nlisten_addresses = '*'\n";

        var conf = before
            + DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, 40)
            + between
            + DarlingManagedPostgres.BuildHardwareSizingConfAppend(thirtyTwoGb, 41)
            + after;

        var rewritten = DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(
            conf, DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixtyFourGb, 42));

        Assert.StartsWith(before, rewritten, StringComparison.Ordinal);
        Assert.Contains(between, rewritten, StringComparison.Ordinal);
        Assert.EndsWith(after, rewritten, StringComparison.Ordinal);
        Assert.Equal(1, CountOccurrences(rewritten, DarlingManagedPostgres.ConfMarkerV8));
    }

    /// <summary>
    /// No existing v8 block falls back to a plain append — the v2-v7 shape — so a cluster's first-ever v8
    /// write is unchanged by #4207.
    /// </summary>
    [Fact]
    public void ReplaceOrAppendHardwareSizingBlock_NoExistingBlock_AppendsOne()
    {
        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        var conf = "port = 5432\n";
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, 40);

        Assert.Equal(conf + block, DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(conf, block));
    }

    /// <summary>
    /// The #4207 field defect end to end: a data directory whose postgresql.conf already carries three v8
    /// blocks (the append-not-replace bug) collapses to one on the very next <c>EnsureConfAppended</c> call,
    /// through the real heal path rather than the pure function directly, and the survivor carries
    /// <c>work_mem</c>. A second heal is a no-op for v8: the fingerprint the first heal just wrote matches
    /// this machine, so nothing is rewritten.
    /// </summary>
    [Fact]
    public void EnsureConfAppended_ThreeExistingV8Blocks_CollapseToOneOnTheNextHeal()
    {
        var root = Directory.CreateTempSubdirectory("darling-v8collapse-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");

            /* Fingerprints built from values no real test host will match (1-3 GB RAM, 1-3 hypertables), so
               the v8 check is guaranteed to find the last one stale and act - exactly what the field stores
               hit on every hypertable-count change once their RAM had already resized past the oldest
               fingerprint. */
            var stale1 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(1L * 1024 * 1024 * 1024, 1);
            var stale2 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(2L * 1024 * 1024 * 1024, 2);
            var stale3 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(3L * 1024 * 1024 * 1024, 3);
            File.WriteAllText(confPath, DarlingManagedPostgres.BuildConfAppend(5993) + stale1 + stale2 + stale3);

            var pg = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 5993, DataDirectory = dataDirectory }, NullLogger.Instance);

            pg.EnsureConfAppended(dataDirectory);

            var healed = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV8));
            Assert.Contains("\nwork_mem = ", healed, StringComparison.Ordinal);

            pg.EnsureConfAppended(dataDirectory);
            var healedAgain = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(healedAgain, DarlingManagedPostgres.ConfMarkerV8));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The v8 block AS AN OLDER BUILD WROTE IT, before <c>work_mem</c> rejoined the formula in #4207: every
    /// line <see cref="DarlingManagedPostgres.BuildHardwareSizingConfAppend"/> emits today, with the
    /// <c>work_mem</c> line removed. This is the literal on-disk shape of a field store's stale v8 blocks —
    /// its fingerprint line is untouched and therefore fully current, only the FORMULA that turned the
    /// fingerprint's own inputs into settings gained a line since this block was written.
    /// </summary>
    private static string BuildLegacyV8BlockWithoutWorkMem(long totalPhysicalMemoryBytes, int hypertableCount)
    {
        var current = DarlingManagedPostgres.BuildHardwareSizingConfAppend(totalPhysicalMemoryBytes, hypertableCount);
        return string.Join('\n', current.Split('\n').Where(line => !line.StartsWith("work_mem = ", StringComparison.Ordinal)));
    }

    /// <summary>
    /// THE FIELD DEFECT #4207 WAS REOPENED FOR (issuecomment-5837990283): a store whose v8 blocks were ALL
    /// written before #4225 shipped has no fingerprint change left to trigger #2845's original check — its
    /// RAM and hypertable count have not moved since its LAST v8 write, so
    /// <see cref="DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint"/> already says "current" — yet
    /// the surviving blocks are still missing <c>work_mem</c>, and there are three of them (the
    /// append-not-replace bug's own leftovers, #4225 fixed going forward but never retroactively). The v3
    /// block, frozen since the store's original initdb, is the field's own report of a stale
    /// <c>work_mem = 31MB</c> line that nothing ever re-applies.
    /// </summary>
    [Fact]
    public void ShouldAppendHardwareSizing_FieldCase_CurrentFingerprintButStaleContent_Heals()
    {
        const long currentRam = 33_788_809_216L;  /* the fleet's actual reading (#4207): nominally "31.5 GiB" */
        const int hypertables = 40;

        var fingerprint = DarlingManagedPostgres.BuildHardwareFingerprint(currentRam, hypertables);
        var expectedAppend = DarlingManagedPostgres.BuildHardwareSizingConfAppend(currentRam, hypertables);
        var expectedWorkMemMb = DarlingManagedPostgres.DeriveMemorySettings(
            DarlingManagedPostgres.QuantizeRam(currentRam)).WorkMemMb;
        Assert.Equal(62, expectedWorkMemMb);  /* the doc comment's own figure - sanity-checks the fixture */

        var legacyBlock = BuildLegacyV8BlockWithoutWorkMem(currentRam, hypertables);
        /* Leading "\n" boundary: a bare "work_mem" substring search would also match inside
           "maintenance_work_mem", which the legacy block still legitimately carries. */
        Assert.DoesNotContain("\nwork_mem = ", legacyBlock, StringComparison.Ordinal);

        /* The v3 block: marker-guarded, so it is written ONCE at initdb and never rewritten again - frozen
           at whatever the very first start derived, exactly the field's own report. */
        const string v3Block =
            "\n" + DarlingManagedPostgres.ConfMarkerV3 + "\n"
            + "shared_buffers = 1024MB\n"
            + "effective_cache_size = 12288MB\n"
            + "maintenance_work_mem = 2047MB\n"
            + "work_mem = 31MB\n";

        var conf = "port = 5432\n" + v3Block + legacyBlock + legacyBlock + legacyBlock;
        Assert.Equal(3, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV8));
        Assert.True(
            DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(conf, fingerprint),
            "the fixture's premise: the newest v8 block's fingerprint already matches today's inputs");

        /* THE FIX: heals anyway, because of the duplicate-block and stale-content conditions #4207 added -
           see ShouldAppendHardwareSizing_FieldCase_OldFingerprintOnlyPredicate_WouldWronglySkip for what the
           original single-condition check does with this exact fixture. */
        Assert.True(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            conf, ramReadingIsAuthoritative: true, fingerprint, expectedAppend));

        var healed = DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(conf, expectedAppend);
        Assert.Equal(1, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV8));

        /* The v3 line is untouched - still there, still 31MB - but the v8 line lands LATER in the file, so
           postgresql.conf's last-occurrence-wins rule makes IT the value in force. */
        Assert.Contains("work_mem = 31MB", healed, StringComparison.Ordinal);
        Assert.Equal($"{expectedWorkMemMb}MB", LastSettingValue(healed, "work_mem"));
        Assert.True(
            healed.IndexOf(DarlingManagedPostgres.ConfMarkerV8, StringComparison.Ordinal)
                > healed.IndexOf(DarlingManagedPostgres.ConfMarkerV3, StringComparison.Ordinal));

        /* IDEMPOTENT: a second pass over the healed text changes nothing, byte for byte. */
        Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            healed, ramReadingIsAuthoritative: true, fingerprint, expectedAppend));
        Assert.Equal(healed, DarlingManagedPostgres.ReplaceOrAppendHardwareSizingBlock(healed, expectedAppend));
    }

    /// <summary>
    /// PIN: the OLD, #2845-only predicate — authoritative-and-fingerprint-mismatch, with neither of #4207's
    /// two new conditions — says "nothing to do" on the exact fixture
    /// <see cref="ShouldAppendHardwareSizing_FieldCase_CurrentFingerprintButStaleContent_Heals"/> proves
    /// needs healing. Reproduced literally rather than called, since the function no longer exposes that
    /// expression alone: this IS the regression #4207 was reopened to describe, and reverting
    /// <see cref="DarlingManagedPostgres.ShouldAppendHardwareSizing"/> to just this line is what #4225
    /// shipped and left three field stores unhealed.
    /// </summary>
    [Fact]
    public void ShouldAppendHardwareSizing_FieldCase_OldFingerprintOnlyPredicate_WouldWronglySkip()
    {
        const long currentRam = 33_788_809_216L;
        const int hypertables = 40;
        var fingerprint = DarlingManagedPostgres.BuildHardwareFingerprint(currentRam, hypertables);
        var legacyBlock = BuildLegacyV8BlockWithoutWorkMem(currentRam, hypertables);
        var conf = "port = 5432\n" + legacyBlock + legacyBlock + legacyBlock;

        var oldPredicateResult = true && !DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(conf, fingerprint);

        Assert.False(oldPredicateResult, "the old predicate misses this store entirely - its fingerprint is already current");
    }

    /// <summary>
    /// A conf that is ALREADY the block this build would write is not rewritten — #4207's two new
    /// conditions must not turn every start into a rewrite of a perfectly healthy store.
    /// </summary>
    [Fact]
    public void ShouldAppendHardwareSizing_AlreadyCurrentSingleBlock_IsNotRewritten()
    {
        const long ram = 32L * 1024 * 1024 * 1024;
        const int hypertables = 40;
        var fingerprint = DarlingManagedPostgres.BuildHardwareFingerprint(ram, hypertables);
        var block = DarlingManagedPostgres.BuildHardwareSizingConfAppend(ram, hypertables);
        var conf = "port = 5432\n" + block;

        Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            conf, ramReadingIsAuthoritative: true, fingerprint, block));

        /* Same property with a CRLF FILE, since a Windows-edited conf can carry them (#4207: the content
           comparison must normalise, not just the fingerprint-line comparison #2845 already did). Only the
           FILE is CRLF'd, not the freshly-rendered append: BuildHardwareSizingConfAppend, like every
           Build*ConfAppend in this class, only ever emits LF, so that is the shape production always passes
           as expectedBlockAppend regardless of what is already on disk. */
        var crlfConf = conf.Replace("\n", "\r\n", StringComparison.Ordinal);
        Assert.False(DarlingManagedPostgres.ShouldAppendHardwareSizing(
            crlfConf, ramReadingIsAuthoritative: true, fingerprint, block));
    }

    /// <summary>
    /// #4207 (reopened) END TO END, proven against a real server: a store whose v8 blocks were ALL written
    /// before <c>work_mem</c> rejoined the formula, with a fingerprint that already matches this host,
    /// adopts the derived <c>work_mem</c> on its very next service-owned start — the live half of
    /// <see cref="ShouldAppendHardwareSizing_FieldCase_CurrentFingerprintButStaleContent_Heals"/>.
    ///
    /// <para>The BEFORE conf is built from a REAL fresh initdb's own v8 block (so the fingerprint is
    /// genuinely current for whatever RAM and hypertable count this runner actually has), with its
    /// <c>work_mem</c> line stripped and the single block tripled — reproducing the exact field shape
    /// without needing a specific RAM figure to land on.</para>
    /// </summary>
    [Fact]
    public async Task ExistingStore_HealsPreFixV8Blocks_OnNextStart_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe; " +
            "Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under artifacts\\pg-runtime-work\\assemble\\pg-runtime) " +
            "to run the #4207 conf-heal E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pgv8heal-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");

        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));

            /* A real store, provisioned the normal way. Step A migrates the v8 block out of
               postgresql.conf into darling-managed.conf during this same call (#4215), so the v8 shape this
               test needs to rewind is only available in the pre-migration BACKUP, not the live file. */
            await owner.EnsureRunningAsync(timeout.Token);
            await owner.StopIfStartedByThisProcessAsync();

            string? derivedWorkMem = null;
            await RewindDataDirectoryToLegacyConfAsync(
                dataDirectory,
                preFixConf =>
                {
                    var spans = DarlingManagedPostgres.FindHardwareSizingBlockSpans(preFixConf);
                    Assert.Single(spans);
                    var (v8Start, v8End) = spans[0];
                    var freshBlock = preFixConf[v8Start..v8End];
                    derivedWorkMem = LastSettingValue(preFixConf, "work_mem");
                    Assert.NotNull(derivedWorkMem);
                    Assert.Contains("work_mem = " + derivedWorkMem, freshBlock, StringComparison.Ordinal);

                    /* Reproduce the field shape: the same block, minus work_mem, three times over - the
                       append-not-replace leftovers (#4225 fixed the bug; this store's blocks predate the fix)
                       with a fingerprint that is ALREADY current, since it came straight from this run's own
                       real (pre-migration) conf. */
                    var legacyBlock = string.Join(
                        '\n',
                        freshBlock.Split('\n').Where(line => !line.StartsWith("work_mem = ", StringComparison.Ordinal)));
                    /* Checked on the single copy, before tripling and splicing: the v3 block elsewhere in this
                       real conf can legitimately carry the SAME derived value on a host whose RAM clamps both
                       formulas to the same ceiling (64MB), so asserting against the whole file would be a
                       false failure on such a host rather than a check of what THIS splice removed. */
                    Assert.DoesNotContain("\nwork_mem = ", legacyBlock, StringComparison.Ordinal);

                    /* A blank line between each copy - the separator every Build*ConfAppend leads with, and
                       so the shape every REAL append-not-replace duplicate carried. preFixConf[..v8Start]
                       already supplies the separator before the first copy, and preFixConf[v8End..] already
                       supplies one after the last, so only the two seams IN BETWEEN need one inserted; without
                       it FindHardwareSizingBlockSpans reads all three copies as a single span (nothing blank
                       to stop it at), and the fixture would not be the three-block shape #4207 describes. */
                    var legacyConf = preFixConf[..v8Start] + legacyBlock + '\n' + legacyBlock + '\n' + legacyBlock + preFixConf[v8End..];
                    Assert.Equal(3, DarlingManagedPostgres.FindHardwareSizingBlockSpans(legacyConf).Count);
                    return legacyConf;
                },
                timeout.Token);
            Assert.NotNull(derivedWorkMem);

            /* The service-owned start: on a Legacy conf, EnsureConfAppended heals BEFORE pg_ctl start (so the
               derived value is live on this very start), and Step A then migrates the healed value into
               darling-managed.conf post-start -- postgresql.conf itself never carries the v8 marker again. */
            var healedOwner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var healedConnectionString = await healedOwner.EnsureRunningAsync(timeout.Token);
            try
            {
                var healedPostgresqlConf = await File.ReadAllTextAsync(confPath, timeout.Token);
                var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
                var healedManagedConf = File.Exists(managedConfPath)
                    ? await File.ReadAllTextAsync(managedConfPath, timeout.Token)
                    : string.Empty;
                var diagnostics =
                    $"LastManagedConfVerification={healedOwner.LastManagedConfVerification}; " +
                    $"Classify={ManagedConfMigrationState.Classify(dataDirectory)}; " +
                    $"files=[{string.Join(", ", Directory.GetFiles(dataDirectory).Select(Path.GetFileName))}]";

                Assert.True(
                    !healedPostgresqlConf.Contains(DarlingManagedPostgres.ConfMarkerV8, StringComparison.Ordinal),
                    $"postgresql.conf should carry no v8 marker after Step A migrates it out. {diagnostics}");
                Assert.True(
                    healedManagedConf.Contains("work_mem = " + derivedWorkMem, StringComparison.Ordinal),
                    $"darling-managed.conf should carry the healed work_mem value. {diagnostics}");

                var (live, expected) = await ReadSettingAndLiteralBytesAsync(
                    healedConnectionString, "work_mem", derivedWorkMem!, timeout.Token);
                Assert.True(expected == live, $"work_mem was not live at the healed value. {diagnostics}");

                Assert.True(
                    ManagedConfMigrationState.Classify(dataDirectory) == ManagedConfMigrationState.Kind.Verified,
                    $"The data directory should classify Verified after the heal start. {diagnostics}");
            }
            finally
            {
                await healedOwner.StopIfStartedByThisProcessAsync();
            }

            /* A third start must not re-append a v8 block into postgresql.conf -- already migrated, nothing
               left to heal there. */
            Assert.DoesNotContain(
                DarlingManagedPostgres.ConfMarkerV8,
                await File.ReadAllTextAsync(confPath, timeout.Token),
                StringComparison.Ordinal);
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            TryDeleteRecursive(root.FullName);
        }
    }

    /* ===================== v15 WAL compression (#4246) ===================== */

    /// <summary>
    /// The v15 block (#4246): <c>wal_compression = lz4</c> only. See
    /// <see cref="DarlingManagedPostgres.ConfMarkerV15"/> for why the checkpoint interval is held rather than
    /// shipped here, and why the block deliberately says nothing about <c>max_wal_size</c>.
    /// </summary>
    [Fact]
    public void WalVolumeConfAppend_PinsV15Marker_AndSetsCompression()
    {
        var block = DarlingManagedPostgres.BuildWalVolumeConfAppend();

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV15, block, StringComparison.Ordinal);
        Assert.Equal("lz4", LastSettingValue(block, "wal_compression"));

        /* No fingerprint or stamp line, or the v8/v12 staleness checks would misread what they scan. */
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, block, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedPostgres.ConfWalSizingStampPrefix, block, StringComparison.Ordinal);

        /* v12 (#3802) is still the only thing that ever sets these: this block leaves the disk-derived
           ceiling exactly where it is. checkpoint_timeout stays out too -- the interval is held, not shipped
           (#4246's amended ruling), so this pin catches either one landing here by accident. */
        Assert.Null(LastSettingValue(block, "max_wal_size"));
        Assert.Null(LastSettingValue(block, "min_wal_size"));
        Assert.Null(LastSettingValue(block, "checkpoint_completion_target"));
        Assert.Null(LastSettingValue(block, "checkpoint_timeout"));
    }

    /// <summary>The generic per-setting source scan (#4214) walks every marker in this list; a block absent
    /// from it would be invisible to that scan even though it is live.</summary>
    [Fact]
    public void ConfMarkerV15_IsInAllManagedConfMarkers()
        => Assert.Contains(DarlingManagedPostgres.ConfMarkerV15, DarlingManagedPostgres.AllManagedConfMarkers);

    /// <summary>
    /// The heal on real files (#4246): a cluster whose conf carries every earlier marker but not v15 gains
    /// exactly one v15 block, with <c>wal_compression</c> live in the file, and a second start appends nothing
    /// more -- the same once-only shape v9-v11 and v13 prove elsewhere (<see
    /// cref="FreshConfHeal_KeepsTimescaleInThePreloadList_AndASecondHealAppendsNoSecondV13"/>), exercised here
    /// through the real <see cref="DarlingManagedPostgres.EnsureConfAppended"/> rather than string
    /// concatenation.
    /// </summary>
    [Fact]
    public void EnsureConfAppended_AppendsV15Once_AndNotAgainOnANextStart()
    {
        var root = Directory.CreateTempSubdirectory("darling-v15-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            File.WriteAllText(confPath, DarlingManagedPostgres.BuildConfAppend(5994));

            var pg = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = 5994, DataDirectory = dataDirectory }, NullLogger.Instance);

            pg.EnsureConfAppended(dataDirectory);
            var first = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(first, DarlingManagedPostgres.ConfMarkerV15));
            Assert.Equal("lz4", LastSettingValue(first, "wal_compression"));

            pg.EnsureConfAppended(dataDirectory);
            var second = File.ReadAllText(confPath);
            Assert.Equal(1, CountOccurrences(second, DarlingManagedPostgres.ConfMarkerV15));
            Assert.Equal("lz4", LastSettingValue(second, "wal_compression"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ===================== v12 wal sizing (#3802) ===================== */

    private const long OneGb = 1024L * 1024 * 1024;

    /// <summary>
    /// The v12 WAL-sizing block (#3802) at the mid case: 64 GB free on a 120 GB data volume, PostgreSQL 18.
    /// <c>max_wal_size</c> lands on 8192MB (64 / 8 = 8 GB, on the ladder exactly), <c>min_wal_size</c> at a
    /// quarter of it, and <c>checkpoint_completion_target</c> is NOT written because 18's default is already
    /// 0.9. Asserted as LAST-OCCURRENCE values, because that is what PostgreSQL honours and what makes this
    /// block an override of v4's fixed 4GB rather than a hope.
    ///
    /// <para>The stamp line is the mechanism: it records the derived rung and the major, and the every-start
    /// check compares against it. The comment line records the headroom the block came from, in the same
    /// formatting as the start's log line, so an operator reading postgresql.conf can see why 8192MB without
    /// the service log. Both are comments to PostgreSQL and neither is an assignment.</para>
    /// </summary>
    [Fact]
    public void WalSizingConfAppend_PinsV12Marker_AndDerivesFromDataVolumeHeadroom()
    {
        var block = DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, postgresMajor: 18);

        Assert.Contains(DarlingManagedPostgres.ConfMarkerV12, block, StringComparison.Ordinal);
        Assert.Equal("8192MB", LastSettingValue(block, "max_wal_size"));
        Assert.Equal("2048MB", LastSettingValue(block, "min_wal_size"));
        Assert.Null(LastSettingValue(block, "checkpoint_completion_target"));

        /* The stamp under the marker is exactly what the every-start check will compare against. */
        var stamp = DarlingManagedPostgres.BuildWalSizingStamp(new DarlingManagedPostgres.WalSettings(8192, 2048), 18);
        Assert.StartsWith(DarlingManagedPostgres.ConfWalSizingStampPrefix, stamp, StringComparison.Ordinal);
        Assert.Contains("\n" + stamp + "\n", block, StringComparison.Ordinal);
        Assert.True(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(block, stamp));

        /* The provenance comment carries the same GB figures the log line does, from the same formatter. */
        Assert.Contains("# derived from 64.0 GB free of 120.0 GB on the data volume", block, StringComparison.Ordinal);
        Assert.Equal("64.0", DarlingManagedPostgres.FormatGb(64 * OneGb));
        Assert.Equal("31.5", DarlingManagedPostgres.FormatGb(31 * OneGb + OneGb / 2));

        /* Exactly two assignments: the blocks compose, they don't compete. Every other block's settings are
           absent, and v4's max_connections in particular is NOT restated. */
        Assert.Equal(
            new[] { "max_wal_size", "min_wal_size" },
            SettingNames(block).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.DoesNotContain("max_connections", block, StringComparison.Ordinal);
        Assert.DoesNotContain("shared_buffers", block, StringComparison.Ordinal);
        Assert.DoesNotContain("maintenance_work_mem", block, StringComparison.Ordinal);

        /* No v8 fingerprint line, or the v8 staleness check silently stops checking. */
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The formula at its boundaries (#3802): <c>clamp(free / 8, 1 GB, 16 GB)</c> floored to the 1-2-4-8-16 GB
    /// ladder, <c>min_wal_size = max(80 MB, max / 4)</c>. The three cases the issue named — tiny disk to the
    /// floor, huge disk to the cap, mid to 8 GB — plus the rung edges that make the ladder a ladder: 31 GB free
    /// floors to 2 GB and 32 GB reaches 4 GB; 127 GB stays at 8 GB and 128 GB reaches the cap. The 32 GB row is
    /// the one that lands where v4's fixed 4GB sat, and everything under it heals DOWN from v4.
    /// </summary>
    [Theory]
    [InlineData(2, 1024, 256)]        /* tiny disk: 2 / 8 = 256 MB -> clamped to the 1 GB floor (PostgreSQL's own default); min at 256 MB */
    [InlineData(8, 1024, 256)]        /* exactly one floor's worth: 8 / 8 = 1 GB */
    [InlineData(16, 2048, 512)]       /* first rung above the floor */
    [InlineData(31, 2048, 512)]       /* 3.875 GB floors to the 2 GB rung, not up to 4 */
    [InlineData(32, 4096, 1024)]      /* v4's old constant, now derived */
    [InlineData(64, 8192, 2048)]      /* mid: the issue's example */
    [InlineData(127, 8192, 2048)]     /* 15.875 GB floors to 8 GB: the last rung before the cap needs a full 128 GB free */
    [InlineData(128, 16384, 4096)]    /* the cap, reached exactly */
    [InlineData(1024, 16384, 4096)]   /* huge disk: 1 TB / 8 = 128 GB -> the maintainer's 16 GB ceiling */
    [InlineData(0, 1024, 256)]        /* nothing free: the floor, never zero */
    public void DeriveWalSettings_PerTier(long freeGb, int maxWalMb, int minWalMb)
    {
        var settings = DarlingManagedPostgres.DeriveWalSettings(freeGb * OneGb);

        Assert.Equal(maxWalMb, settings.MaxWalSizeMb);
        Assert.Equal(minWalMb, settings.MinWalSizeMb);

        /* The block writes exactly these figures in PostgreSQL's MB grammar. */
        var block = DarlingManagedPostgres.BuildWalSizingConfAppend(freeGb * OneGb, 2 * freeGb * OneGb + OneGb, 18);
        Assert.Equal(maxWalMb.ToString(CultureInfo.InvariantCulture) + "MB", LastSettingValue(block, "max_wal_size"));
        Assert.Equal(minWalMb.ToString(CultureInfo.InvariantCulture) + "MB", LastSettingValue(block, "min_wal_size"));
    }

    /// <summary>
    /// A negative reading derives as the floor, not as garbage and not as an exception — the caller gates on an
    /// authoritative read and never passes one, but the formula must be total over its domain regardless.
    /// And the 80 MB <c>min_wal_size</c> floor is real code, not a comment: it is never binding on the ladder
    /// (the 1 GB rung yields 256 MB), which this asserts so a future rung below 320 MB knows where the floor
    /// would bite.
    /// </summary>
    [Fact]
    public void DeriveWalSettings_NegativeReading_DerivesTheFloor_AndTheMinFloorIsNotBindingOnTheLadder()
    {
        var settings = DarlingManagedPostgres.DeriveWalSettings(-1);

        Assert.Equal(1024, settings.MaxWalSizeMb);
        Assert.Equal(256, settings.MinWalSizeMb);
        Assert.True(settings.MinWalSizeMb * 1024L * 1024L > DarlingManagedPostgres.MinWalSizeFloorBytes);
    }

    /// <summary>
    /// <c>checkpoint_completion_target = 0.9</c> is written ONLY below PostgreSQL 14, whose release notes read
    /// <i>"Change checkpoint_completion_target default to 0.9 (Stephen Frost). The previous default was
    /// 0.5."</i> On 14 and later the line is omitted: re-stating a default buys nothing and would read as a
    /// decision the block did not make. An unreadable major (0) pins it — a no-op where the default is already
    /// 0.9 and the fix where it is not. The start line's clause is pinned beside the decision it reports.
    /// </summary>
    [Theory]
    [InlineData(12, true)]
    [InlineData(13, true)]
    [InlineData(0, true)]     /* PG_VERSION unreadable */
    [InlineData(14, false)]
    [InlineData(16, false)]
    [InlineData(17, false)]
    [InlineData(18, false)]   /* the bundled runtime */
    public void WalSizingConfAppend_PinsCheckpointCompletionTarget_OnlyBelowPg14(int postgresMajor, bool pinned)
    {
        var block = DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, postgresMajor);

        Assert.Equal(pinned, DarlingManagedPostgres.PinsCheckpointCompletionTarget(postgresMajor));
        Assert.Equal(pinned ? "0.9" : null, LastSettingValue(block, "checkpoint_completion_target"));

        var note = DarlingManagedPostgres.DescribeCheckpointCompletionTarget(postgresMajor);
        if (pinned)
        {
            Assert.StartsWith("pinned at 0.9", note, StringComparison.Ordinal);
            Assert.Contains(postgresMajor > 0 ? "defaulted to 0.5" : "PG_VERSION unreadable", note, StringComparison.Ordinal);
        }
        else
        {
            Assert.Equal(FormattableString.Invariant($"left at PostgreSQL {postgresMajor}'s default 0.9"), note);
        }

        /* The stamp carries the major, so a pg_upgrade across the 14 boundary re-authors the block once — and
           only once: the same major stamps identically. */
        var settings = DarlingManagedPostgres.DeriveWalSettings(64 * OneGb);
        Assert.True(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(block, DarlingManagedPostgres.BuildWalSizingStamp(settings, postgresMajor)));
        Assert.False(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(block, DarlingManagedPostgres.BuildWalSizingStamp(settings, postgresMajor + 1)));
    }

    /// <summary>
    /// THE STABILITY PROPERTY (#3802): free disk moving WITHIN a rung is not a change. The check runs on every
    /// start and compares the last stamp exactly, so — v8's lesson, with more force, because free disk moves
    /// by gigabytes between any two starts on a store that compresses and drops chunks — an exact quotient
    /// would append a fresh block per start, forever. Four readings between 64 GB and just under 128 GB all
    /// stamp as the 8 GB rung; the two readings that cross a rung boundary do not, so the ladder cannot mask a
    /// real change either. The setting lines are identical across the rung even though the provenance comment
    /// records the reading that produced them.
    /// </summary>
    [Fact]
    public void WalSizingStamp_HeadroomDriftWithinARung_IsNotAChange()
    {
        var conf = "max_wal_size = 4GB\n" + DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, 18);

        foreach (var freeGb in new[] { 64L, 70L, 100L, 127L })
        {
            var stamp = DarlingManagedPostgres.BuildWalSizingStamp(DarlingManagedPostgres.DeriveWalSettings(freeGb * OneGb), 18);
            Assert.True(
                DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(conf, stamp),
                $"{freeGb} GB free should be the same rung as 64 GB, not a change");

            var drifted = DarlingManagedPostgres.BuildWalSizingConfAppend(freeGb * OneGb, 120 * OneGb, 18);
            foreach (var setting in SettingNames(drifted))
            {
                Assert.Equal(LastSettingValue(conf, setting), LastSettingValue(drifted, setting));
            }
        }

        /* Crossing a rung boundary in either direction IS a change. */
        Assert.False(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(
            conf, DarlingManagedPostgres.BuildWalSizingStamp(DarlingManagedPostgres.DeriveWalSettings(128 * OneGb), 18)));
        Assert.False(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(
            conf, DarlingManagedPostgres.BuildWalSizingStamp(DarlingManagedPostgres.DeriveWalSettings(63 * OneGb), 18)));
    }

    /// <summary>
    /// THE HEAL-DOWN CASE the issue asked for (#3802): a block written at 16384MB on a roomy volume, a volume
    /// that has since shrunk to 32 GB free. The last stamp is stale, the block is re-authored, and the value in
    /// force is 4096MB by last-occurrence-wins — the old block is preserved, never edited. Then the resize-back
    /// case that makes "last stamp" rather than "any stamp" the rule: with both stamps in the file, the 16 GB
    /// one is PRESENT but not LAST, so a volume that grew back re-derives instead of latching on the 4 GB
    /// block it would otherwise leave in force.
    /// </summary>
    [Fact]
    public void WalSizingStamp_HealsDown_WhenHeadroomShrinks_AndTheLastStampDecides()
    {
        var roomy = DarlingManagedPostgres.DeriveWalSettings(1024 * OneGb);
        var tight = DarlingManagedPostgres.DeriveWalSettings(32 * OneGb);
        Assert.Equal(16384, roomy.MaxWalSizeMb);
        Assert.Equal(4096, tight.MaxWalSizeMb);

        var conf = DarlingManagedPostgres.BuildWriteThroughputConfAppend()
            + DarlingManagedPostgres.BuildWalSizingConfAppend(1024 * OneGb, 2048 * OneGb, 18);
        Assert.Equal("16384MB", LastSettingValue(conf, "max_wal_size"));

        /* The volume shrank: the block in force was derived to a rung this box no longer affords. */
        var tightStamp = DarlingManagedPostgres.BuildWalSizingStamp(tight, 18);
        Assert.False(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(conf, tightStamp));

        var healed = conf + DarlingManagedPostgres.BuildWalSizingConfAppend(32 * OneGb, 120 * OneGb, 18);
        Assert.True(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(healed, tightStamp));
        Assert.Equal("4096MB", LastSettingValue(healed, "max_wal_size"));
        Assert.Equal("1024MB", LastSettingValue(healed, "min_wal_size"));
        Assert.Equal(2, CountOccurrences(healed, DarlingManagedPostgres.ConfMarkerV12));

        /* The older block is preserved, not edited — the heal path only ever appends. */
        Assert.Contains("max_wal_size = 16384MB", healed, StringComparison.Ordinal);

        /* Resize back: the 16 GB stamp IS in the file, so a Contains test would skip. It is not LAST, so the
           box re-derives, and the appended block makes it current again. */
        var roomyStamp = DarlingManagedPostgres.BuildWalSizingStamp(roomy, 18);
        Assert.Contains(roomyStamp, healed, StringComparison.Ordinal);
        Assert.False(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(healed, roomyStamp));
        Assert.True(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(
            healed + DarlingManagedPostgres.BuildWalSizingConfAppend(1024 * OneGb, 2048 * OneGb, 18), roomyStamp));
    }

    /// <summary>
    /// v12 SUPERSEDES v4's fixed <c>max_wal_size = 4GB</c> by last-occurrence-wins, in BOTH directions
    /// (#3802) — up to the cap on a roomy volume, and DOWN below v4's constant on a tight one, which is
    /// deliberate: v4 sized for a bootstrap burst on a box it never measured. Pinned against a conf carrying
    /// initdb's commented default as a decoy, so the count is of live assignments and not of substrings.
    /// v4 keeps its <c>max_connections</c>; v12 does not restate it.
    /// </summary>
    [Fact]
    public void WalSizingConfAppend_SupersedesV4sFixedCeiling_ByLastOccurrence_UpAndDown()
    {
        const string StockPreamble = "#max_wal_size = 1GB\n#min_wal_size = 80MB\n#checkpoint_completion_target = 0.9\n";
        var v4 = StockPreamble + DarlingManagedPostgres.BuildWriteThroughputConfAppend();
        Assert.Equal("4GB", LastSettingValue(v4, "max_wal_size"));
        Assert.Null(LastSettingValue(v4, "min_wal_size"));

        var roomy = v4 + DarlingManagedPostgres.BuildWalSizingConfAppend(1024 * OneGb, 2048 * OneGb, 18);
        Assert.Equal("16384MB", LastSettingValue(roomy, "max_wal_size"));
        Assert.Equal("4096MB", LastSettingValue(roomy, "min_wal_size"));

        var tight = v4 + DarlingManagedPostgres.BuildWalSizingConfAppend(2 * OneGb, 40 * OneGb, 18);
        Assert.Equal("1024MB", LastSettingValue(tight, "max_wal_size"));   /* below v4's 4GB: the box cannot afford it */
        Assert.Equal("256MB", LastSettingValue(tight, "min_wal_size"));

        /* Two live assignments of max_wal_size (v4's and v12's), one of max_connections (v4's alone). */
        Assert.Equal(2, CountAssignments(tight, "max_wal_size"));
        Assert.Equal(1, CountAssignments(tight, "max_connections"));
        Assert.Equal(1, CountOccurrences(tight, DarlingManagedPostgres.ConfMarkerV4));
    }

    /// <summary>
    /// The two every-start heals cannot read each other's line (#3802). v8 keys on the LAST line carrying
    /// <see cref="DarlingManagedPostgres.ConfHardwareFingerprintPrefix"/> in the conf as read at the top of
    /// <c>EnsureConfAppended</c>; v12 is appended after it and carries its own stamp under
    /// <see cref="DarlingManagedPostgres.ConfWalSizingStampPrefix"/>. If either prefix were a substring of
    /// the other, one heal's <c>LastIndexOf</c> would land on the other's line on the next start and compare
    /// against a line it can never match — an append per start, forever. Asserted both ways, and then
    /// end-to-end: a v8 block followed by a v12 block still reports v8 current, and vice versa.
    /// </summary>
    [Fact]
    public void WalSizingStamp_AndHardwareFingerprint_DoNotReadEachOther()
    {
        Assert.DoesNotContain(DarlingManagedPostgres.ConfWalSizingStampPrefix, DarlingManagedPostgres.ConfHardwareFingerprintPrefix, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, DarlingManagedPostgres.ConfWalSizingStampPrefix, StringComparison.Ordinal);

        const long sixteenGb = 16L * 1024 * 1024 * 1024;
        var v8 = DarlingManagedPostgres.BuildHardwareSizingConfAppend(sixteenGb, 40);
        var v12 = DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, 18);
        var v8Fingerprint = DarlingManagedPostgres.BuildHardwareFingerprint(sixteenGb, 40);
        var v12Stamp = DarlingManagedPostgres.BuildWalSizingStamp(DarlingManagedPostgres.DeriveWalSettings(64 * OneGb), 18);

        Assert.True(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(v8 + v12, v8Fingerprint));
        Assert.True(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(v8 + v12, v12Stamp));
        Assert.True(DarlingManagedPostgres.ConfHasCurrentHardwareFingerprint(v12 + v8, v8Fingerprint));
        Assert.True(DarlingManagedPostgres.ConfHasCurrentWalSizingStamp(v12 + v8, v12Stamp));

        /* And neither block writes the other's line at all. */
        Assert.DoesNotContain(DarlingManagedPostgres.ConfWalSizingStampPrefix, v8, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, v12, StringComparison.Ordinal);
    }

    /// <summary>
    /// The pure half of the ALTER SYSTEM check (#3802): the WAL keys <c>postgresql.auto.conf</c> assigns, as
    /// written. The fixture is the file's real shape — the two-line "Do not edit" header ALTER SYSTEM writes,
    /// quoted values, and a setting this block does not own sitting between the ones it does. The header lines
    /// are comments and must not register; <c>shared_buffers</c> is not a WAL key and must not register;
    /// duplicate keys resolve to the LAST one, which is what PostgreSQL honours. No file, or an empty one,
    /// yields nothing.
    /// </summary>
    [Fact]
    public void FindWalSizingAutoConfOverrides_FindsTheWalKeys_IgnoresHeaderAndOtherSettings()
    {
        const string AutoConf =
            "# Do not edit this file manually!\n" +
            "# It will be overwritten by the ALTER SYSTEM command.\n" +
            "max_wal_size = '2GB'\n" +
            "shared_buffers = '512MB'\n" +
            "checkpoint_completion_target = '0.7'\n" +
            "max_wal_size = '16GB'\n";

        var overrides = DarlingManagedPostgres.FindWalSizingAutoConfOverrides(AutoConf);

        Assert.Equal(2, overrides.Count);
        Assert.Contains(("max_wal_size", "'16GB'"), overrides);              /* the LAST assignment, not the first */
        Assert.Contains(("checkpoint_completion_target", "'0.7'"), overrides);
        Assert.DoesNotContain(overrides, o => o.Name == "shared_buffers");
        Assert.DoesNotContain(overrides, o => o.Name == "min_wal_size");

        Assert.Empty(DarlingManagedPostgres.FindWalSizingAutoConfOverrides(null));
        Assert.Empty(DarlingManagedPostgres.FindWalSizingAutoConfOverrides(string.Empty));
        Assert.Empty(DarlingManagedPostgres.FindWalSizingAutoConfOverrides("# Do not edit this file manually!\n"));

        /* The three names the scan looks for are exactly the three the block can write. */
        var block = DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, postgresMajor: 13);
        Assert.Equal(
            DarlingManagedPostgres.WalSizingSettingNames.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
            SettingNames(block).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// THE ALTER SYSTEM PIN the issue asked for (#3802): a fake <c>postgresql.auto.conf</c> carrying
    /// <c>max_wal_size = '2GB'</c> yields ONE warning naming the key, the value as written, the figure the
    /// product derived instead, and the precedence that makes the file's value win — and the block is
    /// unchanged, because the builder is pure and the override feeds only the log. Nothing is edited: the
    /// auto.conf's bytes are the same after the check. A data directory with no auto.conf logs nothing.
    /// </summary>
    [Fact]
    public void AlterSystemOverride_IsLogged_AndTheBlockIsUnchanged()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(dataDirectory);
            var config = new PostgresConfig { Managed = true, Port = 5994, DataDirectory = dataDirectory };
            var derived = DarlingManagedPostgres.DeriveWalSettings(64 * OneGb);

            /* No auto.conf: nothing to say. */
            var quiet = new CapturingTestLogger();
            new DarlingManagedPostgres(config, quiet).LogWalSizingAutoConfOverrides(dataDirectory, derived);
            Assert.Equal("(no log lines captured)", quiet.Joined);

            const string AutoConf = "# Do not edit this file manually!\n# It will be overwritten by the ALTER SYSTEM command.\nmax_wal_size = '2GB'\n";
            var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");
            File.WriteAllText(autoConfPath, AutoConf);

            var before = DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, 18);
            var logger = new CapturingTestLogger();
            new DarlingManagedPostgres(config, logger).LogWalSizingAutoConfOverrides(dataDirectory, derived);

            var line = Assert.Single(logger.Joined.Split(" | "));
            Assert.StartsWith("Warning: ", line, StringComparison.Ordinal);
            Assert.Contains("postgresql.auto.conf sets max_wal_size = '2GB'", line, StringComparison.Ordinal);
            Assert.Contains("AFTER postgresql.conf", line, StringComparison.Ordinal);
            Assert.Contains("derived 8192MB", line, StringComparison.Ordinal);
            Assert.Contains("ALTER SYSTEM RESET max_wal_size", line, StringComparison.Ordinal);

            /* Unchanged block, untouched file. */
            Assert.Equal(before, DarlingManagedPostgres.BuildWalSizingConfAppend(64 * OneGb, 120 * OneGb, 18));
            Assert.Equal(AutoConf, File.ReadAllText(autoConfPath));
        }
        finally
        {
            root.Delete(recursive: true);
        }
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

    /// <summary>Round-1 security review, #4280 Low 3: ResolveDataDirectory itself normalizes a trailing
    /// separator, rather than relying on every caller downstream to trim it before building a quoted "-D"
    /// argument (a trailing backslash there escapes the closing quote).</summary>
    [Fact]
    public void ResolveDataDirectory_TrimsATrailingSeparator()
    {
        Assert.Equal(
            @"D:\darling\pg",
            DarlingManagedPostgres.ResolveDataDirectory(new PostgresConfig { DataDirectory = @"D:\darling\pg\" }));
        Assert.Equal(
            @"D:\darling\pg",
            DarlingManagedPostgres.ResolveDataDirectory(new PostgresConfig { DataDirectory = @"D:\darling\pg" }));
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

            /* #4215: by the time EnsureRunningAsync returns, Step A has already run post-start and
               rewritten postgresql.conf to a single include line -- every v-block that rode the first-run
               append now lives in darling-managed.conf instead. Every check below that used to read
               postgresql.conf for a marker or a written-but-not-live value reads darling-managed.conf now;
               a check that already reads the LIVE pg_settings value (below, against the running server) is
               dropped here rather than duplicated against a file. */
            var conf = File.ReadAllText(Path.Combine(dataDirectory, "postgresql.conf"));
            var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
            var managedConf = File.ReadAllText(managedConfPath);
            var migrationDiagnostics =
                $"LastManagedConfVerification={owner.LastManagedConfVerification}; " +
                $"Classify={ManagedConfMigrationState.Classify(dataDirectory)}; " +
                $"files=[{string.Join(", ", Directory.GetFiles(dataDirectory).Select(Path.GetFileName))}]";

            /* postgresql.conf itself: exactly one include line, no v-marker of any kind. */
            Assert.True(
                CountOccurrences(conf, ManagedConfFile.IncludeLine) == 1,
                $"postgresql.conf should carry exactly one include line. {migrationDiagnostics}");
            foreach (var marker in DarlingManagedPostgres.AllManagedConfMarkers)
            {
                Assert.True(
                    !conf.Contains(marker, StringComparison.Ordinal),
                    $"postgresql.conf should carry no v-marker text ({marker}). {migrationDiagnostics}");
            }

            /* darling-managed.conf's keys, read the same way the migration itself reads them
               (DarlingManagedPostgres.ParseConfText), rather than by matching literal rendered text — the
               render's unit and exact value are host-dependent (#4336). Last-occurrence-wins, same as the
               server. */
            var managedConfKeys = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(managedConf))
            {
                managedConfKeys[name] = value;
            }
            var managedConfKeysDiagnostics =
                $"parsed keys=[{string.Join(", ", managedConfKeys.Select(kv => $"{kv.Key}={kv.Value}"))}]";

            /* No live check exists in this test for shared_preload_libraries or listen_addresses, so both
               move to darling-managed.conf's rendered text rather than being dropped. */
            Assert.True(
                managedConfKeys.TryGetValue("shared_preload_libraries", out var preload)
                && (preload == "'timescaledb,pg_stat_statements'" || preload == "'timescaledb'"),
                $"darling-managed.conf should carry the timescaledb preload. {migrationDiagnostics} {managedConfKeysDiagnostics}");
            Assert.True(
                managedConfKeys.TryGetValue("listen_addresses", out var listenAddresses) && listenAddresses == "'127.0.0.1'",
                $"darling-managed.conf should carry listen_addresses. {migrationDiagnostics} {managedConfKeysDiagnostics}");

            /* max_worker_processes: DROPPED as a conf-text check -- reader.GetString(2) below already proves
               this LIVE against the running server, and duplicating it against a file adds nothing. */

            /* v3 memory sizing rode the same first-run append, derived from THIS host's physical RAM: presence
               moves to darling-managed.conf (the exact MB depend on the runner); the values themselves are
               proven LIVE below (work_mem/shared_buffers NotEqual the stock defaults). */
            Assert.True(
                managedConfKeys.ContainsKey("shared_buffers"),
                $"darling-managed.conf should carry shared_buffers. {migrationDiagnostics} {managedConfKeysDiagnostics}");
            Assert.True(
                managedConfKeys.ContainsKey("work_mem"),
                $"darling-managed.conf should carry work_mem. {migrationDiagnostics} {managedConfKeysDiagnostics}");

            /* v4 write throughput and v5 co-located sizing: no live check of these specific values exists in
               this test, so their settings move to darling-managed.conf rather than being dropped. max_wal_size
               is checked for presence only -- its rendered value depends on the runner's disk size (the same
               ladder BuildWalSizingConfAppend uses), not a fixed "4GB". */
            Assert.True(
                managedConfKeys.TryGetValue("max_connections", out var maxConnections) && maxConnections == "'200'",
                $"darling-managed.conf should carry max_connections. {migrationDiagnostics} {managedConfKeysDiagnostics}");
            Assert.True(
                managedConfKeys.ContainsKey("max_wal_size"),
                $"darling-managed.conf should carry max_wal_size. {migrationDiagnostics} {managedConfKeysDiagnostics}");

            /* v6 log rotation: the logging collector is live, proven by the weekday ring file it creates
               under <data>\log the moment it starts (#1652) -- unaffected by where the setting text lives. */
            var ringFiles = Directory.GetFiles(Path.Combine(dataDirectory, "log"), "postgresql-*.log");
            Assert.NotEmpty(ringFiles);

            /* v7 compression memory (#1777): its EFFECTIVE value (the last assignment, the one the server
               honors) now lives in darling-managed.conf, and is captured here to compare against the live
               setting below. */
            var confMaintenanceWorkMem = LastSettingValue(managedConf, "maintenance_work_mem");
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
                    "pg_size_bytes(current_setting('maintenance_work_mem')), pg_size_bytes(@confMaintenance), " +
                    "current_setting('wal_compression')",
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

                /* #4246: the v15 block is LIVE, not merely written -- wal_compression takes effect from
                   postgresql.conf on a reload (superuser-context, not sighup, but reload-eligible all the
                   same) and this is the very start that appended it, so a fresh cluster proves it without a
                   separate reload. Not the PostgreSQL stock default (off). checkpoint_timeout is untouched:
                   the interval is held, not shipped (#4246's amended ruling). */
                Assert.Equal("lz4", reader.GetString(7));
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

            var secondDiagnostics =
                $"LastManagedConfVerification={second.LastManagedConfVerification}; " +
                $"Classify={ManagedConfMigrationState.Classify(dataDirectory)}; " +
                $"files=[{string.Join(", ", Directory.GetFiles(dataDirectory).Select(Path.GetFileName))}]";

            /* The idempotent second run: this data directory is Verified after the first run, so the second
               run's pre-start write (EnsureManagedConfReadyAsync) re-renders darling-managed.conf and finds
               it byte-identical to what is already on disk -- no v-marker ever re-enters postgresql.conf,
               and neither file changes. */
            var confAfterSecond = File.ReadAllText(Path.Combine(dataDirectory, "postgresql.conf"));
            var managedConfAfterSecond = File.ReadAllText(managedConfPath);
            Assert.True(conf == confAfterSecond, $"postgresql.conf should be byte-identical after the idempotent second run. {secondDiagnostics}");
            Assert.True(managedConf == managedConfAfterSecond, $"darling-managed.conf should be byte-identical after the idempotent second run. {secondDiagnostics}");
            Assert.True(
                CountOccurrences(confAfterSecond, ManagedConfFile.IncludeLine) == 1,
                $"postgresql.conf should still carry exactly one include line. {secondDiagnostics}");

            Assert.True(
                ManagedConfMigrationState.Classify(dataDirectory) == ManagedConfMigrationState.Kind.Verified,
                $"The data directory should classify Verified after the first run. {migrationDiagnostics}");
            Assert.True(
                ManagedConfMigrationState.Classify(dataDirectory) == ManagedConfMigrationState.Kind.Verified,
                $"The data directory should classify Verified after the idempotent second run. {secondDiagnostics}");

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

            /* A real store, provisioned the normal way. Step A migrates the v7 block out of
               postgresql.conf into darling-managed.conf during this same call (#4215), so the pre-#1777
               shape this test needs to rewind is only available in the pre-migration BACKUP. */
            await owner.EnsureRunningAsync(timeout.Token);
            await owner.StopIfStartedByThisProcessAsync();

            string? derivedValue = null;
            await RewindDataDirectoryToLegacyConfAsync(
                dataDirectory,
                preFixConf =>
                {
                    /* Rewind the conf to its pre-#1777 shape: drop the v7 block (appended last, so the marker
                       is a clean truncation point) and put the OLD formula's 16 GB landing value in the v3
                       block. */
                    var v7Index = preFixConf.IndexOf(DarlingManagedPostgres.ConfMarkerV7, StringComparison.Ordinal);
                    Assert.True(v7Index > 0, "The pre-migration conf should carry the v7 block before it is rewound.");
                    derivedValue = LastSettingValue(preFixConf, "maintenance_work_mem");
                    Assert.NotNull(derivedValue);

                    var legacyConf = preFixConf[..v7Index]
                        .Replace($"maintenance_work_mem = {derivedValue}", $"maintenance_work_mem = {legacyValue}", StringComparison.Ordinal);
                    Assert.DoesNotContain(DarlingManagedPostgres.ConfMarkerV7, legacyConf, StringComparison.Ordinal);
                    Assert.Equal(legacyValue, LastSettingValue(legacyConf, "maintenance_work_mem"));
                    return legacyConf;
                },
                timeout.Token);
            Assert.NotNull(derivedValue);
            /* The whole test turns on before != after. A host with ~3.2 GB RAM would derive exactly 819MB
               through the 25% guard and make the comparison vacuous — that is a property of the RUNNER,
               not a product failure, so skip rather than pass emptily. */
            Assert.SkipWhen(string.Equals(derivedValue, legacyValue, StringComparison.Ordinal),
                $"This host derives maintenance_work_mem = {derivedValue}, the same value the test uses as the legacy reading.");

            /* The service-owned start: on a Legacy conf, EnsureConfAppended heals BEFORE pg_ctl start (so the
               raised value is live on this very start), and Step A then migrates the healed value into
               darling-managed.conf post-start — postgresql.conf itself never carries the v7 marker again. */
            var healedOwner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var healedConnectionString = await healedOwner.EnsureRunningAsync(timeout.Token);
            try
            {
                var healedPostgresqlConf = await File.ReadAllTextAsync(confPath, timeout.Token);
                var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
                var healedManagedConf = File.Exists(managedConfPath)
                    ? await File.ReadAllTextAsync(managedConfPath, timeout.Token)
                    : string.Empty;
                var diagnostics =
                    $"LastManagedConfVerification={healedOwner.LastManagedConfVerification}; " +
                    $"Classify={ManagedConfMigrationState.Classify(dataDirectory)}; " +
                    $"files=[{string.Join(", ", Directory.GetFiles(dataDirectory).Select(Path.GetFileName))}]";

                Assert.True(
                    !healedPostgresqlConf.Contains(DarlingManagedPostgres.ConfMarkerV7, StringComparison.Ordinal),
                    $"postgresql.conf should carry no v7 marker after Step A migrates it out. {diagnostics}");
                Assert.True(
                    healedManagedConf.Contains($"maintenance_work_mem = {derivedValue}", StringComparison.Ordinal),
                    $"darling-managed.conf should carry the raised maintenance_work_mem value. {diagnostics}");

                var (live, expected) = await ReadSettingAndLiteralBytesAsync(
                    healedConnectionString, "maintenance_work_mem", derivedValue!, timeout.Token);
                Assert.True(expected == live, $"maintenance_work_mem was not live at the raised value. {diagnostics}");
                Assert.True(live != 819L * 1024 * 1024, $"maintenance_work_mem was still the legacy value live. {diagnostics}");

                Assert.True(
                    ManagedConfMigrationState.Classify(dataDirectory) == ManagedConfMigrationState.Kind.Verified,
                    $"The data directory should classify Verified after the heal start. {diagnostics}");
            }
            finally
            {
                await healedOwner.StopIfStartedByThisProcessAsync();
            }

            /* A third start must not re-append a v7 block into postgresql.conf — already migrated, nothing
               left to heal there. */
            Assert.DoesNotContain(
                DarlingManagedPostgres.ConfMarkerV7,
                await File.ReadAllTextAsync(confPath, timeout.Token),
                StringComparison.Ordinal);
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// #3175 PROPAGATION, proven against a real server: a cluster whose conf carries the v1 marker (and
    /// every later one) but NO v11 block — the shape of every store initdb'd before this fix — must gain
    /// <c>timescaledb.enable_job_execution_logging = on</c> on its next service-owned start, and must be
    /// serving it, not merely carrying it in a file.
    ///
    /// <para>The pre-fix conf is reconstructed exactly rather than approximated: v11 is the last block
    /// appended, so truncating at its marker restores the old file byte-for-byte, and the GUC then has no
    /// assignment anywhere — which is precisely the field shape (all ten markers present, no GUC line,
    /// effective <c>off</c> with <c>source = default</c>).</para>
    ///
    /// <para><b>The positive control, because "on" alone would be worthless here.</b> The reading is taken
    /// together with <c>boot_val</c> and <c>source</c> in one row. <c>boot_val = 'off'</c> proves the
    /// compiled-in default is off, so an observed <c>on</c> cannot be the value this server would have had
    /// regardless — the exact confusion this whole issue is about, one level up. <c>source</c> then names
    /// where the <c>on</c> came from, and <c>sourcefile</c> proves it was OUR postgresql.conf rather than
    /// an <c>ALTER SYSTEM</c> in postgresql.auto.conf. Without those three the assertion would pass on a
    /// server that was already on for an unrelated reason.</para>
    ///
    /// <para><c>context</c> is pinned as well, against the BUNDLED TimescaleDB rather than only the version
    /// this was measured on locally: the marker's doc comment says a reload would carry this setting and
    /// that the append-before-start is what makes a reload unnecessary. If TimescaleDB ever made it
    /// restart-only, that reasoning would be wrong and this goes red instead of the comment quietly
    /// becoming fiction.</para>
    /// </summary>
    [Fact]
    public async Task ExistingStore_GainsJobExecutionLogging_OnNextStart_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe; " +
            "Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under artifacts\\pg-runtime-work\\assemble\\pg-runtime) " +
            "to run the #3175 conf-propagation E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pgv11-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");

        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));

            /* A real store, provisioned the normal way. Step A migrates the v11 block out of
               postgresql.conf into darling-managed.conf during this same call (#4215), so the pre-#3175
               shape this test needs to rewind is only available in the pre-migration BACKUP. */
            await owner.EnsureRunningAsync(timeout.Token);
            await owner.StopIfStartedByThisProcessAsync();

            await RewindDataDirectoryToLegacyConfAsync(
                dataDirectory,
                preFixConf =>
                {
                    /* Rewind to the pre-#3175 shape: v11 is appended last, so its marker is a clean
                       truncation point. */
                    var v11Index = preFixConf.IndexOf(DarlingManagedPostgres.ConfMarkerV11, StringComparison.Ordinal);
                    Assert.True(v11Index > 0, "The pre-migration conf should carry the v11 block before it is rewound.");

                    var legacyConf = preFixConf[..v11Index];

                    /* The rewound file is the field shape, asserted on both axes: the v1 marker IS present
                       (so the v1 check will skip, which is the whole defect) and the GUC has NO assignment
                       anywhere — not an assignment set to off, an absence. */
                    Assert.Contains(DarlingManagedPostgres.ConfMarker, legacyConf, StringComparison.Ordinal);
                    Assert.DoesNotContain(DarlingManagedPostgres.ConfMarkerV11, legacyConf, StringComparison.Ordinal);
                    Assert.Null(LastSettingValue(legacyConf, StoreSelfMetrics.JobExecutionLoggingSetting));
                    return legacyConf;
                },
                timeout.Token);

            /* The service-owned start: on a Legacy conf, EnsureConfAppended heals BEFORE pg_ctl start (so the
               setting is live on this very start), and Step A then migrates the healed value into
               darling-managed.conf post-start — postgresql.conf itself never carries the v11 marker again. */
            var healedOwner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var healedConnectionString = await healedOwner.EnsureRunningAsync(timeout.Token);
            try
            {
                var healedPostgresqlConf = await File.ReadAllTextAsync(confPath, timeout.Token);
                var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
                var healedManagedConf = File.Exists(managedConfPath)
                    ? await File.ReadAllTextAsync(managedConfPath, timeout.Token)
                    : string.Empty;
                var diagnostics =
                    $"LastManagedConfVerification={healedOwner.LastManagedConfVerification}; " +
                    $"Classify={ManagedConfMigrationState.Classify(dataDirectory)}; " +
                    $"files=[{string.Join(", ", Directory.GetFiles(dataDirectory).Select(Path.GetFileName))}]";

                Assert.True(
                    !healedPostgresqlConf.Contains(DarlingManagedPostgres.ConfMarkerV11, StringComparison.Ordinal),
                    $"postgresql.conf should carry no v11 marker after Step A migrates it out. {diagnostics}");
                Assert.True(
                    !healedPostgresqlConf.Contains(DarlingManagedPostgres.ConfMarker, StringComparison.Ordinal),
                    $"postgresql.conf should carry no v-marker at all after Step A migrates it out. {diagnostics}");
                Assert.True(
                    healedManagedConf.Contains(
                        $"{StoreSelfMetrics.JobExecutionLoggingSetting} = 'on'", StringComparison.Ordinal)
                    || string.Equals("on", LastSettingValue(healedManagedConf, StoreSelfMetrics.JobExecutionLoggingSetting), StringComparison.Ordinal),
                    $"darling-managed.conf should carry the healed job-execution-logging setting. {diagnostics}");

                /* Live ASSIGNMENTS in darling-managed.conf, not substring hits: initdb's generated
                   postgresql.conf carries a commented #shared_preload_libraries line, but that file no
                   longer holds any of our blocks after migration — the managed file is the only place the
                   product's own shared_preload_libraries line can live now. */
                Assert.True(
                    CountAssignments(healedManagedConf, "shared_preload_libraries") == 1,
                    $"darling-managed.conf should carry exactly one shared_preload_libraries assignment. {diagnostics}");
                Assert.True(
                    string.Equals("timescaledb,pg_stat_statements", LastConfAssignment(healedManagedConf, "shared_preload_libraries"), StringComparison.Ordinal),
                    $"darling-managed.conf's shared_preload_libraries should merge timescaledb and pg_stat_statements. {diagnostics}");

                /* A PRECONDITION of the reading below, not part of what the heal is judged on — and the trap
                   in this whole area. `timescaledb` in shared_preload_libraries loads the LOADER, and the
                   loader pulls in the VERSIONED library only for a database that has the extension. The
                   GUCs the versioned library defines, this one among them, are therefore not registered
                   until then and pg_settings returns NO ROW for the name at all. Measured on 2.30.0/PG17
                   with the loader preloaded: from a database with the extension, one row; from a database
                   created TEMPLATE template0 without it, ZERO rows for this GUC while the LOADER-defined
                   timescaledb.max_background_workers still had one. EnsureRunningAsync creates the store
                   database; TimescaleSupport creates the extension later in the worker's bootstrap, so this
                   stands in for that step.

                   Through LiveTimescaleProbe, not a raw CREATE EXTENSION: that statement TERMINATES THE
                   BACKEND when the library is on disk but unpreloaded (#1922), and the probe both risks a
                   connection nobody else holds and sets the search_path the extension lands in. Asserted
                   rather than fire-and-forget, so a precondition that silently did not hold cannot present
                   as the setting being absent. */
                Assert.True(
                    await LiveTimescaleProbe.TryEnableAsync(healedConnectionString, timeout.Token),
                    "TimescaleDB could not be enabled on the healed store, so the versioned library never "
                    + "loaded and its GUCs were never registered — the reading below would be absent for that "
                    + "reason rather than for anything the v11 heal did.");

                var reading = await ReadJobExecutionLoggingSettingAsync(healedConnectionString, timeout.Token);

                /* The positive control first: the compiled-in default is OFF, so the value below cannot be
                   what this server would have served anyway. */
                Assert.Equal("off", reading.BootValue);
                Assert.Equal("on", reading.Setting);
                Assert.Equal("configuration file", reading.Source);

                /* #4215: EnsureConfAppended's v11 heal lands the setting directly in postgresql.conf on
                   THIS start, but Step A runs post-start and rewrites postgresql.conf to the single include
                   line before the next start ever boots on it -- so darling-managed.conf, not a v11 heal
                   line, is what pg_settings reports as the source. */
                Assert.True(
                    Path.GetFullPath(Path.Combine(dataDirectory, ManagedConfFile.FileName))
                        == Path.GetFullPath(reading.SourceFile ?? string.Empty),
                    $"sourcefile should be darling-managed.conf, was {reading.SourceFile}. {diagnostics}");

                /* SIGHUP-context, which is what makes "the append before pg_ctl start is enough, and no
                   reload is issued" a decision rather than a gamble. */
                Assert.True("sighup" == reading.Context, $"context should be sighup, was {reading.Context}. {diagnostics}");

                Assert.True(
                    ManagedConfMigrationState.Classify(dataDirectory) == ManagedConfMigrationState.Kind.Verified,
                    $"The data directory should classify Verified after the heal start. {diagnostics}");
            }
            finally
            {
                await healedOwner.StopIfStartedByThisProcessAsync();
            }

            /* A third start must not re-append a v11 block into postgresql.conf — already migrated, nothing
               left to heal there. */
            Assert.DoesNotContain(
                DarlingManagedPostgres.ConfMarkerV11,
                await File.ReadAllTextAsync(confPath, timeout.Token),
                StringComparison.Ordinal);
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// One <c>pg_settings</c> row for the job-execution-logging GUC, read through an UNPOOLED connection so
    /// the reading always costs real I/O against the server running right now. All five columns in one row
    /// rather than five reads: the value only means anything beside its default and its provenance.
    /// </summary>
    private static async Task<(string? Setting, string? Source, string? SourceFile, string? BootValue, string? Context)>
        ReadJobExecutionLoggingSettingAsync(string connectionString, CancellationToken cancellationToken)
    {
        var unpooled = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;
        await using var connection = new NpgsqlConnection(unpooled);
        await connection.OpenAsync(cancellationToken);
        using var command = new NpgsqlCommand(
            "SELECT setting, source, sourcefile, boot_val, context FROM pg_settings WHERE name = @name",
            connection);
        command.Parameters.AddWithValue("name", StoreSelfMetrics.JobExecutionLoggingSetting);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        /* A missing row is NOT "the setting is off", and it is not only "the library is not preloaded"
           either: the versioned TimescaleDB library that defines this GUC loads only for a database that
           has the extension, so an unenabled store answers the same way. Both causes are named in the
           message below rather than one guessed at — which cost a CI round to learn. */
        Assert.True(
            await reader.ReadAsync(cancellationToken),
            $"pg_settings has no row for {StoreSelfMetrics.JobExecutionLoggingSetting}. Either the v1 " +
            "shared_preload_libraries line did not take effect, or the timescaledb extension is not installed in " +
            "this database — the versioned library that defines this GUC is only loaded for a database that has it.");

        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));
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

    internal static int FindFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>Postgres releases its files a beat after fast shutdown — retry the temp-dir delete.</summary>
    internal static void TryDeleteRecursive(string path)
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

    /// <summary>
    /// Rewinds <paramref name="dataDirectory"/> to a Legacy shape a next start will re-classify as such
    /// (#4215/#4336): after one real provisioning start and stop, Step A has already migrated the v-blocks
    /// out of <c>postgresql.conf</c> and into <c>darling-managed.conf</c>, so the LEGACY conf a pre-fix store
    /// would have carried is not on disk any more as text to slice — it must be rebuilt. The first start's own
    /// backup (<c>postgresql.conf.pre-4215.*.bak</c>) is exactly that Legacy conf, since it is the snapshot Step
    /// A took immediately before rewriting the file; <paramref name="transform"/> applies the test's own rewind
    /// (stripping/duplicating/truncating a block) to that text. Every migration artifact this start produced —
    /// the managed file itself, its verified stamp, any pending file, the last-good copy, and the backup — is
    /// then deleted, so <see cref="ManagedConfMigrationState.Classify"/> reads <see
    /// cref="ManagedConfMigrationState.Kind.Legacy"/> again on the very next call, exactly as a real pre-fix
    /// store would.
    /// </summary>
    private static async Task RewindDataDirectoryToLegacyConfAsync(
        string dataDirectory, Func<string, string> transform, CancellationToken cancellationToken)
    {
        var backups = Directory.GetFiles(dataDirectory, "postgresql.conf.pre-4215.*.bak");
        Assert.True(
            backups.Length > 0,
            $"No postgresql.conf.pre-4215.*.bak found in {dataDirectory} after the first provisioning start; " +
            "Step A should have written one. Files present: " +
            string.Join(", ", Directory.GetFiles(dataDirectory).Select(Path.GetFileName)));
        Array.Sort(backups, StringComparer.Ordinal);
        var preFixConf = await File.ReadAllTextAsync(backups[0], cancellationToken);

        var legacyConf = transform(preFixConf);

        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        await File.WriteAllTextAsync(confPath, legacyConf, cancellationToken);

        TryDelete(Path.Combine(dataDirectory, ManagedConfFile.FileName));
        TryDelete(Path.Combine(dataDirectory, ManagedConfMigrationSteps.StampFileName));
        TryDelete(Path.Combine(dataDirectory, ManagedConfMigrationSteps.PendingFileName));
        TryDelete(Path.Combine(dataDirectory, ManagedConfFile.LastGoodFileName));
        foreach (var backup in backups)
        {
            TryDelete(backup);
        }

        Assert.Equal(
            ManagedConfMigrationState.Kind.Legacy,
            ManagedConfMigrationState.Classify(dataDirectory));
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
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

    /// <summary>
    /// #4280: a server <see cref="DarlingStoreUpgrade.CarryAutoConfAsync(string, string, string, CancellationToken)"/>'s
    /// auto.conf trial left on a private port must be stopped BEFORE <c>IsRunningAsync</c> below — <c>pg_ctl
    /// status</c> cannot tell that orphan apart from the store's own postmaster (it answers "running" for a
    /// postmaster on ANY port), so checking first would read a leftover trial as the store already being up.
    /// Pinned at the source: the whole point is the ORDER of two calls inside one method, which no
    /// behavioral test can isolate without a live cluster and a trial deliberately made un-stoppable.
    /// </summary>
    [Fact]
    public void EnsureRunningAsync_StopsAQuiescedOrphan_BeforeItChecksIfAlreadyRunning()
    {
        var source = ReadManagedPostgresSource();

        var methodStart = source.IndexOf(
            "public async Task<string> EnsureRunningAsync(CancellationToken cancellationToken)", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "could not find EnsureRunningAsync's declaration");

        var methodEnd = source.IndexOf(
            "public async Task StopIfStartedByThisProcessAsync()", methodStart, StringComparison.Ordinal);
        Assert.True(methodEnd > methodStart, "could not find the next method, to bound the search to EnsureRunningAsync alone");

        var method = source[methodStart..methodEnd];

        var orphanStop = method.IndexOf("StopQuiescedUpdateOrphanAsync(binDirectory, _dataDirectory)", StringComparison.Ordinal);
        var runningCheck = method.IndexOf("await IsRunningAsync(binDirectory, cancellationToken)", StringComparison.Ordinal);

        Assert.True(orphanStop >= 0, "EnsureRunningAsync must stop a quiesced-start orphan before it does anything else with the data directory");
        Assert.True(runningCheck > orphanStop, "the orphan stop must run BEFORE IsRunningAsync, or a leftover trial server is read as the store already running");
    }

    /// <summary>
    /// #4280 item 1: the real-start fallback exists for a "trial-passed" carry alone, and never for a
    /// cancellation the caller itself requested — a service stop during the first real start after an
    /// upgrade must not read as "the start failed" and fall back to dropping settings that already passed
    /// their trial. <see cref="DarlingManagedPostgres.ShouldFallBackToHeaderOnly"/> is the catch clause's own
    /// <c>when</c> filter, tested directly because a live cancelled start needs a running cluster the source
    /// pin below cannot exercise.
    /// </summary>
    [Theory]
    [InlineData(DarlingStoreUpgrade.AutoConfCarryStateTrialPassed, false, true)]
    [InlineData(DarlingStoreUpgrade.AutoConfCarryStateTrialPassed, true, false)]
    [InlineData(DarlingStoreUpgrade.AutoConfCarryStateCarrying, false, false)]
    public void ShouldFallBackToHeaderOnly_TrialPassedAndNotCancelled_IsTheOnlyTrueCase(string state, bool cancelled, bool expected)
    {
        var marker = new DarlingStoreUpgrade.AutoConfCarryMarker(state, Array.Empty<string>());
        using var cts = new CancellationTokenSource();
        if (cancelled)
        {
            cts.Cancel();
        }

        Assert.Equal(expected, DarlingManagedPostgres.ShouldFallBackToHeaderOnly(marker, cts.Token));
    }

    /// <summary>A null marker (no carry in progress — the overwhelmingly common start) is never a fallback
    /// case, same as before this fix.</summary>
    [Fact]
    public void ShouldFallBackToHeaderOnly_NullMarker_IsFalse()
    {
        Assert.False(DarlingManagedPostgres.ShouldFallBackToHeaderOnly(null, CancellationToken.None));
    }

    /// <summary>
    /// #4280 item 1: pins that the real-start fallback's own filter is
    /// <see cref="DarlingManagedPostgres.ShouldFallBackToHeaderOnly"/> and not a restated inline condition —
    /// a future edit to the condition has one place to change, so the catch clause and the behavioral tests
    /// above can never drift apart.
    /// </summary>
    [Fact]
    public void EnsureRunningAsync_RealStartFallback_FiltersThroughShouldFallBackToHeaderOnly()
    {
        var source = ReadManagedPostgresSource();

        Assert.Contains(
            "catch (Exception) when (ShouldFallBackToHeaderOnly(autoConfCarryMarker, cancellationToken))",
            source, StringComparison.Ordinal);
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
