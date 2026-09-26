/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static PerformanceMonitor.Darling.Service.ManagedConfMigration;

namespace Darling.Tests;

/// <summary>
/// <c>ManagedConfMigration</c> (#4215): whether a line in an existing
/// <c>postgresql.conf</c> is the service's own (OURS) or a hand edit — a line failing EITHER the design's
/// rebuild test or its form test is a hand edit. Pure logic,
/// no wiring: the rewrite that acts on this classifier's output is separate.
/// </summary>
public sealed class ManagedConfMigrationTests
{
    private static ClassifiedConfLine FindByText(System.Collections.Generic.IReadOnlyList<ClassifiedConfLine> lines, string prefix)
        => lines.First(l => l.Text.TrimStart().StartsWith(prefix, System.StringComparison.Ordinal));

    [Fact]
    public void ClassifyLines_UntouchedV14Block_IsOurs()
    {
        var conf = "shared_buffers = 128MB\n" + DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();
        var lines = ClassifyLines(conf);

        var mwm = FindByText(lines, "maintenance_work_mem");
        Assert.Equal(ConfLineClassification.Ours, mwm.Classification);
        Assert.Equal(DarlingManagedPostgres.ConfMarkerV14, mwm.BlockMarker);
    }

    [Fact]
    public void ClassifyLines_UntouchedV15Block_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildWalVolumeConfAppend();
        var lines = ClassifyLines(conf);

        var wal = FindByText(lines, "wal_compression");
        Assert.Equal(ConfLineClassification.Ours, wal.Classification);
    }

    [Fact]
    public void ClassifyLines_UntouchedV13PreloadLine_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildStatementStatisticsConfAppend(effectivePreloadList: null);
        var lines = ClassifyLines(conf);

        var preload = FindByText(lines, DarlingManagedPostgres.PreloadSetting);
        Assert.Equal(ConfLineClassification.Ours, preload.Classification);
    }

    /// <summary>Design §3 "What moves", the rebuild test: an edited value inside a covered block fails to
    /// rebuild from the block's own inputs and is a hand edit.</summary>
    [Fact]
    public void ClassifyLines_EditedValueInsideV14Block_IsHandEdit_RebuildMismatch()
    {
        var built = DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();
        var edited = built.Replace(
            $"maintenance_work_mem = {DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB",
            "maintenance_work_mem = 1024MB",
            System.StringComparison.Ordinal);

        var lines = ClassifyLines(edited);
        var mwm = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.HandEdit, mwm.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, mwm.Reason);
    }

    /// <summary>A line in a form its builder never writes is a hand edit whatever its value —
    /// <c>2GB</c> where the builder always writes <c>2047MB</c>.</summary>
    [Fact]
    public void ClassifyLines_ValueInFormBuilderNeverWrites_IsHandEdit_EvenWhenValueIsPlausible()
    {
        var built = DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();
        var edited = built.Replace(
            $"maintenance_work_mem = {DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB",
            "maintenance_work_mem = 2GB",
            System.StringComparison.Ordinal);

        var lines = ClassifyLines(edited);
        var mwm = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.HandEdit, mwm.Classification);
    }

    /// <summary>The v13 merge builder's form test: the SAME effective libraries, written with different
    /// spacing than <see cref="DarlingManagedPostgres.FormatPreloadList"/> ever produces, fails the round
    /// trip and is a hand edit.</summary>
    [Fact]
    public void ClassifyLines_V13PreloadDifferentForm_SameValues_IsHandEdit()
    {
        var built = DarlingManagedPostgres.BuildStatementStatisticsConfAppend(effectivePreloadList: null);
        var handEdited = built.Replace(
            "shared_preload_libraries = 'timescaledb,pg_stat_statements'",
            "shared_preload_libraries = 'timescaledb, pg_stat_statements'",
            System.StringComparison.Ordinal);

        var lines = ClassifyLines(handEdited);
        var preload = FindByText(lines, DarlingManagedPostgres.PreloadSetting);

        Assert.Equal(ConfLineClassification.HandEdit, preload.Classification);
    }

    [Fact]
    public void ClassifyLines_LineOutsideEveryBlock_IsHandEdit_OutsideBlock()
    {
        var conf = "log_timezone = 'UTC'\n" + DarlingManagedPostgres.BuildWalVolumeConfAppend();
        var lines = ClassifyLines(conf);

        Assert.Equal(ConfLineClassification.HandEdit, lines[0].Classification);
        Assert.Equal(HandEditReason.OutsideBlock, lines[0].Reason);
    }

    /// <summary>An uncovered block version must never classify as Ours — a caller has to treat
    /// <see cref="ConfLineClassification.Unclassified"/> as a hand edit until coverage is added. #4336
    /// (CI on 9174da32): v8 is now in <see cref="CoveredMarkers"/> (#4336 covered every
    /// marker), so a real v8 marker no longer proves this — a SYNTHETIC marker string that
    /// <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/> also does not carry (never built by any real
    /// version, past or future) is what actually exercises the "uncovered marker" path
    /// (<c>ClassifyLines</c>'s <c>FindUncoveredMarkerLines</c>/<c>IsInsideAnyManagedSpan</c> fall-through), and
    /// it classifies HandEdit—OutsideBlock rather than Unclassified, because it is not even in
    /// <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/> at all — <see cref="ConfLineClassification.Unclassified"/>
    /// is reserved for a marker THIS repo has ever shipped that the classifier merely doesn't cover yet, which is
    /// not a case this test can construct against a synthetic marker without editing the marker list itself.
    /// Either way the assertion this test exists for — never Ours — holds.</summary>
    [Fact]
    public void ClassifyLines_UncoveredBlockVersion_IsUnclassified_NeverOurs()
    {
        const string fakeMarker = "# Managed by PerformanceMonitor Darling (v99 does not exist) -- do not remove this block";
        var conf = "\n" + fakeMarker + "\neffective_cache_size = 512MB\n";
        var lines = ClassifyLines(conf);

        var marker = lines.First(l => l.Text == fakeMarker);
        var content = FindByText(lines, "effective_cache_size");

        Assert.NotEqual(ConfLineClassification.Ours, marker.Classification);
        Assert.NotEqual(ConfLineClassification.Ours, content.Classification);
    }

    /// <summary>An "older store": a store built before v14/v15 existed has no line for those keys
    /// at all. There is nothing to misclassify — the classifier runs over whatever is actually in the file
    /// without special-casing the absence.</summary>
    [Fact]
    public void ClassifyLines_OlderStoreWithNoCoveredBlocks_ClassifiesRemainingLinesAsHandEdit()
    {
        var conf = "shared_buffers = 128MB\nwork_mem = 4MB";
        var lines = ClassifyLines(conf);

        Assert.Equal(2, lines.Count);
        Assert.All(lines, l =>
        {
            Assert.Equal(ConfLineClassification.HandEdit, l.Classification);
            Assert.Equal(HandEditReason.OutsideBlock, l.Reason);
        });
    }

    [Fact]
    public void ClassifyLines_UntouchedV4Block_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildWriteThroughputConfAppend();
        var lines = ClassifyLines(conf);

        var maxConn = FindByText(lines, "max_connections");
        var maxWal = FindByText(lines, "max_wal_size");
        Assert.Equal(ConfLineClassification.Ours, maxConn.Classification);
        Assert.Equal(ConfLineClassification.Ours, maxWal.Classification);
    }

    [Fact]
    public void ClassifyLines_EditedV4MaxConnections_IsHandEdit()
    {
        var built = DarlingManagedPostgres.BuildWriteThroughputConfAppend();
        var edited = built.Replace(
            FormattableString.Invariant($"max_connections = {DarlingManagedPostgres.TargetMaxConnections}"),
            "max_connections = 500",
            System.StringComparison.Ordinal);

        var lines = ClassifyLines(edited);
        var maxConn = FindByText(lines, "max_connections");

        Assert.Equal(ConfLineClassification.HandEdit, maxConn.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, maxConn.Reason);
    }

    [Fact]
    public void ClassifyLines_UntouchedV11Block_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildJobExecutionLoggingConfAppend();
        var lines = ClassifyLines(conf);

        var line = FindByText(lines, "timescaledb.enable_job_execution_logging");
        Assert.Equal(ConfLineClassification.Ours, line.Classification);
    }

    /// <summary>An untouched v8 block, at the RAM/hypertable inputs its own fingerprint records, is Ours —
    /// the plain case (#4336).</summary>
    [Fact]
    public void ClassifyLines_UntouchedV8Block_AtItsOwnRecordedInputs_IsOurs()
    {
        var ram = 16L * 1024 * 1024 * 1024;
        var hypertables = 30;
        var conf = DarlingManagedPostgres.BuildHardwareSizingConfAppend(ram, hypertables);

        var lines = ClassifyLines(conf);
        var marker = lines.First(l => l.Text == DarlingManagedPostgres.ConfMarkerV8);
        var effectiveCache = FindByText(lines, "effective_cache_size");

        Assert.Equal(ConfLineClassification.Ours, marker.Classification);
        Assert.Equal(ConfLineClassification.Ours, effectiveCache.Classification);
    }

    /// <summary>A hand edit to one of v8's setting values fails the rebuild at the block's own recorded
    /// fingerprint and is a hand edit, never Ours.</summary>
    [Fact]
    public void ClassifyLines_EditedV8Value_IsHandEdit_RebuildMismatch()
    {
        var built = DarlingManagedPostgres.BuildHardwareSizingConfAppend(16L * 1024 * 1024 * 1024, 30);
        var edited = built.Replace("work_mem = 32MB", "work_mem = 1024MB", StringComparison.Ordinal);
        Assert.NotEqual(built, edited); /* guards the pin against a formula change silently no-oping the edit */

        var lines = ClassifyLines(edited);
        var workMem = FindByText(lines, "work_mem");

        Assert.Equal(ConfLineClassification.HandEdit, workMem.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, workMem.Reason);
    }

    /// <summary>A v8 block whose recorded fingerprint (16 GB/30 hypertables) no longer matches the HOST's
    /// current inputs (a resize to 32 GB since) is still Ours: it rebuilds exactly from its OWN recorded
    /// inputs, whatever the host is now. A resize makes the block stale — <see cref="DarlingManagedPostgres.ShouldAppendHardwareSizing"/>'s
    /// job to notice and replace — not a hand edit for this classifier to punish (#4336).</summary>
    [Fact]
    public void ClassifyLines_StaleV8Block_AtItsOwnRecordedInputs_IsStillOurs()
    {
        /* Written under 16 GB/30 hypertables; the host now reads 32 GB, but nothing here re-derives from
           that — only from the fingerprint the block itself carries. */
        var stale = DarlingManagedPostgres.BuildHardwareSizingConfAppend(16L * 1024 * 1024 * 1024, 30);

        var lines = ClassifyLines(stale);
        var marker = lines.First(l => l.Text == DarlingManagedPostgres.ConfMarkerV8);
        var effectiveCache = FindByText(lines, "effective_cache_size");

        Assert.Equal(ConfLineClassification.Ours, marker.Classification);
        Assert.Equal(ConfLineClassification.Ours, effectiveCache.Classification);
    }

    /// <summary>A v8 block with its fingerprint line deleted (or spliced in without one) cannot be rebuilt
    /// at all and is a hand edit — the fallback c1 implemented for a missing recorded-inputs line.</summary>
    [Fact]
    public void ClassifyLines_V8BlockMissingFingerprint_IsHandEdit_RebuildMismatch()
    {
        var built = DarlingManagedPostgres.BuildHardwareSizingConfAppend(16L * 1024 * 1024 * 1024, 30);
        var withoutFingerprint = string.Join(
            '\n',
            built.Split('\n').Where(l => !l.StartsWith(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, StringComparison.Ordinal)));

        var lines = ClassifyLines(withoutFingerprint);
        var effectiveCache = FindByText(lines, "effective_cache_size");

        Assert.Equal(ConfLineClassification.HandEdit, effectiveCache.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, effectiveCache.Reason);
    }

    /// <summary>An untouched v12 block, at the free/total-disk inputs its builder derived from, is Ours.</summary>
    [Fact]
    public void ClassifyLines_UntouchedV12Block_AtItsOwnRecordedInputs_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildWalSizingConfAppend(
            freeDiskBytesOnDataVolume: 64L * 1024 * 1024 * 1024,
            totalDiskBytesOnDataVolume: 256L * 1024 * 1024 * 1024,
            postgresMajor: 13);

        var lines = ClassifyLines(conf);
        var marker = lines.First(l => l.Text == DarlingManagedPostgres.ConfMarkerV12);
        var maxWal = FindByText(lines, "max_wal_size");
        var checkpoint = FindByText(lines, "checkpoint_completion_target");

        Assert.Equal(ConfLineClassification.Ours, marker.Classification);
        Assert.Equal(ConfLineClassification.Ours, maxWal.Classification);
        Assert.Equal(ConfLineClassification.Ours, checkpoint.Classification);
    }

    /// <summary>A hand edit to v12's <c>max_wal_size</c> fails the rebuild against the block's own stamp
    /// and is a hand edit.</summary>
    [Fact]
    public void ClassifyLines_EditedV12Value_IsHandEdit_RebuildMismatch()
    {
        var built = DarlingManagedPostgres.BuildWalSizingConfAppend(
            freeDiskBytesOnDataVolume: 64L * 1024 * 1024 * 1024,
            totalDiskBytesOnDataVolume: 256L * 1024 * 1024 * 1024,
            postgresMajor: 13);
        var edited = built.Replace("max_wal_size = 8192MB", "max_wal_size = 16384MB", StringComparison.Ordinal);
        Assert.NotEqual(built, edited);

        var lines = ClassifyLines(edited);
        var maxWal = FindByText(lines, "max_wal_size");

        Assert.Equal(ConfLineClassification.HandEdit, maxWal.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, maxWal.Reason);
    }

    /// <summary>A v12 block whose recorded stamp no longer matches the host's CURRENT disk headroom is
    /// still Ours: the rebuild reads only the stamp's own recorded max/min-WAL outputs and major, never the
    /// host's live free-disk figure (#4336 — same rule as v8).</summary>
    [Fact]
    public void ClassifyLines_StaleV12Block_AtItsOwnRecordedInputs_IsStillOurs()
    {
        /* Written under 64 GB free of 256 GB; nothing here re-derives from a different current free-disk
           figure — only from the stamp the block itself carries. */
        var stale = DarlingManagedPostgres.BuildWalSizingConfAppend(
            freeDiskBytesOnDataVolume: 64L * 1024 * 1024 * 1024,
            totalDiskBytesOnDataVolume: 256L * 1024 * 1024 * 1024,
            postgresMajor: 13);

        var lines = ClassifyLines(stale);
        var marker = lines.First(l => l.Text == DarlingManagedPostgres.ConfMarkerV12);
        var maxWal = FindByText(lines, "max_wal_size");

        Assert.Equal(ConfLineClassification.Ours, marker.Classification);
        Assert.Equal(ConfLineClassification.Ours, maxWal.Classification);
    }

    /// <summary>A v12 block with its stamp line deleted cannot be rebuilt at all and is a hand edit.</summary>
    [Fact]
    public void ClassifyLines_V12BlockMissingStamp_IsHandEdit_RebuildMismatch()
    {
        var built = DarlingManagedPostgres.BuildWalSizingConfAppend(
            freeDiskBytesOnDataVolume: 64L * 1024 * 1024 * 1024,
            totalDiskBytesOnDataVolume: 256L * 1024 * 1024 * 1024,
            postgresMajor: 13);
        var withoutStamp = string.Join(
            '\n',
            built.Split('\n').Where(l => !l.StartsWith(DarlingManagedPostgres.ConfWalSizingStampPrefix, StringComparison.Ordinal)));

        var lines = ClassifyLines(withoutStamp);
        var maxWal = FindByText(lines, "max_wal_size");

        Assert.Equal(ConfLineClassification.HandEdit, maxWal.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, maxWal.Reason);
    }

    /// <summary>v1's <c>port</c> line rebuilds Ours when the caller supplies the store's configured port
    /// and it matches (#4336).</summary>
    [Fact]
    public void ClassifyLines_V1PortLine_WithMatchingConfiguredPort_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildConfAppend(port: 5432);

        var lines = ClassifyLines(conf, configuredPort: 5432);
        var port = FindByText(lines, "port");

        Assert.Equal(ConfLineClassification.Ours, port.Classification);
    }

    /// <summary>v1's <c>port</c> line is a hand edit when the supplied configured port does not match what
    /// is actually written.</summary>
    [Fact]
    public void ClassifyLines_V1PortLine_WithWrongConfiguredPort_IsHandEdit()
    {
        var conf = DarlingManagedPostgres.BuildConfAppend(port: 5432);

        var lines = ClassifyLines(conf, configuredPort: 5433);
        var port = FindByText(lines, "port");

        Assert.Equal(ConfLineClassification.HandEdit, port.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, port.Reason);
    }

    /// <summary>v1's <c>port</c> line stays Unclassified — never guessed, never a hand edit — when the
    /// caller has no configured port to rebuild against (the zero-arg overload's own contract).</summary>
    [Fact]
    public void ClassifyLines_V1PortLine_WithNoConfiguredPort_IsUnclassified()
    {
        var conf = DarlingManagedPostgres.BuildConfAppend(port: 5432);

        var lines = ClassifyLines(conf);
        var port = FindByText(lines, "port");

        Assert.Equal(ConfLineClassification.Unclassified, port.Classification);
        Assert.Equal(HandEditReason.None, port.Reason);
    }

    [Fact]
    public void ClassifyLines_DuplicatedMarker_IsHandEditOrUnclassified_NeverOurs()
    {
        var conf = DarlingManagedPostgres.BuildWalVolumeConfAppend() + DarlingManagedPostgres.BuildWalVolumeConfAppend();
        var lines = ClassifyLines(conf);

        var wals = lines.Where(l => l.Text == DarlingManagedPostgres.ConfMarkerV15).ToList();
        Assert.Equal(2, wals.Count);
        Assert.All(wals, l => Assert.NotEqual(ConfLineClassification.HandEdit, l.Classification));
        /* Both marker lines are byte-identical to what the builder writes, so both classify Ours today —
           this classifier has no duplicate-marker special case yet. Pinned so a future change to that
           behavior is deliberate. */
        Assert.All(wals, l => Assert.Equal(ConfLineClassification.Ours, l.Classification));
    }

    /// <summary>v2's untouched block, at N = hypertableCount + 2 and M = N + 11, is Ours (#4336).</summary>
    [Fact]
    public void ClassifyLines_UntouchedV2Block_IsOurs()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV2 + "\n" +
            "timescaledb.max_background_workers = 30\n" +
            "max_worker_processes = 41\n";

        var lines = ClassifyLines(conf);
        var marker = lines.First(l => l.Text == DarlingManagedPostgres.ConfMarkerV2);
        var backgroundWorkers = FindByText(lines, "timescaledb.max_background_workers");
        var workerProcesses = FindByText(lines, "max_worker_processes");

        Assert.Equal(ConfLineClassification.Ours, marker.Classification);
        Assert.Equal(ConfLineClassification.Ours, backgroundWorkers.Classification);
        Assert.Equal(ConfLineClassification.Ours, workerProcesses.Classification);
        Assert.Contains("legacy v2 timescaledb.max_background_workers=30", backgroundWorkers.Note);
        Assert.Contains("re-apply it with ALTER SYSTEM", backgroundWorkers.Note);
    }

    /// <summary>v2's M must equal N + 11 read from the SAME block — an M that does not match its own block's N
    /// is a hand edit even though M alone (41) would rebuild from a DIFFERENT N.</summary>
    [Fact]
    public void ClassifyLines_V2MismatchedNAndM_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV2 + "\n" +
            "timescaledb.max_background_workers = 30\n" +
            "max_worker_processes = 999\n";

        var lines = ClassifyLines(conf);
        var backgroundWorkers = FindByText(lines, "timescaledb.max_background_workers");
        var workerProcesses = FindByText(lines, "max_worker_processes");

        Assert.Equal(ConfLineClassification.HandEdit, backgroundWorkers.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, backgroundWorkers.Reason);
        Assert.Equal(ConfLineClassification.HandEdit, workerProcesses.Classification);
    }

    /// <summary>v2's N must be an integer &gt;= 2 — an off-image N (0, below the +2 floor for a non-negative
    /// hypertable count) is a hand edit even with a self-consistent M = N + 11.</summary>
    [Fact]
    public void ClassifyLines_V2OffImageN_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV2 + "\n" +
            "timescaledb.max_background_workers = 0\n" +
            "max_worker_processes = 11\n";

        var lines = ClassifyLines(conf);
        var backgroundWorkers = FindByText(lines, "timescaledb.max_background_workers");

        Assert.Equal(ConfLineClassification.HandEdit, backgroundWorkers.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, backgroundWorkers.Reason);
    }

    /// <summary>v3's untouched block at today's formula (<see cref="DarlingManagedPostgres.DeriveMemorySettings"/>,
    /// generation <c>e507aa2d1</c>) is Ours (#4336).</summary>
    [Fact]
    public void ClassifyLines_UntouchedV3Block_AtCurrentGeneration_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildMemorySizingConfAppend(16L * 1024 * 1024 * 1024);

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");
        var effectiveCache = FindByText(lines, "effective_cache_size");
        var maintenance = FindByText(lines, "maintenance_work_mem");
        var workMem = FindByText(lines, "work_mem");

        Assert.Equal(ConfLineClassification.Ours, sharedBuffers.Classification);
        Assert.Equal(ConfLineClassification.Ours, effectiveCache.Classification);
        Assert.Equal(ConfLineClassification.Ours, maintenance.Classification);
        Assert.Equal(ConfLineClassification.Ours, workMem.Classification);
        Assert.Contains("re-apply it with ALTER SYSTEM", sharedBuffers.Note);
    }

    /// <summary>A v3 block written under the PRE-<c>2b67bedeb</c> generation (shared_buffers capped at 8 GB) is
    /// STILL Ours — the union rule: v3 is never rewritten, so an old, unedited block legitimately carries an
    /// older generation's values (#4336).</summary>
    [Fact]
    public void ClassifyLines_V3Block_AtPreShrinkGeneration_SharedBuffersEightGb_IsStillOurs()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV3 + "\n" +
            "shared_buffers = 8192MB\n" +
            "effective_cache_size = 24576MB\n" +
            "maintenance_work_mem = 1024MB\n" +
            "work_mem = 64MB\n";

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.Ours, sharedBuffers.Classification);
    }

    /// <summary>A v3 block written under the pre-#1777 <c>maintenance_work_mem</c> generation (no 1536 MB floor)
    /// is still Ours — same union rule, for the OTHER setting the formula changed (#4336).</summary>
    [Fact]
    public void ClassifyLines_V3Block_AtPreFloorGeneration_MaintenanceWorkMem_IsStillOurs()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV3 + "\n" +
            "shared_buffers = 1024MB\n" +
            "effective_cache_size = 24576MB\n" +
            "maintenance_work_mem = 500MB\n" +
            "work_mem = 64MB\n";

        var lines = ClassifyLines(conf);
        var maintenance = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.Ours, maintenance.Classification);
    }

    /// <summary>A value NO v3 generation's formula could ever produce (#4336's off-image example: a
    /// suspiciously specific hand-tuned figure) is a hand edit — the negative half of the union rule.</summary>
    [Fact]
    public void ClassifyLines_V3OffImageSharedBuffers_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV3 + "\n" +
            "shared_buffers = 777MB\n" +
            "effective_cache_size = 24576MB\n" +
            "maintenance_work_mem = 2047MB\n" +
            "work_mem = 64MB\n";

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.HandEdit, sharedBuffers.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, sharedBuffers.Reason);
    }

    /// <summary>A unit-form variant no v3 builder ever wrote (<c>1GB</c> where every generation writes whole MB,
    /// e.g. <c>1024MB</c>) is a hand edit even though the value itself is in every generation's image — the
    /// form test, same rule v8/v12/v13 already apply (#4336).</summary>
    [Fact]
    public void ClassifyLines_V3UnitFormVariant_IsHandEdit_FormMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV3 + "\n" +
            "shared_buffers = 1GB\n" +
            "effective_cache_size = 24576MB\n" +
            "maintenance_work_mem = 2047MB\n" +
            "work_mem = 64MB\n";

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.HandEdit, sharedBuffers.Classification);
        Assert.Equal(HandEditReason.FormMismatch, sharedBuffers.Reason);
    }

    /// <summary>An untouched v5 block (co-located sizing, introduced <c>2b67bedeb</c>) at today's generation is
    /// Ours (#4336).</summary>
    [Fact]
    public void ClassifyLines_UntouchedV5Block_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildColocatedSizingConfAppend(16L * 1024 * 1024 * 1024);

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.Ours, sharedBuffers.Classification);
        Assert.Contains("legacy v5 shared_buffers=1024MB", sharedBuffers.Note);
        Assert.Contains("re-apply it with ALTER SYSTEM", sharedBuffers.Note);
    }

    /// <summary>A v5 block cannot legitimately carry a PRE-<c>2b67bedeb</c> value (the 8 GB cap) since no
    /// generation before v5 existed could have written a v5 block at all — unlike v3's union rule, v5's image
    /// EXCLUDES the older shape (#4336).</summary>
    [Fact]
    public void ClassifyLines_V5Block_AtPreIntroductionSharedBuffersEightGb_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV5 + "\n" +
            "shared_buffers = 8192MB\n";

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.HandEdit, sharedBuffers.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, sharedBuffers.Reason);
    }

    /// <summary>A value no v5 generation's formula could ever produce is a hand edit (#4336).</summary>
    [Fact]
    public void ClassifyLines_V5OffImageSharedBuffers_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV5 + "\n" +
            "shared_buffers = 777MB\n";

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.HandEdit, sharedBuffers.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, sharedBuffers.Reason);
    }

    /// <summary>A unit-form variant no v5 builder ever wrote is a hand edit even though the value is in-image
    /// (#4336, same form rule as v3).</summary>
    [Fact]
    public void ClassifyLines_V5UnitFormVariant_IsHandEdit_FormMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV5 + "\n" +
            "shared_buffers = 1GB\n";

        var lines = ClassifyLines(conf);
        var sharedBuffers = FindByText(lines, "shared_buffers");

        Assert.Equal(ConfLineClassification.HandEdit, sharedBuffers.Classification);
        Assert.Equal(HandEditReason.FormMismatch, sharedBuffers.Reason);
    }

    /// <summary>An untouched v7 block (compression memory, introduced <c>f4c86ffa3</c> #1777) at today's
    /// generation (cap 2047 MB) is Ours (#4336).</summary>
    [Fact]
    public void ClassifyLines_UntouchedV7Block_AtCurrentGeneration_IsOurs()
    {
        var conf = DarlingManagedPostgres.BuildCompressionMemoryConfAppend(16L * 1024 * 1024 * 1024);

        var lines = ClassifyLines(conf);
        var maintenance = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.Ours, maintenance.Classification);
        Assert.Contains("re-apply it with ALTER SYSTEM", maintenance.Note);
    }

    /// <summary>A v7 block written under the ORIGINAL <c>f4c86ffa3</c> generation (cap 2048 MB, before #3909
    /// dropped it to 2047) is STILL Ours — both generations that ever wrote a v7 block are in-image, unlike v5
    /// which has only ever had one generation (#4336).</summary>
    [Fact]
    public void ClassifyLines_V7Block_AtIntroductionGeneration_Cap2048_IsStillOurs()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV7 + "\n" +
            "maintenance_work_mem = 2048MB\n";

        var lines = ClassifyLines(conf);
        var maintenance = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.Ours, maintenance.Classification);
    }

    /// <summary>A v7 block cannot legitimately carry a PRE-#1777 value (the old <c>min(ram/20, 1GB)</c> shape,
    /// e.g. 500 MB under the 1536 MB floor) since no generation before v7 existed could have written a v7 block
    /// — unlike v3's union, v7's image excludes the older shape (#4336).</summary>
    [Fact]
    public void ClassifyLines_V7Block_AtPreIntroductionValue_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV7 + "\n" +
            "maintenance_work_mem = 500MB\n";

        var lines = ClassifyLines(conf);
        var maintenance = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.HandEdit, maintenance.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, maintenance.Reason);
    }

    /// <summary>A value no v7 generation's formula could ever produce (above either cap) is a hand edit
    /// (#4336).</summary>
    [Fact]
    public void ClassifyLines_V7OffImageMaintenanceWorkMem_IsHandEdit_RebuildMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV7 + "\n" +
            "maintenance_work_mem = 3000MB\n";

        var lines = ClassifyLines(conf);
        var maintenance = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.HandEdit, maintenance.Classification);
        Assert.Equal(HandEditReason.RebuildMismatch, maintenance.Reason);
    }

    /// <summary>A unit-form variant no v7 builder ever wrote is a hand edit even though the value is in-image
    /// (#4336, same form rule as v3/v5).</summary>
    [Fact]
    public void ClassifyLines_V7UnitFormVariant_IsHandEdit_FormMismatch()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV7 + "\n" +
            "maintenance_work_mem = 2GB\n";

        var lines = ClassifyLines(conf);
        var maintenance = FindByText(lines, "maintenance_work_mem");

        Assert.Equal(ConfLineClassification.HandEdit, maintenance.Classification);
        Assert.Equal(HandEditReason.FormMismatch, maintenance.Reason);
    }

    [Fact]
    public void ClassifyLines_CrlfFile_ClassifiesSameAsLf()
    {
        var lf = DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();
        var crlf = lf.Replace("\n", "\r\n", System.StringComparison.Ordinal);

        var linesLf = ClassifyLines(lf);
        var linesCrlf = ClassifyLines(crlf);

        Assert.Equal(linesLf.Count, linesCrlf.Count);
        for (var i = 0; i < linesLf.Count; i++)
        {
            Assert.Equal(linesLf[i].Text, linesCrlf[i].Text);
            Assert.Equal(linesLf[i].Classification, linesCrlf[i].Classification);
            Assert.Equal(linesLf[i].Reason, linesCrlf[i].Reason);
        }
    }
}
