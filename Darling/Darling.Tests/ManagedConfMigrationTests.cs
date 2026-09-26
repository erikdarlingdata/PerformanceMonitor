/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static PerformanceMonitor.Darling.Service.ManagedConfMigration;

namespace Darling.Tests;

/// <summary>
/// <c>ManagedConfMigration</c> (#4215 A2 "classify" lane): whether a line in an existing
/// <c>postgresql.conf</c> is the service's own (OURS) or a hand edit, per ruling comment-5836185470's rule
/// 4 — a line failing EITHER the design's rebuild test or review L3's form test is a hand edit. Pure logic,
/// no wiring: the rewrite that acts on this classifier's output is a later lane.
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

    /// <summary>Review L3: a line in a form its builder never writes is a hand edit whatever its value —
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

    /// <summary>Uncovered block versions (v1-v12) must never classify as Ours — a caller has to treat
    /// <see cref="ConfLineClassification.Unclassified"/> as a hand edit until coverage is added.</summary>
    [Fact]
    public void ClassifyLines_UncoveredBlockVersion_IsUnclassified_NeverOurs()
    {
        var conf = "\n" + DarlingManagedPostgres.ConfMarkerV8 + "\neffective_cache_size = 512MB\n";
        var lines = ClassifyLines(conf);

        var marker = lines.First(l => l.Text == DarlingManagedPostgres.ConfMarkerV8);
        var content = FindByText(lines, "effective_cache_size");

        Assert.Equal(ConfLineClassification.Unclassified, marker.Classification);
        Assert.Equal(ConfLineClassification.Unclassified, content.Classification);
    }

    /// <summary>Review M6's "older store": a store built before v14/v15 existed has no line for those keys
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
