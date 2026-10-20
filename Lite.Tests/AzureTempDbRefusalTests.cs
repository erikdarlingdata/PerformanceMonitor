/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6177: on Azure SQL Database a contained database user is refused on the tempdb DMVs every run. The tempdb
/// reads answer <c>precondition</c> with the stored refusal (which carries the tempdb hint), and the Collection
/// Health OUTPUT sentence agrees with the NO_PERMISSIONS band instead of saying the collector "read".
/// </summary>
public class AzureTempDbRefusalTests
{
    private const string Refusal = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";

    [Fact]
    public void TempDbRefusal_AnswersPrecondition_WithTheContainedUserHint()
    {
        var stored = Refusal + AzureDmvPermissionHint.For(262, isAzureSqlDb: true, Refusal);

        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "azure-db", "tempdb_stats", CollectorRuntimePrecondition.DegradedStatus, stored, DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains("contained database user", message, StringComparison.Ordinal);
        Assert.Contains("##MS_ServerStateReader##", message, StringComparison.Ordinal);
        Assert.Contains("tempdb_stats", message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoPermissionsBand_OutputFinding_SaysUnableToRead_EvenWhenDenialIsNotCurrentAndNotesExist()
    {
        var finding = CollectorHealthClassifier.FormatOutputFinding(
            rowsStored: 0, totalRuns: 11, deniedSinceLastSuccess: false, noteCount: 3, faultedRuns: 0,
            isEventCollector: false, isZeroNormal: false, bandedNoPermissions: true);

        Assert.Contains("NO_PERMISSIONS", finding, StringComparison.Ordinal);
        Assert.Contains("unable to read", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("read rather than", finding, StringComparison.Ordinal);
    }

    [Fact]
    public void OtherBands_OutputFinding_IsUnchanged()
    {
        var finding = CollectorHealthClassifier.FormatOutputFinding(
            rowsStored: 0, totalRuns: 11, deniedSinceLastSuccess: false, noteCount: 3, faultedRuns: 0,
            isEventCollector: false, isZeroNormal: false);

        Assert.Contains("read rather than being unable to read", finding, StringComparison.Ordinal);
    }
}
