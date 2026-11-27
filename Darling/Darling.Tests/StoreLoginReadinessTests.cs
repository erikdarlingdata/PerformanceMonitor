/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9698: the start-up line for a store login that lacks what the service uses. Each missing privilege is named with the grant that fixes it,
/// a privilege that degrades nothing on this store is not named, and a login that holds everything gets no line.
/// </summary>
public sealed class StoreLoginReadinessTests
{
    private static StoreLoginReadiness.LoginPrivileges Probe(
        bool canListWal = true, bool canListLog = true, bool canReadLogFile = true, bool storeKeepsLogFiles = true, bool canCreateInInternalSchema = true) =>
        new("byo_owner", "darling", canListWal, canListLog, canReadLogFile, storeKeepsLogFiles, canCreateInInternalSchema);

    [Fact]
    public void ALoginThatHoldsEverything_GetsNoLine()
    {
        Assert.Null(StoreLoginReadiness.DescribeMissingPrivileges(Probe(), aggregatesMissing: 0, aggregatesDeclared: 32));
    }

    [Fact]
    public void ALoginThatCannotListTheLog_IsToldToGrantPgMonitor_AndNotPgReadServerFiles()
    {
        var report = StoreLoginReadiness.DescribeMissingPrivileges(Probe(canListLog: false), 0, 32);

        Assert.NotNull(report);
        Assert.Equal(["GRANT pg_monitor TO \"byo_owner\""], report!.Statements);
        Assert.Contains("the store's own log is not captured", report.Message, StringComparison.Ordinal);
        Assert.Contains("database \"darling\"", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_read_server_files", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ALoginThatCannotReadTheLogFile_IsToldToGrantTheThreeArgumentRead()
    {
        var report = StoreLoginReadiness.DescribeMissingPrivileges(Probe(canReadLogFile: false), 0, 32);

        Assert.NotNull(report);
        Assert.Equal(["GRANT EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint) TO \"byo_owner\""], report!.Statements);
    }

    [Fact]
    public void AStoreThatKeepsNoLogFiles_DoesNotNameTheLogGrants()
    {
        Assert.Null(StoreLoginReadiness.DescribeMissingPrivileges(Probe(canListLog: false, canReadLogFile: false, storeKeepsLogFiles: false), 0, 32));
    }

    [Fact]
    public void ALoginThatCannotListWal_IsToldToGrantPgMonitorOnce_EvenWhenTheLogNeedsItToo()
    {
        var report = StoreLoginReadiness.DescribeMissingPrivileges(Probe(canListWal: false, canListLog: false), 0, 32);

        Assert.NotNull(report);
        Assert.Equal(["GRANT pg_monitor TO \"byo_owner\""], report!.Statements);
        Assert.Contains("pg_wal", report.Message, StringComparison.Ordinal);
        Assert.Contains("the store's own log is not captured", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MissingAggregates_WithoutTheSchemaRight_NameTheCountAndTheSchemaGrant()
    {
        var report = StoreLoginReadiness.DescribeMissingPrivileges(Probe(canCreateInInternalSchema: false), aggregatesMissing: 8, aggregatesDeclared: 32);

        Assert.NotNull(report);
        Assert.Equal(["GRANT CREATE ON SCHEMA _timescaledb_internal TO \"byo_owner\""], report!.Statements);
        Assert.Contains("8 of 32 continuous aggregates are not built", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutTheSchemaRight_EveryAggregateBuilt_DegradesNothing_SoNoLine()
    {
        Assert.Null(StoreLoginReadiness.DescribeMissingPrivileges(Probe(canCreateInInternalSchema: false), aggregatesMissing: 0, aggregatesDeclared: 32));
    }

    [Fact]
    public void ALoginNameIsQuotedAsAnIdentifier()
    {
        var probe = Probe(canListWal: false) with { Login = "odd\"name" };

        var report = StoreLoginReadiness.DescribeMissingPrivileges(probe, 0, 32);

        Assert.Equal(["GRANT pg_monitor TO \"odd\"\"name\""], report!.Statements);
    }
}
