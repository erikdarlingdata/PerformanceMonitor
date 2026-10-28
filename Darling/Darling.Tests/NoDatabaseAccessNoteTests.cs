// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6889: an online user database the monitoring login has no user in is dropped by every per-database list query's
/// HAS_DBACCESS screen. The list query now reports it through the probe-failure channel, and the shared note builder turns the
/// rows into one sentence on the run's SUCCESS row, which is what makes a Query Store read of those databases refuse to say
/// "true negative". The connection test counts the same databases.
/// </summary>
public sealed class NoDatabaseAccessNoteTests
{
    private static EnumerationProbeFailure NoAccess(string database) => new(database, NoDatabaseAccessNote.ProbeErrorText);

    [Fact]
    public void NoNote_WhenEveryDatabaseIsAccessible()
    {
        Assert.Null(NoDatabaseAccessNote.Sentence(Array.Empty<string>(), awsRds: false));
        Assert.Null(EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, Array.Empty<EnumerationProbeFailure>(), awsRds: true));
    }

    [Fact]
    public void Note_ForSome_GivesTheCountAndTheNames_AndNoCreateUserOffRds()
    {
        var note = EnumeratedCollectorDriver.BuildNote(false, new[] { NoAccess("Sales"), NoAccess("Billing") }, awsRds: false);

        Assert.NotNull(note);
        Assert.Contains("2 online user database(s)", note);
        Assert.Contains("Sales, Billing", note);
        Assert.Contains("no user in", note);
        Assert.Contains("nothing was collected", note);
        Assert.DoesNotContain("CREATE USER", note);
    }

    [Fact]
    public void Note_OnRds_AlsoNamesTheCreateUserStatement()
    {
        var note = EnumeratedCollectorDriver.BuildNote(false, new[] { NoAccess("Sales") }, awsRds: true);

        Assert.NotNull(note);
        Assert.Contains("CREATE USER [login] FOR LOGIN [login]", note);
        Assert.Contains("1 online user database(s)", note);
    }

    [Fact]
    public void Note_NamesAtMostTen_AndCountsTheRest()
    {
        var failures = Enumerable.Range(1, 12).Select(i => NoAccess("Db" + i)).ToList();
        var note = EnumeratedCollectorDriver.BuildNote(false, failures, awsRds: false)!;

        Assert.Contains("12 online user database(s)", note);
        Assert.Contains("Db10", note);
        Assert.DoesNotContain("Db11,", note);
        Assert.Contains("and 2 more", note);
    }

    [Fact]
    public void OtherProbeFailures_KeepTheirOwnCount_BesideTheNoAccessSentence()
    {
        var note = EnumeratedCollectorDriver.BuildNote(
            false, new[] { NoAccess("Sales"), new EnumerationProbeFailure("Mid", "database is restoring") }, awsRds: false)!;

        Assert.Contains("1 item(s) failed their enumeration probe", note);
        Assert.Contains("1 online user database(s)", note);
    }

    [Fact]
    public async Task EnumerationRead_TurnsTheSecondResultSetIntoTheNote()
    {
        var items = new DataTable();
        items.Columns.Add("name", typeof(string));
        items.Rows.Add("Open");
        var probes = new DataTable();
        probes.Columns.Add("name", typeof(string));
        probes.Columns.Add("error_text", typeof(string));
        probes.Rows.Add("Locked", NoDatabaseAccessNote.ProbeErrorText);

        using var reader = new DataTableReader(new[] { items, probes });
        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None, awsRds: true);

        Assert.Equal(new[] { "Open" }, outcome.Items);
        Assert.Single(outcome.ProbeFailures);
        Assert.Contains("Locked", outcome.Note);
        Assert.Contains("CREATE USER", outcome.Note);
    }

    [Fact]
    public void CycleNote_AccumulatesNoAccessAcrossDatabases()
    {
        var cycle = new CycleProbeFailures(awsRds: false);
        cycle.Add(new ProbeFailureOutcome(new[] { NoAccess("A") }, null));
        cycle.Add(new ProbeFailureOutcome(new[] { NoAccess("B") }, null));

        Assert.Contains("2 online user database(s)", cycle.Note);
        Assert.Contains("A, B", cycle.Note);
    }

    [Fact]
    public void AnEmptyQueryStoreRead_IsNotATrueNegative_WhenTheNewestSuccessRunCarriesTheNote()
    {
        var note = EnumeratedCollectorDriver.BuildNote(true, new[] { NoAccess("Sales") }, awsRds: false);
        var at = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);

        Assert.Null(CollectorRuntimePrecondition.HealthyEmptyMessage("srv", "query_store", "Query Store rows", "SUCCESS", note, at));
        Assert.NotNull(CollectorRuntimePrecondition.HealthyEmptyMessage("srv", "query_store", "Query Store rows", "SUCCESS", null, at));
    }

    [Fact]
    public void ConnectionTest_CountsTheDatabasesTheLoginCannotEnter_ForAServerTargetOnly()
    {
        var server = MonitoringGrantScript.BuildPermissionCheckSql(MonitoringGrantScript.RequiredServerGrants);
        var azure = MonitoringGrantScript.BuildPermissionCheckSql(MonitoringGrantScript.RequiredAzureGrants);

        Assert.Contains("HAS_DBACCESS(d.name) = 0", server);
        Assert.Contains("d.name <> N'rdsadmin'", server);
        Assert.DoesNotContain("HAS_DBACCESS", azure);
    }

    [Fact]
    public void ConnectionTestWarning_GivesTheCountAndTheCreateUser()
    {
        Assert.Null(MonitoringGrantScript.DescribeNoUserDatabases(0, "pm", awsRds: true));

        var box = MonitoringGrantScript.DescribeNoUserDatabases(3, "pm", awsRds: false)!;
        Assert.Contains("3 online user database(s)", box);
        Assert.Contains("CREATE USER [pm] FOR LOGIN [pm]", box);
        Assert.Contains("CONNECT ANY DATABASE", box);

        var rds = MonitoringGrantScript.DescribeNoUserDatabases(3, "pm", awsRds: true)!;
        Assert.Contains("CREATE USER [pm] FOR LOGIN [pm]", rds);
        Assert.Contains("Amazon RDS has no CONNECT ANY DATABASE", rds);
    }

    [Fact]
    public void JobHistory_BuildQuery_KeepsItsParameters_AndBalancedQuotes_OnEveryFilterPath()
    {
        var first = JobHistoryCollector.Instance.BuildQuery(NewContext(null, false));
        var archival = JobHistoryCollector.Instance.BuildQuery(NewContext(null, true));
        var incremental = JobHistoryCollector.Instance.BuildQuery(NewContext(42L, true));

        Assert.Empty(first.Parameters);
        Assert.Empty(archival.Parameters);
        Assert.Equal(new[] { "@last_instance_id", "@min_run_datetime" }, incremental.Parameters.Select(p => p.Name).ToArray());

        /* The statement is a string literal run through sp_executesql, so an unescaped quote in the body would break the batch. */
        foreach (var query in new[] { first, archival, incremental })
        {
            Assert.Equal(0, query.Text.Count(c => c == '\'') % 2);
        }
    }

    private static CollectorContext NewContext(long? numericWatermark, bool hasCollectedBefore) =>
        new()
        {
            ServerId = 1,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo(),
            NumericWatermark = numericWatermark,
            HasCollectedBefore = hasCollectedBefore,
        };
}
