/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// A mute rule can name one external alert source (#5654): it matches only an alert from that source. A rule without a
/// source keeps the matching it always had, so it still mutes an external alert on its other dimensions, and an alert
/// with no source (every built-in alert, and every Lite alert) is never matched by a source rule. The store keeps the
/// source across a write and a read, and the viewer's copy of the statements names the same column.
/// </summary>
public sealed class MuteRuleExternalSourceTests
{
    private static readonly DateTime Now = new(2026, 10, 13, 12, 0, 0, DateTimeKind.Utc);

    private static AlertMuteContext Ctx(string? source, string name = "SQL01", int? id = 7, string? metric = null) =>
        new() { ServerName = name, ServerId = id, MetricName = metric ?? (source is null ? "High CPU" : $"External:{source}:disk"), ExternalSource = source };

    [Fact]
    public void ASourceRule_MatchesItsOwnSource_AndNoOtherSource()
    {
        var rule = new MuteRule { ExternalSource = "nagios" };

        Assert.True(rule.MatchesAt(Ctx("nagios"), Now));
        Assert.True(rule.MatchesAt(Ctx("NAGIOS"), Now));
        Assert.False(rule.MatchesAt(Ctx("zabbix"), Now));
    }

    [Fact]
    public void ASourceRule_NeverMatchesAnAlertWithNoSource()
    {
        var rule = new MuteRule { ExternalSource = "nagios" };

        /* Lite's contexts and every built-in alert's carry no source. */
        Assert.False(rule.MatchesAt(Ctx(null), Now));
        Assert.False(rule.MatchesAt(new AlertMuteContext { ServerName = "SQL01", MetricName = "High CPU" }, Now));
    }

    [Fact]
    public void ASourceRule_StillNarrowsByItsOtherDimensions()
    {
        var rule = new MuteRule { ExternalSource = "nagios", ServerId = 7, MetricName = "External:nagios:disk" };

        Assert.True(rule.MatchesAt(Ctx("nagios"), Now));
        Assert.False(rule.MatchesAt(Ctx("nagios", id: 8), Now));
        Assert.False(rule.MatchesAt(Ctx("nagios", metric: "External:nagios:cpu"), Now));
    }

    [Fact]
    public void ARuleWithNoSource_KeepsItsOldMatching_AndStillMutesAnExternalAlertForItsServer()
    {
        var serverRule = new MuteRule { ServerId = 7 };
        var nameRule = new MuteRule { ServerName = "SQL01" };
        var metricRule = new MuteRule { MetricName = "High CPU" };

        Assert.True(serverRule.MatchesAt(Ctx("nagios"), Now));
        Assert.True(serverRule.MatchesAt(Ctx(null), Now));
        Assert.True(nameRule.MatchesAt(Ctx("zabbix"), Now));
        Assert.True(metricRule.MatchesAt(Ctx(null), Now));
        Assert.False(metricRule.MatchesAt(Ctx("nagios"), Now));
    }

    [Fact]
    public void ASourceRule_IsNotABlanketRule_AndSaysItsSourceInItsSummary()
    {
        var rule = new MuteRule { ExternalSource = "nagios" };

        Assert.False(rule.MatchesEveryAlert);
        Assert.Contains("nagios", rule.Summary, StringComparison.Ordinal);
        Assert.True(new MuteRule().MatchesEveryAlert);
    }

    [Fact]
    public void ACloneKeepsTheSource()
    {
        Assert.Equal("nagios", new MuteRule { ExternalSource = "nagios" }.Clone().ExternalSource);
    }

    [Theory]
    [InlineData("External:nagios:disk", "nagios")]
    [InlineData("External:nagios:a:b", "nagios")]
    [InlineData("External::disk", null)]
    [InlineData("External:nagios", null)]
    [InlineData("High CPU", null)]
    [InlineData("Custom:7", null)]
    [InlineData(null, null)]
    public void TheSourceIsReadFromAnExternalMetricName(string? metric, string? expected)
    {
        Assert.Equal(expected, AlertMuteContext.ExternalSourceOf(metric));
    }

    private static ViewerAlertRow Row(string metric) => new()
    {
        AlertTime = Now, ServerName = "SQL01", MetricName = metric, CurrentValue = 1, ThresholdValue = 0,
        AlertSent = false, NotificationType = "", Muted = false,
    };

    [Fact]
    public void AViewerAlertRowForAnExternalAlert_CarriesItsSourceIntoTheMuteContext()
    {
        var row = Row("External:nagios:disk");

        Assert.Equal("nagios", row.ToMuteContext().ExternalSource);
        Assert.Null(Row("High CPU").ToMuteContext().ExternalSource);
    }

    [Fact]
    public void ASourceRuleIsNotAWholeServerSilence_SoUnsilenceLeavesIt()
    {
        var sourceRule = new MuteRule { ServerId = 7, ServerName = "SQL01", ExternalSource = "nagios" };
        var wholeServer = new MuteRule { ServerId = 7, ServerName = "SQL01" };

        Assert.False(ViewerDataService.IsWholeServerSilence(sourceRule, 7, "SQL01"));
        Assert.True(ViewerDataService.IsWholeServerSilence(wholeServer, 7, "SQL01"));
    }

    [Fact]
    public void TheServiceStoreAndTheViewerStatements_ReadAndWriteTheSameColumn()
    {
        Assert.Contains("external_source", ViewerDataService.MuteRulesSelectSql, StringComparison.Ordinal);
        Assert.Contains("external_source", ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        Assert.Contains("external_source = $13", ViewerDataService.MuteRuleUpdateSql, StringComparison.Ordinal);

        var store = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "PgMuteRuleStore.cs");
        Assert.Contains("client_app_pattern, external_source", store, StringComparison.Ordinal);
        Assert.Contains("external_source = $13", store, StringComparison.Ordinal);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(store, @"AddNullableText\(command, rule\.ExternalSource\)").Count);
    }

    [Fact]
    public void TheDrainPassesTheRowsSourceIntoItsMuteContext()
    {
        /* The live drain fact proves the effect; this holds the wiring where the live fact cannot run. */
        var drain = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "ExternalAlertDrain.cs");
        Assert.Contains("ExternalSource = row.SourceName,", drain, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheStoreKeepsTheSourceAcrossInsertUpdateAndLoad()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-store test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var seed = new NpgsqlConnection(scratch.ConnectionString))
        {
            await seed.OpenAsync(ct);
            await PgMigrations.MigrateAsync(seed, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new PgMuteRuleStore(postgres);
        var withSource = new MuteRule { Id = "m-src", ServerId = 7, ServerName = "SQL01", ExternalSource = "nagios", Reason = "noisy" };
        var withoutSource = new MuteRule { Id = "m-none", ServerName = "SQL01" };

        var bodySucceeded = false;
        try
        {
            await store.InsertAsync(withSource);
            await store.InsertAsync(withoutSource);

            var loaded = await store.LoadAllAsync(ct);
            Assert.Equal("nagios", loaded.Single(r => r.Id == "m-src").ExternalSource);
            Assert.Null(loaded.Single(r => r.Id == "m-none").ExternalSource);
            Assert.True(loaded.Single(r => r.Id == "m-src").Matches(Ctx("nagios")));
            Assert.False(loaded.Single(r => r.Id == "m-src").Matches(Ctx("zabbix")));

            /* An update writes the source, and clearing it writes NULL back. */
            withSource.ExternalSource = "zabbix";
            withoutSource.ExternalSource = "nagios";
            await store.UpdateAsync(withSource);
            await store.UpdateAsync(withoutSource);
            loaded = await store.LoadAllAsync(ct);
            Assert.Equal("zabbix", loaded.Single(r => r.Id == "m-src").ExternalSource);
            Assert.Equal("nagios", loaded.Single(r => r.Id == "m-none").ExternalSource);

            withSource.ExternalSource = null;
            await store.UpdateAsync(withSource);
            Assert.Null((await store.LoadAllAsync(ct)).Single(r => r.Id == "m-src").ExternalSource);

            /* The enable switch rewrites no other column. */
            await store.SetEnabledAsync("m-none", false);
            Assert.Equal("nagios", (await store.LoadAllAsync(ct)).Single(r => r.Id == "m-none").ExternalSource);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
