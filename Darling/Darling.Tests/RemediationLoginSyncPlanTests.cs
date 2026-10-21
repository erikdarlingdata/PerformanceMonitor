/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The pairing and the checks behind the login sync (#6262), without a store: which registered server a darling.json entry's login may
/// go to, and what the sync leaves alone. The sync runs as the store owner, so these rules are the only thing standing between a file
/// entry and a row that names another server.
/// </summary>
public sealed class RemediationLoginSyncPlanTests
{
    private const string Reference = "env:ALPHA_REMEDIATION_PASSWORD_6262";
    private const string OtherReference = "env:BRAVO_REMEDIATION_PASSWORD_6262";
    private const string DpapiShaped = "AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAAdpapi6262blobXYZ==";

    private static MonitoredServer Entry(int id, string name, string? user = null, string? secret = null, string host = "", string engine = "sqlserver")
    {
        return new MonitoredServer
        {
            StoredServerId = id,
            Name = name,
            Host = host.Length == 0 ? name + ".example.test" : host,
            Engine = engine,
            Port = engine == "postgresql" ? 5432 : 1433,
            RemediationUsername = user,
            RemediationEncryptedPassword = secret,
            HypotheticalIndexUsername = user,
            HypotheticalIndexEncryptedPassword = secret,
        };
    }

    /// <summary>A registry row that names the same connection as <paramref name="entry"/> and holds no login.</summary>
    private static StoreConfigProvider.StoredLoginRow Row(
        int id, string name, MonitoredServer entry, string? user = null, string? secret = null) =>
        new(id, name, user, secret, new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified), StoreConfigProvider.ConnectionIdentityOf(entry));

    private static StoreConfigProvider.StoredLoginRow Moved(StoreConfigProvider.StoredLoginRow row, string host) =>
        row with { Identity = row.Identity with { Host = host } };

    /// <summary>An entry takes the row with its server id, even when another row carries its name.</summary>
    [Fact]
    public void ARowWithTheEntrysIdIsPairedBeforeARowWithItsName()
    {
        var alpha = Entry(1, "alpha-sql-01", "alpha_fix", Reference);
        var rows = new[]
        {
            Row(1, "renamed-sql-01", alpha),
            Row(2, "alpha-sql-01", alpha),
        };

        var plan = StoreConfigProvider.PlanRemediationSync([alpha], rows);

        var change = Assert.Single(plan.Changes);
        Assert.Equal(1, change.ServerId);
        Assert.Empty(plan.Skips);
    }

    /// <summary>An entry with no row by id pairs by name, but never with a row that another entry claimed by id; the entry that holds the id
    /// keeps the row in either file order, and the other entry gets nothing.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ARowAnotherEntryClaimsByIdIsNotTakenByName(bool nameMatchFirst)
    {
        var holder = Entry(1, "moved-sql-01", "holder_fix", Reference);
        var byName = Entry(99, "alpha-sql-01", "name_fix", OtherReference, host: holder.Host);
        var rows = new[] { Row(1, "alpha-sql-01", holder) };

        var plan = StoreConfigProvider.PlanRemediationSync(nameMatchFirst ? [byName, holder] : [holder, byName], rows);

        var change = Assert.Single(plan.Changes);
        Assert.Equal(1, change.ServerId);
        Assert.Equal("holder_fix", change.Username);
        Assert.Equal(Reference, change.Secret);
        Assert.Empty(plan.Skips);
    }

    /// <summary>A name-paired entry with NO keys cannot take a row from the entry that holds its id (it must not clear or block it).</summary>
    [Fact]
    public void AKeylessEntryNeverBlocksTheEntryThatHoldsTheId()
    {
        var holder = Entry(1, "moved-sql-01", "holder_fix", Reference);
        var keyless = Entry(99, "alpha-sql-01", host: holder.Host);
        var rows = new[] { Row(1, "alpha-sql-01", holder, "old_fix", OtherReference) };

        var plan = StoreConfigProvider.PlanRemediationSync([keyless, holder], rows);

        var change = Assert.Single(plan.Changes);
        Assert.Equal("holder_fix", change.Username);
        Assert.Empty(plan.Skips);
    }

    /// <summary>Two entries that claim one row, by the same id or by the same name, are both refused; the pairing does not pick one.</summary>
    [Theory]
    [InlineData("id")]
    [InlineData("name")]
    public void ARowTwoEntriesClaimIsRefusedForBoth(string by)
    {
        var first = by == "id" ? Entry(1, "alpha-sql-01", "first_fix", Reference) : Entry(11, "alpha-sql-01", "first_fix", Reference);
        var second = by == "id" ? Entry(1, "other-name", "second_fix", OtherReference, host: first.Host) : Entry(12, "ALPHA-sql-01", "second_fix", OtherReference);
        var rows = new[] { Row(by == "id" ? 1 : 5, "alpha-sql-01", first) };

        foreach (var order in new[] { new[] { first, second }, new[] { second, first } })
        {
            var plan = StoreConfigProvider.PlanRemediationSync(order, rows);

            Assert.Empty(plan.Changes);
            Assert.Equal(2, plan.Skips.Count);
            Assert.All(plan.Skips, skip => Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.AmbiguousPairing, skip.Reason));
        }
    }

    /// <summary>A name that matches more than one registered server pairs with none of them. A server with a clean pair is still written.</summary>
    [Fact]
    public void ANameThatMatchesTwoRowsIsRefused_AndTheNextServerIsStillPlanned()
    {
        var alpha = Entry(99, "alpha-sql-01", "alpha_fix", Reference);
        var bravo = Entry(2, "bravo-sql-01", "bravo_fix", OtherReference);
        var rows = new[] { Row(5, "alpha-sql-01", alpha), Row(6, "Alpha-SQL-01", alpha), Row(2, "bravo-sql-01", bravo) };

        var plan = StoreConfigProvider.PlanRemediationSync([alpha, bravo], rows);

        var skip = Assert.Single(plan.Skips);
        Assert.Equal("alpha-sql-01", skip.Name);
        Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.AmbiguousPairing, skip.Reason);
        Assert.Equal(2, Assert.Single(plan.Changes).ServerId);
    }

    /// <summary>A PostgreSQL entry never pairs with a SQL Server row of the same name.</summary>
    [Fact]
    public void AnEngineMismatchIsNotWritten()
    {
        var pg = Entry(99, "alpha-sql-01", "alpha_fix", Reference, engine: "postgresql");
        var sqlServerRow = Row(5, "alpha-sql-01", Entry(5, "alpha-sql-01"));

        var plan = StoreConfigProvider.PlanRemediationSync([pg], [sqlServerRow]);

        Assert.Empty(plan.Changes);
        Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.ConnectionDiffers, Assert.Single(plan.Skips).Reason);
    }

    /// <summary>A row that was moved to another host after the entry was written is skipped, whether it pairs by id or by name, and in both slots.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ARowThatNamesAnotherHostIsSkipped(bool pairedById)
    {
        var entry = Entry(pairedById ? 1 : 99, "alpha-sql-01", "alpha_fix", Reference);
        var row = Moved(Row(1, "alpha-sql-01", entry), "elsewhere.example.test");

        foreach (var plan in new[]
                 {
                     StoreConfigProvider.PlanRemediationSync([entry], [row]),
                     StoreConfigProvider.PlanHypotheticalIndexSync([entry], [row]),
                 })
        {
            Assert.Empty(plan.Changes);
            var skip = Assert.Single(plan.Skips);
            Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.ConnectionDiffers, skip.Reason);
            Assert.Equal("alpha-sql-01", skip.Name);
        }
    }

    /// <summary>A row that keeps its host and engine but names a different port, database, read-only intent, authentication, user name,
    /// encryption mode, certificate trust or multi-subnet setting is another connection, so it is skipped in both slots.</summary>
    [Theory]
    [InlineData("port")]
    [InlineData("database")]
    [InlineData("readOnlyIntent")]
    [InlineData("auth")]
    [InlineData("username")]
    [InlineData("encryptMode")]
    [InlineData("trust")]
    [InlineData("multiSubnet")]
    public void ARowThatKeepsItsHostAndEngineButChangesAnotherConnectionSettingIsSkipped(string changed)
    {
        var entry = Entry(1, "alpha-sql-01", "alpha_fix", Reference);
        var row = Row(1, "alpha-sql-01", entry);
        var identity = row.Identity;
        row = row with
        {
            Identity = changed switch
            {
                "port" => identity with { Port = identity.Port + 1 },
                "database" => identity with { Database = "other_db" },
                "readOnlyIntent" => identity with { ReadOnlyIntent = !identity.ReadOnlyIntent },
                "auth" => identity with { Auth = "sql" },
                "username" => identity with { Username = "someone_else" },
                "encryptMode" => identity with { EncryptMode = "Optional" },
                "trust" => identity with { TrustServerCertificate = !identity.TrustServerCertificate },
                "multiSubnet" => identity with { MultiSubnetFailover = !identity.MultiSubnetFailover },
                _ => throw new ArgumentOutOfRangeException(nameof(changed)),
            },
        };
        Assert.Equal(entry.Host, row.Identity.Host);
        Assert.Equal(entry.Engine, row.Identity.Engine, ignoreCase: true);

        foreach (var plan in new[]
                 {
                     StoreConfigProvider.PlanRemediationSync([entry], [row]),
                     StoreConfigProvider.PlanHypotheticalIndexSync([entry], [row]),
                 })
        {
            Assert.Empty(plan.Changes);
            var skip = Assert.Single(plan.Skips);
            Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.ConnectionDiffers, skip.Reason);
        }
    }

    /// <summary>A change carries the modified_at the row had when it was read, which the write is conditional on.</summary>
    [Fact]
    public void AChangeCarriesTheModifiedAtOfTheRowItWasPlannedFrom()
    {
        var entry = Entry(1, "alpha-sql-01", "alpha_fix", Reference);
        var row = Row(1, "alpha-sql-01", entry);

        var change = Assert.Single(StoreConfigProvider.PlanRemediationSync([entry], [row]).Changes);

        Assert.Equal(row.ModifiedAt, change.ExpectedModifiedAt);
        Assert.Equal("set", change.Action);
    }

    /// <summary>Only an entry with both keys is written. A half login is reported and changes nothing; no keys is silent and never clears.</summary>
    [Fact]
    public void OnlyBothKeysWrite_AHalfLoginIsReported_AndAbsentKeysNeverClear()
    {
        var userOnly = Entry(1, "alpha-sql-01", "alpha_fix");
        var secretOnly = Entry(2, "bravo-sql-01", secret: Reference);
        var none = Entry(3, "charlie-sql-01");
        var rows = new[]
        {
            Row(1, "alpha-sql-01", userOnly, "held_fix", OtherReference),
            Row(2, "bravo-sql-01", secretOnly, "held_fix", OtherReference),
            Row(3, "charlie-sql-01", none, "held_fix", OtherReference),
        };

        foreach (var plan in new[]
                 {
                     StoreConfigProvider.PlanRemediationSync([userOnly, secretOnly, none], rows),
                     StoreConfigProvider.PlanHypotheticalIndexSync([userOnly, secretOnly, none], rows),
                 })
        {
            Assert.Empty(plan.Changes);
            Assert.Equal(["alpha-sql-01", "bravo-sql-01"], plan.Skips.Select(s => s.Name).ToArray());
            Assert.All(plan.Skips, skip => Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.HalfLogin, skip.Reason));
        }
    }

    /// <summary>A pair that already holds the file's login is not listed; a changed secret is "changed".</summary>
    [Fact]
    public void AnEqualPairIsNotListed_AndAChangedOneIsChanged()
    {
        var same = Entry(1, "alpha-sql-01", "alpha_fix", Reference);
        var differs = Entry(2, "bravo-sql-01", "bravo_fix", Reference);
        var rows = new[] { Row(1, "alpha-sql-01", same, "alpha_fix", Reference), Row(2, "bravo-sql-01", differs, "bravo_fix", OtherReference) };

        var plan = StoreConfigProvider.PlanRemediationSync([same, differs], rows);

        var change = Assert.Single(plan.Changes);
        Assert.Equal(2, change.ServerId);
        Assert.Equal("changed", change.Action);
        Assert.Empty(plan.Skips);
    }

    /// <summary>The remediation connection opens a reference, a sealed value and a DPAPI blob and has no arm for a plain password, so a plain
    /// password is refused (its shape named, never the value).</summary>
    [Theory]
    [InlineData("env:ALPHA_PASSWORD", true)]
    [InlineData("file:/run/secrets/alpha", true)]
    [InlineData("sealed:v1:abc", true)]
    [InlineData(DpapiShaped, true)]
    [InlineData("hunter2-not-a-reference", false)]
    public void TheRemediationSlotAcceptsTheShapesItsConnectionCodeAccepts(string secret, bool accepted)
    {
        var entry = Entry(1, "alpha-sql-01", "alpha_fix", secret);

        var plan = StoreConfigProvider.PlanRemediationSync([entry], [Row(1, "alpha-sql-01", entry)]);

        if (accepted)
        {
            Assert.Single(plan.Changes);
            Assert.Empty(plan.Skips);
        }
        else
        {
            Assert.Empty(plan.Changes);
            var skip = Assert.Single(plan.Skips);
            Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.SecretShapeRefused, skip.Reason);
            Assert.Equal("plain text value", skip.Detail);
        }
    }

    /// <summary>The index-test connection takes a reference and nothing else: a sealed value, a DPAPI blob and a plain password are all refused.</summary>
    [Theory]
    [InlineData("env:ALPHA_PASSWORD", true)]
    [InlineData("file:/run/secrets/alpha", true)]
    [InlineData("sealed:v1:abc", false)]
    [InlineData(DpapiShaped, false)]
    [InlineData("hunter2-not-a-reference", false)]
    public void TheIndexTestSlotAcceptsOnlyAReference(string secret, bool accepted)
    {
        var entry = Entry(1, "alpha-sql-01", "alpha_check", secret);

        var plan = StoreConfigProvider.PlanHypotheticalIndexSync([entry], [Row(1, "alpha-sql-01", entry)]);

        Assert.Equal(accepted ? 1 : 0, plan.Changes.Count);
        Assert.Equal(accepted ? 0 : 1, plan.Skips.Count);
        Assert.All(plan.Skips, skip => Assert.Equal(StoreConfigProvider.LoginSyncSkipReason.SecretShapeRefused, skip.Reason));
        Assert.DoesNotContain(plan.Skips, skip => skip.Detail.Contains("hunter2", StringComparison.Ordinal));
    }
}
