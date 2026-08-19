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
using System.Text.Json;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2361: <c>Database</c> and <c>LastEventUtc</c> on the incident projection.
///
/// <para><b>Why Database belongs on the incident.</b> A consumer previously string-searched <c>Details[]</c> for
/// a section whose fields contain a <c>Database</c> label. That is exact only for deadlocks — they are
/// self-contained, built with <c>includeDetailFields: true</c> — while every other fingerprinted alert appends a
/// BARE Incident item beside its data item, so the incident's own section carries no Database and the fallback
/// degrades to "any Database anywhere in the payload". On a multi-incident alert spanning databases that is not
/// an approximation; it is the wrong value with nothing marking it wrong.</para>
///
/// <para>Both members are shared: <c>AlertIncident</c> and <c>AlertIncidentDto</c> live in
/// <c>PerformanceMonitor.Notifications</c>, so Lite and Darling get this by construction rather than by parity
/// maintenance.</para>
/// </summary>
public class IncidentProjectionTests
{
    /// <summary>
    /// <b>The compatibility guarantee.</b> Both members are trailing and optional, so contextJson persisted
    /// before they existed still round-trips — the same property the DTO's own comment claims for
    /// <c>RemediationActionDto</c>. Alert history is durable; a deserialization failure here would break the
    /// reading of alerts that were written correctly at the time.
    /// </summary>
    [Fact]
    public void LegacyContextJson_WithoutTheNewMembers_StillDeserializes()
    {
        const string legacy = """
            {"DedupKey":"abc123","InvolvedObjects":["dbo.Orders"],"OccurrenceCount":3,
             "WaitRange":"1s-4s","TotalOccurrences":11,"IncidentStartedUtc":"2026-08-19T07:59:10Z"}
            """;

        var dto = JsonSerializer.Deserialize<AlertIncidentDto>(legacy);

        Assert.NotNull(dto);
        Assert.Equal("abc123", dto!.DedupKey);
        Assert.Equal(11, dto.TotalOccurrences);
        Assert.Null(dto.Database);
        Assert.Null(dto.LastEventUtc);
    }

    /// <summary>Both members survive a full serialize/deserialize cycle, which is what the webhook token and the
    /// persisted alert-history ContextJson both depend on.</summary>
    [Fact]
    public void TheNewMembers_RoundTrip()
    {
        var dto = new AlertIncidentDto(
            "key", new List<string> { "dbo.Orders" }, 2, "1s-4s", 7,
            new DateTime(2026, 8, 19, 7, 59, 10, DateTimeKind.Utc),
            "OrdersDB",
            new DateTime(2026, 8, 19, 8, 42, 0, DateTimeKind.Utc));

        var back = JsonSerializer.Deserialize<AlertIncidentDto>(JsonSerializer.Serialize(dto));

        Assert.Equal("OrdersDB", back!.Database);
        Assert.Equal(new DateTime(2026, 8, 19, 8, 42, 0, DateTimeKind.Utc), back.LastEventUtc);
    }

    /// <summary>
    /// A fingerprint kind that is not database-scoped carries null rather than an empty string — a disk or a job
    /// has no database, and "" would read downstream as a database whose name is blank.
    /// </summary>
    [Fact]
    public void ANonDatabaseScopedIncident_CarriesNull()
    {
        var disk = AlertFingerprint.ForKey("SQLPROD01", AlertFingerprint.Disk, @"C:\", new[] { @"C:\" });
        Assert.NotNull(disk);
        Assert.Null(disk!.Database);

        var blank = AlertFingerprint.ForKey("SQLPROD01", AlertFingerprint.Query, "0xABC", null, database: "   ");
        Assert.NotNull(blank);
        Assert.Null(blank!.Database);
    }

    /// <summary>A database-scoped incident carries it.</summary>
    [Fact]
    public void ADatabaseScopedIncident_CarriesTheDatabase()
    {
        var pvs = AlertFingerprint.ForKey(
            "SQLPROD01", AlertFingerprint.Database, "OrdersDB", new[] { "OrdersDB" }, database: "OrdersDB");

        Assert.NotNull(pvs);
        Assert.Equal("OrdersDB", pvs!.Database);
    }

    /// <summary>
    /// Blocking incidents pick up the group's database, and the grouper's <c>unknown</c> sentinel becomes null.
    /// That sentinel is fine as a display string and wrong as a data member: a consumer routing on
    /// <c>Database</c> would file tickets against a database literally named "unknown".
    /// </summary>
    [Fact]
    public void BlockingIncidents_CarryTheirDatabase_AndTheUnknownSentinelBecomesNull()
    {
        var withDb = BlockingIncidentGrouper.Group("SQLPROD01", new[]
        {
            new BlockingIncidentGrouper.BlockedEvent("OrdersDB", "dbo.Orders", "UPDATE", "SELECT", 1200, "LCK_M_X"),
        });
        Assert.Single(withDb);
        Assert.Equal("OrdersDB", withDb[0].Incident.Database);

        var noDb = BlockingIncidentGrouper.Group("SQLPROD01", new[]
        {
            new BlockingIncidentGrouper.BlockedEvent(null, "dbo.Orders", "UPDATE", "SELECT", 1200, "LCK_M_X"),
        });
        Assert.Single(noDb);
        Assert.Null(noDb[0].Incident.Database);
    }

    /// <summary>
    /// Deadlocks carry the database as a #2109 discrete fact on their detail fields, so the projection reads it
    /// from there — the same lookup a consumer was doing by hand, done once where it is exact.
    /// </summary>
    [Fact]
    public void DeadlockIncidents_TakeTheirDatabaseFromTheDetailFact()
    {
        var groups = DeadlockIncidentGrouper.Group("SQLPROD01", new[]
        {
            new DeadlockIncidentGrouper.DeadlockEvent(
                new[] { "dbo.Orders", "dbo.Items" },
                new List<AlertIncidentField> { new("Database", "OrdersDB"), new("Victim SQL", "UPDATE ...") }),
        });

        Assert.Single(groups);
        Assert.Equal("OrdersDB", groups[0].Incident.Database);
    }

    /// <summary>A deadlock with no Database fact carries null rather than guessing from another field.</summary>
    [Fact]
    public void ADeadlockWithNoDatabaseFact_CarriesNull()
    {
        var groups = DeadlockIncidentGrouper.Group("SQLPROD01", new[]
        {
            new DeadlockIncidentGrouper.DeadlockEvent(
                new[] { "dbo.Orders" },
                new List<AlertIncidentField> { new("Victim SQL", "UPDATE ...") }),
        });

        Assert.Single(groups);
        Assert.Null(groups[0].Incident.Database);
    }

    /// <summary>
    /// The DTO projection carries both through. <c>Serialize</c> and <c>SerializeIncidents</c> share one private
    /// <c>ToDto</c>, so the webhook token and the persisted ContextJson cannot disagree — a member added to the
    /// record but not the mapping would silently serialize as null everywhere.
    /// </summary>
    [Fact]
    public void TheSharedProjection_CarriesBothMembers()
    {
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident>
            {
                new("key", new[] { "dbo.Orders" }, 2, null, null, 9,
                    new DateTime(2026, 8, 19, 7, 0, 0, DateTimeKind.Utc),
                    "OrdersDB",
                    new DateTime(2026, 8, 19, 9, 30, 0, DateTimeKind.Utc)),
            },
        };

        var json = AlertContextSerializer.SerializeIncidents(context);

        Assert.Contains("OrdersDB", json, StringComparison.Ordinal);
        Assert.Contains("LastEventUtc", json, StringComparison.Ordinal);
    }
}
