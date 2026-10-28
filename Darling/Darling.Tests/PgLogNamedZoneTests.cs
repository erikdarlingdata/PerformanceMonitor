/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6866: a log stamped in a NAMED local zone is converted only where the server proves the instant. The map the
/// server answers with is built here from the reporter's own measured rows, and the live class below asks a real
/// server for the same rows.
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogNamedZoneTests
{
    private static string Line(string stamp, string zone, int pid, string text) =>
        $"{stamp} {zone} [{pid}] LOG:  {text}\n";

    private static PgLogZoneMap Proven(params (string Stamp, string Zone, int Matches, string Utc)[] rows) =>
        PgLogZoneMap.FromMatches(rows.Select(r => (r.Stamp, r.Zone, r.Matches, DateTime.Parse(r.Utc, System.Globalization.CultureInfo.InvariantCulture))));

    [Fact]
    public void TheReportersFourCetRows_StoreTheirTrueUtcInstants_AndTheSkippedHourIsSkipped()
    {
        var body =
            Line("2026-10-25 02:30:00.000", "CEST", 1, "repeated hour first pass")
            + Line("2026-10-25 02:30:00.000", "CET", 2, "repeated hour second pass")
            + Line("2026-03-29 01:30:00.000", "CET", 3, "before the spring change")
            + Line("2026-03-29 03:30:00.000", "CEST", 4, "after the spring change")
            + Line("2026-03-29 02:30:00.000", "CEST", 5, "an hour the clock skipped");

        var zones = Proven(
            ("2026-10-25 02:30:00.000", "CEST", 1, "2026-10-25 00:30:00"),
            ("2026-10-25 02:30:00.000", "CET", 1, "2026-10-25 01:30:00"),
            ("2026-03-29 01:30:00.000", "CET", 1, "2026-03-29 00:30:00"),
            ("2026-03-29 03:30:00.000", "CEST", 1, "2026-03-29 01:30:00"));

        var entries = PgLogEntryAssembler.Assemble(body, false, null, zones, out var foreign);

        Assert.Equal(4, entries.Count);
        Assert.Equal(1, foreign);
        Assert.Equal(new DateTime(2026, 10, 25, 0, 30, 0, DateTimeKind.Utc), entries[0].OccurredAtUtc);
        Assert.Equal(new DateTime(2026, 10, 25, 1, 30, 0, DateTimeKind.Utc), entries[1].OccurredAtUtc);
        Assert.Equal(new DateTime(2026, 3, 29, 0, 30, 0, DateTimeKind.Utc), entries[2].OccurredAtUtc);
        Assert.Equal(new DateTime(2026, 3, 29, 1, 30, 0, DateTimeKind.Utc), entries[3].OccurredAtUtc);
    }

    [Fact]
    public void ALineFromAnEarlierSetting_IsSkippedAndCounted_NeverConverted()
    {
        var body = Line("2026-10-24 12:00:00.000", "PST", 7, "written when log_timezone was another zone")
            + Line("2026-10-24 12:00:01.000", "CEST", 8, "genuine");

        var zones = Proven(("2026-10-24 12:00:01.000", "CEST", 1, "2026-10-24 10:00:01"));

        var entries = PgLogEntryAssembler.Assemble(body, false, null, zones, out var foreign);

        var only = Assert.Single(entries);
        Assert.Equal(8, only.Pid);
        Assert.Equal(1, foreign);
    }

    [Fact]
    public void TwoMatchingInstants_AreSkippedAndCounted_NeverGuessed()
    {
        var body = Line("2014-10-26 01:30:00.000", "MSK", 9, "one abbreviation across a transition");
        var zones = Proven(("2014-10-26 01:30:00.000", "MSK", 2, "2014-10-25 21:30:00"));

        var entries = PgLogEntryAssembler.Assemble(body, false, null, zones, out var foreign);

        Assert.Empty(entries);
        Assert.Equal(1, foreign);
    }

    [Fact]
    public void ANonNamedSetting_StillRefuses()
    {
        var body = Line("2026-10-24 12:00:00.000", "-03", 1, "x");

        Assert.Throws<PgLogTimezoneUnsupportedException>(() => PgLogEntryAssembler.Assemble(body, false, null, null, out _));
    }

    [Fact]
    public async Task WithoutAResolver_ANamedLocalSetting_KeepsTheRefusal()
    {
        var zones = await PgLogNamedZone.ResolveBodyAsync(
            Line("2026-10-24 12:00:00.000", "CEST", 1, "x"), false, "Europe/Berlin", null, CancellationToken.None);

        Assert.Null(zones);
    }

    /// <summary>The real server renders the reporter's rows, Berlin and Los Angeles, and Moscow's one ambiguous stamp.</summary>
    [Theory]
    [InlineData("Europe/Berlin", "2026-10-25 02:30:00.123", "CEST", "2026-10-25 00:30:00.123")]
    [InlineData("Europe/Berlin", "2026-10-25 02:30:00.123", "CET", "2026-10-25 01:30:00.123")]
    [InlineData("CET", "2026-03-29 01:30:00.000", "CET", "2026-03-29 00:30:00.000")]
    [InlineData("America/Los_Angeles", "2026-11-01 01:30:00.000", "PDT", "2026-11-01 08:30:00.000")]
    [InlineData("America/Los_Angeles", "2026-11-01 01:30:00.000", "PST", "2026-11-01 09:30:00.000")]
    [InlineData("America/Los_Angeles", "2026-07-04 12:00:00", "PDT", "2026-07-04 19:00:00")]
    public async Task TheServerProvesTheTrueUtcInstant(string setting, string stamp, string abbrev, string expectedUtc)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        var zones = await PgLogNamedZone.ResolveAsync(
            connection, setting, new[] { new PgLogZonePair(stamp, abbrev) }, 30, CancellationToken.None);

        Assert.NotNull(zones);
        Assert.True(zones!.TryGet(stamp, abbrev, out var utc));
        Assert.Equal(DateTime.Parse(expectedUtc, System.Globalization.CultureInfo.InvariantCulture), utc);
    }

    [Fact]
    public async Task TheServer_RejectsTheSkippedHour_TheWrongAbbreviation_TwoMatches_AndANonNamedSetting()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        var pairs = new[]
        {
            new PgLogZonePair("2026-03-29 02:30:00.000", "CEST"),
            new PgLogZonePair("2026-03-29 02:30:00.000", "CET"),
            new PgLogZonePair("2026-07-01 12:00:00.000", "CET"),
            new PgLogZonePair("2026-13-45 99:99:99.000", "CET"),
        };

        var berlin = await PgLogNamedZone.ResolveAsync(connection, "Europe/Berlin", pairs, 30, CancellationToken.None);
        Assert.NotNull(berlin);
        Assert.Equal(0, berlin!.Count);

        var moscow = await PgLogNamedZone.ResolveAsync(
            connection, "Europe/Moscow", new[] { new PgLogZonePair("2014-10-26 01:30:00.000", "MSK") }, 30, CancellationToken.None);
        Assert.NotNull(moscow);
        Assert.Equal(0, moscow!.Count);

        var posix = await PgLogNamedZone.ResolveAsync(connection, "-03", pairs, 30, CancellationToken.None);
        Assert.Null(posix);
    }
}
