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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
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

    private static PgLogEntry Entry(string stamp, string zone) =>
        new($"{stamp} {zone}", zone, DateTime.MinValue, 1, "", "ERROR", "boom", null, null, null, null, null, null, null, "raw");

    /// <summary>
    /// The csvlog, jsonlog and RDS route and the stderr route agree when there is nothing the server can be asked
    /// about (every stamp malformed): both keep the refusal, neither asks, and neither reports the read as clean.
    /// Before, the record route sent an empty ask and got an empty map back, which dropped every record and counted
    /// them as skipped lines instead of refusing.
    /// </summary>
    [Fact]
    public async Task WhenNoStampCanBeAsked_TheRecordRouteKeepsTheRefusal_AsTheStderrRouteDoes()
    {
        var calls = 0;
        PgLogZoneResolver resolver = (_, _, _) =>
        {
            calls++;
            return Task.FromResult<PgLogZoneMap?>(PgLogZoneMap.Empty);
        };

        var forged = new List<PgLogEntry> { Entry("2026-13-45 99:99:99.000", "CEST") };

        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(() =>
            PgLogNamedZone.FilterEntriesAsync(forged, false, "Europe/Berlin", resolver, CancellationToken.None));

        var body = Line("2026-13-45 99:99:99.000", "CEST", 1, "forged");
        var zones = await PgLogNamedZone.ResolveBodyAsync(body, false, "Europe/Berlin", resolver, CancellationToken.None);
        Assert.Null(zones);
        Assert.Throws<PgLogTimezoneUnsupportedException>(() => PgLogEntryAssembler.Assemble(body, false, null, zones, out _));

        Assert.Equal(0, calls);
    }

    /// <summary>A setting that is not a named zone is the resolver's refusal (null): the record route refuses with it.</summary>
    [Fact]
    public async Task WhenTheResolverRefuses_TheRecordRouteRefuses()
    {
        PgLogZoneResolver resolver = (_, _, _) => Task.FromResult<PgLogZoneMap?>(null);
        var entries = new List<PgLogEntry> { Entry("2026-10-24 12:00:00.000", "-03") };

        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(() =>
            PgLogNamedZone.FilterEntriesAsync(entries, false, "-03", resolver, CancellationToken.None));
    }

    /// <summary>With some stamps askable, a malformed one is skipped and counted while the proven one is stored, in one ask.</summary>
    [Fact]
    public async Task WhenSomeStampsCanBeAsked_TheOthersAreSkippedAndCounted_InOneAsk()
    {
        var calls = 0;
        PgLogZoneResolver resolver = (_, pairs, _) =>
        {
            calls++;
            Assert.Equal(new PgLogZonePair("2026-10-24 12:00:00.000", "CEST"), Assert.Single(pairs));
            return Task.FromResult<PgLogZoneMap?>(Proven(("2026-10-24 12:00:00.000", "CEST", 1, "2026-10-24 10:00:00")));
        };

        var entries = new List<PgLogEntry> { Entry("2026-10-24 12:00:00.000", "CEST"), Entry("2026-13-45 99:99:99.000", "CEST") };

        var (kept, foreign) = await PgLogNamedZone.FilterEntriesAsync(entries, false, "Europe/Berlin", resolver, CancellationToken.None);

        Assert.Equal(1, calls);
        Assert.Equal(1, foreign);
        Assert.Equal(new DateTime(2026, 10, 24, 10, 0, 0, DateTimeKind.Utc), Assert.Single(kept).OccurredAtUtc);
    }

    /// <summary>
    /// A connection the way the service opens a monitored target (#6143): from <see cref="PostgresTargetProvider"/>,
    /// with type loading off and <c>search_path</c> pinned to <c>pg_catalog,pg_temp</c>, so
    /// <see cref="PgLogNamedZone.ResolveSql"/>'s <c>text[]</c> parameters and <c>timestamp</c> result are proven to read
    /// on it. Skips, with the reason, when the cluster variable is not set, so a run without it says skipped.
    /// </summary>
    internal static async Task<NpgsqlConnection> OpenPinnedAsync(string variable = "DARLING_TEST_PG")
    {
        var target = Environment.GetEnvironmentVariable(variable);
        Assert.SkipWhen(string.IsNullOrWhiteSpace(target), $"Set {variable} to a PostgreSQL connection string to run the #6866 server checks.");

        var connection = PostgresTargetProvider.CreatePinnedConnection(PinnedConnectionString(target!));
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    internal static string PinnedConnectionString(string target) =>
        new NpgsqlConnectionStringBuilder(target) { Options = MonitoredServerConnection.PinnedSearchPathOption }.ConnectionString;

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
        await using var connection = await OpenPinnedAsync();

        var zones = await PgLogNamedZone.ResolveAsync(
            connection, setting, new[] { new PgLogZonePair(stamp, abbrev) }, 30, CancellationToken.None);

        Assert.NotNull(zones);
        Assert.True(zones!.TryGet(stamp, abbrev, out var utc));
        Assert.Equal(DateTime.Parse(expectedUtc, System.Globalization.CultureInfo.InvariantCulture), utc);
    }

    [Fact]
    public async Task TheServer_RejectsTheSkippedHour_TheWrongAbbreviation_TwoMatches_AndANonNamedSetting()
    {
        await using var connection = await OpenPinnedAsync();

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

    /// <summary>
    /// #7397: the check that the setting is a named zone reads <c>pg_timezone_names</c>, which opens every zone file on the
    /// target, so it runs once per server and setting and not on every read. The server's own statement counter proves a
    /// repeat read does not run it, a named zone still resolves, a POSIX offset and an unknown name still return no row
    /// (the repeat of those without any round trip), and the connection's session zone is as it was.
    /// </summary>
    [Fact]
    public async Task TheZoneNameIsLookedUpOncePerSetting_AndTheConnectionKeepsItsZone()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenPinnedAsync();
        /* The counter reads pg_stat_statements from a scratch database of its own, not from the shared store: CREATE
           EXTENSION below is a persistent, database-level change, and in the shared store it is residue the live-postgres
           collection's cleanup check fails the whole leg on (#7415). The view lists the statements of every database on the
           cluster, so the scans the pinned connection above runs in the shared store are still counted. */
        await using var scratch = await ScratchPostgres.CreateAsync(
            Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct, emptyStore: true);
        await using var counter = new NpgsqlConnection(scratch.ConnectionString);
        await counter.OpenAsync(ct);

        long? ZoneScans()
        {
            try
            {
                using var create = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", counter);
                create.ExecuteNonQuery();

                /* The pattern is built from literals so this statement's own text does not match it. */
                using var count = new NpgsqlCommand(
                    "SELECT COALESCE(sum(calls), 0)::bigint FROM pg_stat_statements "
                    + "WHERE query LIKE '%' || 'pg_timezone' || '_names AS n%' AND query LIKE '%' || 'set_' || 'config%'", counter);
                return (long?)count.ExecuteScalar();
            }
            catch (PostgresException)
            {
                return null;
            }
        }

        var baseline = ZoneScans();
        Assert.SkipWhen(baseline is null, "pg_stat_statements is not loaded on DARLING_TEST_PG, so the scans cannot be counted.");

        var pairs = new[] { new PgLogZonePair("2026-07-04 12:00:00", "PDT") };
        var expected = new DateTime(2026, 7, 4, 19, 0, 0, DateTimeKind.Utc);

        async Task<string?> SessionZoneAsync()
        {
            await using var read = new NpgsqlCommand("SELECT pg_catalog.current_setting('TimeZone')", connection);
            return (string?)await read.ExecuteScalarAsync(ct);
        }

        var before = await SessionZoneAsync();
        PgLogNamedZone.ForgetZoneNames();

        /* The case a log setting may be spelled in: the name is looked up case-insensitively, once. */
        for (var read = 0; read < 3; read++)
        {
            var zones = await PgLogNamedZone.ResolveAsync(connection, "america/los_angeles", pairs, 30, ct);
            Assert.NotNull(zones);
            Assert.True(zones!.TryGet("2026-07-04 12:00:00", "PDT", out var utc));
            Assert.Equal(expected, utc);
        }

        Assert.Equal(baseline + 1, ZoneScans());

        foreach (var notNamed in new[] { "-03", "Mars/Olympus_Mons" })
        {
            var scansBefore = ZoneScans();
            Assert.Null(await PgLogNamedZone.ResolveAsync(connection, notNamed, pairs, 30, ct));

            /* The repeat is answered from what the first found: no statement reaches the target, so a connection that
               was never opened is not even noticed. */
            await using var unopened = new NpgsqlConnection(connection.ConnectionString);
            Assert.Null(await PgLogNamedZone.ResolveAsync(unopened, notNamed, pairs, 30, ct));
            Assert.Equal(scansBefore + 1, ZoneScans());
        }

        Assert.Equal(before, await SessionZoneAsync());
    }
}
