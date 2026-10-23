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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6334: the Viewer's known-pattern editor. The form turns the operator's text into an input or the shared
/// <see cref="KnownPatternInput.Validate"/> refusal; the data service lists, adds, edits and deletes entries through the V214 store
/// functions; and a store probed below V214 has no editor to offer.
/// </summary>
public sealed class ViewerKnownPatternEditorTests
{
    private static string LongNote => new('n', KnownPatternMatcher.MaxNoteLength + 1);

    public static IEnumerable<object?[]> RefusedForms()
    {
        // note empty, note over 500, window over the maximum, unknown zone, server id below 1
        yield return new object?[] { null, "High CPU", "02:10", "UTC", "10", "   ", "" };
        yield return new object?[] { null, "High CPU", "02:10", "UTC", "10", LongNote, "" };
        yield return new object?[] { null, "High CPU", "02:10", "UTC", "721", "nightly job", "" };
        yield return new object?[] { null, "High CPU", "02:10", "Not/AZone", "10", "nightly job", "" };
        yield return new object?[] { 0, "High CPU", "02:10", "UTC", "10", "nightly job", "" };
    }

    [Theory]
    [MemberData(nameof(RefusedForms))]
    public void ARefusedForm_ShowsTheSharedValidateText_AndBuildsNothing(
        int? serverId, string metric, string time, string zone, string window, string note, string expires)
    {
        var refusal = KnownPatternsEditorModel.TryBuild(serverId, metric, time, zone, window, note, expires, out var input);

        var expected = new KnownPatternInput(
            serverId, metric, TimeOnly.Parse(time, System.Globalization.CultureInfo.InvariantCulture), zone, int.Parse(window, System.Globalization.CultureInfo.InvariantCulture), note, null).Validate();
        Assert.NotNull(expected);
        Assert.Equal(expected, refusal);
        Assert.Null(input);
    }

    [Theory]
    [InlineData("2am", "10", "", "time of day")]
    [InlineData("02:10", "ten", "", "window")]
    [InlineData("02:10", "10", "next week", "expiry")]
    public void AnUnreadableField_IsRefusedByName_AndBuildsNothing(string time, string window, string expires, string field)
    {
        var refusal = KnownPatternsEditorModel.TryBuild(null, null, time, "UTC", window, "nightly job", expires, out var input);

        Assert.NotNull(refusal);
        Assert.Contains(field, refusal, StringComparison.OrdinalIgnoreCase);
        Assert.Null(input);
    }

    [Fact]
    public void AGoodForm_BuildsTheInput_WithBlanksMeaningAny()
    {
        var refusal = KnownPatternsEditorModel.TryBuild(
            7, "High CPU", "02:10", "America/New_York", "15", "  nightly index job ", "2030-01-02 03:04", out var input);

        Assert.Null(refusal);
        Assert.NotNull(input);
        Assert.Equal(7, input!.ServerId);
        Assert.Equal(new TimeOnly(2, 10), input.TimeOfDay);
        Assert.Equal(15, input.WindowMinutes);
        Assert.Equal(new DateTime(2030, 1, 2, 3, 4, 0, DateTimeKind.Utc), input.ExpiresAtUtc);

        var blank = KnownPatternsEditorModel.TryBuild(null, "", "", "", "", "a fleet-wide note", "", out var anyInput);
        Assert.Null(blank);
        Assert.Null(anyInput!.TimeOfDay);
        Assert.Null(anyInput.WindowMinutes);
        Assert.Null(anyInput.ExpiresAtUtc);
    }

    [Fact]
    public void TheDefaultTimeZone_IsOneTheMatcherKnows()
    {
        var zone = ViewerDataService.DefaultKnownPatternTimeZone();

        Assert.False(string.IsNullOrWhiteSpace(zone));
        Assert.True(KnownPatternMatcher.IsKnownTimeZone(zone), $"the editor's default zone '{zone}' must be accepted by the store's check");
    }

    [Theory]
    [InlineData(213, false)]
    [InlineData(150, false)]
    [InlineData(214, true)]
    [InlineData(215, true)]
    [InlineData(null, true)]
    public void TheEditor_IsOfferedOnlyOnAStoreAtV214OrLater(int? probedVersion, bool offered) =>
        Assert.Equal(offered, ViewerDataService.KnownPatternsAvailable(probedVersion));

    [Fact]
    public void AnEntryRow_ShowsTheServerByName_AndAMissingOneByIdAndNever()
    {
        var names = new Dictionary<int, string> { [7] = "Prod GP" };
        var created = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var scoped = new KnownPatternRow(new KnownPattern(1, 7, "High CPU", new TimeOnly(2, 10), "UTC", 10, "n", "erik (viewer)", created, null), names);
        var fleet = new KnownPatternRow(new KnownPattern(2, null, null, null, null, 10, "n", null, created, null), names);
        var gone = new KnownPatternRow(new KnownPattern(3, 99, null, null, null, 10, "n", null, created, null), names);

        Assert.Equal("Prod GP", scoped.ServerDisplay);
        Assert.Equal("02:10", scoped.TimeDisplay);
        Assert.Equal("Never", scoped.ExpiresDisplay);
        Assert.Equal("(any)", fleet.ServerDisplay);
        Assert.Equal("(any)", fleet.TimeDisplay);
        Assert.Contains("#99", gone.ServerDisplay, StringComparison.Ordinal);
    }
}

/* #1776 own-store: the facts mint their own scratch database through ScratchPostgres and touch no other test's rows, so the class is
   deliberately NOT [Collection("live-postgres")]. */

/// <summary>#6334: the Viewer's known-pattern writes against a migrated scratch store, and the editor's gate on a store below V214.</summary>
public sealed class ViewerKnownPatternEditorLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> MigratedStoreAsync(System.Threading.CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(connection, ct, DarlingManagedRoles.BuildKnownPatternFunctionsSql("config"));
        return (scratch, connection);
    }

    private static async Task<long> BeaconAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT config_version FROM config.config_service WHERE id = 1", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task TheViewer_AddsListsEditsAndDeletesEntries_AndARefusedInputWritesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the known pattern editor live test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await MigratedStoreAsync(ct);
        await using var _ = scratch;
        await using var __ = connection;
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        Assert.True(await viewer.KnownPatternsSupportedAsync(ct), "a fully migrated store carries the entries");
        Assert.Empty(await viewer.GetKnownPatternsAsync(ct));

        var beforeInsert = await BeaconAsync(connection, ct);
        var expires = DateTime.UtcNow.AddDays(30);
        var id = await viewer.InsertKnownPatternAsync(
            new KnownPatternInput(null, "High CPU", new TimeOnly(2, 10), "America/New_York", null, "  nightly index job  ", expires), ct);
        Assert.True(await BeaconAsync(connection, ct) > beforeInsert, "the viewer's write must move config_version so a running service re-reads");

        var listed = Assert.Single(await viewer.GetKnownPatternsAsync(ct));
        Assert.Equal(id, listed.Id);
        Assert.Null(listed.ServerId);
        Assert.Equal("High CPU", listed.MetricName);
        Assert.Equal(new TimeOnly(2, 10), listed.TimeOfDay);
        Assert.Equal("America/New_York", listed.TimeZone);
        Assert.Equal(KnownPatternMatcher.DefaultWindowMinutes, listed.WindowMinutes);
        Assert.Equal("nightly index job", listed.Note);
        Assert.EndsWith("(viewer)", listed.CreatedBy, StringComparison.Ordinal);
        Assert.Null(listed.UpdatedBy);
        Assert.Null(listed.UpdatedAtUtc);
        Assert.Equal(DateTimeKind.Utc, listed.CreatedAtUtc.Kind);
        Assert.NotNull(listed.ExpiresAtUtc);
        Assert.True(Math.Abs((listed.ExpiresAtUtc!.Value - expires).TotalSeconds) < 1, "the expiry must read back as the UTC instant written");

        Assert.True(await viewer.UpdateKnownPatternAsync(id, new KnownPatternInput(null, null, null, null, 30, "edited note", null), ct));
        var edited = Assert.Single(await viewer.GetKnownPatternsAsync(ct));
        Assert.Equal("edited note", edited.Note);
        Assert.Null(edited.MetricName);
        Assert.Null(edited.TimeOfDay);
        Assert.Equal(30, edited.WindowMinutes);
        Assert.Null(edited.ExpiresAtUtc);
        Assert.EndsWith("(viewer)", edited.UpdatedBy, StringComparison.Ordinal);
        Assert.NotNull(edited.UpdatedAtUtc);
        Assert.Equal(listed.CreatedBy, edited.CreatedBy);

        // A server that is not registered is refused with the store's sentence, and nothing changes.
        var unknownServer = await Assert.ThrowsAsync<ArgumentException>(() => viewer.UpdateKnownPatternAsync(id, new KnownPatternInput(987654, null, null, null, null, "ghost server", null), ct));
        Assert.Contains("No monitored server", unknownServer.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertKnownPatternAsync(new KnownPatternInput(987654, null, null, null, null, "ghost server", null), ct));
        Assert.Equal("edited note", Assert.Single(await viewer.GetKnownPatternsAsync(ct)).Note);

        // A refused input throws the shared Validate text and writes nothing.
        var refused = new KnownPatternInput(null, null, null, null, null, "   ", null);
        var thrown = await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertKnownPatternAsync(refused, ct));
        Assert.Contains(refused.Validate()!, thrown.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<ArgumentException>(() => viewer.UpdateKnownPatternAsync(id, refused, ct));
        Assert.Equal("edited note", Assert.Single(await viewer.GetKnownPatternsAsync(ct)).Note);

        Assert.True(await viewer.DeleteKnownPatternAsync(id, ct));
        Assert.False(await viewer.DeleteKnownPatternAsync(id, ct), "deleting an entry that is gone reports false");
        Assert.False(await viewer.UpdateKnownPatternAsync(id, new KnownPatternInput(null, null, null, null, null, "late edit", null), ct));
        Assert.Empty(await viewer.GetKnownPatternsAsync(ct));
    }

    [Fact]
    public async Task OnAStoreBelowV214_TheViewerOffersNoEditor_AndAWriteIsTheSchemaSkewMessage()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the known pattern editor live test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await MigratedStoreAsync(ct);
        await using var _ = scratch;
        await using var __ = connection;

        // Take V214's table away: the probe's newest sentinel goes, and the store reads as the version before it.
        await DarlingMcpTestData.ExecAsync(connection, ct, "DROP TABLE config.config_known_patterns CASCADE");
        // The probe answers the NEWEST sentinel present, so a real store before V214 has no V215 indexes either.
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 214, ct);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var probed = await viewer.GetStoreSchemaVersionAsync(ct);
        Assert.True(probed < ViewerDataService.KnownPatternsStoreSchemaVersion, $"probed {probed}");
        Assert.False(await viewer.KnownPatternsSupportedAsync(ct));
        Assert.Contains("newer store", ViewerDataService.KnownPatternsNeedNewerStoreMessage, StringComparison.Ordinal);

        await Assert.ThrowsAsync<ViewerSchemaSkewException>(() => viewer.GetKnownPatternsAsync(ct));
    }
}
