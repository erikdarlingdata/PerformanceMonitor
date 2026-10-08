using System;
using System.Globalization;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The decision half of the start-of-run sweep (#4981): which database names it may ever drop. The sweep is aimed at
/// whatever the test connection string names, so the property that matters is the negative one, that a name that merely
/// looks like a scratch database is never selected. The live half, against a real cluster, is
/// <see cref="ScratchDatabaseSweepLiveTests"/>.
/// </summary>
public sealed class ScratchDatabaseSweepTests
{
    private static readonly DateTime Now = new(2026, 10, 3, 12, 0, 0, DateTimeKind.Utc);

    private const string OldStamp = "20260101000000";

    private const string Id = "0123456789ab";

    private static string Name(string stamp = OldStamp, string id = Id) => "darling_scratch_" + stamp + "_" + id;

    [Fact]
    public void AFreshName_IsAFactoryName_AndCarriesItsCreationTime()
    {
        var created = new DateTime(2026, 10, 3, 11, 59, 58, DateTimeKind.Utc);

        var name = ScratchDatabaseSweep.NewName(created);

        Assert.True(ScratchDatabaseSweep.IsFactoryName(name), name);
        Assert.True(ScratchDatabaseSweep.TryParseCreatedUtc(name, out var parsed));
        Assert.Equal(created, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        Assert.True(name.Length <= 63, "a PostgreSQL identifier is at most 63 bytes");
    }

    [Fact]
    public void TwoFreshNames_DifferEvenInTheSameSecond()
    {
        Assert.NotEqual(ScratchDatabaseSweep.NewName(Now), ScratchDatabaseSweep.NewName(Now));
    }

    /// <summary>
    /// Every one of these looks like a scratch database to a person skimming a database list, and none may be
    /// dropped, however old it looks. The first is what the factory minted before names carried a stamp: it has no
    /// age to compare, so the sweep leaves it.
    /// </summary>
    [Theory]
    [InlineData("darling_scratch_0123456789ab")]                         // the earlier shape, no stamp
    [InlineData("darling_scratch_20260101000000_0123456789a")]           // 11 hex
    [InlineData("darling_scratch_20260101000000_0123456789abc")]         // 13 hex
    [InlineData("darling_scratch_20260101000000_0123456789AB")]          // uppercase hex
    [InlineData("darling_scratch_20260101000000_0123456789ag")]          // not hex
    [InlineData("darling_scratch_20260101000000_0123456789ab_x")]        // trailing extra
    [InlineData("darling_scratch_20260101000000_0123456789ab\n")]        // trailing newline
    [InlineData("darling_scratch_2026010100000_0123456789ab")]           // 13-digit stamp
    [InlineData("darling_scratch_202601010000000_0123456789ab")]         // 15-digit stamp
    [InlineData("darling_scratch_20261301000000_0123456789ab")]          // month 13 is not a date
    [InlineData("darling_scratch_20260230000000_0123456789ab")]          // 30 February is not a date
    [InlineData("darling_scratch_20260101250000_0123456789ab")]          // hour 25 is not a time
    [InlineData("darling_scratch_2026010100000a_0123456789ab")]          // a letter in the stamp
    [InlineData("darling_scratch_\u0662\u0660\u0662\u0666\u0660\u0661\u0660\u0661\u0660\u0660\u0660\u0660\u0660\u0660_0123456789ab")]          // non-ASCII digits in the stamp
    [InlineData("Darling_Scratch_20260101000000_0123456789ab")]          // different case
    [InlineData("xdarling_scratch_20260101000000_0123456789ab")]         // prefixed
    [InlineData(" darling_scratch_20260101000000_0123456789ab")]         // leading space
    [InlineData("darling_scratch__0123456789ab")]                        // no stamp, extra underscore
    [InlineData("darling_scratch_20260101000000-0123456789ab")]          // wrong separator
    [InlineData("darling_scratch20260101000000_0123456789ab")]           // missing separator
    [InlineData("darlingtest_scratch_20260101000000_0123456789ab")]      // another family's prefix
    [InlineData("darling_upgrade_ladder_test_3_7_0")]                    // another test fixture's database
    [InlineData("darlingtest_4981")]
    [InlineData("darling")]
    [InlineData("postgres")]
    [InlineData("template0")]
    [InlineData("template1")]
    [InlineData("")]
    public void ANameThatLooksCloseButIsNotTheFactoryPattern_IsNeverSelected(string name)
    {
        Assert.False(ScratchDatabaseSweep.IsFactoryName(name), name);
        Assert.Empty(ScratchDatabaseSweep.SelectAbandoned(new[] { name }, Now, TimeSpan.Zero));
        Assert.Empty(ScratchDatabaseSweep.SelectAbandoned(new[] { name }, DateTime.MaxValue.AddDays(-1), TimeSpan.Zero));
    }

    [Fact]
    public void ANullName_IsNotAFactoryName()
    {
        Assert.False(ScratchDatabaseSweep.IsFactoryName(null));
    }

    [Fact]
    public void OnlyFactoryNamesOlderThanTheThreshold_AreSelected()
    {
        var threshold = ScratchDatabaseSweep.AbandonedAfter;
        string Stamped(TimeSpan age) => Name(Now.Subtract(age).ToString(ScratchDatabaseSweep.StampFormat, CultureInfo.InvariantCulture));

        var exactlyAtTheThreshold = Stamped(threshold);
        var justPast = Stamped(threshold + TimeSpan.FromSeconds(1));
        var daysOld = Stamped(TimeSpan.FromDays(9));
        var justInside = Stamped(threshold - TimeSpan.FromSeconds(1));
        var young = Stamped(TimeSpan.FromMinutes(2));
        var aheadOfTheClock = Name(Now.AddHours(3).ToString(ScratchDatabaseSweep.StampFormat, CultureInfo.InvariantCulture), "fedcba987654");

        var selected = ScratchDatabaseSweep.SelectAbandoned(
            new[] { young, justInside, aheadOfTheClock, exactlyAtTheThreshold, justPast, daysOld, "postgres", "darling_scratch_0123456789ab" },
            Now,
            threshold);

        Assert.Equal(
            new[] { exactlyAtTheThreshold, justPast, daysOld }.OrderBy(n => n, StringComparer.Ordinal),
            selected.OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public void ARepeatedName_IsSelectedOnce()
    {
        var selected = ScratchDatabaseSweep.SelectAbandoned(new[] { Name(), Name() }, Now, ScratchDatabaseSweep.AbandonedAfter);

        Assert.Equal(new[] { Name() }, selected);
    }

    [Fact]
    public void TheThreshold_IsLongerThanAnyTestRunsAScratchDatabase()
    {
        /* A test keeps its database for the length of the test. The longest live tests here run for minutes, so a
           margin under ten would put a slow neighbour's databases at risk. */
        Assert.True(ScratchDatabaseSweep.AbandonedAfter >= TimeSpan.FromMinutes(10));
    }
}
