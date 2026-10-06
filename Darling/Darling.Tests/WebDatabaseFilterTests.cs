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
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5245 (part of #5244): the C# foundation of the web database filter. The picker sends one
/// <c>database_name</c> key per chosen database, so the web route has to read a repeated key as a list
/// (<see cref="DarlingWebEndpoints.DatabaseNames"/>, over <see cref="DatabaseFilter"/>'s one blank-name rule), and
/// every single-value read has to read a repeated key as its FIRST value.
///
/// <para>Database names are chosen by anyone who can create a database on a monitored server, so the awkward ones
/// (a comma, a bracket, a leading space, a quote, a percent sign, markup) are the cases that matter here.</para>
/// </summary>
public sealed class WebDatabaseFilterTests
{
    /// <summary>The six names the filter's tests use for "anything a database can be called": each one has to
    /// survive as ONE name, exactly as typed.</summary>
    public static readonly string[] AwkwardNames =
    [
        "A,B",
        "x]",
        " SalesDb",
        "O'Brien",
        "50%+off",
        "<img src=x onerror=alert(1)>",
    ];

    /// <summary>The expected names, as an array (an <c>Assert.Equal</c> expectation, not a collection expression).</summary>
    private static string[] L(params string[] names) => names;

    private static DefaultHttpContext Ask(string query)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        return context;
    }

    /// <summary>What the page's <c>buildQuery</c> puts on the wire for one value, written independently of the
    /// product's byte table: .NET's RFC 3986 escape, with the five characters <c>encodeURIComponent</c> also leaves
    /// alone (<c>! ' ( ) *</c>) put back.</summary>
    private static string EncodeUriComponent(string value) =>
        Uri.EscapeDataString(value)
            .Replace("%21", "!", StringComparison.Ordinal)
            .Replace("%27", "'", StringComparison.Ordinal)
            .Replace("%28", "(", StringComparison.Ordinal)
            .Replace("%29", ")", StringComparison.Ordinal)
            .Replace("%2A", "*", StringComparison.Ordinal);

    /// <summary>The query the page builds for a list: one <c>database_name</c> key per name.</summary>
    private static string QueryOf(IEnumerable<string> names) =>
        "?" + string.Join("&", names.Select(name => "database_name=" + EncodeUriComponent(name)));

    private static IReadOnlyList<string> NamesOf(string query)
    {
        var databases = DarlingWebEndpoints.DatabaseNames(Ask(query));
        Assert.NotNull(databases);
        return databases.Value.Names;
    }

    /// <summary>A refused request: <c>DatabaseNames</c> answers null (never "all databases"), and the dispatch
    /// entry's refusal is the existing <c>invalid</c> envelope for <c>database_name</c>.</summary>
    private static async Task<string> RefusalOf(string query)
    {
        var context = Ask(query);
        Assert.Null(DarlingWebEndpoints.DatabaseNames(context));
        var envelope = await DarlingWebEndpoints.DatabaseNamesRefusal(context);
        Assert.True(McpHelpers.IsRefusalEnvelope(envelope), envelope);
        using var doc = JsonDocument.Parse(envelope);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("database_name", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        return doc.RootElement.GetProperty("message").GetString()!;
    }

    // ── First(): the first non-empty value, never the repeated values joined with a comma ──

    /// <summary>The four query helpers #5245 fixes, by the name the theory rows use. Each one is the first
    /// non-empty value for a key, and each is its own method, so each is read here.</summary>
    private static string? Read(string helper, HttpContext context, string key) => helper switch
    {
        "read-surface" => DarlingWebEndpoints.First(context, key),
        "fleet-sweep" => DarlingFleetSweepEndpoints.Query(context, key),
        "alert-notebook" => AlertNotebookEndpoint.Query(context, key),
        "triage" => DarlingTriageEndpoint.Query(context, key),
        _ => throw new ArgumentOutOfRangeException(nameof(helper), helper, "Unknown query helper."),
    };

    /// <summary>
    /// A repeated key used to come back as <c>A,B</c> (ASP.NET joins repeated values when a
    /// <c>StringValues</c> is turned into a string), which no database, server or collector is called. The
    /// helper's own comment says "the first non-empty value", and this makes that true for all four copies.
    /// </summary>
    [Theory]
    [InlineData("read-surface")]
    [InlineData("fleet-sweep")]
    [InlineData("alert-notebook")]
    [InlineData("triage")]
    public void First_OnARepeatedKey_ReturnsTheFirstValue(string helper) =>
        Assert.Equal("A", Read(helper, Ask("?database_name=A&database_name=B"), "database_name"));

    /// <summary>The server key is the one that matters most: <c>Server()</c> resolves a name against the
    /// registry, and <c>A,B</c> would resolve to nothing.</summary>
    [Theory]
    [InlineData("read-surface")]
    [InlineData("fleet-sweep")]
    [InlineData("alert-notebook")]
    [InlineData("triage")]
    public void First_OnARepeatedServerKey_ReturnsTheFirstValue(string helper) =>
        Assert.Equal("A", Read(helper, Ask("?server=A&server=B&server=C"), "server"));

    [Theory]
    [InlineData("read-surface")]
    [InlineData("fleet-sweep")]
    [InlineData("alert-notebook")]
    [InlineData("triage")]
    public void First_SkipsAnEmptyValue_AndReturnsTheFirstNonEmptyOne(string helper) =>
        Assert.Equal("B", Read(helper, Ask("?x=&x=B&x=C"), "x"));

    [Theory]
    [InlineData("read-surface")]
    [InlineData("fleet-sweep")]
    [InlineData("alert-notebook")]
    [InlineData("triage")]
    public void First_OnOneValueAnEmptyValueOrNoKey_IsUnchanged(string helper)
    {
        Assert.Equal("A", Read(helper, Ask("?x=A"), "x"));
        Assert.Equal("a,b", Read(helper, Ask("?x=a%2Cb"), "x"));
        Assert.Null(Read(helper, Ask("?x="), "x"));
        Assert.Null(Read(helper, Ask("?x=&x="), "x"));
        Assert.Null(Read(helper, Ask("?y=A"), "x"));
        Assert.Null(Read(helper, Ask(""), "x"));
    }

    // ── DatabaseFilter: the one blank-name rule ──

    [Fact]
    public void DatabaseFilter_ABlankName_IsAll()
    {
        foreach (var blank in new[] { null, "", " ", "   ", "\t", " \t\r\n " })
        {
            var one = DatabaseFilter.One(blank);
            Assert.True(one.IsAll, $"One({blank ?? "null"})");
            Assert.Empty(one.Names);
            Assert.Null(one.Describe());
        }

        Assert.True(DatabaseFilter.All.IsAll);
        Assert.True(default(DatabaseFilter).IsAll);
        Assert.True(DatabaseFilter.Of(null).IsAll);
        Assert.True(DatabaseFilter.Of([]).IsAll);
        Assert.True(DatabaseFilter.Of([null, "", "  ", "\t"]).IsAll);
        Assert.Equal(DatabaseFilter.All, DatabaseFilter.Of([" ", ""]));
    }

    [Fact]
    public void DatabaseFilter_AKeptName_IsKeptExactlyAsGiven()
    {
        Assert.Equal(L(" SalesDb"), DatabaseFilter.One(" SalesDb").Names);
        Assert.Equal(L("SalesDb "), DatabaseFilter.One("SalesDb ").Names);
        Assert.Equal(L("salesdb", "SalesDb"), DatabaseFilter.Of(["salesdb", "SalesDb"]).Names);
        Assert.Equal(AwkwardNames, DatabaseFilter.Of(AwkwardNames).Names);
    }

    [Fact]
    public void DatabaseFilter_Of_DropsBlanksAndRepeats_AndKeepsTheOrderFirstSeen()
    {
        var filter = DatabaseFilter.Of(["B", " ", "A", "B", null, "a", "A"]);

        Assert.False(filter.IsAll);
        Assert.Equal(L("B", "A", "a"), filter.Names);
    }

    [Fact]
    public void DatabaseFilter_Describe_IsNullForAll_TheNameForOne_AndTheChosenDatabasesForMore()
    {
        Assert.Null(DatabaseFilter.All.Describe());
        Assert.Equal("Sales", DatabaseFilter.One("Sales").Describe());
        Assert.Equal(" SalesDb", DatabaseFilter.One(" SalesDb").Describe());
        Assert.Equal("the chosen databases", DatabaseFilter.Of(["A", "B"]).Describe());
        Assert.Equal("A", DatabaseFilter.Of(["A", "A"]).Describe());
    }

    [Fact]
    public void DatabaseFilter_Parameter_IsOneTextArray_NullWhenAll_AndAFreshCopyEveryTime()
    {
        var all = DatabaseFilter.All.Parameter();
        Assert.Equal(NpgsqlDbType.Array | NpgsqlDbType.Text, all.NpgsqlDbType);
        Assert.Equal(DBNull.Value, all.Value);

        var filter = DatabaseFilter.Of(["A,B", "C"]);
        var first = filter.Parameter();
        Assert.Equal(NpgsqlDbType.Array | NpgsqlDbType.Text, first.NpgsqlDbType);
        Assert.Equal(L("A,B", "C"), Assert.IsType<string[]>(first.Value));
        Assert.NotSame(first, filter.Parameter());

        // A caller that edits the array it was handed does not change the filter.
        ((string[])first.Value!)[0] = "changed";
        Assert.Equal(L("A,B", "C"), filter.Names);
        Assert.Equal(L("A,B", "C"), Assert.IsType<string[]>(filter.Parameter().Value));
    }

    [Fact]
    public void DatabaseFilter_IsAValue_EqualWhenItNamesTheSameSet()
    {
        Assert.Equal(DatabaseFilter.Of(["A", "B"]), DatabaseFilter.Of(["B", "A"]));
        Assert.Equal(DatabaseFilter.Of(["A", "B"]).GetHashCode(), DatabaseFilter.Of(["B", "A", "A"]).GetHashCode());
        Assert.NotEqual(DatabaseFilter.Of(["A", "B"]), DatabaseFilter.Of(["A"]));
        Assert.NotEqual(DatabaseFilter.Of(["A"]), DatabaseFilter.Of(["a"]));
        Assert.NotEqual(DatabaseFilter.All, DatabaseFilter.One("A"));
        Assert.Equal(DatabaseFilter.All, default);
    }

    // ── the clause and the parameter are the desktop's, byte for byte ──

    /// <summary>The SQL arm every reader splices is the desktop viewer's <c>DatabaseFilterClause</c> to the byte,
    /// whatever the selection (the predicate never depends on it), so the two surfaces run one statement shape.</summary>
    [Theory]
    [InlineData("database_name", 1)]
    [InlineData("database_name", 4)]
    [InlineData("d.database_name", 12)]
    [InlineData("s.\"database_name\"", 2)]
    public void Clause_IsByteEqualToTheDesktopsDatabaseFilterClause(string column, int index)
    {
        var desktop = ViewerDataService.DatabaseFilterClause(column, index);

        Assert.Equal(desktop, DatabaseFilter.All.Clause(column, index));
        Assert.Equal(desktop, DatabaseFilter.One("A").Clause(column, index));
        Assert.Equal(desktop, DatabaseFilter.Of(["A", "B"]).Clause(column, index));
        Assert.Equal($" AND (${index}::text[] IS NULL OR {column} = ANY(${index}))", desktop);
    }

    [Fact]
    public void Parameter_BindsLikeTheDesktopsDatabaseFilterParameter()
    {
        foreach (var selection in new IReadOnlyList<string>?[] { null, [], ["A"], ["A", "B"], AwkwardNames })
        {
            var desktop = ViewerDataService.DatabaseFilterParameter(selection);
            var web = (selection is { Count: > 0 } ? DatabaseFilter.Of(selection) : DatabaseFilter.All).Parameter();

            Assert.Equal(desktop.NpgsqlDbType, web.NpgsqlDbType);
            if (desktop.Value is string[] desktopNames)
            {
                Assert.Equal(desktopNames, Assert.IsType<string[]>(web.Value));
            }
            else
            {
                Assert.Same(DBNull.Value, desktop.Value);
                Assert.Same(DBNull.Value, web.Value);
            }
        }
    }

    // ── the web route: DatabaseNames(c) ──

    [Fact]
    public void DatabaseNames_NoKey_IsAll_NotARefusal()
    {
        foreach (var query in new[] { "", "?", "?server=A", "?databases=A" })
        {
            var databases = DarlingWebEndpoints.DatabaseNames(Ask(query));

            Assert.NotNull(databases);
            Assert.True(databases.Value.IsAll, query);
        }
    }

    [Fact]
    public void DatabaseNames_ReadsOneNamePerKey_InTheOrderSent()
    {
        Assert.Equal(L("A"), NamesOf("?database_name=A"));
        Assert.Equal(L("B", "A", "C"), NamesOf("?database_name=B&database_name=A&database_name=C"));
        Assert.Equal(L("A"), NamesOf("?server=S&database_name=A&hours=4"));
    }

    [Fact]
    public void DatabaseNames_DropsAWhitespaceOnlyName_AndKeepsTheOthers()
    {
        Assert.Equal(L("Sales"), NamesOf("?database_name=%20%20&database_name=Sales"));
        Assert.Equal(L("Sales"), NamesOf("?database_name=&database_name=Sales&database_name=%09"));
        Assert.Equal(L("A", "B"), NamesOf("?database_name=A&database_name=%0D%0A&database_name=B"));
    }

    [Fact]
    public void DatabaseNames_KeepsANameWithALeadingSpaceExactlyAsTyped()
    {
        Assert.Equal(L(" SalesDb"), NamesOf("?database_name=%20SalesDb"));
        Assert.Equal(L("SalesDb "), NamesOf("?database_name=SalesDb%20"));
        Assert.Equal(L(" Sales Db "), NamesOf("?database_name=%20Sales%20Db%20"));
        Assert.Equal(L("salesdb", "SalesDb", " SalesDb"), NamesOf("?database_name=salesdb&database_name=SalesDb&database_name=%20SalesDb"));
    }

    [Fact]
    public void DatabaseNames_DeDuplicates_OrdinalAndKeepingTheOrder()
    {
        Assert.Equal(L("B", "A", "a"), NamesOf("?database_name=B&database_name=A&database_name=B&database_name=a&database_name=A"));
    }

    [Theory]
    [InlineData("?database_name=")]
    [InlineData("?database_name")]
    [InlineData("?database_name=%20")]
    [InlineData("?database_name=%20&database_name=%09&database_name=")]
    [InlineData("?server=S&database_name=&hours=4")]
    public async Task DatabaseNames_KeysSentButNoneSurvive_IsRefused_NeverAll(string query)
    {
        var message = await RefusalOf(query);

        Assert.Contains("every value was empty or only spaces", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DatabaseNamesRefusal_ForAnAcceptableRequest_IsABug_SoItThrows()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await DarlingWebEndpoints.DatabaseNamesRefusal(Ask("?database_name=A")));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await DarlingWebEndpoints.DatabaseNamesRefusal(Ask("")));
    }

    /// <summary>A value that holds a comma is ONE database. A list is the repeated key, never a comma list, because
    /// a database may be called <c>A,B</c>.</summary>
    [Fact]
    public void DatabaseNames_NeverSplitsAValueOnAComma()
    {
        Assert.Equal(L("A,B"), NamesOf("?database_name=A,B"));
        Assert.Equal(L("A,B"), NamesOf("?database_name=A%2CB"));
        Assert.Equal(L("A,B", "C"), NamesOf("?database_name=A,B&database_name=C"));
        Assert.Equal(L(",", " , "), NamesOf("?database_name=%2C&database_name=%20%2C%20"));
    }

    [Theory]
    [InlineData("A,B")]
    [InlineData("x]")]
    [InlineData(" SalesDb")]
    [InlineData("O'Brien")]
    [InlineData("50%+off")]
    [InlineData("<img src=x onerror=alert(1)>")]
    public void DatabaseNames_TheAwkwardNames_EachRoundTripAsOneName(string name)
    {
        Assert.Contains(name, AwkwardNames);
        Assert.Equal(L(name), NamesOf(QueryOf([name])));

        // The same name as the fixed encoder of a browser or a client library that escapes more than the browser does.
        Assert.Equal(L(name), NamesOf("?database_name=" + Uri.EscapeDataString(name)));

        // And as a one-element list, the binding the reader gets: one text[] value, not a split one.
        var filter = DarlingWebEndpoints.DatabaseNames(Ask(QueryOf([name])))!.Value;
        Assert.Equal(L(name), Assert.IsType<string[]>(filter.Parameter().Value));
    }

    [Fact]
    public void DatabaseNames_TheSixAwkwardNamesTogether_AreSixNamesInOrder()
    {
        Assert.Equal(6, AwkwardNames.Length);
        Assert.Equal(AwkwardNames, NamesOf(QueryOf(AwkwardNames)));
    }

    // ── the caps are counted after de-duplication ──

    [Fact]
    public void DatabaseNames_FiftyDistinctNames_AreAccepted()
    {
        var fifty = Enumerable.Range(1, 50).Select(i => "db" + i.ToString("D2")).ToArray();

        Assert.Equal(fifty, NamesOf(QueryOf(fifty)));
        Assert.Equal(50, DarlingWebEndpoints.MaxDatabaseNames);
    }

    [Fact]
    public async Task DatabaseNames_FiftyOneDistinctNames_AreRefused()
    {
        var names = Enumerable.Range(1, 51).Select(i => "db" + i.ToString("D2"));

        var message = await RefusalOf(QueryOf(names));

        Assert.Contains("51 different databases", message, StringComparison.Ordinal);
        Assert.Contains("at most 50", message, StringComparison.Ordinal);
    }

    /// <summary>The cap counts DISTINCT names: three names sent forty times each are three names, not a hundred and
    /// twenty, and the same name sent a hundred times with a long spelling is one name's worth of bytes.</summary>
    [Fact]
    public void DatabaseNames_TheCapAndTheByteBudget_AreCountedAfterDeDuplication()
    {
        var threeNamesRepeated = Enumerable.Range(0, 120).Select(i => new[] { "A", "B", "C" }[i % 3]);
        Assert.Equal(L("A", "B", "C"), NamesOf(QueryOf(threeNamesRepeated)));

        var longName = new string('x', 100);
        var oneLongNameRepeated = Enumerable.Repeat(longName, 100);
        Assert.Equal(L(longName), NamesOf(QueryOf(oneLongNameRepeated)));

        // 51 values over 50 distinct names are still 50 names.
        var fiftyDistinctPlusARepeat = Enumerable.Range(1, 50).Select(i => "db" + i.ToString("D2")).Append("db01");
        Assert.Equal(50, NamesOf(QueryOf(fiftyDistinctPlusARepeat)).Count);
    }

    [Fact]
    public async Task DatabaseNames_FiftyLongNonAsciiNames_AreRefusedForGoingOverTheByteBudget()
    {
        // 50 names is at the cap and 100 characters is under the name limit, so only the byte budget refuses this:
        // each name is 2 digits and 98 accented letters, which the browser sends as 2 + 98 x 6 = 590 characters.
        var names = Enumerable.Range(1, 50).Select(i => i.ToString("D2") + new string('é', 98)).ToArray();
        Assert.Equal(50, names.Distinct().Count());
        Assert.All(names, name => Assert.Equal(100, name.Length));

        var message = await RefusalOf(QueryOf(names));

        Assert.Contains("encoded bytes", message, StringComparison.Ordinal);
        Assert.DoesNotContain("different databases", message, StringComparison.Ordinal);
    }

    /// <summary>Each name costs the 15 bytes of <c>&amp;database_name=</c> plus its encoded length, and 4,096 is
    /// the most. Thirty-five names of 100 letters cost 35 x 115 = 4,025; a 36th that costs 71 (56 letters) lands on
    /// exactly 4,096 and is accepted, and one letter more is refused.</summary>
    [Fact]
    public async Task DatabaseNames_TheByteBudget_IsInclusiveAt4096()
    {
        var names = Enumerable.Range(1, 35).Select(i => "n" + i.ToString("D2") + new string('x', 97)).ToList();
        Assert.All(names, name => Assert.Equal(100, name.Length));

        var atTheLimit = names.Append("z" + new string('y', 55)).ToList();
        Assert.Equal(4096, atTheLimit.Sum(name => 15 + EncodeUriComponent(name).Length));
        Assert.Equal(atTheLimit, NamesOf(QueryOf(atTheLimit)));

        var overTheLimit = names.Append("z" + new string('y', 56)).ToList();
        Assert.Equal(4097, overTheLimit.Sum(name => 15 + EncodeUriComponent(name).Length));
        var message = await RefusalOf(QueryOf(overTheLimit));
        Assert.Contains("4097 encoded bytes", message, StringComparison.Ordinal);
        Assert.Contains("at most 4096", message, StringComparison.Ordinal);
    }

    /// <summary>The product's byte count is the length <c>encodeURIComponent</c> really gives, for ASCII, the
    /// characters it leaves alone, accented letters, and a character outside the BMP (a surrogate pair).</summary>
    [Fact]
    public void EncodedQueryLength_IsTheLengthEncodeUriComponentGives()
    {
        var corpus = new List<string>(AwkwardNames)
        {
            "plain", "A-Z_a.z~0!9*'()", "with space", "a/b?c#d&e=f;g:h@i$j", "café", "中文", "😀", "tab\there",
            new string('x', 128), string.Concat(Enumerable.Range(32, 95).Select(c => (char)c)),
        };

        foreach (var value in corpus)
        {
            Assert.Equal(EncodeUriComponent(value).Length, DarlingWebEndpoints.EncodedQueryLength(value));
        }

        Assert.Equal(15, DarlingWebEndpoints.DatabaseKeyBytes);
        Assert.Equal(DarlingWebEndpoints.DatabaseKeyBytes, "&database_name=".Length);
        Assert.Equal(4096, DarlingWebEndpoints.MaxDatabaseQueryBytes);
    }

    [Fact]
    public async Task DatabaseNames_ANameOver128Characters_IsRefused_AndOneOf128IsKept()
    {
        var atTheLimit = new string('d', 128);
        Assert.Equal(L(atTheLimit), NamesOf(QueryOf([atTheLimit])));
        Assert.Equal(128, DarlingWebEndpoints.MaxDatabaseNameLength);

        var message = await RefusalOf(QueryOf(["ok", new string('d', 129)]));
        Assert.Contains("129 characters", message, StringComparison.Ordinal);
        Assert.Contains("at most 128", message, StringComparison.Ordinal);
    }

    /// <summary>A refusal never echoes a name back: a name is chosen by whoever can create a database, and the
    /// sentence only counts and says what is accepted.</summary>
    [Fact]
    public async Task TheRefusals_NeverEchoAName()
    {
        const string Marker = "ZZ-MARKER-ZZ";
        var tooLong = Marker + new string('d', 120);
        var tooMany = Enumerable.Range(1, 51).Select(i => Marker + i.ToString("D2"));
        var tooManyBytes = Enumerable.Range(1, 50).Select(i => Marker + i.ToString("D2") + new string('é', 80));

        foreach (var names in new[] { new[] { "ok", tooLong }, tooMany.ToArray(), tooManyBytes.ToArray() })
        {
            var message = await RefusalOf(QueryOf(names));

            Assert.DoesNotContain("MARKER", message, StringComparison.Ordinal);
        }
    }

    // ── the catalog row ──

    /// <summary>The picker's catalog helper is the same row as the single-name text param, so the served catalog keeps
    /// its shape (text, optional, no default).</summary>
    [Fact]
    public void PDatabases_IsTheSameCatalogParamAsTheSingleNameTextParam()
    {
        var existing = DarlingWebEndpoints.CatalogDescriptors["get_active_queries"].Params.Single(p => p.Name == "database_name");

        Assert.Equal(existing, DarlingWebEndpoints.PDatabases());
        Assert.Equal(new DarlingWebEndpoints.CatalogParam("database_name", "text", false, null), DarlingWebEndpoints.PDatabases());
    }
}
