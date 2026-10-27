/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6840: the web pages of the front door, from the shipped scripts run under Node (<c>web-front-door-harness.mjs</c>)
/// against a fake DOM and a recording fetch. Skipped when Node is not installed. The facts: without a front door every request
/// and link is what it was before (no header, no prefix); with one, every API helper and the two pages that call fetch
/// themselves name the picked store; the store rides in the address and comes back off it; a server name more than one store
/// has gets the front door's answer and a choice; and the fleet page draws a section per store, one failing store showing its
/// own error while the others draw.
/// </summary>
public sealed class FrontDoorWebPagesTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-front-door-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the front door web harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the front door web harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string?[] Stores(JsonElement r) =>
        r.GetProperty("calls").EnumerateArray()
            .Select(c => c.GetProperty("store").ValueKind == JsonValueKind.Null ? null : c.GetProperty("store").GetString()).ToArray();

    [Fact]
    public void WithoutAFrontDoor_NoRequestCarriesAStoreAndEveryRequestKeepsItsShape()
    {
        var r = Run("plain");
        Assert.False(r.GetProperty("frontDoor").GetBoolean());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("store").ValueKind);
        Assert.Equal("#/server/x", r.GetProperty("link").GetString());

        var calls = r.GetProperty("calls").EnumerateArray().ToArray();
        // apiGet, apiGetJoined, apiGetFleet, apiSend, apiWrite, apiSendRead, mute-rules send, known-patterns send
        Assert.Equal(8, calls.Length);
        foreach (var c in calls)
        {
            var names = c.GetProperty("headers").EnumerateObject().Select(p => p.Name).OrderBy(n => n).ToArray();
            Assert.DoesNotContain("X-Darling-Store", names);
            Assert.True(names.SequenceEqual(new[] { "Accept" }) || names.SequenceEqual(new[] { "Accept", "Content-Type" }), string.Join(",", names));
        }

        // reads send exactly {headers, signal}; writes exactly {body, headers, method}, as before
        foreach (var c in calls.Take(3))
            Assert.Equal(new[] { "headers", "signal" }, c.GetProperty("initKeys").EnumerateArray().Select(k => k.GetString()).ToArray());
        foreach (var c in calls.Skip(3))
            Assert.Equal(new[] { "body", "headers", "method" }, c.GetProperty("initKeys").EnumerateArray().Select(k => k.GetString()).ToArray());
    }

    [Fact]
    public void WithAFrontDoor_EveryHelperAndBothRawFetchPagesNameTheStore()
    {
        var r = Run("headers");
        var calls = r.GetProperty("calls").EnumerateArray().ToArray();
        Assert.Equal(
            new[] { "/api/a", "/api/read/b", "/api/fleet", "/api/c", "/api/d", "/api/e", "/api/mute-rules", "/api/known-patterns", "/api/fleet" },
            calls.Select(c => c.GetProperty("url").GetString()).ToArray());
        // the last read asked for another store's fleet by name, the rest use the picked one
        Assert.Equal(Enumerable.Repeat("use1", 8).Append("use2").ToArray(), Stores(r));
    }

    [Fact]
    public void WithAFrontDoorButNoStorePicked_NoHeaderIsSent()
    {
        Assert.All(Stores(Run("noStore")), s => Assert.Null(s));
    }

    [Fact]
    public void ASharedRead_IsSharedOnlyWithinOneStore()
    {
        // two callers in use1 share one request; the same path in use2 is its own request
        Assert.Equal(new[] { "use1", "use2" }, Stores(Run("joined")));
    }

    [Fact]
    public void TheStoreHashPrefix_IsAddedAndStrippedAndRoundTrips()
    {
        var r = Run("address");
        var split = r.GetProperty("split").EnumerateArray().Select(e => (
            Store: e.GetProperty("store").ValueKind == JsonValueKind.Null ? null : e.GetProperty("store").GetString(),
            Hash: e.GetProperty("hash").GetString())).ToArray();
        Assert.Equal(("use1", "#/server/x/queries"), split[0]);
        Assert.Equal(("use1", "#/fleet"), split[1]);          // a store alone opens the fleet
        Assert.Equal(("use1", "#/fleet"), split[2]);          // so does a store and a trailing slash
        Assert.Equal(("a b/c", "#/alerts"), split[3]);        // names are URL-encoded in the hash
        Assert.Null(split[4].Store);                          // no prefix: the hash as it came
        Assert.Equal("#/server/x", split[4].Hash);
        Assert.Null(split[5].Store);
        Assert.Equal("", split[5].Hash);
        Assert.Null(split[6].Store);                          // an empty name is no store
        Assert.Null(split[7].Store);                          // a name that cannot be decoded is no store

        Assert.Equal("#/s/use1/server/x", r.GetProperty("link").GetString());
        Assert.Equal("#/s/use9/server/x", r.GetProperty("linkOther").GetString());
        Assert.Equal("#/s/a%20b%2Fc/fleet", r.GetProperty("linkEncoded").GetString());
        Assert.Equal("server/x", r.GetProperty("notAHash").GetString());
        Assert.Equal("#/server/web%2F01/queries", r.GetProperty("routeHash").GetString());
        Assert.Equal("a b/c", r.GetProperty("roundTrip").GetProperty("store").GetString());
        Assert.Equal("#/finops/web%2F01/io", r.GetProperty("roundTrip").GetProperty("hash").GetString());
    }

    [Fact]
    public void ABareServerLink_AsksTheFrontDoor_AndEachAnswerHasItsOwnShape()
    {
        var r = Run("locate");
        Assert.Equal("/api/front-door/locate?server=found", r.GetProperty("url").GetString());

        var a = r.GetProperty("answers");
        Assert.Equal("found", a.GetProperty("found").GetProperty("kind").GetString());
        Assert.Equal("use1", a.GetProperty("found").GetProperty("store").GetString());

        // 409: the front door's sentence and the stores both survive (apiGet would have dropped the body)
        var amb = a.GetProperty("ambiguous");
        Assert.Equal("ambiguous", amb.GetProperty("kind").GetString());
        Assert.Equal("web02 is in use1 and use2.", amb.GetProperty("message").GetString());
        Assert.Equal(new[] { "use1", "use2" }, amb.GetProperty("stores").EnumerateArray().Select(s => s.GetString()).ToArray());

        Assert.Equal("notfound", a.GetProperty("missing").GetProperty("kind").GetString());
        Assert.Equal("No store has web03.", a.GetProperty("missing").GetProperty("message").GetString());
        Assert.Equal("error", a.GetProperty("broken").GetProperty("kind").GetString());
        Assert.Equal("error", a.GetProperty("network").GetProperty("kind").GetString());

        // the choice: one link per store, each opening the same route in that store
        Assert.Contains("web02 is in use1 and use2.", r.GetProperty("message").GetString());
        var links = r.GetProperty("links").EnumerateArray().Select(l => (l.GetProperty("text").GetString(), l.GetProperty("href").GetString())).ToArray();
        Assert.Equal(new[] { ("use1", "#/s/use1/server/web02/queries"), ("use2", "#/s/use2/server/web02/queries") }, links);

        // a store name that looks like markup is text in the strip and encoded in the link
        Assert.Contains("<img src=x>", r.GetProperty("evilText").GetString());
        Assert.Equal("#/s/%3Cimg%20src%3Dx%3E/server/web02", r.GetProperty("evilLinks")[0].GetProperty("href").GetString());
    }

    [Fact]
    public void TheStorePicker_StartsInTheAddressStore_ThenTheLastOne_ThenTheFirst_AndReloadsOnAChange()
    {
        var r = Run("init");
        Assert.Equal("use2", r.GetProperty("fromHash").GetString());      // the address wins over the remembered store
        Assert.Equal("use2", r.GetProperty("fromMemory").GetString());    // then the one this browser used last
        Assert.Equal("use1", r.GetProperty("first").GetString());         // then the first listed
        Assert.Equal("use1", r.GetProperty("unknownInHash").GetString()); // a name the front door does not list is not used
        Assert.True(r.GetProperty("frontDoor").GetBoolean());
        Assert.Equal(new[] { "use1", "use2 (stale)", "<b>use3</b> (unreachable)" }, r.GetProperty("options").EnumerateArray().Select(o => o.GetString()).ToArray());

        // choosing use2 while on use1's server page: same route in use2, remembered, and the page reloads
        Assert.Equal("#/s/use2/server/web01/queries", r.GetProperty("hashAfter").GetString());
        Assert.Equal(1, r.GetProperty("reloads").GetInt32());
        Assert.Equal("use2", r.GetProperty("remembered").GetString());
        Assert.Equal("use2", r.GetProperty("storeAfter").GetString());
    }

    [Fact]
    public void TheFleetPage_DrawsASectionPerStore_AndAFailingStoreShowsItsOwnError()
    {
        var r = Run("fleet");
        var sections = r.GetProperty("sections").EnumerateArray().ToArray();
        Assert.Equal(new[] { "use1", "use2", "<i>use3</i>" }, sections.Select(s => s.GetProperty("store").GetString()).ToArray());

        // each section's heading names the store and its health
        Assert.Contains("use1", sections[0].GetProperty("head").GetString());
        Assert.Contains("ok", sections[0].GetProperty("head").GetString());
        Assert.Contains("unreachable", sections[1].GetProperty("head").GetString());
        Assert.Contains("<i>use3</i>", sections[2].GetProperty("head").GetString());

        // use2's read failed: its section holds the error and no cards; the others drew their cards
        Assert.Equal(new[] { "use2 did not answer in time" }, sections[1].GetProperty("errors").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(0, sections[1].GetProperty("cards").GetInt32());
        Assert.Equal(new[] { "alpha", "bravo" }, sections[0].GetProperty("titles").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "charlie" }, sections[2].GetProperty("titles").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Empty(sections[0].GetProperty("errors").EnumerateArray());
        Assert.Empty(sections[2].GetProperty("errors").EnumerateArray());

        // one fleet read per store, each naming its own store
        Assert.Equal(new[] { "<i>use3</i>", "use1", "use2" }, r.GetProperty("fleetCalls").EnumerateArray().Select(e => e.GetString()).ToArray());

        // a card opens its server in ITS store
        Assert.Equal("#/s/use1/server/alpha", r.GetProperty("hashes").GetProperty("use1").GetString());
        Assert.Equal("#/s/%3Ci%3Euse3%3C%2Fi%3E/server/charlie", r.GetProperty("hashes").GetProperty("<i>use3</i>").GetString());
    }

    [Fact]
    public void TheSidebar_GroupsServersByStore_AndAFailingStoreShowsItsOwnLine()
    {
        var r = Run("sidebar");
        Assert.Equal(new[] { "use1", "use2unreachable", "<i>use3</i>stale" }, r.GetProperty("headers").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "use2 is not answering" }, r.GetProperty("errors").EnumerateArray().Select(e => e.GetString()).ToArray());

        // only the current store's server lights up as the open page
        var items = r.GetProperty("items").EnumerateArray().Select(i => (i.GetProperty("server").GetString(), i.GetProperty("active").GetBoolean())).ToArray();
        Assert.Equal(new[] { ("alpha", true), ("charlie", false) }, items);

        Assert.Equal("#/s/%3Ci%3Euse3%3C%2Fi%3E/server/charlie", r.GetProperty("otherHash").GetString());
        Assert.Equal("#/s/use1/server/alpha", r.GetProperty("ownHash").GetString());
        // the favourite star belongs to the store in use: one row has it, the other store's row does not
        Assert.Equal(1, r.GetProperty("stars").GetInt32());
    }
}
