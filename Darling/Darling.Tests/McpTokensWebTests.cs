/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The My MCP tokens page (#8737): the shipped <c>pages/mcp-tokens.js</c>, with the real <c>util.js</c>, run under Node
/// (<c>mcp-tokens-harness.mjs</c>) against stand-in <c>/api/mcp-tokens</c> routes. Pins what the page draws for each seat, that a token
/// value shows once and not after a reload, that the revoke request carries the JSON content type the server demands, that a refusal
/// shows the server's message, and that every value the signed-in party and the store supply is drawn as text. Skipped when Node is not installed.
/// </summary>
public sealed class McpTokensWebTests
{
    private const string Times = "\"times\":[\"2026-11-01T10:00:00Z\",\"2026-11-10T08:00:00Z\",\"2026-12-01T10:00:00Z\"]";
    private const string Fill = "\"fill\":{\"name\":\"ci\",\"lifetime_days\":\"30\",\"role\":\"reader\"}";
    private const string Evil = "<img src=x onerror=alert(1)>";
    private const string OidcAdmin = """{"method":"oidc","seat":"admin","subject":"a@b.c","personal_tokens":true}""";
    private const string OidcReader = """{"method":"oidc","seat":"read_only","subject":"a@b.c","personal_tokens":true}""";

    private static JsonElement Run(string scenario, string json, string harness = "mcp-tokens-harness.mjs")
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", harness));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        psi.ArgumentList.Add(json);

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
                Assert.Fail("the MCP tokens harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the MCP tokens harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string Row(string id, string name, string state, string role = "reader", string? revokedBy = null, string? reason = null, string? subject = null, string? lastUsed = null) =>
        JsonSerializer.Serialize(new
        {
            id, name, role,
            created_at = "2026-11-01T10:00:00Z", expires_at = "2026-12-01T10:00:00Z", last_used_at = lastUsed,
            state, revoked_by = revokedBy, revoke_reason = reason, subject,
        });

    private static string Listing(string rows, string roles = """["reader"]""", int max = 90, bool truncated = false) =>
        "{\"status\":200,\"body\":{\"tokens\":[" + rows + "],\"max_lifetime_days\":" + max + ",\"allowed_roles\":" + roles
        + ",\"truncated\":" + (truncated ? "true" : "false") + "}}";

    private static string Config(string me, string? own = null, string? all = null, string? create = null, string? revoke = null, string? extra = null) =>
        "{" + string.Join(",", new[]
        {
            "\"me\":" + me,
            own is null ? null : "\"own\":" + own,
            all is null ? null : "\"all\":" + all,
            create is null ? null : "\"create\":" + create,
            revoke is null ? null : "\"revoke\":" + revoke,
            extra,
        }.Where(p => p is not null)) + "}";

    private static string[] Cells(JsonElement row) => row.GetProperty("cells").EnumerateArray().Select(c => c.GetString()!).ToArray();

    [Fact]
    public void RendersOwnTokens_StatesAndReasons()
    {
        var rows = string.Join(",",
            Row("a", "laptop", "live", lastUsed: "2026-11-10T08:00:00Z"),
            Row("b", "old", "expired"),
            Row("c", "gone", "revoked", revokedBy: "admin:boss@x.y", reason: "revoked_by_admin"));
        var r = Run("render", Config(OidcReader, own: Listing(rows), extra: Times));

        var first = r.GetProperty("first");
        var table = Assert.Single(first.GetProperty("tables").EnumerateArray());
        Assert.Equal(new[] { "Name", "Role", "Created", "Expires", "Last used", "State", "" },
            table.GetProperty("head").EnumerateArray().Select(h => h.GetString()).ToArray());
        var tableRows = table.GetProperty("rows").EnumerateArray().ToArray();
        var local = r.GetProperty("local");
        Assert.Equal(new[]
        {
            "laptop", "reader", local.GetProperty("2026-11-01T10:00:00Z").GetString(), local.GetProperty("2026-12-01T10:00:00Z").GetString(),
            local.GetProperty("2026-11-10T08:00:00Z").GetString(), "live", "Revoke",
        }, Cells(tableRows[0]));
        Assert.Equal("never", Cells(tableRows[1])[4]);
        Assert.Equal("expired", Cells(tableRows[1])[5]);
        Assert.Equal("revoked by an administrator (admin:boss@x.y)", Cells(tableRows[2])[5]);
        Assert.Equal(new[] { 1, 0, 0 }, tableRows.Select(t => t.GetProperty("revoke").GetInt32()).ToArray());
        Assert.Equal(new[] { "My tokens" }, first.GetProperty("h3s").EnumerateArray().Select(h => h.GetString()).ToArray());
    }

    /// <summary>M2: each reason code reads as a sentence; a code the page does not know is shown as it is, with who.</summary>
    [Fact]
    public void RevokeReasons_ReadAsSentences()
    {
        var rows = string.Join(",",
            Row("a", "a", "revoked", revokedBy: "person:me@x.y", reason: "revoked_by_owner"),
            Row("b", "b", "revoked", revokedBy: "admin:boss@x.y", reason: "revoked_by_admin"),
            Row("c", "c", "revoked", revokedBy: "sign-in", reason: "sign_in_refused"),
            Row("d", "d", "revoked", revokedBy: "sign-in", reason: "seat_lowered_to_read_only"),
            Row("e", "e", "revoked", revokedBy: "config", reason: "operator_tokens_disabled"),
            Row("f", "f", "revoked", revokedBy: "someone", reason: "future_code"));
        var r = Run("render", Config(OidcReader, own: Listing(rows), extra: Times));

        var states = Assert.Single(r.GetProperty("first").GetProperty("tables").EnumerateArray()).GetProperty("rows").EnumerateArray()
            .Select(t => Cells(t)[5]).ToArray();
        Assert.Equal(new[]
        {
            "revoked by its owner",
            "revoked by an administrator (admin:boss@x.y)",
            "revoked at sign-in: the sign-in was refused",
            "revoked at sign-in: the seat is now read-only",
            "revoked: operator tokens are turned off",
            "revoked: future_code by someone",
        }, states);
    }

    [Fact]
    public void MarkupInNameSubjectReason_DrawnAsText()
    {
        var own = Listing(Row("a", Evil + "n", "revoked", revokedBy: Evil + "by", reason: Evil + "why"));
        var all = Listing(Row("a", Evil + "n", "revoked", revokedBy: Evil + "by", reason: Evil + "why", subject: Evil + "s"));
        var r = Run("render", Config(OidcAdmin, own: own, all: all));

        Assert.Equal(0, r.GetProperty("innerHtmlSets").GetArrayLength());
        var tables = r.GetProperty("first").GetProperty("tables").EnumerateArray().ToArray();
        Assert.Equal(2, tables.Length);
        var mine = Cells(tables[0].GetProperty("rows")[0]);
        Assert.Equal(Evil + "n", mine[0]);
        Assert.Equal("revoked: " + Evil + "why by " + Evil + "by", mine[5]);
        var everyone = Cells(tables[1].GetProperty("rows")[0]);
        Assert.Equal(Evil + "n", everyone[0]);
        Assert.Equal(Evil + "s", everyone[1]);
    }

    [Fact]
    public void Create_ShowsValueOnce_ThenGoneAfterReload()
    {
        const string token = "dmcp_live_secret_value";
        var created = "{\"status\":201,\"body\":{\"token\":\"" + token + "\",\"record\":" + Row("n1", "ci", "live") + "}}";
        var r = Run("create", Config(OidcReader, own: Listing(""), create: created,
            extra: Fill));

        Assert.Empty(r.GetProperty("first").GetProperty("valuePanel").EnumerateArray());
        var request = Assert.Single(r.GetProperty("createRequests").EnumerateArray(), q => q.GetProperty("method").GetString() == "POST");
        Assert.Equal("/api/mcp-tokens", request.GetProperty("url").GetString());
        Assert.Equal("application/json", request.GetProperty("contentType").GetString());
        Assert.Equal("ci", request.GetProperty("body").GetProperty("name").GetString());
        Assert.Equal(30, request.GetProperty("body").GetProperty("lifetime_days").GetInt32());
        Assert.Equal("reader", request.GetProperty("body").GetProperty("role").GetString());

        var panel = Assert.Single(r.GetProperty("afterCreate").GetProperty("valuePanel").EnumerateArray());
        Assert.Equal(token, panel.GetProperty("value").GetString());
        Assert.Equal("This is the only time this token is shown. Copy it now; it cannot be shown again.", panel.GetProperty("note").GetString());
        Assert.Equal(token, r.GetProperty("copied").GetString());
        Assert.Equal("Copied", r.GetProperty("copyText").GetString());
        Assert.Equal("", r.GetProperty("nameAfter").GetString());
        Assert.Empty(r.GetProperty("afterReload").GetProperty("valuePanel").EnumerateArray());
        Assert.DoesNotContain(token, r.GetProperty("afterReload").ToString());
    }

    [Theory]
    [InlineData("91")]
    [InlineData("0")]
    [InlineData("30.5")]
    [InlineData("")]
    public void Create_LifetimeNotAWholeNumberUpToTheMax_SendsNothing(string lifetime)
    {
        var fill = "\"fill\":{\"name\":\"ci\",\"lifetime_days\":\"" + lifetime + "\",\"role\":\"reader\"}";
        var r = Run("create", Config(OidcReader, own: Listing(""), create: "{\"status\":201,\"body\":{}}", extra: fill));

        Assert.DoesNotContain(r.GetProperty("createRequests").EnumerateArray(), q => q.GetProperty("method").GetString() == "POST");
        Assert.Contains("The lifetime must be a whole number of days from 1 to 90.", r.GetProperty("afterCreate").GetProperty("strips").EnumerateArray().Select(s => s.GetString()));
    }

    [Fact]
    public void Create_ValueIsGoneOnceTheAddressChanges()
    {
        var created = "{\"status\":201,\"body\":{\"token\":\"dmcp_x\",\"record\":" + Row("n1", "ci", "live") + "}}";
        var r = Run("leave", Config(OidcReader, own: Listing(""), create: created, extra: Fill));

        Assert.Single(r.GetProperty("afterCreate").GetProperty("valuePanel").EnumerateArray());
        Assert.Empty(r.GetProperty("afterLeave").GetProperty("valuePanel").EnumerateArray());
        Assert.Equal(0, r.GetProperty("copyButtonsAfterLeave").GetInt32());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Revoke_AsksFirst_AndSendsOnlyWhenAccepted(bool accept)
    {
        var r = Run("revoke", Config(OidcReader, own: Listing(Row("abc123", "laptop", "live")), revoke: "{\"status\":204}", extra: "\"confirm\":" + (accept ? "true" : "false")));

        var confirm = Assert.Single(r.GetProperty("confirms").EnumerateArray()).GetString()!;
        Assert.Contains("\"laptop\"", confirm);
        Assert.Equal(accept, r.GetProperty("revokeRequests").EnumerateArray().Any(q => q.GetProperty("method").GetString() == "POST"));
    }

    [Fact]
    public void Revoke_InEveryone_NamesThePerson()
    {
        var r = Run("revoke", Config("{\"method\":\"shared_token\",\"seat\":\"admin\",\"personal_tokens\":true}",
            all: Listing(Row("z1", "theirs", "live", subject: "z@b.c")), revoke: "{\"status\":204}"));

        var confirm = Assert.Single(r.GetProperty("confirms").EnumerateArray()).GetString()!;
        Assert.Contains("\"theirs\"", confirm);
        Assert.Contains("z@b.c", confirm);
        Assert.Contains(r.GetProperty("revokeRequests").EnumerateArray(), q => q.GetProperty("url").GetString() == "/api/mcp-tokens/z1/revoke");
    }

    [Fact]
    public void RolePicker_LimitedToAllowedRoles_AndLifetimeToTheMax()
    {
        var reader = Run("render", Config(OidcReader, own: Listing("", """["reader"]""", 45)));
        var admin = Run("render", Config(OidcAdmin, own: Listing("", """["operator","reader"]""", 45), all: Listing("")));

        Assert.Equal(new[] { "reader" }, reader.GetProperty("first").GetProperty("roleOptions").EnumerateArray().Select(o => o.GetString()).ToArray());
        Assert.Equal(new[] { "operator", "reader" }, admin.GetProperty("first").GetProperty("roleOptions").EnumerateArray().Select(o => o.GetString()).ToArray());
        Assert.Equal("45", reader.GetProperty("first").GetProperty("maxAttr")[0].GetString());
    }

    [Fact]
    public void Revoke_CallsRouteAndRefreshes()
    {
        var r = Run("revoke", Config(OidcReader, own: Listing(Row("abc123", "laptop", "live")), revoke: """{"status":204}"""));

        var requests = r.GetProperty("revokeRequests").EnumerateArray().ToArray();
        var post = requests[0];
        Assert.Equal("POST", post.GetProperty("method").GetString());
        Assert.Equal("/api/mcp-tokens/abc123/revoke", post.GetProperty("url").GetString());
        Assert.Equal("application/json", post.GetProperty("contentType").GetString());
        Assert.False(post.GetProperty("hasBody").GetBoolean());
        Assert.Contains(requests, q => q.GetProperty("method").GetString() == "GET" && q.GetProperty("url").GetString() == "/api/mcp-tokens");
    }

    [Fact]
    public void AdminSeesEveryoneSection()
    {
        var r = Run("render", Config(OidcAdmin, own: Listing(Row("a", "mine", "live")), all: Listing(Row("a", "mine", "live", subject: "a@b.c") + "," + Row("z", "theirs", "live", subject: "z@b.c"))));

        var first = r.GetProperty("first");
        Assert.Equal(new[] { "My tokens", "Everyone" }, first.GetProperty("h3s").EnumerateArray().Select(h => h.GetString()).ToArray());
        var tables = first.GetProperty("tables").EnumerateArray().ToArray();
        Assert.Equal(new[] { "Name", "Subject", "Role", "Created", "Expires", "Last used", "State", "" },
            tables[1].GetProperty("head").EnumerateArray().Select(h => h.GetString()).ToArray());
        Assert.Equal(new[] { "a@b.c", "z@b.c" }, tables[1].GetProperty("rows").EnumerateArray().Select(t => Cells(t)[1]).ToArray());
        var urls = r.GetProperty("requestsOnLoad").EnumerateArray().Select(q => q.GetProperty("method").GetString() + " " + q.GetProperty("url").GetString()).ToArray();
        Assert.Equal(new[] { "GET /api/mcp-tokens", "GET /api/mcp-tokens?all=1" }, urls);
    }

    /// <summary>I2: a cut list says so above the table, and that dead rows are left out before live ones; a whole one says nothing.</summary>
    [Fact]
    public void EveryoneList_Truncated_SaysDeadRowsAreLeftOutFirst()
    {
        var cut = Run("render", Config(OidcAdmin, own: Listing(Row("a", "mine", "live")), all: Listing(Row("z", "theirs", "live", subject: "z@b.c"), truncated: true)));
        var whole = Run("render", Config(OidcAdmin, own: Listing(Row("a", "mine", "live")), all: Listing(Row("z", "theirs", "live", subject: "z@b.c"))));

        Assert.Contains(cut.GetProperty("first").GetProperty("strips").EnumerateArray(), s => s.GetString()!.Contains("left out first", StringComparison.Ordinal));
        Assert.Equal(2, cut.GetProperty("first").GetProperty("tables").GetArrayLength());
        Assert.DoesNotContain(whole.GetProperty("first").GetProperty("strips").EnumerateArray(), s => s.GetString()!.Contains("left out", StringComparison.Ordinal));
    }

    [Fact]
    public void ReadOnlySeat_HasNoEveryoneSection_AndAsksForNoOtherPeoplesRows()
    {
        var r = Run("render", Config(OidcReader, own: Listing(Row("a", "mine", "live"))));

        Assert.DoesNotContain("Everyone", r.GetProperty("first").GetProperty("h3s").EnumerateArray().Select(h => h.GetString()));
        Assert.DoesNotContain(r.GetProperty("requestsOnLoad").EnumerateArray(), q => q.GetProperty("url").GetString()!.Contains("all=1", StringComparison.Ordinal));
    }

    [Fact]
    public void NonOidcAdmin_OnlyEveryoneSection()
    {
        var me = """{"method":"shared_token","seat":"admin","personal_tokens":true}""";
        var r = Run("render", Config(me, all: Listing(Row("z", "theirs", "live", subject: "z@b.c"))));

        var first = r.GetProperty("first");
        Assert.Equal(new[] { "Everyone" }, first.GetProperty("h3s").EnumerateArray().Select(h => h.GetString()).ToArray());
        Assert.Equal(0, first.GetProperty("hasForm").GetInt32());
        Assert.Single(first.GetProperty("tables").EnumerateArray());
        var urls = r.GetProperty("requestsOnLoad").EnumerateArray().Select(q => q.GetProperty("url").GetString()).ToArray();
        Assert.Equal(new[] { "/api/mcp-tokens?all=1" }, urls);
    }

    [Theory]
    [InlineData("""{"method":"oidc","seat":"admin","personal_tokens":false}""")]
    [InlineData("""{"method":"oidc","seat":"admin"}""")]
    [InlineData("null")]
    public void WhenTheHostServesNoPersonalTokens_NothingIsRead(string me)
    {
        var r = Run("render", Config(me));

        Assert.Equal(0, r.GetProperty("requestsOnLoad").GetArrayLength());
        Assert.Single(r.GetProperty("first").GetProperty("strips").EnumerateArray());
    }

    [Theory]
    [InlineData("at_cap", 409, "You already have 5 live tokens. Revoke one first.")]
    [InlineData("name_taken", 409, "You already have a live token with that name.")]
    [InlineData("too_many_recent", 429, "You made too many tokens in the last 24 hours. Try again later.")]
    [InlineData("role_not_allowed", 400, "This sign-in may not make a token with that role.")]
    public void RefusalCodes_ShownAsMessages(string code, int status, string message)
    {
        var refusal = JsonSerializer.Serialize(new { status, body = new { error = code, message } });
        var r = Run("create", Config(OidcReader, own: Listing(""), create: refusal,
            extra: Fill));

        var strips = r.GetProperty("afterCreate").GetProperty("strips").EnumerateArray().Select(s => s.GetString()).ToArray();
        Assert.Contains(message, strips);
        Assert.Empty(r.GetProperty("afterCreate").GetProperty("valuePanel").EnumerateArray());
    }

    [Theory]
    [InlineData("""{"method":"oidc","seat":"admin","subject":"s","personal_tokens":true,"expires_at":"2026-11-13T20:00:00Z"}""", true)]
    [InlineData("""{"method":"oidc","seat":"admin","subject":"s","personal_tokens":false,"expires_at":"2026-11-13T20:00:00Z"}""", false)]
    [InlineData("""{"method":"oidc","seat":"admin","subject":"s","expires_at":"2026-11-13T20:00:00Z"}""", false)]
    public void AccountMenu_LinkOnlyWhenPersonalTokens(string me, bool linked)
    {
        var items = Run("init", me, "account-menu-harness.mjs").GetProperty("menuItems").EnumerateArray().ToArray();

        var link = items.SingleOrDefault(i => i.GetProperty("text").GetString() == "My MCP tokens");
        Assert.Equal(linked, link.ValueKind == JsonValueKind.Object);
        if (linked)
        {
            Assert.Equal("#/mcp-tokens", link.GetProperty("href").GetString());
        }

        Assert.Contains(items, i => i.GetProperty("href").GetString() == "/auth/logout");
    }
}
