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
/// The account menu (#8737): the shipped <c>account-menu.js</c>, with the real <c>util.js</c>, run under Node
/// (<c>account-menu-harness.mjs</c>) against a stand-in <c>/api/me</c>. Pins the line and the rows each sign-in method gets, that
/// Sign out is offered only for a session that ends, and that the signed-in party's own text is drawn as text, never as markup.
/// Skipped when Node is not installed.
/// </summary>
public sealed class AccountMenuWebTests
{
    private static JsonElement Run(string scenario, string json)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "account-menu-harness.mjs"));
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
                Assert.Fail("the account menu harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the account menu harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Theory]
    [InlineData("""{"method":"oidc","seat":"admin","subject":"erik@example.com","display_name":"Erik","granted_by":"dba-admins","expires_at":"2026-11-13T20:00:00Z"}""", "erik@example.com · admin")]
    [InlineData("""{"method":"shared_token","seat":"admin"}""", "shared token · admin")]
    [InlineData("""{"method":"shared_token","seat":"read_only","expires_at":"2026-11-13T20:00:00Z"}""", "shared token · read-only")]
    [InlineData("""{"method":"named_token","seat":"read_only","token_name":"grafana"}""", "token grafana · read-only")]
    [InlineData("""{"method":"managed_group","seat":"admin","group_name":"dbas","expires_at":"2026-11-13T20:00:00Z"}""", "group dbas · admin")]
    [InlineData("""{"method":"reads_only","seat":"read_only"}""", "read-only access")]
    public void AccountLine_PerMethod(string me, string expected)
    {
        Assert.Equal(expected, Run("line", me).GetProperty("line").GetString());
    }

    private static string[][] Rows(JsonElement r) =>
        r.GetProperty("details").EnumerateArray().Select(p => new[] { p[0].GetString()!, p[1].GetString()! }).ToArray();

    [Fact]
    public void Details_OidcWithoutMapping_SaysNoRoleMappingSet()
    {
        var r = Run("details", """{"method":"oidc","seat":"admin","subject":"erik@example.com","display_name":null,"granted_by":null,"role_mapping":false,"expires_at":null}""");

        var rows = Rows(r);
        Assert.Equal(new[] { "Signed in as", "Seat", "Signed in with" }, rows.Select(x => x[0]).ToArray());
        Assert.Equal("admin (no role mapping set)", rows.Single(x => x[0] == "Seat")[1]);
        Assert.Equal("OpenID Connect", rows.Single(x => x[0] == "Signed in with")[1]);
    }

    [Theory]
    [InlineData("""{"method":"oidc","seat":"read_only","subject":"s","granted_by":null,"role_mapping":true}""", "read-only")]
    [InlineData("""{"method":"oidc","seat":"admin","subject":"s","granted_by":null,"role_mapping":null}""", "admin")]
    [InlineData("""{"method":"oidc","seat":"read_only","subject":"s","granted_by":"readers","role_mapping":false}""", "read-only (granted by readers)")]
    public void Details_OidcSeatRow_SaysNoRoleMappingOnlyWhenTheHostHasNone(string me, string expected)
    {
        var rows = Rows(Run("details", me));

        Assert.Equal(expected, rows.Single(x => x[0] == "Seat")[1]);
    }

    [Fact]
    public void Details_OidcWithMapping_NamesTheGroup_AndTheSessionEndInLocalTime()
    {
        var r = Run("details", """{"method":"oidc","seat":"read_only","subject":"a@b.c","display_name":"Ann","granted_by":"readers","expires_at":"2026-11-13T20:00:00Z"}""");

        var rows = Rows(r);
        Assert.Equal(new[] { "Name", "Signed in as", "Seat", "Signed in with", "Session ends" }, rows.Select(x => x[0]).ToArray());
        Assert.Equal("read-only (granted by readers)", rows.Single(x => x[0] == "Seat")[1]);
        Assert.Equal(r.GetProperty("localExpiry").GetString(), rows.Single(x => x[0] == "Session ends")[1]);
    }

    [Theory]
    [InlineData("""{"method":"shared_token","seat":"admin"}""", "shared token")]
    [InlineData("""{"method":"named_token","seat":"read_only","token_name":"t"}""", "web token")]
    [InlineData("""{"method":"managed_group","seat":"admin","group_name":"g"}""", "access group")]
    public void Details_TokenAndGroupSessions_NameTheirMethod(string me, string word)
    {
        var rows = Rows(Run("details", me));

        Assert.Equal(word, rows.Single(x => x[0] == "Signed in with")[1]);
    }

    [Fact]
    public void Details_ReadsOnly_HasNoSignedInWithRow()
    {
        var rows = Rows(Run("details", """{"method":"reads_only","seat":"read_only"}"""));

        Assert.DoesNotContain(rows, x => x[0] == "Signed in with");
        Assert.Equal("read-only", rows.Single(x => x[0] == "Seat")[1]);
    }

    [Fact]
    public void Details_NoExpiry_HasNoSessionEndsRow_AndNoSignOut()
    {
        var me = """{"method":"shared_token","seat":"admin","expires_at":null}""";

        Assert.DoesNotContain(Rows(Run("details", me)), x => x[0] == "Session ends");
        var drawn = Run("init", me);
        Assert.Equal(0, drawn.GetProperty("menuItems").GetArrayLength());
    }

    [Fact]
    public void WithAnExpiry_SignOutIsAPlainLinkToTheLogout()
    {
        var drawn = Run("init", """{"method":"oidc","seat":"admin","subject":"s","expires_at":"2026-11-13T20:00:00Z"}""");

        var item = Assert.Single(drawn.GetProperty("menuItems").EnumerateArray());
        Assert.Equal("a", item.GetProperty("tag").GetString());
        Assert.Equal("/auth/logout", item.GetProperty("href").GetString());
        Assert.Equal("Sign out", item.GetProperty("text").GetString());
    }

    [Fact]
    public void ThePanel_IsADisclosure_NotAMenu()
    {
        var drawn = Run("init", """{"method":"oidc","seat":"admin","subject":"s","expires_at":"2026-11-13T20:00:00Z"}""");

        Assert.Equal(0, drawn.GetProperty("rolesUsed").GetInt32());
        Assert.False(drawn.GetProperty("hasPopupAttr").GetBoolean());
        Assert.False(string.IsNullOrEmpty(drawn.GetProperty("panelId").GetString()));
        Assert.Equal(drawn.GetProperty("panelId").GetString(), drawn.GetProperty("controls").GetString());
    }

    [Fact]
    public void Panel_EscapeReturnsFocusToTheButton_TabOutClosesAndLeavesFocus_AClickOutsideCloses_AClickInsideDoesNot()
    {
        var r = Run("interact", """{"method":"oidc","seat":"admin","subject":"s","expires_at":"2026-11-13T20:00:00Z"}""");

        Assert.Equal(1, r.GetProperty("openedPanels").GetInt32());
        Assert.Equal(1, r.GetProperty("panelsAfterClickInside").GetInt32());
        Assert.Equal(0, r.GetProperty("panelsAfterClickOutside").GetInt32());
        Assert.Equal("false", r.GetProperty("expandedAfterClickOutside").GetString());
        Assert.Equal(0, r.GetProperty("outsideListenersLeft").GetInt32());
        Assert.Equal(0, r.GetProperty("panelsAfterEscape").GetInt32());
        Assert.True(r.GetProperty("focusAfterEscape").GetBoolean());
        Assert.Equal(0, r.GetProperty("keydownListenersLeft").GetInt32());
        Assert.Equal(0, r.GetProperty("panelsAfterEscapeWithoutFocus").GetInt32());
        Assert.Equal(0, r.GetProperty("panelsAfterTab").GetInt32());
        Assert.True(r.GetProperty("focusAfterTab").GetBoolean());
        Assert.Equal(1, r.GetProperty("panelsAfterFocusMovesInside").GetInt32());
        Assert.Equal(1, r.GetProperty("panelsAfterFocusLost").GetInt32());
        var ids = r.GetProperty("topAndBrandIds").EnumerateArray().Select(x => x.GetString()).ToArray();
        Assert.NotEqual(ids[0], ids[1]);
    }

    [Theory]
    [InlineData("""{"method":"named_token","seat":"read_only","token_name":""}""", "web token · read-only")]
    [InlineData("""{"method":"named_token","seat":"read_only","token_name":"t1"}""", "token t1 · read-only")]
    [InlineData("""{"method":"named_token","seat":"read_only"}""", "web token · read-only")]
    [InlineData("""{"method":"managed_group","seat":"admin"}""", "access group · admin")]
    public void AccountLine_AMissingName_IsNeverUndefined(string me, string expected)
    {
        Assert.Equal(expected, Run("line", me).GetProperty("line").GetString());
    }

    [Fact]
    public void TheControl_IsShownInBothHosts_OpensAsADisclosure_AndEscapeCloses()
    {
        var drawn = Run("init", """{"method":"shared_token","seat":"admin"}""");

        Assert.False(drawn.GetProperty("brandHidden").GetBoolean());
        Assert.False(drawn.GetProperty("topbarHidden").GetBoolean());
        Assert.Equal(1, drawn.GetProperty("topbarHasControl").GetInt32());
        Assert.Equal("shared token · admin", drawn.GetProperty("buttonText").GetString());
        var caret = drawn.GetProperty("caret").EnumerateArray().Single();
        Assert.Equal("true", caret.GetProperty("hidden").GetString());
        Assert.Contains("▾", caret.GetProperty("text").GetString());
        Assert.Equal("false", drawn.GetProperty("buttonAttrs").GetProperty("aria-expanded").GetString());
        Assert.Equal("true", drawn.GetProperty("expandedAfterOpen").GetString());
        Assert.Equal("false", drawn.GetProperty("expandedAfterEscape").GetString());
        Assert.Equal(0, drawn.GetProperty("popupAfterEscape").GetInt32());
        Assert.Equal(1, drawn.GetProperty("fetched").GetArrayLength());
    }

    [Theory]
    [InlineData("401")]
    [InlineData("503")]
    public void WhenMeIsNotAnswered_BothHostsStayHidden(string status)
    {
        var drawn = Run("init", status);

        Assert.True(drawn.GetProperty("brandHidden").GetBoolean());
        Assert.True(drawn.GetProperty("topbarHidden").GetBoolean());
    }

    [Fact]
    public void Markup_InTheSessionsOwnText_IsDrawnAsText()
    {
        const string evil = "<img src=x onerror=alert(1)>";
        var me = JsonSerializer.Serialize(new { method = "oidc", seat = "admin", subject = evil + "s", display_name = evil + "d", granted_by = evil + "g", expires_at = (string?)null });

        var drawn = Run("init", me);

        var texts = drawn.GetProperty("texts").EnumerateArray().Select(t => t.GetString()).ToArray();
        Assert.Equal(0, drawn.GetProperty("innerHtmlSets").GetArrayLength());
        var rows = drawn.GetProperty("popupRows").EnumerateArray().ToDictionary(row => row[0].GetString()!, row => row[1].GetString());
        Assert.Equal(evil + "d", rows["Name"]);
        Assert.Equal(evil + "s", rows["Signed in as"]);
        Assert.Equal("admin (granted by " + evil + "g)", rows["Seat"]);
        Assert.Contains(evil + "d", texts);
        Assert.Contains(evil + "s", texts);
        Assert.Contains("admin (granted by " + evil + "g)", texts);
    }
}
