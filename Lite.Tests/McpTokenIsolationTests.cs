/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The MCP token is its own Credential Manager entry, no server's saved password is used for a server whose Id is not a GUID,
/// the token copies stay out of Clipboard History and the cloud clipboard, and Import Settings never brings in the settings that
/// loosen the MCP server. All Credential Manager use is under a prefix of the test's own.
/// </summary>
[Collection("credential-manager")]
public sealed class McpTokenIsolationTests : IDisposable
{
    private const string Marker = "saved-password-marker-7f3a";

    /* The two production prefixes, each followed by a tag of this test's own: the server-password prefix and the token's differ at
       the same character as in production, and neither extends the other. */
    private static readonly string Tag = "Test_" + Guid.NewGuid().ToString("N") + "_";
    private readonly string _prefix = CredentialService.CredentialPrefix + Tag;
    private readonly string _tokenPrefix = CredentialManagerMcpTokenVault.CredentialPrefix + Tag;
    private readonly string _folder = Path.Combine(Path.GetTempPath(), "lite-mcp-isolation-" + Guid.NewGuid().ToString("N"));

    public McpTokenIsolationTests() => Directory.CreateDirectory(_folder);

    public void Dispose()
    {
        var credentials = new CredentialService(new CredentialStore(_prefix, "Performance Monitor Lite test"));
        credentials.DeleteCredential("McpToken");
        credentials.DeleteCredential("Mcp_McpToken");

        try { Directory.Delete(_folder, recursive: true); } catch { /* best effort */ }
    }

    private CredentialService TestCredentials() => new(new CredentialStore(_prefix, "Performance Monitor Lite test"));

    private static ServerConnection SqlServer(string id) => new()
    {
        Id = id,
        ServerName = "sql-test-host",
        DisplayName = "Test " + id,
        AuthenticationType = AuthenticationTypes.SqlServer,
    };

    // ---- the token has an entry of its own ----

    [Fact]
    public void TheToken_LivesApartFromTheServerPasswordEntries_AndNewTokenReplacesIt()
    {
        var tokens = McpTokenStore.ForPrefix(_tokenPrefix);
        var credentials = TestCredentials();

        var first = tokens.GetOrCreate(out var failure, out var created);
        Assert.Null(failure);
        Assert.True(created);

        /* A server entry named McpToken, in the server-password prefix, is another entry: it neither reads nor replaces the token. */
        Assert.False(_tokenPrefix.StartsWith(_prefix, StringComparison.OrdinalIgnoreCase));
        Assert.False(_prefix.StartsWith(_tokenPrefix, StringComparison.OrdinalIgnoreCase));
        Assert.True(credentials.SaveCredential("McpToken", "McpToken", Marker));
        Assert.True(credentials.SaveCredential("Mcp_McpToken", "McpToken", Marker));
        Assert.Equal(first, tokens.Read(out _));

        var replacement = tokens.Replace(out failure);
        Assert.Null(failure);
        Assert.NotNull(replacement);
        Assert.NotEqual(first, replacement);
        Assert.Equal(replacement, tokens.Read(out _));
        Assert.Equal(Marker, credentials.GetCredential("McpToken")!.Value.Password);

        TestCredentialsOf(_tokenPrefix).DeleteCredential("McpToken");
    }

    private static CredentialService TestCredentialsOf(string prefix) => new(new CredentialStore(prefix, "Performance Monitor Lite test"));

    // ---- a saved password is used only for a server whose Id is a GUID ----

    [Fact]
    public void AServerWhoseIdIsNotAGuid_GetsNoSavedPassword_WhateverIsStoredUnderThatName()
    {
        var credentials = TestCredentials();
        Assert.True(credentials.SaveCredential("McpToken", "McpToken", Marker));
        var guidId = Guid.NewGuid().ToString();
        Assert.True(credentials.SaveCredential(guidId, "sa", Marker));

        var named = ServerConnection.ResolveConnectionString(SqlServer("McpToken"), credentials, profileLookup: null);
        var other = ServerConnection.ResolveConnectionString(SqlServer("some-other-name"), credentials, profileLookup: null);
        var guid = ServerConnection.ResolveConnectionString(SqlServer(guidId), credentials, profileLookup: null);

        Assert.DoesNotContain(Marker, named, StringComparison.Ordinal);
        Assert.DoesNotContain(Marker, other, StringComparison.Ordinal);
        Assert.Contains(Marker, guid, StringComparison.Ordinal);
        Assert.False(ServerConnection.HasStoredCredentials(SqlServer("McpToken"), credentials, profileLookup: null));
        Assert.True(ServerConnection.HasStoredCredentials(SqlServer(guidId), credentials, profileLookup: null));

        credentials.DeleteCredential(guidId);
    }

    [Fact]
    public void AddingAServerWhoseIdIsNotAGuid_KeepsTheServer_AndSavesNoPassword()
    {
        var id = "not-a-guid-" + _folder.GetHashCode();
        var manager = new ServerManager(_folder);
        var server = SqlServer(id);

        try
        {
            manager.AddServer(server, "sa", Marker);

            Assert.Contains(manager.GetAllServers(), s => s.Id == id);
            Assert.False(new CredentialService().CredentialExists(id));
        }
        finally
        {
            /* Only matters if the guard failed: the entry would be an unused one under this test's own name. */
            new CredentialService().DeleteCredential(id);
        }
    }

    // ---- the clipboard ----

    [Fact]
    public void TheTokenCopy_CarriesTheThreeOptOutFormats_AndTheText()
    {
        var data = SecretClipboard.Build("the-token-text");

        Assert.Equal("the-token-text", data.GetText());
        Assert.True(data.GetDataPresent(SecretClipboard.ExcludeFromMonitorFormat));
        Assert.Equal(0, DwordOf(data, SecretClipboard.HistoryFormat));
        Assert.Equal(0, DwordOf(data, SecretClipboard.CloudFormat));
    }

    private static int DwordOf(System.Windows.DataObject data, string format)
    {
        using var stream = Assert.IsAssignableFrom<MemoryStream>(data.GetData(format));
        var bytes = stream.ToArray();
        Assert.Equal(4, bytes.Length);
        return BitConverter.ToInt32(bytes, 0);
    }

    // ---- Import Settings ----

    [Fact]
    public void ImportSettings_NeverBringsInTheSettingsThatLoosenTheMcpServer_AndKeepsTheRest()
    {
        var oldConfig = Path.Combine(_folder, "old", "config");
        var newConfig = Path.Combine(_folder, "new", "config");
        Directory.CreateDirectory(oldConfig);
        Directory.CreateDirectory(newConfig);
        File.WriteAllText(Path.Combine(oldConfig, "settings.json"),
            """{ "mcp_enabled": true, "mcp_port": 6123, "mcp_allow_tokenless_loopback": true, "mcp_allow_writes": true, "mcp_allow_investigations": true }""");

        SettingsImport.CopyMissing(oldConfig, Path.Combine(_folder, "old"), newConfig, Path.Combine(_folder, "new"));

        var copied = JsonNode.Parse(File.ReadAllText(Path.Combine(newConfig, "settings.json")))!.AsObject();
        Assert.False(copied.ContainsKey("mcp_allow_tokenless_loopback"));
        Assert.False(copied.ContainsKey("mcp_allow_writes"));
        Assert.False(copied.ContainsKey("mcp_allow_investigations"));
        Assert.Equal(6123, (int)copied["mcp_port"]!);

        var loaded = McpSettings.Load(newConfig);
        Assert.True(loaded.Enabled);
        Assert.Equal(6123, loaded.Port);
        Assert.False(loaded.AllowTokenlessLoopback);
        Assert.False(loaded.AllowWrites);
        Assert.False(loaded.AllowInvestigations);
    }

    [Fact]
    public void ImportSettings_KeepsTheCurrentUsersValue_WhenTheyAlreadyHaveASettingsFile()
    {
        var oldConfig = Path.Combine(_folder, "old2", "config");
        var newConfig = Path.Combine(_folder, "new2", "config");
        Directory.CreateDirectory(oldConfig);
        Directory.CreateDirectory(newConfig);
        File.WriteAllText(Path.Combine(oldConfig, "settings.json"), """{ "mcp_allow_tokenless_loopback": true }""");
        File.WriteAllText(Path.Combine(newConfig, "settings.json"), """{ "mcp_allow_tokenless_loopback": false, "mcp_port": 7000 }""");

        SettingsImport.CopyMissing(oldConfig, Path.Combine(_folder, "old2"), newConfig, Path.Combine(_folder, "new2"));

        Assert.False(McpSettings.Load(newConfig).AllowTokenlessLoopback);
        Assert.Equal(7000, McpSettings.Load(newConfig).Port);
    }

    [Theory]
    [InlineData("utf8bom")]
    [InlineData("utf16")]
    public void ImportSettings_DropsTheOptOut_FromAFileWithAByteOrderMarkOrUtf16(string encoding)
    {
        var oldConfig = Path.Combine(_folder, "old3" + encoding, "config");
        var newConfig = Path.Combine(_folder, "new3" + encoding, "config");
        Directory.CreateDirectory(oldConfig);
        Directory.CreateDirectory(newConfig);
        System.Text.Encoding fileEncoding = encoding == "utf16" ? System.Text.Encoding.Unicode : new System.Text.UTF8Encoding(true);
        File.WriteAllText(Path.Combine(oldConfig, "settings.json"),
            """{ "mcp_enabled": true, "mcp_port": 6124, "mcp_allow_tokenless_loopback": true }""", fileEncoding);

        SettingsImport.CopyMissing(oldConfig, Path.Combine(_folder, "old3" + encoding), newConfig, Path.Combine(_folder, "new3" + encoding));

        var copied = JsonNode.Parse(File.ReadAllText(Path.Combine(newConfig, "settings.json")))!.AsObject();
        Assert.False(copied.ContainsKey("mcp_allow_tokenless_loopback"));
        Assert.Equal(6124, (int)copied["mcp_port"]!);
        Assert.False(McpSettings.Load(newConfig).AllowTokenlessLoopback);
    }

    [Fact]
    public void ImportSettings_SkipsASettingsFileThatIsNotAJsonObject()
    {
        var oldConfig = Path.Combine(_folder, "old4", "config");
        var newConfig = Path.Combine(_folder, "new4", "config");
        Directory.CreateDirectory(oldConfig);
        Directory.CreateDirectory(newConfig);
        File.WriteAllText(Path.Combine(oldConfig, "settings.json"), "{ \"mcp_allow_tokenless_loopback\": true,, }");
        File.WriteAllText(Path.Combine(oldConfig, "collection_schedule.json"), "[]");

        var copied = SettingsImport.CopyMissing(oldConfig, Path.Combine(_folder, "old4"), newConfig, Path.Combine(_folder, "new4"));

        Assert.False(File.Exists(Path.Combine(newConfig, "settings.json")));
        Assert.True(File.Exists(Path.Combine(newConfig, "collection_schedule.json")));
        Assert.Equal(1, copied);
    }

    // ---- the serving record never prints the token ----

    [Fact]
    public void McpServing_ToString_DoesNotPrintTheToken()
    {
        var serving = McpHostService.DecideServing("a-token-value-that-must-stay-out-of-logs", false, true, false);

        var text = serving.ToString();

        Assert.DoesNotContain("a-token-value-that-must-stay-out-of-logs", text);
        Assert.Contains("<set>", text);
        Assert.Contains("<none>", McpHostService.DecideServing(null, true, false, false).ToString());
    }
}
