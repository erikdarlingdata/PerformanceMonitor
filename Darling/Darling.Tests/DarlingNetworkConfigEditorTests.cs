/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using Xunit;
using PerformanceMonitor.Darling.Service.Hosting;
using Editor = PerformanceMonitor.Darling.Service.DarlingNetworkConfigEditor;
using Host = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpHostService;
using WebHost = PerformanceMonitor.Darling.Service.Mcp.DarlingWebHostService;

namespace Darling.Tests;

/// <summary>
/// The comment-preserving TEXT SURGERY (<see cref="Editor"/>) that the <c>--configure-network</c> wizard
/// (#1561) uses, exercised against the REAL <c>darling.sample.json</c> as a fixture (loaded like
/// <see cref="DarlingConfigTests"/> / <see cref="DarlingMcpToolsTests"/>). These are PURE + ungated: no
/// I/O, no DPAPI. The load-bearing proof is <see cref="Classify_ScannerIgnoresTheCommentedTemplate"/> —
/// the sample ships both network blocks COMMENTED OUT, and the scanner must be blind to them or a naive
/// scan would match the template and corrupt the file. Every splice is verified end-to-end: the edited
/// text still PARSES, the surviving commented template is intact, and the REAL resolvers
/// (<see cref="DarlingManagedPostgres.ResolveNetworkExposure"/> / <see cref="Host.ResolveMcpBind"/>)
/// accept or reject it exactly as the running service would.
/// </summary>
public sealed class DarlingNetworkConfigEditorTests
{
    /* Space-free cert/key paths so the store resolver's whitespace-degrade never fires here (that path is
       pinned separately in DarlingNetworkTests). */
    private const string CertPath = @"C:\ProgramData\PerformanceMonitorDarling\server.crt";
    private const string KeyPath = @"C:\ProgramData\PerformanceMonitorDarling\server.key";

    private static string LoadSample() =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "darling.sample.json"));

    /* ============================ the core correctness proof ============================ */

    [Fact]
    public void Classify_ScannerIgnoresTheCommentedTemplate()
    {
        /* The sample's ONLY "network" keys are the commented-out templates in postgres and mcp. A
           code-region-aware scan must therefore find NO live network key in either parent — this is the
           whole reason a real classifier is used instead of IndexOf. */
        var sample = LoadSample();
        var regions = Editor.Classify(sample);
        var rootOpen = Editor.FindRootObjectOpen(sample, regions);
        var rootClose = Editor.FindMatchingClose(sample, regions, rootOpen);

        var postgres = Editor.FindObjectValueSpan(sample, regions, rootOpen, rootClose, "postgres");
        Assert.NotNull(postgres);
        Assert.Null(Editor.FindObjectValueSpan(sample, regions, postgres!.Value.ValueStart, postgres.Value.ValueEnd - 1, "network"));

        var mcp = Editor.FindObjectValueSpan(sample, regions, rootOpen, rootClose, "mcp");
        Assert.NotNull(mcp);
        Assert.Null(Editor.FindObjectValueSpan(sample, regions, mcp!.Value.ValueStart, mcp.Value.ValueEnd - 1, "network"));
    }

    [Fact]
    public void Classify_LabelsStringsAndBothCommentStyles()
    {
        /* A brace inside a string and a quote inside a comment must NOT be read as structural. */
        var json = "{ \"a\": \"has } brace\", /* } */ \"b\": 1 // \" quote\n }";
        var regions = Editor.Classify(json);

        var braceInString = json.IndexOf("has }", StringComparison.Ordinal) + 4; // the '}' inside the string
        Assert.Equal(Editor.RegionKind.StringLiteral, regions[braceInString]);

        var braceInBlockComment = json.IndexOf("/* }", StringComparison.Ordinal) + 3; // the '}' inside /* */
        Assert.Equal(Editor.RegionKind.BlockComment, regions[braceInBlockComment]);

        var quoteInLineComment = json.IndexOf("// \"", StringComparison.Ordinal) + 3; // the '"' inside //
        Assert.Equal(Editor.RegionKind.LineComment, regions[quoteInLineComment]);
    }

    /* ============================ store insert ============================ */

    [Fact]
    public void UpsertStore_IntoSample_TemplateSurvives_ParsesAndResolverExposed_AfterConnectAs()
    {
        var sample = LoadSample();
        var block = Editor.BuildStoreNetworkBlock("192.168.1.205", "192.168.1.0/24", "viewer");
        var edited = Editor.UpsertNetworkBlock(sample, "postgres", block, Editor.StoreNetworkOwnedKeys);

        /* Still parses. */
        var config = DarlingConfig.Parse(edited);

        /* The commented template survives verbatim (it is documentation). */
        Assert.Contains("// \"network\": {", edited, StringComparison.Ordinal);

        /* The REAL resolver now reports the store exposed with the canonical values. */
        var decision = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath);
        Assert.True(decision.Exposed);
        Assert.Equal("192.168.1.205", decision.ListenIp);
        Assert.Equal("192.168.1.0/24", decision.Cidr);
        Assert.Equal("viewer", decision.Roles?[0]);

        /* The live block lands AFTER connectAs (the deliberate insert anchor). */
        var regions = Editor.Classify(edited);
        var rootOpen = Editor.FindRootObjectOpen(edited, regions);
        var rootClose = Editor.FindMatchingClose(edited, regions, rootOpen);
        var pg = Editor.FindObjectValueSpan(edited, regions, rootOpen, rootClose, "postgres")!.Value;
        var liveNetwork = Editor.FindObjectValueSpan(edited, regions, pg.ValueStart, pg.ValueEnd - 1, "network");
        var connectAs = Editor.FindObjectValueSpan(edited, regions, pg.ValueStart, pg.ValueEnd - 1, "connectAs");
        Assert.NotNull(liveNetwork);
        Assert.NotNull(connectAs);
        Assert.True(liveNetwork!.Value.KeyStart > connectAs!.Value.KeyStart);
    }

    [Fact]
    public void UpsertStore_MinimalConfig_StringValuedLastMemberNoTrailingComma_PutsTheCommaAfterTheValue()
    {
        /* #2073 (gotqn's repro): a minimal darling.json whose postgres section ENDS with a string-valued
           member and no trailing comma. A string value is entirely StringLiteral region — quotes
           included — so the code-only backward walk from the closing brace skipped the whole value,
           landed on the member's COLON, and spliced the synthesized separator there:
           "dataDirectory":, "E:\\Foo\\pg" — invalid JSON. The wizard's write-time parse gate caught the
           damage and refused to write, so the shipped symptom was "--configure-network cannot configure
           this file", never a corrupted config. The shipped sample cannot trip it: every section's last
           member there carries a trailing comma, which is Code and is found first. */
        var json = "{\n"
            + "  \"postgres\": {\n"
            + "    \"managed\": true,\n"
            + "    \"dataDirectory\": \"E:\\\\Foo\\\\pg\"\n"
            + "  },\n"
            + "  \"servers\": []\n"
            + "}\n";

        var block = Editor.BuildStoreNetworkBlock("192.168.1.205", "192.168.1.0/24", "viewer");
        var edited = Editor.UpsertNetworkBlock(json, "postgres", block, Editor.StoreNetworkOwnedKeys);

        /* Parses — the exact gate the wizard enforces before writing. */
        var config = DarlingConfig.Parse(edited);

        /* The separator rides AFTER the string value, and the colon splice shape never appears. */
        Assert.Contains("\"dataDirectory\": \"E:\\\\Foo\\\\pg\",", edited, StringComparison.Ordinal);
        Assert.DoesNotContain(":,", edited, StringComparison.Ordinal);

        /* And the block is live, not just syntactically present. */
        var decision = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath);
        Assert.True(decision.Exposed);
        Assert.Equal("192.168.1.205", decision.ListenIp);
    }

    /* ============================ mcp insert ============================ */

    [Fact]
    public void UpsertMcp_IntoSample_TemplateSurvives_ParsesAndResolverExposed()
    {
        var sample = LoadSample();
        /* A non-empty encryptedToken satisfies the resolver's PRESENCE check (no DPAPI decryption here). */
        var block = Editor.BuildMcpNetworkBlock("192.168.1.205", "192.168.1.0/24", encryptedToken: "DPAPI-BLOB", plaintextToken: null);
        var edited = Editor.UpsertNetworkBlock(sample, "mcp", block, Editor.McpNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.Contains("// \"network\": {", edited, StringComparison.Ordinal);

        var decision = Host.ResolveMcpBind(config.Mcp, managed: true);
        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, decision.Mode);
        Assert.Equal(Host.McpBindReason.NetworkExposed, decision.Reason);
        Assert.Equal("DPAPI-BLOB", config.Mcp.Network!.EncryptedToken);
    }

    [Fact]
    public void UpsertBothParents_EachResolverExposed_BothTemplatesSurvive()
    {
        var sample = LoadSample();
        var edited = Editor.UpsertNetworkBlock(sample, "postgres",
            Editor.BuildStoreNetworkBlock("10.0.0.5", "10.0.0.0/24", "admin"), Editor.StoreNetworkOwnedKeys);
        edited = Editor.UpsertNetworkBlock(edited, "mcp",
            Editor.BuildMcpNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null), Editor.McpNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.True(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);
        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(config.Mcp, managed: true).Mode);

        /* Both commented templates still present (count of the commented marker unchanged from the sample). */
        Assert.Equal(CountOccurrences(sample, "// \"network\": {"), CountOccurrences(edited, "// \"network\": {"));
    }

    /* ============================ web insert (#1617) ============================ */

    [Fact]
    public void UpsertWeb_IntoSample_TemplateSurvives_ParsesAndResolverExposed()
    {
        var sample = LoadSample();
        /* A non-empty encryptedToken satisfies the resolver's PRESENCE check (no DPAPI decryption here). */
        var block = Editor.BuildWebNetworkBlock("192.168.1.205", "192.168.1.0/24", encryptedToken: "DPAPI-BLOB", plaintextToken: null);
        var edited = Editor.UpsertNetworkBlock(sample, "web", block, Editor.WebNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.Contains("// \"network\": {", edited, StringComparison.Ordinal);

        var decision = WebHost.ResolveWebBind(config.Web, managed: true);
        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, decision.Reason);
        Assert.Equal("DPAPI-BLOB", config.Web.Network!.EncryptedToken);
    }

    [Fact]
    public void UpsertAllThreeParents_EachResolverExposed_AllTemplatesSurvive()
    {
        var sample = LoadSample();
        var edited = Editor.UpsertNetworkBlock(sample, "postgres",
            Editor.BuildStoreNetworkBlock("10.0.0.5", "10.0.0.0/24", "viewer"), Editor.StoreNetworkOwnedKeys);
        edited = Editor.UpsertNetworkBlock(edited, "mcp",
            Editor.BuildMcpNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null), Editor.McpNetworkOwnedKeys);
        edited = Editor.UpsertNetworkBlock(edited, "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null), Editor.WebNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.True(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);
        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(config.Mcp, managed: true).Mode);
        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, WebHost.ResolveWebBind(config.Web, managed: true).Mode);

        /* All commented templates still present (count of the commented marker unchanged from the sample). */
        Assert.Equal(CountOccurrences(sample, "// \"network\": {"), CountOccurrences(edited, "// \"network\": {"));
    }

    [Fact]
    public void UpsertWeb_MissingWebSection_CreatesParentAtRoot()
    {
        /* An older darling.json with NO "web" section at all: the upsert must create the parent, not throw
           — the exact shape a pre-#1562 config presents when the wizard adds Web exposure. */
        var json = """
            {
              "postgres": { "managed": true },
              "servers": [ { "host": "S" } ]
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "web",
            Editor.BuildWebNetworkBlock("192.168.1.205", "192.168.1.0/24", encryptedToken: "BLOB", plaintextToken: null), Editor.WebNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, WebHost.ResolveWebBind(config.Web, managed: true).Mode);
    }

    /* ============================ replace (idempotent-ish upsert) ============================ */

    [Fact]
    public void UpsertStore_Twice_ReplacesInPlace_SecondValueWins_SingleLiveBlock()
    {
        var sample = LoadSample();
        var once = Editor.UpsertNetworkBlock(sample, "postgres",
            Editor.BuildStoreNetworkBlock("192.168.1.205", "192.168.1.0/24", "viewer"), Editor.StoreNetworkOwnedKeys);
        var twice = Editor.UpsertNetworkBlock(once, "postgres",
            Editor.BuildStoreNetworkBlock("10.9.9.9", "10.9.9.0/24", "admin"), Editor.StoreNetworkOwnedKeys);

        var config = DarlingConfig.Parse(twice);
        var decision = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath);
        Assert.True(decision.Exposed);
        Assert.Equal("10.9.9.9", decision.ListenIp);
        Assert.Equal("10.9.9.0/24", decision.Cidr);
        Assert.Equal("admin", decision.Roles?[0]);

        /* The LIVE block holds the second value and NOT the first (replaced in place, not appended). The
           first value survives only in the sample's commented template, so scope the check to the live span. */
        var regions = Editor.Classify(twice);
        var rootOpen = Editor.FindRootObjectOpen(twice, regions);
        var rootClose = Editor.FindMatchingClose(twice, regions, rootOpen);
        var pg = Editor.FindObjectValueSpan(twice, regions, rootOpen, rootClose, "postgres")!.Value;
        var net = Editor.FindObjectValueSpan(twice, regions, pg.ValueStart, pg.ValueEnd - 1, "network")!.Value;
        var liveBlock = twice[net.ValueStart..net.ValueEnd];
        Assert.Contains("10.9.9.9", liveBlock, StringComparison.Ordinal);
        Assert.DoesNotContain("192.168.1.205", liveBlock, StringComparison.Ordinal);
    }

    /* ============================ remove (the inverse) ============================ */

    [Fact]
    public void RemoveStore_AfterInsert_IsInverse_ResolverLoopbackAndTemplateSurvives()
    {
        var sample = LoadSample();
        var withBlock = Editor.UpsertNetworkBlock(sample, "postgres",
            Editor.BuildStoreNetworkBlock("192.168.1.205", "192.168.1.0/24", "viewer"), Editor.StoreNetworkOwnedKeys);
        var removed = Editor.RemoveNetworkBlock(withBlock, "postgres");

        var config = DarlingConfig.Parse(removed);
        Assert.False(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);
        Assert.Contains("// \"network\": {", removed, StringComparison.Ordinal); // template intact

        /* No live network key remains. */
        var regions = Editor.Classify(removed);
        var rootOpen = Editor.FindRootObjectOpen(removed, regions);
        var rootClose = Editor.FindMatchingClose(removed, regions, rootOpen);
        var pg = Editor.FindObjectValueSpan(removed, regions, rootOpen, rootClose, "postgres")!.Value;
        Assert.Null(Editor.FindObjectValueSpan(removed, regions, pg.ValueStart, pg.ValueEnd - 1, "network"));
    }

    [Fact]
    public void RemoveNetworkBlock_NoLiveBlock_IsNoOp()
    {
        /* The sample has only the commented template — nothing live to remove, so the text is unchanged. */
        var sample = LoadSample();
        Assert.Equal(sample, Editor.RemoveNetworkBlock(sample, "postgres"));
        Assert.Equal(sample, Editor.RemoveNetworkBlock(sample, "mcp"));
    }

    /* ============================ comma / structural edge cases ============================ */

    [Fact]
    public void UpsertStore_WhenPreviousMemberHasNoTrailingComma_StillParses()
    {
        /* The needComma path: "port" is the last member with NO trailing comma. */
        const string json = """
            {
              "postgres": {
                "managed": true,
                "port": 5641
              },
              "servers": [ { "host": "S" } ]
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "postgres",
            Editor.BuildStoreNetworkBlock("192.168.1.205", "192.168.1.0/24", "viewer"), Editor.StoreNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.True(DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath).Exposed);
    }

    [Fact]
    public void UpsertMcp_WhenSectionAbsent_CreatesItAtRoot_AndResolverExposed()
    {
        /* A minimal darling.json with NO mcp section — the block must create one. */
        const string json = """
            {
              "postgres": { "managed": true },
              "servers": [ { "host": "S" } ]
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "mcp",
            Editor.BuildMcpNetworkBlock("192.168.1.205", "192.168.1.0/24", encryptedToken: "BLOB", plaintextToken: null), Editor.McpNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        Assert.NotNull(config.Mcp.Network);
        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(config.Mcp, managed: true).Mode);
    }

    /* ============================ replace keeps what the wizard does not own (#4743) ============================ */

    /// <summary>The text of one member of <c>parentKey.network</c>, key through value, or null when it is not there.</summary>
    private static string? NetworkMemberText(string json, string parentKey, string key)
    {
        var regions = Editor.Classify(json);
        var rootOpen = Editor.FindRootObjectOpen(json, regions);
        var rootClose = Editor.FindMatchingClose(json, regions, rootOpen);
        var parent = Editor.FindObjectValueSpan(json, regions, rootOpen, rootClose, parentKey)!.Value;
        var network = Editor.FindObjectValueSpan(json, regions, parent.ValueStart, parent.ValueEnd - 1, "network")!.Value;
        var member = Editor.FindObjectValueSpan(json, regions, network.ValueStart, network.ValueEnd - 1, key);
        return member is null ? null : json[member.Value.KeyStart..member.Value.ValueEnd];
    }

    /// <summary>
    /// Parses with comments skipped but trailing commas REFUSED, so a comma the edit got wrong fails here even
    /// though <see cref="DarlingConfig.Parse"/> would forgive it.
    /// </summary>
    private static void AssertStrictJson(string json) =>
        JsonDocument.Parse(json, new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip }).Dispose();

    private const string WebWithTlsOidcAndUnknownKey = """
        {
          "postgres": { "managed": true },
          "web": {
            "port": 5153,
            "network": {
              "listen": "192.168.1.205",  // old bind
              "allowFrom": "192.168.1.0/24",
              "encryptedToken": "OLD-BLOB",
              "tls": {
                "certPath": "C:\\certs\\web.crt",
                "keyPath": "C:\\certs\\web.key"
              },  // HTTPS for the dashboard
              "oidc": { "authority": "https://login.example.test/v2.0", "clientId": "dash", "encryptedClientSecret": "S" },
              "futureKey": 1  // a setting this build has never heard of
            }
          },
          "servers": [ { "host": "S" } ]
        }
        """;

    [Fact]
    public void UpsertWeb_OverLiveBlock_KeepsTlsOidcAndUnknownKeyByteForByte_PromptedKeysTakeNewValues()
    {
        var edited = Editor.UpsertNetworkBlock(WebWithTlsOidcAndUnknownKey, "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "NEW-BLOB", plaintextToken: null),
            Editor.WebNetworkOwnedKeys, out var kept);

        AssertStrictJson(edited);
        var config = DarlingConfig.Parse(edited);

        /* The keys the wizard prompts for took the new answers, once each. */
        Assert.Equal("10.0.0.5", config.Web.Network!.Listen);
        Assert.Equal("10.0.0.0/24", config.Web.Network!.AllowFrom);
        Assert.Equal("NEW-BLOB", config.Web.Network!.EncryptedToken);
        foreach (var key in new[] { "listen", "allowFrom", "encryptedToken" })
        {
            Assert.NotNull(NetworkMemberText(edited, "web", key));
        }

        Assert.DoesNotContain("OLD-BLOB", edited, StringComparison.Ordinal);
        Assert.DoesNotContain("192.168.1.205", edited, StringComparison.Ordinal);

        /* tls, oidc and the key this build has never heard of came through byte for byte, comments included. */
        foreach (var key in new[] { "tls", "oidc", "futureKey" })
        {
            Assert.Contains(NetworkMemberText(WebWithTlsOidcAndUnknownKey, "web", key)!, edited, StringComparison.Ordinal);
            Assert.Equal(NetworkMemberText(WebWithTlsOidcAndUnknownKey, "web", key), NetworkMemberText(edited, "web", key));
        }

        Assert.Contains("},  // HTTPS for the dashboard", edited, StringComparison.Ordinal);
        Assert.Contains("\"futureKey\": 1  // a setting this build has never heard of", edited, StringComparison.Ordinal);
        Assert.Equal(@"C:\certs\web.crt", config.Web.Network!.Tls!.CertPath);
        Assert.Equal("dash", config.Web.Network!.Oidc!.ClientId);

        /* Outside the network block nothing moved, and the report names exactly what was kept, in order. */
        Assert.Contains("\"port\": 5153,", edited, StringComparison.Ordinal);
        Assert.Equal(["tls", "oidc", "futureKey"], kept);
    }

    [Fact]
    public void UpsertWeb_KeptMembers_LandBeforeTheClosingBrace_WithExactCommasAndIndent()
    {
        const string json = """
            {
              "web": {
                "network": {
                  "listen": "192.168.1.205",
                  "tls": { "pfxPath": "a.pfx" },  // cert
                  "futureKey": 1
                }
              }
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null),
            Editor.WebNetworkOwnedKeys);

        AssertStrictJson(edited);

        /* The builder's last member gains its comma right after its value (before its trailing comment); the
           kept members follow at the builder's field indent; the last one has no comma; the brace closes its own line. */
        Assert.Contains("\"encryptedToken\": \"BLOB\",  // DPAPI", edited, StringComparison.Ordinal);
        Assert.Contains("\n      \"tls\": { \"pfxPath\": \"a.pfx\" },  // cert\n      \"futureKey\": 1\n    }\n  }\n}",
            edited.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void UpsertWeb_PlaintextTokenReplacedByEncryptedToken_LeavesExactlyOneTokenKey()
    {
        const string json = """
            {
              "postgres": { "managed": true },
              "web": {
                "network": {
                  "listen": "192.168.1.205",
                  "allowFrom": "192.168.1.0/24",
                  "token": "plain-secret",  // dev only
                  "tls": { "pfxPath": "a.pfx" }
                }
              }
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null),
            Editor.WebNetworkOwnedKeys, out var kept);

        AssertStrictJson(edited);
        Assert.Null(NetworkMemberText(edited, "web", "token"));
        Assert.NotNull(NetworkMemberText(edited, "web", "encryptedToken"));
        Assert.DoesNotContain("plain-secret", edited, StringComparison.Ordinal);
        Assert.Equal(["tls"], kept);

        /* And the other way: an encrypted token replaced by a kept-as-plaintext one leaves no encryptedToken. */
        var back = Editor.UpsertNetworkBlock(edited, "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: null, plaintextToken: "plain-secret"),
            Editor.WebNetworkOwnedKeys);
        AssertStrictJson(back);
        Assert.Null(NetworkMemberText(back, "web", "encryptedToken"));
        Assert.NotNull(NetworkMemberText(back, "web", "token"));
        Assert.NotNull(NetworkMemberText(back, "web", "tls"));
    }

    [Fact]
    public void UpsertMcp_OverLiveBlock_KeepsAnUnknownKey_AndBothTokenKeysAreOwned()
    {
        const string json = """
            {
              "postgres": { "managed": true },
              "mcp": {
                "network": {
                  "listen": "192.168.1.205",
                  "allowFrom": "192.168.1.0/24",
                  "token": "plain-secret",
                  "futureKey": { "nested": [1, 2, { "a": "}" }] }
                }
              }
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "mcp",
            Editor.BuildMcpNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null),
            Editor.McpNetworkOwnedKeys, out var kept);

        AssertStrictJson(edited);
        Assert.Equal(NetworkMemberText(json, "mcp", "futureKey"), NetworkMemberText(edited, "mcp", "futureKey"));
        Assert.Null(NetworkMemberText(edited, "mcp", "token"));
        Assert.Equal(["futureKey"], kept);
        Assert.Equal(Host.McpBindMode.NetworkAndLoopback, Host.ResolveMcpBind(DarlingConfig.Parse(edited).Mcp, managed: true).Mode);
    }

    [Fact]
    public void UpsertStore_OverLiveBlock_ReplacesItsRole_KeepsAnUnknownKey()
    {
        const string json = """
            {
              "postgres": {
                "managed": true,
                "network": { "listen": "192.168.1.205", "allowFrom": "192.168.1.0/24", "role": "admin", "futureKey": true }
              }
            }
            """;
        var edited = Editor.UpsertNetworkBlock(json, "postgres",
            Editor.BuildStoreNetworkBlock("10.9.9.9", "10.9.9.0/24", "viewer"),
            Editor.StoreNetworkOwnedKeys, out var kept);

        AssertStrictJson(edited);
        var decision = DarlingManagedPostgres.ResolveNetworkExposure(DarlingConfig.Parse(edited).Postgres.Network, CertPath, KeyPath);
        Assert.True(decision.Exposed);
        Assert.Equal("10.9.9.9", decision.ListenIp);
        Assert.Equal("viewer", decision.Roles?[0]);
        Assert.Equal("\"futureKey\": true", NetworkMemberText(edited, "postgres", "futureKey"));
        Assert.Equal("\"viewer\"", NetworkMemberText(edited, "postgres", "role")!.Split(": ")[1]);
        Assert.Equal(["futureKey"], kept);
    }

    [Fact]
    public void UpsertWeb_OverBlockOfOnlyOwnedKeys_IsTheOldWholesaleReplacement()
    {
        /* Nothing to keep means the edit is exactly what it was before #4743 — and the differently cased and
           escaped spellings of owned keys count as owned, because the parser reads them as the same keys. */
        const string oldBlock = "\"network\": { \"Listen\": \"a\", \"\\u0061llowFrom\": \"b\", \"token\": \"c\" }";
        var json = "{ \"web\": {\n    " + oldBlock + "\n  } }";
        var block = Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null);

        var edited = Editor.UpsertNetworkBlock(json, "web", block, Editor.WebNetworkOwnedKeys, out var kept);

        Assert.Equal(json.Replace(oldBlock, block, StringComparison.Ordinal), edited);
        Assert.Empty(kept);
    }

    [Fact]
    public void UpsertWeb_KeptCasingAndComments_TrailingCommaAndCrlf_StayValidJson()
    {
        /* A trailing comma after the last member, a block comment ending a kept line, an owned key spelled
           "Listen", a differently cased kept key, and CRLF line endings. */
        var json = "{\r\n  \"web\": {\r\n    \"network\": {\r\n      \"Listen\": \"a\",\r\n"
            + "      \"futureKey\": 1, /* note */\r\n      \"TLS\": { \"pfxPath\": \"a.pfx\" },  // cert\r\n    }\r\n  }\r\n}";
        var edited = Editor.UpsertNetworkBlock(json, "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null),
            Editor.WebNetworkOwnedKeys, out var kept);

        AssertStrictJson(edited);
        Assert.Equal(["futureKey", "TLS"], kept);
        Assert.Contains("\"futureKey\": 1, /* note */", edited, StringComparison.Ordinal);
        Assert.Contains("\"TLS\": { \"pfxPath\": \"a.pfx\" }  // cert", edited, StringComparison.Ordinal);
        Assert.DoesNotContain("\r  ", edited.Replace("\r\n", "\n"), StringComparison.Ordinal);
        Assert.Equal("10.0.0.5", DarlingConfig.Parse(edited).Web.Network!.Listen);
    }

    [Fact]
    public void UpsertWeb_OverANullNetworkValue_ReplacesItWithoutThrowing()
    {
        var edited = Editor.UpsertNetworkBlock("{ \"web\": {\n    \"network\": null\n  } }", "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null),
            Editor.WebNetworkOwnedKeys, out var kept);

        AssertStrictJson(edited);
        Assert.Empty(kept);
        Assert.Equal("10.0.0.5", DarlingConfig.Parse(edited).Web.Network!.Listen);
    }

    [Fact]
    public void UpsertNetworkBlock_InsertPathIgnoresOwnedKeys()
    {
        var edited = Editor.UpsertNetworkBlock(LoadSample(), "web",
            Editor.BuildWebNetworkBlock("10.0.0.5", "10.0.0.0/24", encryptedToken: "BLOB", plaintextToken: null),
            Editor.WebNetworkOwnedKeys, out var kept);

        /* The sample itself keeps trailing commas, so this one is judged by the lenient parser only. */
        Assert.Empty(kept);
        Assert.Equal("10.0.0.5", DarlingConfig.Parse(edited).Web.Network!.Listen);
    }

    [Fact]
    public void OwnedKeys_ListEveryKeyTheBuildersWrite_AndBothTokenKeys()
    {
        static IReadOnlyList<string> Written(string block)
        {
            using var doc = JsonDocument.Parse("{" + block + "}", new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip });
            var names = new List<string>();
            foreach (var property in doc.RootElement.GetProperty("network").EnumerateObject())
            {
                names.Add(property.Name);
            }

            return names;
        }

        /* The store block owns exactly what its builder writes. */
        Assert.Equal(
            Written(Editor.BuildStoreNetworkBlock("192.168.1.205", "192.168.1.0/24", "viewer")).Order(),
            Editor.StoreNetworkOwnedKeys.Order());

        /* MCP and web own everything either token variant writes, so neither variant can leave its twin behind. */
        foreach (var owned in new[] { Editor.McpNetworkOwnedKeys, Editor.WebNetworkOwnedKeys })
        {
            Assert.Equal(["allowFrom", "encryptedToken", "listen", "token"], owned.Order());
        }

        foreach (var block in new[]
        {
            Editor.BuildMcpNetworkBlock("a", "b", "BLOB", null), Editor.BuildMcpNetworkBlock("a", "b", null, "plain"),
            Editor.BuildWebNetworkBlock("a", "b", "BLOB", null), Editor.BuildWebNetworkBlock("a", "b", null, "plain"),
        })
        {
            foreach (var name in Written(block))
            {
                Assert.Contains(name, Editor.WebNetworkOwnedKeys);
                Assert.Contains(name, Editor.McpNetworkOwnedKeys);
            }
        }
    }

    [Fact]
    public void FormatKeptLine_NamesWhatWasKept_AndIsNullWhenNothingWas()
    {
        Assert.Null(Editor.FormatKeptLine([]));
        Assert.Equal("Kept web.network.tls from the existing darling.json.",
            Editor.FormatKeptLine(["web.network.tls"]));
        Assert.Equal("Kept web.network.tls and web.network.oidc from the existing darling.json.",
            Editor.FormatKeptLine(["web.network.tls", "web.network.oidc"]));
        Assert.Equal("Kept mcp.network.a, web.network.tls and web.network.oidc from the existing darling.json.",
            Editor.FormatKeptLine(["mcp.network.a", "web.network.tls", "web.network.oidc", "web.network.tls"]));
    }

    /* ============================ delegated rejection (builders emit resolver-checked shapes) ============================ */

    [Fact]
    public void BuildStore_WithFamilyMismatch_IsRejectedByTheDelegatedResolver()
    {
        /* An IPv4 listen with an IPv6 allowFrom parses fine as JSON but the STORE RESOLVER degrades it —
           proving validation is delegated, not re-implemented in the editor. */
        var sample = LoadSample();
        var edited = Editor.UpsertNetworkBlock(sample, "postgres",
            Editor.BuildStoreNetworkBlock("192.168.1.205", "2001:db8::/32", "viewer"), Editor.StoreNetworkOwnedKeys);

        var config = DarlingConfig.Parse(edited);
        var decision = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, CertPath, KeyPath);
        Assert.False(decision.Exposed);
        Assert.NotNull(decision.DegradeReason);
    }

    [Fact]
    public void BuildMcp_EmitsPlaintextTokenWhenNoEncryptedTokenGiven()
    {
        var block = Editor.BuildMcpNetworkBlock("192.168.1.205", "192.168.1.0/24", encryptedToken: null, plaintextToken: "devtoken");
        Assert.Contains("\"token\": \"devtoken\"", block, StringComparison.Ordinal);
        /* The KEY encryptedToken must be absent (the word appears in the "prefer encryptedToken" comment). */
        Assert.DoesNotContain("\"encryptedToken\"", block, StringComparison.Ordinal);
    }

    /* ============================ pure formatters ============================ */

    [Fact]
    public void FormatAdapterMenu_NumbersEntries_AndHandlesEmpty()
    {
        Assert.Contains("no non-loopback", Editor.FormatAdapterMenu(Array.Empty<(string, string)>()), StringComparison.OrdinalIgnoreCase);

        var menu = Editor.FormatAdapterMenu(new (string, string)[] { ("Ethernet", "192.168.1.5"), ("Wi-Fi", "10.0.0.9") });
        Assert.Contains("[1] 192.168.1.5", menu, StringComparison.Ordinal);
        Assert.Contains("[2] 10.0.0.9", menu, StringComparison.Ordinal);
        Assert.Contains("Ethernet", menu, StringComparison.Ordinal);
    }

    [Fact]
    public void FormatExposureState_RendersExposedDegradedAndDefault()
    {
        Assert.Contains("EXPOSED", Editor.FormatExposureState("Store", true, "192.168.1.205", "192.168.1.0/24", "viewer", null), StringComparison.Ordinal);
        Assert.Contains("DEGRADED", Editor.FormatExposureState("Store", false, null, null, null, "bad cidr"), StringComparison.Ordinal);
        Assert.Contains("secure default", Editor.FormatExposureState("MCP", false, null, null, null, null), StringComparison.Ordinal);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
