/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#6840: the front door's <c>frontDoor</c> block. What it accepts, what it refuses, that it asks for no postgres
/// section, and that a config without it behaves as it always did.</summary>
public sealed class FrontDoorConfigTests
{
    private const string Secret = "SECRET-TOKEN-6840";

    private static FrontDoorStoreConfig Store(string name = "use1") => new()
    {
        Name = name,
        McpUrl = "https://peer.example.test:5152/",
        McpToken = Secret,
    };

    private static FrontDoorConfig Block(params FrontDoorStoreConfig[] stores)
    {
        var block = new FrontDoorConfig { Enabled = true };
        block.Stores.AddRange(stores);
        return block;
    }

    private static string[] Problems(params FrontDoorStoreConfig[] stores) => FrontDoorConfig.Validate(Block(stores)).ToArray();

    /* ───────────────────────── a config without the block ───────────────────────── */

    [Fact]
    public void AbsentBlock_IsOff_AndAsksNothing()
    {
        var config = DarlingConfig.Parse("{}");
        Assert.False(config.IsFrontDoor);
        Assert.False(config.FrontDoor.Enabled);
        Assert.Empty(FrontDoorConfig.Validate(config.FrontDoor));
        Assert.Empty(config.FrontDoor.ToStores());
        Assert.Empty(config.FrontDoor.Warnings());
    }

    [Fact]
    public void NullBlock_IsTheSameAsAbsent()
    {
        var config = DarlingConfig.Parse("{\"frontDoor\":null}");
        Assert.False(config.IsFrontDoor);
        Assert.Empty(FrontDoorConfig.Validate(config.FrontDoor));
    }

    [Fact]
    public void DisabledBlock_IsNotValidated()
    {
        var config = DarlingConfig.Parse("{\"frontDoor\":{\"enabled\":false,\"refreshSeconds\":1,\"stores\":[{\"name\":\"bad name\"}]}}");
        Assert.False(config.IsFrontDoor);
        Assert.Empty(FrontDoorConfig.Validate(config.FrontDoor));
    }

    [Fact]
    public void WithoutTheBlock_ThePostgresSectionIsStillRequired()
    {
        var problems = new DarlingConfig { Postgres = null! }.Validate();
        Assert.Contains("postgres section is required.", problems);

        problems = new DarlingConfig().Validate();
        Assert.Contains(problems, p => p.StartsWith("postgres.connectionString is required", StringComparison.Ordinal));
    }

    /* ───────────────────────── front-door mode ───────────────────────── */

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void InFrontDoorMode_NoPostgresSectionIsRequired(bool explicitNull)
    {
        var config = explicitNull
            ? new DarlingConfig { Postgres = null!, FrontDoor = Block(Store()) }
            : new DarlingConfig { FrontDoor = Block(Store()) };

        Assert.True(config.IsFrontDoor);
        Assert.Empty(config.Validate());
    }

    [Fact]
    public void InFrontDoorMode_ABrokenBlockIsStillReported()
    {
        var config = new DarlingConfig { Postgres = null!, FrontDoor = Block() };
        var problems = config.Validate();
        Assert.Single(problems);
        Assert.Contains("frontDoor.stores", problems[0]);
        Assert.DoesNotContain(problems, p => p.Contains("postgres", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void TheBlock_BindsFromJson_WithTheDefaults()
    {
        var config = DarlingConfig.Parse(
            "{\"frontDoor\":{\"enabled\":true,\"stores\":[{\"name\":\"use1\",\"mcpUrl\":\"https://use1:5152/\",\"mcpToken\":\"t\"," +
            "\"mcpAgentToken\":\"a\",\"webUrl\":\"https://use1:5153/\",\"webToken\":\"w\",\"certificateSha256\":\"" + new string('a', 64) + "\"}]}}");

        Assert.True(config.IsFrontDoor);
        Assert.Equal(300, config.FrontDoor.RefreshSeconds);
        Assert.Equal(150, config.FrontDoor.PeerTimeoutSeconds);
        Assert.Empty(FrontDoorConfig.Validate(config.FrontDoor));

        var store = Assert.Single(config.FrontDoor.ToStores());
        Assert.Equal("use1", store.Name);
        Assert.Equal(new Uri("https://use1:5152/"), store.McpUrl);
        Assert.Equal(new Uri("https://use1:5153/"), store.WebUrl);
        Assert.Equal(new string('A', 64), store.CertificateSha256);
    }

    /* ───────────────────────── validation ───────────────────────── */

    [Theory]
    [InlineData("use1")]
    [InlineData("US-East.1_a")]
    [InlineData("a")]
    public void AGoodName_IsAccepted(string name) => Assert.Empty(Problems(Store(name)));

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("has space")]
    [InlineData("slash/name")]
    [InlineData("hash#name")]
    [InlineData("colon:name")]
    [InlineData("question?")]
    [InlineData("tab\tname")]
    public void ABadName_IsRefused(string name)
    {
        var problems = Problems(Store(name));
        Assert.Single(problems);
        Assert.Contains("name is required", problems[0]);
    }

    [Fact]
    public void ANameOver64Characters_IsRefused()
    {
        Assert.Empty(Problems(Store(new string('a', 64))));
        Assert.Single(Problems(Store(new string('a', 65))));
    }

    [Fact]
    public void ACredentialShapedName_IsRefused()
    {
        var problems = Problems(Store("connectionstring"));
        Assert.Single(problems);
        Assert.Contains("credential", problems[0]);
    }

    [Fact]
    public void ADuplicateName_IsRefused_IgnoringCase()
    {
        var problems = Problems(Store("use1"), Store("USE1"));
        Assert.Single(problems);
        Assert.Contains("frontDoor.stores[1]", problems[0]);
        Assert.Contains("used by another store", problems[0]);
    }

    [Theory]
    [InlineData("ftp://host/")]
    [InlineData("host:5152")]
    [InlineData("/relative")]
    [InlineData("https://host/?x=1")]
    [InlineData("https://user:pw@host/")]
    public void ABadMcpUrl_IsRefused_WithoutEchoingIt(string url)
    {
        var store = Store();
        store.McpUrl = url;
        var problems = Problems(store);
        Assert.Single(problems);
        Assert.StartsWith("frontDoor.stores[0]: mcpUrl", problems[0], StringComparison.Ordinal);
        Assert.DoesNotContain(url, problems[0], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void AMissingMcpUrl_IsRefused(string url)
    {
        var store = Store();
        store.McpUrl = url;
        Assert.Contains("mcpUrl is required", Assert.Single(Problems(store)));
    }

    [Theory]
    [InlineData("http://host:5153/")]
    [InlineData("https://host:5153/base")]
    [InlineData("")]
    [InlineData(null)]
    public void TheWebUrl_IsOptional_AndAGoodOneIsAccepted(string? url)
    {
        var store = Store();
        store.WebUrl = url;
        Assert.Empty(Problems(store));
    }

    [Theory]
    [InlineData("ftp://host/")]
    [InlineData("https://host/?x=1")]
    public void ABadWebUrl_IsRefused(string url)
    {
        var store = Store();
        store.WebUrl = url;
        var problems = Problems(store);
        Assert.Single(problems);
        Assert.Contains("webUrl", problems[0]);
        Assert.DoesNotContain(url, problems[0], StringComparison.Ordinal);
    }

    [Fact]
    public void AnMcpToken_IsRequired_ButEitherFormCounts()
    {
        var none = Store();
        none.McpToken = null;
        Assert.Contains("an MCP token is required", Assert.Single(Problems(none)));

        var onlyAgent = Store();
        onlyAgent.McpToken = "  ";
        onlyAgent.McpAgentToken = "agent";
        Assert.Single(Problems(onlyAgent));

        var encrypted = Store();
        encrypted.McpToken = null;
        encrypted.McpEncryptedToken = "blob";
        Assert.Empty(Problems(encrypted));

        var reference = Store();
        reference.McpToken = "env:SOME_TOKEN";
        Assert.Empty(Problems(reference));
    }

    [Theory]
    [InlineData(29, false)]
    [InlineData(30, true)]
    [InlineData(300, true)]
    [InlineData(3600, true)]
    [InlineData(3601, false)]
    [InlineData(0, false)]
    [InlineData(-5, false)]
    public void RefreshSeconds_IsThirtyToThirtySixHundred(int seconds, bool valid)
    {
        var block = Block(Store());
        block.RefreshSeconds = seconds;
        var problems = FrontDoorConfig.Validate(block);
        Assert.Equal(valid, problems.Count == 0);
        if (!valid)
        {
            Assert.Contains("refreshSeconds", Assert.Single(problems));
        }
    }

    [Theory]
    [InlineData(29, false)]
    [InlineData(30, true)]
    [InlineData(150, true)]
    [InlineData(600, true)]
    [InlineData(601, false)]
    public void PeerTimeoutSeconds_IsThirtyToSixHundred(int seconds, bool valid)
    {
        var block = Block(Store());
        block.PeerTimeoutSeconds = seconds;
        var problems = FrontDoorConfig.Validate(block);
        Assert.Equal(valid, problems.Count == 0);
        if (!valid)
        {
            Assert.Contains("peerTimeoutSeconds", Assert.Single(problems));
        }
    }

    [Fact]
    public void AnEnabledBlockWithNoStores_IsRefused()
    {
        Assert.Contains("at least one store", Assert.Single(FrontDoorConfig.Validate(Block())));
        Assert.Contains("at least one store", Assert.Single(FrontDoorConfig.Validate(new FrontDoorConfig { Enabled = true, Stores = null! })));
    }

    [Fact]
    public void ANullEntry_IsRefused_AndLeftOutOfTheStores()
    {
        var block = Block(Store("a"), null!, Store("b"));
        Assert.Contains("frontDoor.stores[1] is empty", Assert.Single(FrontDoorConfig.Validate(block)));
        Assert.Equal(new[] { "a", "b" }, block.ToStores().Select(s => s.Name));
    }

    [Theory]
    [InlineData("zz", false)]
    [InlineData("AB:CD", false)]
    public void ACertificatePin_MustBeSixtyFourHexCharacters(string pin, bool valid)
    {
        var store = Store();
        store.CertificateSha256 = pin;
        Assert.Equal(valid, Problems(store).Length == 0);
    }

    [Fact]
    public void ACertificatePin_AcceptsColonsAndAnyCase_AndIsNormalized()
    {
        var store = Store();
        store.CertificateSha256 = string.Join(":", Enumerable.Repeat("aA", 32));
        Assert.Empty(Problems(store));
        Assert.Equal(new string('A', 64), store.ToStore()!.CertificateSha256);
    }

    [Fact]
    public void NoProblem_CarriesATokenOrAnAddress()
    {
        var store = Store();
        store.McpUrl = "https://user:" + Secret + "@host/?key=" + Secret;
        store.CertificateSha256 = "not-a-pin";
        store.Name = "bad name " + Secret;
        Assert.All(Problems(store), p => Assert.DoesNotContain(Secret, p, StringComparison.Ordinal));
    }

    /* ───────────────────────── warnings, not refusals (D3) ───────────────────────── */

    [Fact]
    public void PlainHttpToAnotherMachine_IsAWarning_NotAProblem()
    {
        var store = Store("use1");
        store.McpUrl = "http://peer.example.test:5152/";
        store.WebUrl = "http://peer.example.test:5153/";
        var block = Block(store);

        Assert.Empty(FrontDoorConfig.Validate(block));
        var warnings = block.Warnings();
        Assert.Equal(2, warnings.Count);
        Assert.All(warnings, w => Assert.Contains("use1", w));
        Assert.All(warnings, w => Assert.DoesNotContain("example.test", w, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("http://127.0.0.1:5152/")]
    [InlineData("http://localhost:5152/")]
    [InlineData("http://[::1]:5152/")]
    [InlineData("https://use1.example.test:5152/")]
    public void LoopbackOrHttps_HasNoWarning(string url)
    {
        var store = Store();
        store.McpUrl = url;
        Assert.Empty(Block(store).Warnings());
    }
}
