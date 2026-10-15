/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5878: <c>DarlingMcpWritePolicyTests</c> failed once in a large batch and passed alone. The host's GCF filter follows
/// the process-wide <c>DARLING_OUTPUT_FORMAT</c> variable on every call unless the host registers an
/// <c>IGcfOutputFormat</c>, and the <c>DarlingOutputFormatEnv</c> classes set that variable to <c>gcf</c> while other
/// classes run beside them in the same process. This class sets the variable itself, inside that collection, so the
/// overlap is deterministic: the write-policy host must still answer <c>add_servers</c> as JSON.
/// </summary>
[Collection("DarlingOutputFormatEnv")]
public sealed class DarlingMcpWritePolicyOutputFormatTests : IDisposable
{
    public DarlingMcpWritePolicyOutputFormatTests() =>
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    public void Dispose() => Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    [Fact]
    public async Task AddServersRefusal_StaysJson_WhileTheOutputFormatVariableSaysGcf()
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", "gcf");
        using var server = await DarlingMcpWritePolicyTests.BuildServer(
            DarlingMcpWritePolicyTests.EverythingConfig, DarlingMcpWritePolicyTests.Token,
            "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1");
        var arguments = JsonSerializer.Serialize(new
        {
            servers_json = "[{\"host\":\"sql01\",\"auth\":\"Windows\",\"trust_server_certificate\":true}]",
        });

        var (statusCode, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, "add_servers", arguments, bearer: DarlingMcpWritePolicyTests.Token);

        Assert.Equal(StatusCodes.Status200OK, statusCode);
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var envelope = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var text = envelope.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!;
        using var result = JsonDocument.Parse(text);
        Assert.Equal(1, result.RootElement.GetProperty("failed").GetInt32());
        Assert.Equal("invalid", result.RootElement.GetProperty("results")[0].GetProperty("status").GetString());
    }
}
