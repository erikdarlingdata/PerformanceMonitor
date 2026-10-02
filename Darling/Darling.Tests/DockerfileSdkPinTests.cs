/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The container builds on the SDK <c>global.json</c> pins (#3906).
///
/// <para><c>Darling/Dockerfile</c> pins its build stage to an exact SDK image rather than the floating
/// <c>:10.0</c> tag. <c>global.json</c>'s <c>latestPatch</c> never leaves its feature band, and the floating tag
/// moving to a new band once broke every container build at the same time. That leaves two declarations of
/// one fact. Dependabot's <c>dotnet-sdk</c> updates edit only <c>global.json</c>. They exist so that .NET
/// runtime security servicing reaches the self-contained builds, which is exactly what stopped happening when
/// the 3xx band ended at 10.0.303.</para>
///
/// <para>The container build runs only when the Darling path gate fires, and a <c>global.json</c>-only change
/// does not fire it. This pin runs in the build job on every <c>global.json</c> change, so it names the second
/// line on the Dependabot pull request itself. Otherwise the gap would surface later, when the next Darling
/// change or the nightly image build fails with exit code 155.</para>
/// </summary>
public sealed class DockerfileSdkPinTests
{
    private static readonly Regex s_buildStage = new(
        @"^FROM mcr\.microsoft\.com/dotnet/sdk:(?<tag>\S+) AS build\s*$",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    [Fact]
    public void TheContainerBuildStage_UsesTheSdkGlobalJsonPins()
    {
        using var globalJson = JsonDocument.Parse(ReadRepoFile("global.json"));
        var pinned = globalJson.RootElement.GetProperty("sdk").GetProperty("version").GetString();
        Assert.False(string.IsNullOrWhiteSpace(pinned), "global.json no longer pins sdk.version, so there is nothing to compare against.");

        var from = s_buildStage.Match(ReadRepoFile("Darling", "Dockerfile"));
        Assert.True(from.Success,
            "Darling/Dockerfile no longer has a `FROM mcr.microsoft.com/dotnet/sdk:<tag> AS build` line, so this pin cannot find what it guards.");

        var tag = from.Groups["tag"].Value;
        Assert.True(
            string.Equals(pinned, tag, StringComparison.Ordinal),
            $"global.json pins SDK {pinned} but Darling/Dockerfile builds on sdk:{tag}. Move the Dockerfile's FROM line " +
            "to the same version in the same pull request. Under rollForward latestPatch, an image older than " +
            "global.json fails every container build with exit code 155, and one from another feature band does too.");
    }
}
