/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5452: the four texts an operator reads on collection health when a server's AWS role cannot be used. Each says what
/// happened, names the role ARN, says what to change, and never carries the external ID.
/// </summary>
public sealed class AwsRoleMessageTextTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string Sentinel = "ext-7Hq2mZ9vLx";

    private static AwsRoleAssumeException Make(AwsRoleAssumeKind kind, string? externalId) => kind switch
    {
        AwsRoleAssumeKind.NotAllowed => AwsRoleAssumeException.ForNotAllowed(new AwsRoleKey(Role, externalId), "us-east-1"),
        AwsRoleAssumeKind.PartitionMismatch => AwsRoleAssumeException.ForPartitionMismatch(new AwsRoleKey(Role, externalId), "cn-north-1", "aws", "aws-cn"),
        AwsRoleAssumeKind.Denied => AwsRoleAssumeException.ForStsFailure(
            new AwsRoleKey(Role, externalId), "us-east-1", FakeSts.Error("AccessDenied", $"denied (ExternalId={externalId})")),
        AwsRoleAssumeKind.RegionDisabled => AwsRoleAssumeException.ForStsFailure(
            new AwsRoleKey(Role, externalId), "af-south-1", FakeSts.Error("RegionDisabledException", $"off (ExternalId={externalId})")),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /* What each message must tell the operator to change, as a phrase the text has to hold. */
    public static TheoryData<AwsRoleAssumeKind, string[]> Changes => new()
    {
        { AwsRoleAssumeKind.NotAllowed, ["allowedAwsRoles in darling.json", "restart the service", "clear the role"] },
        { AwsRoleAssumeKind.PartitionMismatch, ["partition", "Set a role from the target's partition", "clear the role", "host name"] },
        { AwsRoleAssumeKind.Denied, ["trust policy", "external ID", "sts:AssumeRole", "does not exist"] },
        { AwsRoleAssumeKind.RegionDisabled, ["Enable that region", "af-south-1", "set a role in a region that is enabled", "host name"] },
    };

    [Theory]
    [MemberData(nameof(Changes))]
    public void EachRoleMessage_SaysWhatHappened_NamesTheRole_AndSaysWhatToChange(AwsRoleAssumeKind kind, string[] phrases)
    {
        var message = Make(kind, Sentinel).Message;

        Assert.Contains(Role, message, StringComparison.Ordinal);
        Assert.Contains("Nothing was read this cycle", message, StringComparison.Ordinal);
        foreach (var phrase in phrases)
        {
            Assert.Contains(phrase, message, StringComparison.Ordinal);
        }

        Assert.DoesNotContain(Sentinel, message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Changes))]
    public void EachRoleMessage_IsAnOrdinaryPlainSentence_NotARawSdkDump(AwsRoleAssumeKind kind, string[] phrases)
    {
        _ = phrases;
        var message = Make(kind, Sentinel).Message;

        Assert.DoesNotContain("Amazon.", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Exception", message, StringComparison.Ordinal);
        Assert.False(message.Contains('\n', StringComparison.Ordinal), "one line, as the collection health column shows it");
    }

    [Fact]
    public void TheDeniedMessage_SaysWhetherAnExternalIdWasSent_AndNeverWhichOne()
    {
        var with = Make(AwsRoleAssumeKind.Denied, Sentinel).Message;
        var without = Make(AwsRoleAssumeKind.Denied, null).Message;

        Assert.Contains("(with the external ID set on this server)", with, StringComparison.Ordinal);
        Assert.Contains("(without an external ID)", without, StringComparison.Ordinal);
        Assert.DoesNotContain(Sentinel, with, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// #5452: Test Connection in the desktop viewer. The viewer sends a <c>test_connect</c> command; the service answers it with
/// <c>DarlingServerConnector.ProbeAsync</c>, which opens a PostgreSQL connection and reads facts from it. Neither the viewer
/// nor that path makes an AWS call, so the server's role changes nothing about the test. These pins keep it so: a test that
/// began to call AWS would have to carry the role and the external ID, and be held to the allowed-roles list.
/// </summary>
public sealed class AwsRoleTestConnectionTests
{
    private static string Repo()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("The repository root was not found.");
    }

    private static string Read(params string[] parts) => File.ReadAllText(Path.Combine(Repo(), Path.Combine(parts)));

    [Fact]
    public void TheServiceSideOfTestConnection_MakesNoAwsCall()
    {
        var service = Path.Combine("Darling", "PerformanceMonitor.Darling.Service");
        foreach (var file in new[] { "DarlingServerConnector.cs", "DarlingCommandExecutor.cs" })
        {
            var text = Read(service, file);
            Assert.DoesNotMatch(new Regex(@"^\s*using\s+Amazon", RegexOptions.Multiline), text);
            foreach (var name in new[] { "AwsRoleClients", "AwsRoleCredentialCache", "AssumeRole", "AmazonRDSClient", "AmazonPIClient", "AwsRoleKey", "RdsEndpointVerifier" })
            {
                Assert.DoesNotContain(name, text, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void TheViewer_HasNoAwsSdk_SoItCannotCallAwsWithAnyCredentials()
    {
        var viewer = Path.Combine(Repo(), "Darling", "PerformanceMonitor.Darling.Viewer");
        var project = File.ReadAllText(Path.Combine(viewer, "PerformanceMonitor.Darling.Viewer.csproj"));
        Assert.DoesNotContain("AWSSDK", project, StringComparison.Ordinal);
        foreach (var file in Directory.EnumerateFiles(viewer, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            Assert.DoesNotMatch(new Regex(@"^\s*using\s+Amazon", RegexOptions.Multiline), File.ReadAllText(file));
        }
    }

    [Fact]
    public void TheProbe_ReadsNoRoleFromTheServerItTests()
    {
        var text = Read("Darling", "PerformanceMonitor.Darling.Service", "DarlingServerConnector.cs");
        var probe = text.IndexOf("public static async Task<ConnectionProbeResult> ProbeAsync", StringComparison.Ordinal);
        Assert.True(probe > 0);
        var end = text.IndexOf("public static string DescribeProbeFacts", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("AwsRole", text[probe..end], StringComparison.Ordinal);
        Assert.DoesNotContain("AwsExternalId", text[probe..end], StringComparison.Ordinal);
    }
}
