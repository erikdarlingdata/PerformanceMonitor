/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which AWS roles the web and MCP may set (#5452): the list type, the <c>allowedAwsRoles</c> darling.json setting,
/// and where the worker sets the list in use.
/// </summary>
public sealed class AwsRoleAllowlistTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string PathRole = "arn:aws:iam::123456789012:role/team/darling-monitor";
    private const string OtherAccountRole = "arn:aws:iam::210987654321:role/darling-monitor";

    [Fact]
    public void Empty_AllowsNothing()
    {
        Assert.False(AwsRoleAllowlist.Empty.IsAllowed(Role));
        Assert.False(AwsRoleAllowlist.Empty.IsAllowed(null));
        Assert.Equal(0, AwsRoleAllowlist.Empty.Count);
    }

    [Fact]
    public void Current_StartsEmpty()
    {
        // Nothing in the test run sets it: a test names its list through the overloads.
        Assert.Same(AwsRoleAllowlist.Empty, AwsRoleAllowlist.Current);
    }

    [Fact]
    public void AnArnEntry_AllowsExactlyThatArn_CaseSensitive()
    {
        var list = AwsRoleAllowlist.From(new[] { Role });

        Assert.True(list.IsAllowed(Role));
        Assert.True(AwsRoleAllowlist.IsAllowed(Role, list));
        Assert.False(list.IsAllowed(Role.ToUpperInvariant()));
        Assert.False(list.IsAllowed("arn:aws:iam::123456789012:role/Darling-Monitor"));
        Assert.False(list.IsAllowed(PathRole));
        Assert.False(list.IsAllowed(OtherAccountRole));
    }

    [Fact]
    public void AnAccountIdEntry_AllowsEveryRoleInThatAccount_AndNoOther()
    {
        var list = AwsRoleAllowlist.From(new[] { "123456789012" });

        Assert.True(list.IsAllowed(Role));
        Assert.True(list.IsAllowed(PathRole));
        Assert.False(list.IsAllowed(OtherAccountRole));
    }

    [Fact]
    public void ABareAccountIdEntry_AllowsThatAccountInTheAwsPartitionOnly()
    {
        var list = AwsRoleAllowlist.From(new[] { "123456789012" });

        Assert.True(list.IsAllowed(Role));
        Assert.False(list.IsAllowed("arn:aws-cn:iam::123456789012:role/darling-monitor"));
        Assert.False(list.IsAllowed("arn:aws-us-gov:iam::123456789012:role/darling-monitor"));
    }

    [Fact]
    public void APartitionQualifiedAccountEntry_AllowsThatAccountInThatPartitionOnly()
    {
        var china = AwsRoleAllowlist.From(new[] { "aws-cn:123456789012" });
        Assert.True(china.IsAllowed("arn:aws-cn:iam::123456789012:role/darling-monitor"));
        Assert.True(china.IsAllowed("arn:aws-cn:iam::123456789012:role/team/darling-monitor"));
        Assert.False(china.IsAllowed(Role));
        Assert.False(china.IsAllowed("arn:aws-us-gov:iam::123456789012:role/darling-monitor"));
        Assert.False(china.IsAllowed("arn:aws-cn:iam::210987654321:role/darling-monitor"));

        var gov = AwsRoleAllowlist.From(new[] { "aws-us-gov:123456789012" });
        Assert.True(gov.IsAllowed("arn:aws-us-gov:iam::123456789012:role/darling-monitor"));
        Assert.False(gov.IsAllowed(Role));
        Assert.False(gov.IsAllowed("arn:aws-cn:iam::123456789012:role/darling-monitor"));
    }

    [Fact]
    public void TheAwsPartitionCanBeWrittenOut_AndMatchesTheBareForm()
    {
        var list = AwsRoleAllowlist.From(new[] { "aws:123456789012" });
        Assert.True(list.IsAllowed(Role));
        Assert.False(list.IsAllowed("arn:aws-cn:iam::123456789012:role/darling-monitor"));
    }

    [Fact]
    public void SeveralPartitionEntries_EachAllowTheirOwnPartition()
    {
        var list = AwsRoleAllowlist.From(new[] { "123456789012", "aws-cn:123456789012", "aws-us-gov:210987654321" });
        Assert.Equal(3, list.Count);
        Assert.True(list.IsAllowed(Role));
        Assert.True(list.IsAllowed("arn:aws-cn:iam::123456789012:role/darling-monitor"));
        Assert.False(list.IsAllowed("arn:aws-us-gov:iam::123456789012:role/darling-monitor"));
        Assert.True(list.IsAllowed("arn:aws-us-gov:iam::210987654321:role/darling-monitor"));
    }

    [Fact]
    public void AnAccountIdEntry_DoesNotMatchAnAccountIdInsideAnotherPartOfTheArn()
    {
        var list = AwsRoleAllowlist.From(new[] { "123456789012" });

        // The account id appears in the role name and path, not in the account field.
        Assert.False(list.IsAllowed("arn:aws:iam::210987654321:role/123456789012"));
        Assert.False(list.IsAllowed("arn:aws:iam::210987654321:role/123456789012/darling-monitor"));
        // Not a role ARN at all.
        Assert.False(list.IsAllowed("arn:aws:iam::123456789012:user/darling-monitor"));
        Assert.False(list.IsAllowed("123456789012"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ABlankRole_IsNeverAllowed(string? role)
    {
        var list = AwsRoleAllowlist.From(new[] { Role, "123456789012" });

        Assert.False(list.IsAllowed(role));
    }

    [Fact]
    public void TheRoleAndEntriesAreTrimmed()
    {
        var list = AwsRoleAllowlist.From(new[] { "  " + Role + " ", " 123456789012 " });

        Assert.True(list.IsAllowed(" " + Role));
        Assert.True(list.IsAllowed(OtherAccountRole.Replace("210987654321", "123456789012")));
        Assert.Equal(2, list.Count);
    }

    [Fact]
    public void From_DropsBlankAndUnusableEntries_AndCountsARepeatOnce()
    {
        var list = AwsRoleAllowlist.From(new string?[] { null, "", "  ", "not-an-arn", "12345", "1234567890123", Role, Role });

        Assert.Equal(1, list.Count);
        Assert.True(list.IsAllowed(Role));
        Assert.Same(AwsRoleAllowlist.Empty, AwsRoleAllowlist.From(new string?[] { null, "junk" }));
        Assert.Same(AwsRoleAllowlist.Empty, AwsRoleAllowlist.From(null));
    }

    [Theory]
    [InlineData(Role, true)]
    [InlineData(PathRole, true)]
    [InlineData("123456789012", true)]
    [InlineData("  123456789012  ", true)]
    [InlineData("aws-cn:123456789012", true)]
    [InlineData("aws-us-gov:123456789012", true)]
    [InlineData("  aws-cn:123456789012  ", true)]
    [InlineData("aws-cn:12345678901", false)]
    [InlineData("aws-cn:", false)]
    [InlineData(":123456789012", false)]
    [InlineData("china:123456789012", false)]
    [InlineData("aws-cn:aws-cn:123456789012", false)]
    [InlineData("12345678901", false)]
    [InlineData("1234567890123", false)]
    [InlineData("12345678901a", false)]
    [InlineData("*", false)]
    [InlineData("", false)]
    [InlineData("arn:aws:iam::123456789012:user/x", false)]
    [InlineData("arn:aws:iam::123456789012:role/", false)]
    public void ValidateEntry_AcceptsAnArnOrATwelveDigitId(string entry, bool ok)
    {
        Assert.Equal(ok, AwsRoleAllowlist.ValidateEntry(entry) is null);
    }

    [Fact]
    public void ValidateEntry_AnswersWithTheFixedSentence()
    {
        Assert.Equal(AwsRoleAllowlist.InvalidEntryMessage, AwsRoleAllowlist.ValidateEntry("junk"));
        Assert.Equal(AwsRoleAllowlist.InvalidEntryMessage, AwsRoleAllowlist.ValidateEntry(null));
    }

    [Fact]
    public void FromConfig_IsTheSettingPlusEveryServersRole()
    {
        var config = new DarlingConfig();
        config.AllowedAwsRoles.Add(Role);
        config.AllowedAwsRoles.Add("210987654321");
        config.Servers.Add(new MonitoredServer { Name = "a", AwsRoleArn = "arn:aws:iam::111111111111:role/from-server" });
        config.Servers.Add(new MonitoredServer { Name = "b" });

        var list = AwsRoleAllowlist.FromConfig(config);

        Assert.True(list.IsAllowed(Role));
        Assert.True(list.IsAllowed(OtherAccountRole));
        Assert.True(list.IsAllowed("arn:aws:iam::111111111111:role/from-server"));
        Assert.False(list.IsAllowed("arn:aws:iam::111111111111:role/other"));
        Assert.Equal(3, list.Count);
    }

    [Fact]
    public void FromConfig_WithNothingSet_AllowsNothing()
    {
        Assert.Same(AwsRoleAllowlist.Empty, AwsRoleAllowlist.FromConfig(new DarlingConfig()));
    }

    [Fact]
    public void TheSettingReadsFromDarlingJson_UnderItsCamelCaseName()
    {
        var config = DarlingConfig.Parse("""
            { "allowedAwsRoles": [ "arn:aws:iam::123456789012:role/darling-monitor", "210987654321" ] }
            """);

        Assert.Equal(new[] { Role, "210987654321" }, config.AllowedAwsRoles);
    }

    [Fact]
    public void WhenTheSettingIsOmitted_TheListIsEmpty_AndTheSampleValidatesClean()
    {
        Assert.Empty(new DarlingConfig().AllowedAwsRoles);

        var sample = DarlingConfig.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "darling.sample.json")));
        Assert.Empty(sample.AllowedAwsRoles);
        Assert.Empty(sample.Validate());
    }

    [Fact]
    public void Validate_RejectsAnEntryThatIsNeitherAnArnNorATwelveDigitId_NamingItsPosition()
    {
        var config = new DarlingConfig();
        config.Postgres.ConnectionString = "Host=localhost;Database=darling;Username=x;Password=y";
        config.Servers.Add(new MonitoredServer { Name = "sql1", Host = "sql1.example", Auth = "integrated" });
        config.AllowedAwsRoles.Add(Role);
        config.AllowedAwsRoles.Add("123456789012");
        config.AllowedAwsRoles.Add("aws-cn:123456789012");
        Assert.Empty(config.Validate());

        config.AllowedAwsRoles.Add("not-a-role");
        config.AllowedAwsRoles.Add("");
        var problems = config.Validate();

        Assert.Equal(2, problems.Count);
        Assert.Contains(problems, p => p.StartsWith("allowedAwsRoles[3]: ", StringComparison.Ordinal)
            && p.EndsWith(AwsRoleAllowlist.InvalidEntryMessage, StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("allowedAwsRoles[4]: ", StringComparison.Ordinal));
    }

    [Fact]
    public void TheWorkerSetsTheListInUseOnce_BeforeItBuildsTheRunner()
    {
        var dir = AppContext.BaseDirectory;
        string? worker = null;
        while (dir is not null)
        {
            var candidate = Path.Combine(dir, "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
            if (File.Exists(candidate))
            {
                worker = candidate;
                break;
            }

            dir = Path.GetDirectoryName(dir);
        }

        if (worker is null)
        {
            // The test host runs away from the source tree (a published layout): nothing to pin.
            return;
        }

        var text = File.ReadAllText(worker);
        var sets = Regex.Matches(text, @"AwsRoleAllowlist\.Current\s*=").Count;
        Assert.Equal(1, sets);
        Assert.True(
            text.IndexOf("AwsRoleAllowlist.Current =", StringComparison.Ordinal)
                < text.IndexOf("new DarlingCollectorRunner(", StringComparison.Ordinal),
            "The list is set before the runner is built.");
    }

    [Fact]
    public void NothingOutsideTheWorkerSetsTheListInUse()
    {
        var dir = AppContext.BaseDirectory;
        string? service = null;
        while (dir is not null)
        {
            var candidate = Path.Combine(dir, "PerformanceMonitor.Darling.Service");
            if (File.Exists(Path.Combine(candidate, "DarlingWorker.cs")))
            {
                service = candidate;
                break;
            }

            dir = Path.GetDirectoryName(dir);
        }

        if (service is null)
        {
            return;
        }

        var setters = Directory.EnumerateFiles(service, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => Regex.IsMatch(File.ReadAllText(f), @"AwsRoleAllowlist\.Current\s*="))
            .Select(Path.GetFileName)
            .ToArray();

        Assert.Equal(new[] { "DarlingWorker.cs" }, setters);
    }
}
