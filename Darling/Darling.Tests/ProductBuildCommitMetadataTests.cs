/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9591: the commit a build was made from rides as <c>SourceCommit</c> assembly metadata, because the informational version carries no
/// <c>+commit</c> suffix on a shipped build (#3222). <see cref="ProductBuildText.CommitForAssembly"/> reads the metadata first and the
/// suffix second, and the service-start history takes its commit from the same two places.
/// </summary>
public sealed class ProductBuildCommitMetadataTests
{
    private const string Metadata = "0123456789abcdef0123456789abcdef01234567";
    private const string Suffix = "fedcba9876543210fedcba9876543210fedcba98";

    [Theory]
    [InlineData("3.14.0", Metadata, Metadata)]
    [InlineData("3.14.0+" + Suffix, Metadata, Metadata)]
    [InlineData("3.14.0-nightly.20261125.7", Metadata, Metadata)]
    [InlineData("3.14.0+" + Suffix, null, Suffix)]
    [InlineData("3.14.0+" + Suffix, "", Suffix)]
    [InlineData("3.14.0+" + Suffix, "   ", Suffix)]
    [InlineData("3.14.0", null, null)]
    [InlineData("3.14.0", "", null)]
    [InlineData("3.14.0", "  \t ", null)]
    [InlineData("3.14.0+", null, null)]
    [InlineData("", null, null)]
    [InlineData(null, null, null)]
    [InlineData(null, Metadata, Metadata)]
    public void Commit_MetadataWinsThenTheSuffixThenNull_AndABlankMetadataValueIsIgnored(string? informational, string? metadata, string? expected)
        => Assert.Equal(expected, ProductBuildText.Commit(informational, metadata));

    [Fact]
    public void Commit_TrimsTheMetadataValue()
        => Assert.Equal(Metadata, ProductBuildText.Commit("3.14.0", "  " + Metadata + "\r\n"));

    [Fact]
    public void CommitForAssembly_ReadsTheSourceCommitMetadataOfACleanVersionBuild()
    {
        var assembly = Build("Meta1", "3.14.0", ("SourceCommit", Metadata));

        Assert.Equal(Metadata, ProductBuildText.CommitForAssembly(assembly));
    }

    [Fact]
    public void CommitForAssembly_PrefersTheMetadataOverTheInformationalVersionSuffix()
    {
        var assembly = Build("Meta2", "3.14.0+" + Suffix, ("SourceCommit", Metadata));

        Assert.Equal(Metadata, ProductBuildText.CommitForAssembly(assembly));
    }

    [Fact]
    public void CommitForAssembly_StillAnswersTheSuffixWhenThereIsNoMetadata()
    {
        var assembly = Build("Meta3", "3.14.0+" + Suffix);

        Assert.Equal(Suffix, ProductBuildText.CommitForAssembly(assembly));
    }

    [Fact]
    public void CommitForAssembly_AnswersNullWhenTheBuildCarriesNeither_EvenWithOtherMetadata()
    {
        var assembly = Build("Meta4", "3.14.0", ("RepositoryUrl", "https://example.invalid/repo"), ("SourceCommitNot", Metadata));

        Assert.Null(ProductBuildText.CommitForAssembly(assembly));
    }

    [Fact]
    public void CommitForAssembly_IgnoresABlankMetadataValueAndFallsToTheSuffix()
    {
        var assembly = Build("Meta5", "3.14.0+" + Suffix, ("SourceCommit", "  "));

        Assert.Equal(Suffix, ProductBuildText.CommitForAssembly(assembly));
    }

    [Fact]
    public void SplitBuild_NamesTheMetadataCommitAndKeepsTheCleanVersion()
    {
        var (version, commit) = ServiceStartRecorder.SplitBuild("3.14.0-nightly.20261125.7", "fallback", Metadata);

        Assert.Equal("3.14.0-nightly.20261125.7", version);
        Assert.Equal(Metadata, commit);
    }

    [Theory]
    [InlineData("3.14.0+" + Suffix, null, "3.14.0", Suffix)]
    [InlineData("3.14.0+" + Suffix, Metadata, "3.14.0", Metadata)]
    [InlineData("3.14.0", null, "3.14.0", null)]
    [InlineData("3.14.0", " ", "3.14.0", null)]
    [InlineData(null, Metadata, "fallback", Metadata)]
    [InlineData("", null, "fallback", null)]
    public void SplitBuild_ReadsTheVersionBeforeThePlusAndTheCommitFromMetadataThenSuffix(string? informational, string? metadata, string expectedVersion, string? expectedCommit)
    {
        var (version, commit) = ServiceStartRecorder.SplitBuild(informational, "fallback", metadata);

        Assert.Equal(expectedVersion, version);
        Assert.Equal(expectedCommit, commit);
    }

    /// <summary>
    /// The service assembly this test loads. CI builds from an <c>actions/checkout</c> tree, which always has a source revision, so there
    /// (<c>GITHUB_ACTIONS</c> is set) a build with no <c>SourceCommit</c> fails: that is the #9591 defect, the target in <c>Directory.Build.props</c>
    /// stamping nothing. Elsewhere a build with no source revision carries no <c>SourceCommit</c>, so there is nothing to assert and the
    /// fact skips. A build that has it must answer the same value from <see cref="DarlingCliCommands.CommitText"/>,
    /// as a full 40-character commit, because that is what the front door's <c>/version</c> and the start history serve.
    /// </summary>
    [Fact]
    public void TheServiceAssembly_WhenBuiltWithASourceRevision_AnswersItsCommitFromTheMetadata()
    {
        var stamped = ProductBuildText.MetadataCommit(typeof(DarlingCliCommands).Assembly);
        Assert.False(Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true" && string.IsNullOrWhiteSpace(stamped),
            "This CI build carries no SourceCommit metadata, though it was built from a git checkout: the AddSourceCommitAssemblyMetadata target stamped nothing.");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(stamped), "This build carries no SourceCommit metadata (no source revision when it was built), so there is no commit to read.");

        Assert.Matches(new Regex("^[0-9a-f]{40}$", RegexOptions.CultureInvariant), stamped!);
        Assert.Equal(stamped, DarlingCliCommands.CommitText());
        Assert.Equal(stamped, ServiceStartRecorder.SplitBuild(
            typeof(DarlingCliCommands).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
            DarlingCliCommands.ProductVersion(),
            stamped).Commit);
    }

    /// <summary>A dynamic assembly that carries the informational version and the given metadata pairs, the way a built one does.</summary>
    private static Assembly Build(string name, string informationalVersion, params (string Key, string Value)[] metadata)
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("ProductBuildCommitMetadataTests." + name), AssemblyBuilderAccess.Run);
        assembly.SetCustomAttribute(new CustomAttributeBuilder(
            typeof(AssemblyInformationalVersionAttribute).GetConstructor(new[] { typeof(string) })!, new object[] { informationalVersion }));
        var ctor = typeof(AssemblyMetadataAttribute).GetConstructor(new[] { typeof(string), typeof(string) })!;
        foreach (var (key, value) in metadata)
        {
            assembly.SetCustomAttribute(new CustomAttributeBuilder(ctor, new object[] { key, value }));
        }

        return assembly;
    }
}
