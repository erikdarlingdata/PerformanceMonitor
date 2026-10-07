/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Darling service ships the assembly the AWS SDK loads to assume a role (#5431).
///
/// <para><b>The defect.</b> Every RDS and Performance Insights client is built with a region only, so
/// credentials come from the SDK's default chain, and a <c>role_arn</c> profile selected through
/// <c>AWS_PROFILE</c> is part of that chain. The SDK resolves such a profile into
/// <c>AssumeRoleAWSCredentials</c>, which calls <c>sts:AssumeRole</c> through <c>AWSSDK.SecurityToken</c>
/// and loads that assembly by name at the moment it first needs credentials. The service carried
/// <c>AWSSDK.Core</c>, <c>.RDS</c> and <c>.PI</c> but not <c>.SecurityToken</c>, so the profile was found and
/// then failed with "Assembly AWSSDK.SecurityToken could not be found or loaded" before any call reached
/// AWS — a runtime failure on a path that compiles, and nothing in the service's own source names the type,
/// so a package that is simply absent is invisible to the compiler.</para>
///
/// <para><b>What this pins.</b> The package reference is what puts the assembly in the service's
/// <c>deps.json</c> and its publish folder, and the test project references the service, so the same
/// assembly is resolvable from this test's own output. Two arms: the service's <c>deps.json</c> lists it
/// (what the publish step copies from), and the type the SDK loads resolves by assembly-qualified name.
/// Removing the <c>PackageReference</c> from the service project fails both.</para>
///
/// <para><b>What this cannot see.</b> That a given profile reaches STS, or that the role's trust policy
/// allows the host: those are AWS-side and need credentials this suite never has. The publish folder itself
/// is not inspected here; the framework-dependent publish in the release workflow copies every
/// <c>deps.json</c> library, and the PR that added this checked it by publishing.</para>
/// </summary>
public sealed class AwsAssumeRolePackageTests
{
    private const string Package = "AWSSDK.SecurityToken";

    [Fact]
    public void ServiceDepsJson_ListsTheSecurityTokenPackage()
    {
        var depsPath = Path.Combine(
            AppContext.BaseDirectory, "PerformanceMonitor.Darling.Service.deps.json");

        Assert.True(
            File.Exists(depsPath),
            $"the service's deps.json is not beside the test binaries ({depsPath}), so the test "
          + "project no longer reaches the service's dependency closure and this arm proves nothing");

        var deps = File.ReadAllText(depsPath);

        /* The library entry is "AWSSDK.SecurityToken/<version>". Matching the slash keeps any other
           mention of the package name from satisfying it. */
        Assert.Contains($"\"{Package}/", deps, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStsClientTheSdkLoadsForAssumeRole_ResolvesFromTheServiceOutput()
    {
        var type = Type.GetType(
            "Amazon.SecurityToken.AmazonSecurityTokenServiceClient, " + Package,
            throwOnError: false);

        Assert.True(
            type is not null,
            $"{Package} cannot be loaded, so a role_arn profile fails with \"Assembly {Package} could "
          + "not be found or loaded\" instead of calling sts:AssumeRole (#5431)");
    }
}
