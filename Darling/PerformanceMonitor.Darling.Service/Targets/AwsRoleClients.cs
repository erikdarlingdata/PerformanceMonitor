/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Amazon;
using Amazon.PI;
using Amazon.RDS;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Builds the AWS clients the RDS readers use, for a server that has its own AWS role or none (#5452).
///
/// <para><b>A server with no role</b> gets the SDK's default credential chain, exactly as before the roles existed.
/// <b>A server with a role</b> gets a client bound to the credentials <see cref="AwsRoleCredentialCache.For"/> hands back,
/// and <c>For</c> checks the allow list and the partition before it returns, so a refused role throws
/// <see cref="AwsRoleAssumeException"/> before any client is built or any AWS call is made.</para>
///
/// <para><b>A role is never read with the process credentials.</b> The public constructors still take a plain
/// <c>Func&lt;string, IAmazonRDS&gt;</c> (and the PI twin), a shape that cannot say which role to read under. Such a factory
/// is adapted so that it serves a server with no role as it always did and refuses a server that has one, with an
/// <see cref="InvalidOperationException"/>: a caller that hands in its own factory and then names a role has asked for
/// something the factory cannot do, and answering it with the host's own credentials would read the wrong account. The
/// default factories with no cache refuse a role the same way.</para>
/// </summary>
internal static class AwsRoleClients
{
    internal const string NoRoleSupportMessage =
        "This server names an AWS role, but the client factory in use cannot read under a role. "
        + "Nothing was read; the host's own credentials are not used for a server that has a role.";

    /// <summary>The RDS client factory: <paramref name="processFactory"/> for a server with no role, a role-bound client from <paramref name="roles"/> for one with a role.</summary>
    internal static Func<string, AwsRoleKey?, IAmazonRDS> Rds(Func<string, IAmazonRDS>? processFactory, AwsRoleCredentialCache? roles)
    {
        if (processFactory is not null)
        {
            return (region, role) => role is null ? processFactory(region) : throw new InvalidOperationException(NoRoleSupportMessage);
        }

        return (region, role) => role is null
            ? new AmazonRDSClient(RegionEndpoint.GetBySystemName(region))
            : new AmazonRDSClient(RequireRoles(roles).For(role.Value, region), RegionEndpoint.GetBySystemName(region));
    }

    /// <summary>The Performance Insights client factory, with the same rules as <see cref="Rds"/>.</summary>
    internal static Func<string, AwsRoleKey?, IAmazonPI> Pi(Func<string, IAmazonPI>? processFactory, AwsRoleCredentialCache? roles)
    {
        if (processFactory is not null)
        {
            return (region, role) => role is null ? processFactory(region) : throw new InvalidOperationException(NoRoleSupportMessage);
        }

        return (region, role) => role is null
            ? new AmazonPIClient(RegionEndpoint.GetBySystemName(region))
            : new AmazonPIClient(RequireRoles(roles).For(role.Value, region), RegionEndpoint.GetBySystemName(region));
    }

    private static AwsRoleCredentialCache RequireRoles(AwsRoleCredentialCache? roles)
        => roles ?? throw new InvalidOperationException(NoRoleSupportMessage);
}
