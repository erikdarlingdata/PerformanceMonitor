/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// The definition of the <c>remediation-live-sql</c> collection (#8164). Its members share one external thing: the
/// test SQL Server named by <c>DARLING_TEST_SQL</c>. <c>RemediationLiveSqlTests</c> counts the sessions that carry the
/// remediation application name on that server, and a force, unforce or forced-plan read running beside it from another
/// member would change the count, so the members run one at a time. Each one makes and drops its own database and its
/// own login, so they share no rows.
///
/// <para>The collection is defined without <see cref="CollectionDefinitionAttribute.DisableParallelization"/>,
/// deliberately. No process-wide static is involved: only the remediation code opens sessions under that application
/// name, and only the members run it, so a class outside the collection cannot move the count and the members cannot
/// disturb a class outside it. The members also skip on a runner with no test SQL Server, so holding the assembly for
/// them would buy nothing there either.</para>
/// </summary>
[CollectionDefinition("remediation-live-sql")]
public sealed class RemediationLiveSqlCollection
{
    public const string Name = "remediation-live-sql";
}
