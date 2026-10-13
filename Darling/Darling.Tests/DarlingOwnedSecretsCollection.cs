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
/// Serializes the test classes that set <c>DarlingOwnedSecrets</c>, the process-wide owned set, and assert what it
/// refuses. Joining the collection serializes them against each other only. Test classes outside it change the set as
/// well, without calling <c>DarlingOwnedSecrets.Set</c>: every <c>DarlingConfig.Load</c> publishes the set computed from
/// the file it loaded, and the CLI verb tests, the host service tests and others reach one. In #5768 such a load landed
/// between <c>DarlingSecretsOwnedReferenceTests</c>' set and its next resolve, and the remediation slot resolved a
/// reference the file declares for another server id. Several of those classes are in other collections, and a class is
/// in one collection only, so <see cref="CollectionDefinitionAttribute.DisableParallelization"/> keeps every other class
/// off the set while these run instead. <c>DarlingOwnedSecretsCensusTests</c> keeps the flag and the membership.
/// </summary>
[CollectionDefinition("darling-owned-secrets", DisableParallelization = true)]
public sealed class DarlingOwnedSecretsCollection
{
}
