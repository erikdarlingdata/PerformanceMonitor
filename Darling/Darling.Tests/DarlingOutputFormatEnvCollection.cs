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
/// Serializes the test classes that set <c>DARLING_OUTPUT_FORMAT</c>, a process-wide environment variable the host's GCF
/// filter reads on every call unless the host pins an <c>IGcfOutputFormat</c>. Joining the collection serializes those
/// classes against each other only; a test host built elsewhere without the pin would still see <c>gcf</c> mid-call and
/// get its JSON answer re-encoded (#5878). A class is in one collection only, so
/// <see cref="CollectionDefinitionAttribute.DisableParallelization"/> keeps every other class off the variable while
/// these run instead.
/// </summary>
[CollectionDefinition("DarlingOutputFormatEnv", DisableParallelization = true)]
public sealed class DarlingOutputFormatEnvCollection
{
}
