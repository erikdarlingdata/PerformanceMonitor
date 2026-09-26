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
/// Serializes every test that flips <c>QueryStoreIntervalWide.ReadRoutingEnabled</c> on via
/// <c>QueryStoreIntervalWide.EnableReadRoutingForTests</c> (#3953). The override is a process-wide static, so two
/// members running in parallel could see each other's setting or clobber the restore on <c>Dispose</c>.
/// <c>DisableParallelization</c> keeps them serialized against each other without slowing down the rest of the
/// assembly's parallel pool.
/// </summary>
[CollectionDefinition("query-store-interval-wide-read-routing", DisableParallelization = true)]
public sealed class QueryStoreIntervalWideReadRoutingCollection
{
}
