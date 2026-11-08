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
/// The definition of the <c>pg-log-format-capability-statics</c> collection (#8164).
/// <c>PgLogFormatCapability</c> keeps its per-target verdicts in one process-wide cache with a TTL and an
/// <c>Invalidate</c>, and the member asserts how many probes a sequence of reads and invalidations costs. A class in
/// another collection that reads a PostgreSQL log through the product code fills and ages the same cache while the
/// member counts. <see cref="CollectionDefinitionAttribute.DisableParallelization"/> runs the collection alone, after
/// the parallel classes.
/// </summary>
[CollectionDefinition("pg-log-format-capability-statics", DisableParallelization = true)]
public sealed class PgLogFormatCapabilityStaticsCollection
{
    public const string Name = "pg-log-format-capability-statics";
}
