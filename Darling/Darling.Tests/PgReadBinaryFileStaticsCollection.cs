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
/// Serializes the test classes that mutate <c>PgReadBinaryFileCapability</c>'s and
/// <c>PgReadBinaryFileAdvisory</c>'s process-wide statics (#4046 part 1c). Both classes reset the
/// capability cache and probe the same target keys. Run in parallel, one class's reset or fresh cache
/// entry can land inside the other's cache-hit or TTL-expiry assertion and fail it on timing alone.
/// </summary>
[CollectionDefinition("pg-read-binary-file-statics", DisableParallelization = true)]
public sealed class PgReadBinaryFileStaticsCollection
{
}
