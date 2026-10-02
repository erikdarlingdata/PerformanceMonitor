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
/// Serializes the test classes that mutate <c>PgFileSettingsCapability</c>'s process-wide statics (#4251),
/// the same reason <c>PgReadBinaryFileStaticsCollection</c> exists for its sibling capability: run in
/// parallel, one class's reset or fresh cache entry can land inside another's cache-hit or TTL-expiry
/// assertion and fail it on timing alone.
/// </summary>
[CollectionDefinition("pg-file-settings-statics", DisableParallelization = true)]
public sealed class PgFileSettingsStaticsCollection
{
}
