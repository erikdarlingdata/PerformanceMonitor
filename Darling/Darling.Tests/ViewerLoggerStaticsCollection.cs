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
/// The definition of the <c>viewer-logger-statics</c> collection (#8164). <c>ViewerLogger</c> is a process-wide
/// static singleton: its member initializes it, flushes it to a file of its own and shuts it down, which disposes the
/// static flush timer. Any viewer class in another collection that logs through the product code it exercises writes
/// into the same queue while the member is between its flush and its assert, and after the shutdown its lines go
/// nowhere. <see cref="CollectionDefinitionAttribute.DisableParallelization"/> runs the collection alone, after the
/// parallel classes. <c>CollectionDefinitionCensusTests</c> keeps it that way.
/// </summary>
[CollectionDefinition("viewer-logger-statics", DisableParallelization = true)]
public sealed class ViewerLoggerStaticsCollection
{
    public const string Name = "viewer-logger-statics";
}
