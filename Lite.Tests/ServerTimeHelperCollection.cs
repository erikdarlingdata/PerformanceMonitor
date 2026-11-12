/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Lite.Tests;

/// <summary>
/// The definition of the <c>server-time-helper</c> collection (#8164). Its members set <c>ServerTimeHelper</c>'s
/// <c>CurrentDisplayMode</c>, <c>ActiveServerClock</c> and <c>UtcOffsetMinutes</c>, three process-wide statics, and
/// restore them when they finish. <c>ServerTimeHelperCollectionTests</c> keeps every class that reads or writes them in
/// the collection, which makes the members safe against each other. That does nothing for the rest of the assembly: the
/// product code that reads the statics (the local data service, the server tab, the main window and the time context)
/// is reached by hundreds of classes in other collections, and xunit runs separate collections in parallel. #4776 was
/// that, a banner four hours off in a class that never named the helper.
/// <see cref="CollectionDefinitionAttribute.DisableParallelization"/> runs the collection alone, after the parallel
/// classes, so no other class is mid-assertion while the clock is swapped.
/// </summary>
[CollectionDefinition("server-time-helper", DisableParallelization = true)]
public sealed class ServerTimeHelperCollection
{
    public const string Name = "server-time-helper";
}
