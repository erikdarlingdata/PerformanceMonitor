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
/// The definition of the <c>gap-cache-serial</c> collection (#8164). Its members share
/// <c>QueryStoreIntervalWide</c>'s collection-gap cache (#4700): each clears it through <c>ResetGapCacheForTests</c>
/// and asserts, through <c>GapReadsForStoreForTests</c>, how many <c>collection_log</c> reads its own store took. The
/// cache is one process-wide static, so a member's reset while another member counted would empty the other's entry
/// and cost it a read it then fails on, which is why the members run one at a time.
///
/// <para>The collection is defined without <see cref="CollectionDefinitionAttribute.DisableParallelization"/>,
/// deliberately. The cache is keyed per store, server, floor day and cadence, and the read counter is per store;
/// every member works in a scratch database of its own (the "#1776 own-store" notes on the classes), so nothing a
/// class outside the collection does can move a member's count. The other direction is harmless too: a member's reset
/// drops a non-member's cached verdict, and the only effect on that class is one more <c>collection_log</c> read,
/// which no non-member asserts on. Most members build and migrate a scratch store, so holding the whole assembly for
/// them would lengthen the run for nothing.</para>
/// </summary>
[CollectionDefinition("gap-cache-serial")]
public sealed class GapCacheSerialCollection
{
    public const string Name = "gap-cache-serial";
}
