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
/// The definition of the <c>clipboard-text-seams</c> collection (#8164). Its members swap the static
/// <c>ClipboardText.WriteOnce</c> seam (and the backoff seams beside it) for fakes that record the calls, and restore
/// them in a <c>finally</c>. A viewer class in another collection that copies text through the product code it
/// exercises would write through the fake while it is installed, and its call would land in the member's record.
/// <see cref="CollectionDefinitionAttribute.DisableParallelization"/> runs the collection alone, after the parallel
/// classes. <c>CollectionDefinitionCensusTests</c> keeps it that way.
/// </summary>
[CollectionDefinition("clipboard-text-seams", DisableParallelization = true)]
public sealed class ClipboardTextSeamsCollection
{
    public const string Name = "clipboard-text-seams";
}
