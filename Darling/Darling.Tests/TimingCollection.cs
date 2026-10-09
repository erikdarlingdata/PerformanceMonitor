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
/// The one collection for every test class that times its own work and judges the time against a limit (#5602).
/// xunit runs a <see cref="CollectionDefinitionAttribute.DisableParallelization"/> collection on its own, after the
/// parallel classes have finished, so the timed calls get a quiet process instead of sharing CPU with the other
/// 17,000 tests (#4788: the setting redactor's scaling check; #5484: the statement filter's warm-up waits; #5561: a
/// timing test on a 27 MB plan; #5600: the cloud identity probe's step limit). The bars stay where they were: a
/// real regression still fails them, and a busy runner no longer does.
///
/// <para>This collection replaced <c>pg-setting-redactor-timing</c> and <c>statement-filter-warm-up</c>. The warm-up
/// wait tests stage a process-wide fake warm-up; running them in this collection keeps them apart from every other
/// class too, which is all their own collection did.</para>
///
/// <para>A class that times work and cannot join it (it needs the <c>live-postgres</c> collection's shared store) or
/// does not judge the time (a hang guard, a diagnostic) is named on the allow list of
/// <see cref="TimingTestCensusTests"/> with its reason.</para>
/// </summary>
[CollectionDefinition("timing", DisableParallelization = true)]
public sealed class TimingCollection
{
}
