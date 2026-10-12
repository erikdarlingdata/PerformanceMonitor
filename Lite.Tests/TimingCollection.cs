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
/// The one collection for every Lite test class that times its own work and judges the time against a limit (#5602).
/// xunit runs a <see cref="CollectionDefinitionAttribute.DisableParallelization"/> collection on its own, after the
/// parallel classes have finished, so the timed calls get a quiet process instead of sharing CPU with the rest of
/// the suite. The bars stay where they were: a real regression still fails them, and a busy runner no longer does.
///
/// <para>A class that times work and cannot join it (it already sits in a collection of its own for process-wide
/// state) or does not judge the time (a hang guard, a diagnostic) is named on the allow list of
/// <c>TimingTestCensusTests</c> with its reason.</para>
/// </summary>
[CollectionDefinition("timing", DisableParallelization = true)]
public sealed class TimingCollection
{
}
