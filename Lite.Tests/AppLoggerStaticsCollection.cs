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
/// The collection for every Lite test class that reads or writes <c>AppLogger</c>'s process-wide state: the one static
/// line buffer behind <c>DrainBufferedLines</c>, the minimum level and the log directory (#6113). A test that drains
/// the buffer, logs, and then counts what it drained counts a line from ANY other test that logs in that window,
/// and a collection only keeps its own classes apart from each other, not from the rest of the suite. Lite CI run
/// 39149468352 got two lines where <c>Warn_whose_text_carries_the_prefix_gets_the_hint</c> wrote one.
///
/// <para>xunit runs a <see cref="CollectionDefinitionAttribute.DisableParallelization"/> collection on its own, after
/// the parallel classes have finished, so nothing else logs while these classes run. Counting only a test's own
/// lines was the other fix; it would have touched about 20 files, where this is one.</para>
/// </summary>
[CollectionDefinition("app-logger-statics", DisableParallelization = true)]
public sealed class AppLoggerStaticsCollection
{
}
