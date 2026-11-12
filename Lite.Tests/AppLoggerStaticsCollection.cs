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
/// The definition of the <c>app-logger-statics</c> collection (#8164). Its members assert on
/// <c>AppLogger.DrainBufferedLines()</c>, which drains the one process-wide queue every <c>AppLogger</c> call in the
/// process writes to: a member that expects one line, or none, is counting what the whole process logged since the last
/// drain. The members drain in turn because they share the collection, but any class in another collection logs through
/// the product code it exercises while a member is between its drain and its assert. #5863 and #6113 were that, each
/// with a different victim. <see cref="CollectionDefinitionAttribute.DisableParallelization"/> runs the collection
/// alone, after the parallel classes, so the queue holds only what the running test logged.
/// </summary>
[CollectionDefinition("app-logger-statics", DisableParallelization = true)]
public sealed class AppLoggerStaticsCollection
{
    public const string Name = "app-logger-statics";
}
