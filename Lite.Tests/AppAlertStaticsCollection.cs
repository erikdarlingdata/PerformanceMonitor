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
/// The definition of the <c>app-alert-statics</c> collection (#8164). Its members set <c>App.AlertsEnabled</c>,
/// <c>App.AlertDeliveryMode</c>, <c>App.GenericWebhookUrl</c> and the other alert settings on <c>App</c>, process-wide
/// statics, and forward alerts through the real deliverer under them. The members take turns because they share the
/// collection, but the readers of those settings (<c>AppAlertEngineSettings</c>, <c>LiteAlertDeliverer</c>,
/// <c>CollectionBackgroundService</c>) are exercised by classes in other collections, which xunit runs beside the
/// members. <see cref="CollectionDefinitionAttribute.DisableParallelization"/> runs the collection alone, after the
/// parallel classes, so no other class reads a setting mid-swap. <c>CollectionDefinitionCensusTests</c> keeps it that
/// way.
/// </summary>
[CollectionDefinition("app-alert-statics", DisableParallelization = true)]
public sealed class AppAlertStaticsCollection
{
    public const string Name = "app-alert-statics";
}
