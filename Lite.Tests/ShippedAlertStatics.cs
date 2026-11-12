/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Runtime.CompilerServices;
using PerformanceMonitorLite;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The values Lite's alert statics hold when the assembly loads, before any test class is constructed: the
/// initializers in <c>App.xaml.cs</c>, as a fresh process ships them. A test that pins a shipped default reads
/// these, because the statics themselves are rewritten by the classes in the <c>app-alert-statics</c> collection
/// (a resetter writes the expected value into the static in its constructor and Dispose), so reading the static
/// in a test proves only what the previous test left there.
/// </summary>
internal static class ShippedAlertStatics
{
    /// <summary><see cref="App.AlertPerEventMaxPerCycle"/> as the assembly found it.</summary>
    internal static int AlertPerEventMaxPerCycle { get; private set; }

    [ModuleInitializer]
    internal static void Record() => AlertPerEventMaxPerCycle = App.AlertPerEventMaxPerCycle;
}
