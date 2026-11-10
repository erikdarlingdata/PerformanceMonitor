/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */
using Xunit;

/* #8395: every test in the assembly is judged after it runs; a class that changes the process-wide theme from a parallel collection fails. */
[assembly: ApplicationThemeGuard]

namespace Lite.Tests;

/// <summary>
/// The definition behind <c>[Collection("application-theme")]</c> (#8395): the collection for a test class that changes the
/// application-scope theme (<see cref="PerformanceMonitor.Ui.ThemeManager.Apply"/>, or a merge into <c>Application.Current.Resources</c>)
/// or reads a card's band brush off the STA thread, and is not already in <c>credential-manager</c>, which also runs alone.
///
/// <para>Those brushes are resolved from process-wide state at bind time, so a reader in one collection binds whatever theme a writer in
/// another collection is holding at that instant. With parallelization disabled, this collection runs on its own and no other collection
/// is mid-assertion while a theme is swapped. <see cref="ApplicationThemeGuardAttribute"/> fails a writer that is in neither this collection
/// nor another that runs alone.</para>
/// </summary>
[CollectionDefinition(ApplicationThemeGuard.CollectionName, DisableParallelization = true)]
public sealed class ApplicationThemeCollection
{
}
