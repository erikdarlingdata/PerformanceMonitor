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
/// Serializes the test classes that set <c>ManagedConfMigrationSteps.FailBetweenSteps</c> (#4773), the same
/// reason <c>PgFileSettingsStaticsCollection</c> exists for its capability statics. The hook is one static
/// field shared by the whole test process, and any <c>WriteTwoSteps</c> call that runs while it is set throws the
/// simulated crash. Two classes set it to throw, and run in parallel one class's crash landed inside the other
/// class's test: <c>ManagedConfMigrationRunnerTests.RunStepA_Mismatch_RestoresAndListsTheKey</c> failed with
/// the steps class's "simulated crash between steps". <see cref="CollectionDefinitionAttribute.DisableParallelization"/>
/// also keeps every other class off the hook while these run, including a class that reaches
/// <c>WriteTwoSteps</c> only through <c>DarlingManagedPostgres</c>.
/// </summary>
[CollectionDefinition("managed-conf-crash-hook", DisableParallelization = true)]
public sealed class ManagedConfCrashHookCollection
{
}
