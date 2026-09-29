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
/// Runs <c>PgSettingRedactorTests</c> with no other test class running (#4788). Its scaling check times
/// <c>PgSettingRedactor.Redact</c> at two input sizes and fails when the larger one takes 64 times as long as
/// the smaller. Run in parallel with the rest of a 17,000-test suite, a busy machine can slow every repeat of
/// the larger size past that bar, and a guard that fails on a busy machine gets re-run instead of believed.
/// <see cref="CollectionDefinitionAttribute.DisableParallelization"/> gives the timed calls a quiet process, so
/// the bar stays at 64 times and a real quadratic pattern still fails it.
/// </summary>
[CollectionDefinition("pg-setting-redactor-timing", DisableParallelization = true)]
public sealed class PgSettingRedactorTimingCollection
{
}
