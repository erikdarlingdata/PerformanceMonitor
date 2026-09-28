/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>
/// Sets <see cref="PgSettingRedactor.MatchTimeoutForTest"/> to a generous 10s for the current async flow and
/// restores the PRIOR value (not null) on <see cref="Dispose"/>. <c>MatchTimeoutForTest</c> is
/// <c>AsyncLocal</c>-backed: a value set in an async test body before an <c>await</c> flows into everything
/// that await reaches, and cannot leak into other tests. For tests whose redacted output must come from the
/// rules, not from a whole-value timeout mask; never for a test that exercises the regex timeout path.
/// </summary>
internal sealed class GenerousRedactorTimeout : IDisposable
{
    internal static readonly TimeSpan Value = TimeSpan.FromSeconds(10);

    private readonly TimeSpan? _prior;

    private GenerousRedactorTimeout()
    {
        _prior = PgSettingRedactor.MatchTimeoutForTest;
        PgSettingRedactor.MatchTimeoutForTest = Value;
    }

    internal static GenerousRedactorTimeout Begin() => new();

    public void Dispose() => PgSettingRedactor.MatchTimeoutForTest = _prior;
}
