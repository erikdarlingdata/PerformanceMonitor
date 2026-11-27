/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8982: <c>pg_plan_capture</c> stood down because the target's newest plan-capture readiness row says
/// <c>auto_explain</c> is not in <c>shared_preload_libraries</c>, so no plan can be in its log. The message is the
/// stored sentence (what readiness saw and the remedy). The worker records it under
/// <see cref="PerformanceMonitor.Collectors.CollectorRuntimePrecondition.ExtensionMissingStatus"/>, the named skip the
/// extension collectors use for a module that is not set up, so every health surface bands it as a setup step and
/// never as a failure or a quiet success.
/// </summary>
public sealed class PgPlanCapturePreconditionException : Exception
{
    public PgPlanCapturePreconditionException(string message)
        : base(message)
    {
    }
}
