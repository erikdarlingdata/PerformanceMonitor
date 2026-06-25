/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// Maps a finding's fact key to its remediation handler. A fact key with no
    /// registered handler yields no Apply affordance (mirrors
    /// <c>FactRemediation.BuildAction</c> returning null). v1 registers exactly
    /// one handler: <see cref="ForcePlanHandler"/> for PLAN_REGRESSION.
    /// </summary>
    public sealed class RemediationHandlerRegistry
    {
        private readonly Dictionary<string, IRemediationHandler> _byKey;

        public RemediationHandlerRegistry(IEnumerable<IRemediationHandler> handlers)
        {
            _byKey = new Dictionary<string, IRemediationHandler>(StringComparer.Ordinal);
            foreach (var handler in handlers)
            {
                if (handler is null || string.IsNullOrEmpty(handler.FactKey))
                    continue;
                _byKey[handler.FactKey] = handler;
            }
        }

        /// <summary>Returns the handler for the fact key, or null if none is registered.</summary>
        public IRemediationHandler? TryGet(string? factKey)
        {
            if (string.IsNullOrEmpty(factKey))
                return null;
            return _byKey.TryGetValue(factKey, out var handler) ? handler : null;
        }
    }
}
