/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// One acknowledge-each-risk disclosure line. In the in-app confirm dialog this
/// renders as a single checkbox the operator must tick (B3 Phase 3); on the
/// read-only surfaces (email / webhook / MCP) it renders as a disclosure bullet —
/// you cannot consent through an email. The text is FIXED/reviewed prose; the only
/// variable parts are a validated database identifier and numeric monitoring
/// figures (no data- or operator-supplied prose — no-injection parity with the SQL).
/// </summary>
public sealed record RiskItem(string Text);

/// <summary>
/// The two-sided, informed-consent risk content for a destructive remediation
/// (B3 Phase 3). "Risks of changing" are fixed per-fix-type prose plus a validated
/// identifier; "risks of not changing" are fixed framing FILLED with this server's
/// already-collected monitoring figures (the honest-both-directions property — when
/// the contention is writer/writer, the inaction side says RCSI will not resolve it
/// rather than overstate the benefit). Lives in the shared Analysis lib so it is
/// consistent across every surface; only the in-app dialog ENFORCES the checkbox gate.
/// </summary>
public sealed record RiskDisclosure(
    IReadOnlyList<RiskItem> RisksOfChanging,
    IReadOnlyList<RiskItem> RisksOfNotChanging);
