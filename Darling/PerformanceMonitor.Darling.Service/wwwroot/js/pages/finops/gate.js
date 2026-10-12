/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * FinOps on a PostgreSQL target. Most FinOps panels read SQL Server data, and on a PostgreSQL server the read answers
 * not_collected with a paragraph that explains the gate (which collector is missing, on which engine). Seven panels in a
 * row repeating that paragraph read as noise, so a panel on a PostgreSQL target that does not apply says one short line.
 * A SQL Server target keeps the server's own sentence: there it names a real reason (an Azure SQL Database without a
 * collector, say) the reader can act on.
 */

import { emptyStrip } from "../../util.js";

export const PG_NOT_COLLECTED = "Not collected for PostgreSQL";

/** True for a list_servers row of a PostgreSQL target (postgres, aurora-postgres...); a row with no engine_kind is SQL Server. */
export function isPostgresRow(row) {
  return !!(row && typeof row.engine_kind === "string" && /postgres/i.test(row.engine_kind));
}

/** The strip for a read that answered `empty`: one plain line when a PostgreSQL target's panel does not apply, else the server's sentence. */
export function gatedEmptyStrip(res, ctx) {
  if (ctx && ctx.postgres && res && res.status === "not_collected") return emptyStrip(PG_NOT_COLLECTED);
  return emptyStrip(res.message);
}

/**
 * The same rule for the sections of a multi-section answer (Optimization, Storage Growth): on a PostgreSQL target each
 * section answered not_collected says the short line instead of the gate paragraph. The sections are copied, not changed.
 */
export function gatedSections(data, ctx) {
  if (!ctx || !ctx.postgres || !data || typeof data !== "object") return data;
  const out = { ...data };
  for (const [k, v] of Object.entries(data)) {
    if (v && typeof v === "object" && !Array.isArray(v) && v.status === "not_collected") out[k] = { ...v, message: PG_NOT_COLLECTED };
  }
  return out;
}
