/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* #7660: the Skipped Cycles panel's words. The service writes `last_message` for an MCP client, so its gap is in UTC. The strip that draws the note
   (noticeStrip) prints every UTC stamp in the browser's zone, like every other time on this page, so this file only decides what the note says.
   The sentence sits in a full-width note under the tiles instead of in a tile of its own (a tile that long made all seven tiles 361 px tall). */

/** The Cause tile's text, in the words the sentence uses for the same cause. `collectors` is the service's name for "no cause was measured" (DarlingCycleSkip.UnmeasuredName). */
export function skippedCycleCause(step, ms) {
  if (step == null || step === "") return null;
  if (step === "gate_wait") return "waiting for a fleet collection slot";
  if (step === "launch_guard_hold") return "memory launch guard";
  if (step === "analysis_pass") return "scheduled analysis";
  if (step === "collectors" && !(ms > 0)) return "none measured";
  return String(step).replace(/_/g, " ");
}

/** Why the Read tile says OK on a server that skipped cycles: the check ran, and the line gives the count it found. Null when the check did not run or found none (a healthy server needs no note). */
export function skippedCyclesReadReason(block) {
  if (!block || block.status !== "ok") return null;
  const n = block.count_24h;
  if (!(n > 0)) return null;
  const slots = block.slots_skipped_24h;
  return "Read OK: the check ran and found " + n + (n === 1 ? " skipped cycle" : " skipped cycles") + " in the last 24 hours"
    + (slots != null ? " (" + slots + (slots === 1 ? " slot)." : " slots).") : ".");
}

/** The full-width note under the tiles: what Read OK means here, then the last skip's sentence. Null when there is neither, so a server that skipped nothing draws no strip. */
export function skippedCyclesNote(data) {
  const block = data && data.skipped_cycles;
  if (!block) return null;
  const parts = [skippedCyclesReadReason(block), block.last_message].filter((p) => typeof p === "string" && p.trim());
  return parts.length ? parts.join(" ") : null;
}
