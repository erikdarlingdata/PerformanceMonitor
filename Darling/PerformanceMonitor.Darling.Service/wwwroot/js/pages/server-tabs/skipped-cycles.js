/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { localTime, parseUtc } from "../../util.js";

/* #7660: the Skipped Cycles panel's words. The service writes `last_message` for an MCP client, so its gap is in UTC. This page prints
   every time in the browser's zone, so the gap is shown in that zone, and the sentence sits in a full-width note under the tiles instead of
   in a tile of its own (a tile that long made all seven tiles 361 px tall). */

/** The service's gap sentence: "The gap ran from 2026-11-04 18:40:00 UTC to 2026-11-04 18:41:26 UTC." */
const GAP_SENTENCE = /The gap ran from (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) UTC to (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) UTC\./;

/** The message with its gap in the browser's zone. A message with no gap sentence, or with times that do not read, is returned as sent. */
export function localGapMessage(message) {
  if (typeof message !== "string") return message;
  const m = GAP_SENTENCE.exec(message);
  if (!m) return message;
  const from = parseUtc(m[1] + "T" + m[2] + "Z");
  const to = parseUtc(m[3] + "T" + m[4] + "Z");
  if (!from || !to) return message;
  return message.replace(GAP_SENTENCE, () => "The gap ran from " + localTime(from.toISOString()) + " to " + localTime(to.toISOString()) + ".");
}

/** The Cause tile's text, in the words the sentence uses for the same cause. */
export function skippedCycleCause(step) {
  if (step == null || step === "") return null;
  if (step === "gate_wait") return "waiting for a fleet collection slot";
  if (step === "launch_guard_hold") return "memory launch guard";
  return String(step).replace(/_/g, " ");
}

/** Why the Read tile says OK: it says the check ran, and the line gives the count it found. Null when the check did not run. */
export function skippedCyclesReadReason(block) {
  if (!block || block.status !== "ok") return null;
  const n = block.count_24h;
  if (n == null) return null;
  if (n === 0) return "Read OK: the check ran and no cycle was skipped in the last 24 hours.";
  const slots = block.slots_skipped_24h;
  return "Read OK: the check ran and found " + n + (n === 1 ? " skipped cycle" : " skipped cycles") + " in the last 24 hours"
    + (slots != null ? " (" + slots + (slots === 1 ? " slot)." : " slots).") : ".");
}

/** The full-width note under the tiles: what Read OK means here, then the last skip's sentence with its gap in local time. Null when there is neither. */
export function skippedCyclesNote(data) {
  const block = data && data.skipped_cycles;
  if (!block) return null;
  const parts = [skippedCyclesReadReason(block), localGapMessage(block.last_message)].filter((p) => typeof p === "string" && p.trim());
  return parts.length ? parts.join(" ") : null;
}
