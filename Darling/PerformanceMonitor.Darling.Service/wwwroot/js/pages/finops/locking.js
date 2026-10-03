/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Locking & Contention" tab. This view is not on the web yet. */

import { noticeStrip } from "../../util.js";

export const tab = {
  id: "locking",
  label: "Locking & Contention",
  build(server, ctx) {
    return noticeStrip("Not on the web yet: Locking & Contention is available in the desktop viewer.");
  },
};
