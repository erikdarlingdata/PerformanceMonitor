/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Version Store (PVS)" tab. This view is not on the web yet. */

import { noticeStrip } from "../../util.js";

export const tab = {
  id: "version-store",
  label: "Version Store (PVS)",
  build(server, ctx) {
    return noticeStrip("Not on the web yet: Version Store (PVS) is available in the desktop viewer.");
  },
};
