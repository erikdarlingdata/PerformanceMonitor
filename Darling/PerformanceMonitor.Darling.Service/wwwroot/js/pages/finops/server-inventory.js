/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Server Inventory" tab. This view is not on the web yet. The view is fleet-wide: the page's server choice is shown but not used. */

import { noticeStrip } from "../../util.js";

export const tab = {
  id: "server-inventory",
  label: "Server Inventory",
  build(server, ctx) {
    return noticeStrip("Not on the web yet: Server Inventory is available in the desktop viewer.");
  },
};
