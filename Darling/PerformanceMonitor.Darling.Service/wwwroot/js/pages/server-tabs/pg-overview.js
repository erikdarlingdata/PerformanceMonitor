/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { renderPanel } from "../../panels.js";

/**
 * stat() for a tile that shows the newest values a windowed read holds and none of its window figures (#4966): the read's
 * partial-window note belongs to the panels that draw the figures, so this tile draws none. The freeze headroom and xmin
 * horizon tiles on the PostgreSQL Overview are the cases: the worst database and the winning holder are the latest reading,
 * and the window peaks and win shares sit in the grids beside the same tiles on the Vacuum tab.
 */
export function momentStat(title, read, params, stats, subtitle, span = 1, emptyText) {
  return renderPanel({ title, subtitle, read, params, viz: "stat", stats, span, emptyText, windowNote: false });
}

/* Stays local: the PostgreSQL CPU read draws one series, where the catalog entry for the SQL Server read draws three. */
export const PG_CPU_SERIES = [{ key: "cpu_percent", label: "CPU %" }];

/* State first: it is the only column anyone scans for, and "available" is the one that means a one-line fix
   is waiting. */
/* The install census (#3425), which is why this panel exists beside the row list rather than instead of it:
   the rows are the PRODUCT of databases and extension names, so a multi-database host outgrows any row cap
   and the ordering takes the CREATED rows first - precisely the population a census needs. This is one row
   per extension, aggregated server-side, so no limit can reach it.

   databases_installed and databases_outdated are reported apart and never summed into one column: both mean
   the extension exists in that database and only one of them means it is current. databases_reporting is
   below databases_total only when collection was uneven, which is a fact about the monitoring rather than
   about the server, so it is shown rather than assumed away. */
export const PG_EXTENSION_CENSUS_COLUMNS = [
  { key: "extension_name", label: "Extension" },
  { key: "databases_created", label: "Created In", format: "int", align: "right" },
  { key: "databases_installed", label: "At Default", format: "int", align: "right" },
  { key: "databases_outdated", label: "Outdated", format: "int", align: "right" },
  { key: "databases_reporting", label: "Reported", format: "int", align: "right" },
  { key: "databases_total", label: "Databases", format: "int", align: "right" },
];

export const PG_EXTENSION_COLUMNS = [
  { key: "state", label: "State" },
  { key: "extension_name", label: "Extension" },
  { key: "database_name", label: "Database" },
  { key: "installed_version", label: "Installed" },
  { key: "default_version", label: "Default" },
  { key: "monitoring_relevant", label: "We Use It", format: "bool" },
];

