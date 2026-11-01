/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { READ_FIELDS } from "../../read-fields.js";

export const SCHEDULER_STATS = [
  { key: "pressure_level", label: "Pressure", format: "text", small: true },
  { key: "schedulers", label: "Schedulers", format: "int" },
  { key: "runnable_tasks", label: "Runnable tasks", format: "int" },
  { key: "avg_runnable_per_scheduler", label: "Runnable/sched", format: "num2" },
  { key: "runnable_percent", label: "Runnable %", format: "num1" },
  { key: "workers", label: "Workers", format: "int" },
  { key: "max_workers", label: "Max workers", format: "int" },
  { key: "worker_utilization_percent", label: "Worker use %", format: "num1" },
  { key: "active_requests", label: "Active requests", format: "int" },
  { key: "queued_requests", label: "Queued requests", format: "int" },
  { key: "recommendation", label: "Recommendation", format: "text", small: true },
];

/* ─────────────────────────── line series ─────────────────────────── */

/* Neutral series colors assigned by the chart's ramp (B1) — no severity colors on chart lines. idle_cpu is
   dropped (B3): it would force a 0-100 domain and crush the real SQL/other/total series. */
export const CPU_SERIES = READ_FIELDS.get_cpu_utilization.line.series;

