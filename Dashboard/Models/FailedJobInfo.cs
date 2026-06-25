/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// A SQL Agent job run (step_id = 0 outcome row) that FAILED within the alert
    /// lookback window. Sourced from a live msdb.dbo.sysjobhistory query at alert-check
    /// time — failure outcomes are not part of the collected running_jobs snapshot.
    /// </summary>
    public class FailedJobInfo
    {
        public string JobName { get; set; } = "";
        public string JobId { get; set; } = "";

        /// <summary>Server-local time the failed run started (from run_date/run_time).</summary>
        public DateTime RunDateTime { get; set; }
        public int StepId { get; set; }
        public string StepName { get; set; } = "";
        public string Message { get; set; } = "";

        public string RunDateTimeFormatted => RunDateTime.ToString("yyyy-MM-dd HH:mm:ss");
    }
}
