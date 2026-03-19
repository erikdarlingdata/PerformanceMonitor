using System;

namespace PerformanceMonitorDashboard.Models
{
    public class MuteRule
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public bool Enabled { get; set; } = true;
        public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
        public DateTime? ExpiresAtUtc { get; set; }
        public string? Reason { get; set; }

        public string? ServerName { get; set; }
        public string? MetricName { get; set; }
        public string? DatabasePattern { get; set; }
        public string? QueryTextPattern { get; set; }
        public string? WaitTypePattern { get; set; }
        public string? JobNamePattern { get; set; }

        public bool IsExpired => ExpiresAtUtc.HasValue && DateTime.UtcNow >= ExpiresAtUtc.Value;

        public MuteRule Clone() => new()
        {
            Id = Id,
            Enabled = Enabled,
            CreatedAtUtc = CreatedAtUtc,
            ExpiresAtUtc = ExpiresAtUtc,
            Reason = Reason,
            ServerName = ServerName,
            MetricName = MetricName,
            DatabasePattern = DatabasePattern,
            QueryTextPattern = QueryTextPattern,
            WaitTypePattern = WaitTypePattern,
            JobNamePattern = JobNamePattern
        };

        public string ExpiresDisplay => ExpiresAtUtc.HasValue
            ? (IsExpired ? "Expired" : ExpiresAtUtc.Value.ToLocalTime().ToString("g"))
            : "Never";

        public string Summary
        {
            get
            {
                var parts = new System.Collections.Generic.List<string>();
                if (MetricName != null) parts.Add(MetricName);
                if (ServerName != null) parts.Add($"on {ServerName}");
                if (DatabasePattern != null) parts.Add($"db≈{DatabasePattern}");
                if (QueryTextPattern != null) parts.Add($"query≈{QueryTextPattern}");
                if (WaitTypePattern != null) parts.Add($"wait≈{WaitTypePattern}");
                if (JobNamePattern != null) parts.Add($"job≈{JobNamePattern}");
                return parts.Count > 0 ? string.Join(", ", parts) : "(matches all alerts)";
            }
        }

        public bool Matches(AlertMuteContext context)
        {
            if (!Enabled || IsExpired) return false;

            if (ServerName != null &&
                !string.Equals(ServerName, context.ServerName, StringComparison.OrdinalIgnoreCase))
                return false;

            if (MetricName != null &&
                !string.Equals(MetricName, context.MetricName, StringComparison.OrdinalIgnoreCase))
                return false;

            if (DatabasePattern != null &&
                (context.DatabaseName == null ||
                 !context.DatabaseName.Contains(DatabasePattern, StringComparison.OrdinalIgnoreCase)))
                return false;

            if (QueryTextPattern != null &&
                (context.QueryText == null ||
                 !context.QueryText.Contains(QueryTextPattern, StringComparison.OrdinalIgnoreCase)))
                return false;

            if (WaitTypePattern != null &&
                (context.WaitType == null ||
                 !context.WaitType.Contains(WaitTypePattern, StringComparison.OrdinalIgnoreCase)))
                return false;

            if (JobNamePattern != null &&
                (context.JobName == null ||
                 !context.JobName.Contains(JobNamePattern, StringComparison.OrdinalIgnoreCase)))
                return false;

            return true;
        }
    }

    public class AlertMuteContext
    {
        public string ServerName { get; set; } = "";
        public string MetricName { get; set; } = "";
        public string? DatabaseName { get; set; }
        public string? QueryText { get; set; }
        public string? WaitType { get; set; }
        public string? JobName { get; set; }

        /// <summary>
        /// Extracts context fields (Database, Query, Wait Type, Job Name) from the
        /// structured detail_text stored with each alert. The format is label/value
        /// pairs indented with two spaces, e.g. "  Database: MyDB".
        /// Query values may span multiple lines and use variant labels
        /// (Blocked Query, Blocking Query, Victim SQL).
        /// </summary>
        public void PopulateFromDetailText(string? detailText)
        {
            if (string.IsNullOrEmpty(detailText)) return;

            System.Text.StringBuilder? queryBuilder = null;
            var lines = detailText.Split('\n');

            foreach (var line in lines)
            {
                var trimmed = line.TrimStart();

                if (DatabaseName == null && trimmed.StartsWith("Database: ", StringComparison.Ordinal))
                {
                    FlushQuery(ref queryBuilder);
                    DatabaseName = trimmed.Substring("Database: ".Length).Trim();
                }
                else if (WaitType == null && trimmed.StartsWith("Wait Type: ", StringComparison.Ordinal))
                {
                    FlushQuery(ref queryBuilder);
                    WaitType = trimmed.Substring("Wait Type: ".Length).Trim();
                }
                else if (JobName == null && trimmed.StartsWith("Job Name: ", StringComparison.Ordinal))
                {
                    FlushQuery(ref queryBuilder);
                    JobName = trimmed.Substring("Job Name: ".Length).Trim();
                }
                else if (QueryText == null && queryBuilder == null && TryExtractQueryValue(trimmed, out var qv))
                {
                    queryBuilder = new System.Text.StringBuilder(qv);
                }
                else if (queryBuilder != null)
                {
                    // Continuation lines from multi-line query values don't start
                    // with the two-space indent used by ContextToDetailText fields.
                    if (string.IsNullOrWhiteSpace(trimmed) || line.StartsWith("  ", StringComparison.Ordinal))
                    {
                        FlushQuery(ref queryBuilder);
                    }
                    else
                    {
                        queryBuilder.Append(' ').Append(trimmed.Trim());
                    }
                }
            }

            FlushQuery(ref queryBuilder);
        }

        private void FlushQuery(ref System.Text.StringBuilder? builder)
        {
            if (builder != null && QueryText == null)
                QueryText = builder.ToString();
            builder = null;
        }

        private static bool TryExtractQueryValue(string trimmed, out string value)
        {
            foreach (var prefix in new[] { "Query: ", "Blocked Query: ", "Blocking Query: ", "Victim SQL: " })
            {
                if (trimmed.StartsWith(prefix, StringComparison.Ordinal))
                {
                    value = trimmed.Substring(prefix.Length).Trim();
                    return true;
                }
            }
            value = "";
            return false;
        }
    }
}
