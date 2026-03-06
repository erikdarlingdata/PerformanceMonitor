/*
 * SQL Server Performance Monitor Dashboard
 *
 * Helper for parsing and evaluating date filter expressions
 * Supports: >, <, >=, <=, ranges, relative dates (today, yesterday, last 7 days, etc.)
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitorDashboard.Helpers
{
    public static class DateFilterHelper
    {
        public static bool MatchesFilter(object? value, string? filterText)
        {
            if (value == null || string.IsNullOrWhiteSpace(filterText))
                return true;

            filterText = filterText.Trim();

            // Try to convert the value to DateTime
            if (!TryConvertToDateTime(value, out DateTime dateValue))
                return true; // If can't convert, don't filter out

            // Parse the filter expression
            try
            {
                // Check for range with hyphen delimiter between quoted dates: '2026-01-01'-'2026-01-10' or "2026-01-01"-"2026-01-10"
                if (filterText.Contains("'-'", StringComparison.Ordinal) || filterText.Contains("\"-\"", StringComparison.Ordinal))
                {
                    return EvaluateQuotedRange(dateValue, filterText);
                }
                // Check for range: "2026-01-01..2026-01-31" or "today..yesterday"
                if (filterText.Contains("..", StringComparison.Ordinal))
                {
                    return EvaluateRange(dateValue, filterText);
                }
                // Check for >=
                else if (filterText.StartsWith(">=", StringComparison.Ordinal))
                {
                    var threshold = ParseDateExpression(filterText.Substring(2).Trim());
                    if (threshold.HasValue)
                        return dateValue >= threshold.Value;
                }
                // Check for <=
                else if (filterText.StartsWith("<=", StringComparison.Ordinal))
                {
                    var threshold = ParseDateExpression(filterText.Substring(2).Trim());
                    if (threshold.HasValue)
                        return dateValue <= threshold.Value;
                }
                // Check for >
                else if (filterText.StartsWith('>'))
                {
                    var threshold = ParseDateExpression(filterText.Substring(1).Trim());
                    if (threshold.HasValue)
                        return dateValue > threshold.Value;
                }
                // Check for <
                else if (filterText.StartsWith('<'))
                {
                    var threshold = ParseDateExpression(filterText.Substring(1).Trim());
                    if (threshold.HasValue)
                        return dateValue < threshold.Value;
                }
                // Exact match or relative expression
                else
                {
                    var threshold = ParseDateExpression(filterText);
                    if (threshold.HasValue)
                    {
                        // For relative expressions like "today", match the entire day
                        if (IsRelativeExpression(filterText))
                        {
                            return dateValue.Date == threshold.Value.Date;
                        }
                        else
                        {
                            // For absolute dates, match within 1 second
                            return Math.Abs((dateValue - threshold.Value).TotalSeconds) < 1;
                        }
                    }
                }
            }
            catch
            {
                // If parsing fails, don't filter out
            }

            return true; // If filter is invalid, don't filter out
        }

        private static bool TryConvertToDateTime(object value, out DateTime result)
        {
            result = DateTime.MinValue;

            if (value == null)
                return false;

            if (value is DateTime dt)
            {
                result = dt;
                return true;
            }

            if (value is string str)
            {
                return DateTime.TryParse(str, out result);
            }

            try
            {
                result = Convert.ToDateTime(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static DateTime? ParseDateExpression(string expression)
        {
            expression = expression.Trim();

            // Strip surrounding quotes (single or double)
            if ((expression.StartsWith('\'') && expression.EndsWith('\'')) ||
                (expression.StartsWith('"') && expression.EndsWith('"')))
            {
                expression = expression.Substring(1, expression.Length - 2).Trim();
            }

            string expressionLower = expression.ToLowerInvariant();

            // Relative expressions
            switch (expressionLower)
            {
                case "today":
                    return ServerTimeHelper.ServerNow.Date;
                case "yesterday":
                    return ServerTimeHelper.ServerNow.Date.AddDays(-1);
                case "tomorrow":
                    return ServerTimeHelper.ServerNow.Date.AddDays(1);
                case "now":
                    return ServerTimeHelper.ServerNow;
            }

            // "last N hours/days/weeks" expressions
            var lastMatch = Regex.Match(expressionLower, @"last\s+(\d+)\s+(hour|hours|day|days|week|weeks|month|months)");
            if (lastMatch.Success)
            {
                int count = int.Parse(lastMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                string unit = lastMatch.Groups[2].Value;

                if (unit.StartsWith("hour", StringComparison.Ordinal))
                    return ServerTimeHelper.ServerNow.AddHours(-count);
                else if (unit.StartsWith("day", StringComparison.Ordinal))
                    return ServerTimeHelper.ServerNow.AddDays(-count);
                else if (unit.StartsWith("week", StringComparison.Ordinal))
                    return ServerTimeHelper.ServerNow.AddDays(-count * 7);
                else if (unit.StartsWith("month", StringComparison.Ordinal))
                    return ServerTimeHelper.ServerNow.AddMonths(-count);
            }

            // Try to parse as absolute date (use original case for proper parsing)
            if (DateTime.TryParse(expression, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
            {
                return result;
            }

            return null;
        }

        private static bool EvaluateRange(DateTime value, string rangeText)
        {
            var parts = rangeText.Split(new[] { ".." }, StringSplitOptions.None);
            if (parts.Length == 2)
            {
                var min = ParseDateExpression(parts[0].Trim());
                var max = ParseDateExpression(parts[1].Trim());

                if (min.HasValue && max.HasValue)
                {
                    // For date ranges, include the entire end date (up to 23:59:59)
                    var maxEndOfDay = max.Value.Date.AddDays(1).AddTicks(-1);
                    return value >= min.Value.Date && value <= maxEndOfDay;
                }
            }

            return true; // Invalid range, don't filter
        }

        private static bool EvaluateQuotedRange(DateTime value, string rangeText)
        {
            // Handle formats like: '2026-01-01'-'2026-01-10' or "2026-01-01"-"2026-01-10"
            string[] delimiters = { "'-'", "\"-\"" };

            foreach (var delimiter in delimiters)
            {
                if (rangeText.Contains(delimiter, StringComparison.Ordinal))
                {
                    var parts = rangeText.Split(new[] { delimiter }, StringSplitOptions.None);
                    if (parts.Length == 2)
                    {
                        // After split, parts have unbalanced quotes - strip all quotes
                        var minText = parts[0].Trim().Trim('\'', '"');
                        var maxText = parts[1].Trim().Trim('\'', '"');

                        if (DateTime.TryParse(minText, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime minDate) &&
                            DateTime.TryParse(maxText, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime maxDate))
                        {
                            // For date ranges, include the entire end date (up to 23:59:59)
                            var maxEndOfDay = maxDate.Date.AddDays(1).AddTicks(-1);
                            return value >= minDate.Date && value <= maxEndOfDay;
                        }
                    }
                    break;
                }
            }

            return true; // Invalid range, don't filter
        }

        private static bool IsRelativeExpression(string expression)
        {
            expression = expression.Trim().ToLowerInvariant();
            return expression == "today" ||
                   expression == "yesterday" ||
                   expression == "tomorrow" ||
                   Regex.IsMatch(expression, @"last\s+\d+\s+(hour|hours|day|days|week|weeks|month|months)");
        }
    }
}
