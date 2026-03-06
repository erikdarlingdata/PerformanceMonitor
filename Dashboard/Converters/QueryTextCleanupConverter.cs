/*
 * SQL Server Performance Monitor Dashboard
 *
 * Converter to clean up query text for display
 * Removes line breaks and excessive whitespace
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Windows.Data;

namespace PerformanceMonitorDashboard.Converters
{
    public class QueryTextCleanupConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
                return string.Empty;

            string text = value.ToString() ?? string.Empty;

            // Replace line breaks with spaces
            text = text.Replace("\r\n", " ", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal).Replace("\r", " ", StringComparison.Ordinal);

            // Replace tabs with spaces
            text = text.Replace("\t", " ", StringComparison.Ordinal);

            // Replace multiple spaces with single space
            text = Regex.Replace(text, @"\s+", " ");

            // Trim leading/trailing whitespace
            text = text.Trim();

            return text;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
