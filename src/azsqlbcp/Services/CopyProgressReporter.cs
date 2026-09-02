using System.Diagnostics;
using Spectre.Console;

namespace AzSqlBcp.Services;

public sealed class CopyProgressReporter
{
    private readonly object _lock = new();
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private readonly ProgressTask _overall;
    private readonly ProgressTask[] _partitions;
    private readonly long[] _partitionRows;
    private readonly long _totalRowCount;
    private readonly PartitionPlanEntry[]? _planEntries;
    private readonly BulkCopyService.IdRange[]? _legacyRanges;

    public CopyProgressReporter(
        ProgressContext ctx,
        IReadOnlyList<PartitionPlanEntry> entries,
        long totalRowCount)
    {
        _totalRowCount = Math.Max(totalRowCount, 1);
        _partitionRows = new long[entries.Count];
        _partitions = new ProgressTask[entries.Count];
        _planEntries = entries.ToArray();
        _legacyRanges = null;

        _overall = ctx.AddTask(FormatOverall(0, 0), maxValue: _totalRowCount);

        foreach (var entry in entries)
        {
            var maxValue = Math.Max(1L, entry.ExpectedRows);
            var isPending = entry.Status is not PartitionStatus.Completed and not PartitionStatus.InProgress;
            var initialRows = entry.Status == PartitionStatus.Completed
                ? entry.RowsCopied
                : 0L;
            var displayValue = entry.Status == PartitionStatus.Completed
                ? maxValue
                : initialRows;

            _partitions[entry.Index] = ctx.AddTask(
                FormatPartitionLabel(entry, displayValue, maxValue, pending: isPending),
                autoStart: false,
                maxValue: maxValue);

            if (entry.Status == PartitionStatus.Completed)
            {
                _partitionRows[entry.Index] = entry.RowsCopied;
                _partitions[entry.Index].Value = maxValue;
                _partitions[entry.Index].StopTask();
            }
            else if (entry.Status == PartitionStatus.InProgress)
            {
                _partitions[entry.Index].StartTask();
            }
        }

        var initialTotal = _partitionRows.Sum();
        _overall.Value = Math.Min(initialTotal, _totalRowCount);
        _overall.Description = FormatOverall(initialTotal, initialTotal / ElapsedSeconds);
    }

    public CopyProgressReporter(
        ProgressContext ctx,
        IReadOnlyList<BulkCopyService.IdRange> ranges,
        long totalRowCount,
        long minId,
        long maxId)
    {
        _totalRowCount = Math.Max(totalRowCount, 1);
        _partitionRows = new long[ranges.Count];
        _partitions = new ProgressTask[ranges.Count];
        _planEntries = null;
        _legacyRanges = ranges.ToArray();

        var idSpan = Math.Max(1L, unchecked(maxId - minId + 1));

        _overall = ctx.AddTask(FormatOverall(0, 0), maxValue: _totalRowCount);

        foreach (var range in ranges)
        {
            var span = Math.Max(1L, unchecked(range.Hi - range.Lo + 1));
            var partitionMax = Math.Max(1L, (long)Math.Round(totalRowCount * (double)span / idSpan));
            _partitions[range.Index] = ctx.AddTask(
                FormatLegacyPartitionLabel(range, pending: true),
                autoStart: false,
                maxValue: partitionMax);
        }
    }

    public long TotalRows
    {
        get
        {
            lock (_lock)
                return _partitionRows.Sum();
        }
    }

    public double ElapsedSeconds => Math.Max(_stopwatch.Elapsed.TotalSeconds, 0.001);

    public void Start(int partitionIndex)
    {
        lock (_lock)
            EnsureStarted(partitionIndex);
    }

    public void Report(int partitionIndex, long rowsCopied)
    {
        lock (_lock)
            UpdateCore(partitionIndex, rowsCopied, complete: false);
    }

    public void Complete(int partitionIndex, long rowsCopied)
    {
        lock (_lock)
            UpdateCore(partitionIndex, rowsCopied, complete: true);
    }

    private void EnsureStarted(int partitionIndex)
    {
        var task = _partitions[partitionIndex];
        if (!task.IsStarted)
            task.StartTask();

        RefreshPartitionLabel(partitionIndex, task.Value, task.MaxValue, pending: false);
    }

    private void UpdateCore(int partitionIndex, long rowsCopied, bool complete)
    {
        EnsureStarted(partitionIndex);

        var task = _partitions[partitionIndex];
        _partitionRows[partitionIndex] = Math.Max(rowsCopied, 0);

        task.Value = complete
            ? task.MaxValue
            : Math.Min(_partitionRows[partitionIndex], task.MaxValue);

        RefreshPartitionLabel(partitionIndex, task.Value, task.MaxValue, pending: false);

        var total = _partitionRows.Sum();
        _overall.Value = Math.Min(total, _totalRowCount);
        _overall.Description = FormatOverall(total, total / ElapsedSeconds);

        if (complete && !task.IsFinished)
            task.StopTask();

        if (_partitions.All(t => t.IsFinished) && !_overall.IsFinished)
        {
            _overall.Value = _totalRowCount;
            _overall.StopTask();
        }
    }

    private void RefreshPartitionLabel(int partitionIndex, double value, double maxValue, bool pending)
    {
        if (_planEntries is not null)
        {
            _partitions[partitionIndex].Description =
                FormatPartitionLabel(_planEntries[partitionIndex], value, maxValue, pending);
            return;
        }

        if (_legacyRanges is not null)
        {
            _partitions[partitionIndex].Description =
                FormatLegacyPartitionLabel(_legacyRanges[partitionIndex], pending);
        }
    }

    private static string FormatPartitionLabel(
        PartitionPlanEntry entry,
        double value,
        double maxValue,
        bool pending)
    {
        var text = Markup.Escape($"P{entry.Index} [{entry.Lo}-{entry.Hi}] {value:N0}/{maxValue:N0}");
        return pending ? $"[grey]{text}[/]" : text;
    }

    private static string FormatLegacyPartitionLabel(BulkCopyService.IdRange range, bool pending)
    {
        var text = Markup.Escape($"P{range.Index} [{range.Lo}-{range.Hi}]");
        return pending ? $"[grey]{text}[/]" : text;
    }

    private static string FormatOverall(long total, double rowsPerSec) =>
        $"Overall  {total:N0} rows @ {rowsPerSec:N0} rows/s";
}
