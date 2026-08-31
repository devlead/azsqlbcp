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

        var idSpan = Math.Max(1L, unchecked(maxId - minId + 1));

        _overall = ctx.AddTask(FormatOverall(0, 0), maxValue: _totalRowCount);

        foreach (var range in ranges)
        {
            var span = Math.Max(1L, unchecked(range.Hi - range.Lo + 1));
            var partitionMax = Math.Max(1L, (long)Math.Round(totalRowCount * (double)span / idSpan));
            _partitions[range.Index] = ctx.AddTask($"P{range.Index}", maxValue: partitionMax);
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

    private void UpdateCore(int partitionIndex, long rowsCopied, bool complete)
    {
        var task = _partitions[partitionIndex];
        _partitionRows[partitionIndex] = Math.Max(rowsCopied, 0);

        task.Value = complete
            ? task.MaxValue
            : Math.Min(_partitionRows[partitionIndex], task.MaxValue);

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

    private static string FormatOverall(long total, double rowsPerSec) =>
        $"Overall  {total:N0} rows @ {rowsPerSec:N0} rows/s";
}
