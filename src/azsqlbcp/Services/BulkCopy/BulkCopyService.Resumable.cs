using AzSqlBcp.Commands;
using AzSqlBcp.Services;
using Spectre.Console;

namespace AzSqlBcp.Services.BulkCopy;

public sealed partial class BulkCopyService
{
    public async Task<int> CopyAsync(CopySettings settings, CancellationToken cancellationToken = default)
    {
        if (settings.IsResumable)
            return await CopyResumableAsync(settings, cancellationToken).ConfigureAwait(false);

        await CopyLegacyAsync(settings, cancellationToken).ConfigureAwait(false);
        return 0;
    }

    private async Task<int> CopyResumableAsync(CopySettings settings, CancellationToken cancellationToken)
    {
        if (settings.NoPartitionColumn)
            throw new InvalidOperationException("Resumable mode requires --partition-column.");

        var checkpointPath = Path.GetFullPath(settings.CheckpointFile!);
        using var store = CopyCheckpointStore.Open(checkpointPath);

        var sourceQualified = QuoteTable(settings.SourceTable);
        var targetQualified = QuoteTable(settings.TargetTable);
        var partitionQuoted = QuoteIdent(settings.PartitionColumn!);

        CopyCheckpoint checkpoint;
        var isResume = settings.Resume;

        if (store.Exists && settings.Resume)
        {
            checkpoint = store.Load();
            ValidateCheckpointJob(checkpoint, settings);
            ApplyRuntimeKnobs(checkpoint, settings, store);
            AnsiConsole.MarkupLine($"[grey]Resuming from checkpoint[/] {Markup.Escape(checkpointPath)}...");
        }
        else if (store.Exists && settings.Force)
        {
            checkpoint = await CreateNewCheckpointAsync(
                settings, store, sourceQualified, targetQualified, partitionQuoted, cancellationToken)
                .ConfigureAwait(false);
            isResume = false;
        }
        else if (store.Exists)
        {
            throw new InvalidOperationException(
                $"Checkpoint file already exists: {checkpointPath}. Use --resume to continue or --force to restart.");
        }
        else
        {
            checkpoint = await CreateNewCheckpointAsync(
                settings, store, sourceQualified, targetQualified, partitionQuoted, cancellationToken)
                .ConfigureAwait(false);
            isResume = false;
        }

        if (settings.EffectivePartitions < settings.Parallelism)
        {
            AnsiConsole.MarkupLine(
                "[yellow]Warning:[/] --partitions is less than --parallelism; not all parallel workers may be utilized.");
        }

        if (!isResume)
        {
            AnsiConsole.MarkupLine($"[grey]Truncating[/] {Markup.Escape(targetQualified)}...");
            var token = await tokenCache.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);
            await TruncateAsync(
                settings.TargetServer, settings.TargetDatabase, targetQualified, token,
                settings.TargetPort, settings.TargetTrustServerCertificate, cancellationToken)
                .ConfigureAwait(false);
            AnsiConsole.MarkupLine("[grey]Truncate complete.[/]");
        }

        await ReconcileCheckpointAsync(
            settings, checkpoint, sourceQualified, targetQualified, partitionQuoted, store, cancellationToken)
            .ConfigureAwait(false);

        var pending = checkpoint.Plan.Ranges
            .Where(r => r.Status is PartitionStatus.Pending or PartitionStatus.InProgress or PartitionStatus.Failed)
            .ToList();

        if (pending.Count == 0)
        {
            AnsiConsole.MarkupLine("[green]All partitions already completed.[/]");
            WriteDoneFromCheckpoint(checkpoint);
            return 0;
        }

        AnsiConsole.MarkupLine(
            $"[grey]Copying[/] {Markup.Escape(sourceQualified)} [grey]->[/] {Markup.Escape(targetQualified)} " +
            $"[grey]({checkpoint.Plan.Ranges.Count} partitions, {pending.Count} remaining, {checkpoint.Plan.TotalRowCount:N0} rows, batch {settings.BatchSize:N0})[/]");

        if (checkpoint.ChangeTracking.Enabled && checkpoint.ChangeTracking.SyncStartVersion is long version)
        {
            AnsiConsole.MarkupLine(
                $"[grey]Change tracking baseline version[/] {version}");
        }

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = settings.Parallelism,
            CancellationToken = cancellationToken
        };

        CopyProgressReporter? progress = null;
        var failed = 0;

        await AnsiConsole.Progress()
            .AutoClear(false)
            .HideCompleted(false)
            .Columns(
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new FrozenElapsedTimeColumn(),
                new SpinnerColumn())
            .StartAsync(async ctx =>
            {
                progress = new CopyProgressReporter(ctx, checkpoint.Plan.Ranges, checkpoint.Plan.TotalRowCount);

                await Parallel.ForEachAsync(pending, options, async (entry, ct) =>
                {
                    var success = await ExecutePartitionWithRetryAsync(
                        settings, checkpoint, store, entry, sourceQualified, targetQualified,
                        partitionQuoted, progress, ct).ConfigureAwait(false);

                    if (!success)
                        Interlocked.Increment(ref failed);
                }).ConfigureAwait(false);
            }).ConfigureAwait(false);

        if (failed > 0)
        {
            AnsiConsole.MarkupLine($"[red]{failed} partition(s) failed after retries. Re-run with --resume to continue.[/]");
            return 1;
        }

        WriteDone(progress);
        return 0;
    }

    private async Task<CopyCheckpoint> CreateNewCheckpointAsync(
        CopySettings settings,
        CopyCheckpointStore store,
        string sourceQualified,
        string targetQualified,
        string partitionQuoted,
        CancellationToken cancellationToken)
    {
        var token = await tokenCache.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);

        AnsiConsole.MarkupLine($"[grey]Reading bounds/count for[/] {Markup.Escape(sourceQualified)}...");
        var (minId, maxId, totalRowCount) = await GetBoundsAsync(
            settings.SourceServer, settings.SourceDatabase, sourceQualified, partitionQuoted, token,
            settings.SourcePort, settings.SourceReadOnly, settings.SourceTrustServerCertificate, cancellationToken)
            .ConfigureAwait(false);

        if (minId is null || maxId is null || totalRowCount == 0)
            throw new InvalidOperationException("Source table is empty; nothing to copy.");

        AnsiConsole.MarkupLine($"[grey]Building partition plan ({settings.PartitionStrategy})...[/]");
        var plan = await CopyPartitionPlan.BuildAsync(
            settings, sourceQualified, partitionQuoted, minId.Value, maxId.Value, totalRowCount,
            token, cancellationToken).ConfigureAwait(false);

        var changeTracking = await ChangeTracking.CaptureBaselineAsync(
            settings.SourceServer, settings.SourceDatabase, settings.SourceTable, token,
            settings.SourcePort, settings.SourceReadOnly, settings.SourceTrustServerCertificate, cancellationToken)
            .ConfigureAwait(false);

        if (changeTracking.Enabled && changeTracking.SyncStartVersion is long version)
        {
            AnsiConsole.MarkupLine(
                $"[grey]Change tracking enabled; sync baseline version[/] {version}");
        }

        var checkpoint = new CopyCheckpoint
        {
            Job = CopyCheckpointJob.FromSettings(settings),
            ChangeTracking = changeTracking,
            Plan = plan
        };

        store.Save(checkpoint);
        return checkpoint;
    }

    private static void ValidateCheckpointJob(CopyCheckpoint checkpoint, CopySettings settings)
    {
        var expected = CopyCheckpointJob.FromSettings(settings).ComputeFingerprint();
        var actual = checkpoint.Job.ComputeFingerprint();
        if (!string.Equals(expected, actual, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Checkpoint job parameters do not match current command-line arguments.");
        }
    }

    private static void ApplyRuntimeKnobs(CopyCheckpoint checkpoint, CopySettings settings, CopyCheckpointStore store)
    {
        var job = checkpoint.Job;
        if (job.BatchSize == settings.BatchSize && job.Parallelism == settings.Parallelism)
            return;

        if (job.BatchSize != settings.BatchSize)
        {
            AnsiConsole.MarkupLine(
                $"[grey]Updating batch size[/] {job.BatchSize:N0} [grey]->[/] {settings.BatchSize:N0}");
        }

        if (job.Parallelism != settings.Parallelism)
        {
            AnsiConsole.MarkupLine(
                $"[grey]Updating parallelism[/] {job.Parallelism} [grey]->[/] {settings.Parallelism}");
        }

        checkpoint.Job = new CopyCheckpointJob
        {
            SourceServer = job.SourceServer,
            SourceDatabase = job.SourceDatabase,
            SourceTable = job.SourceTable,
            TargetServer = job.TargetServer,
            TargetDatabase = job.TargetDatabase,
            TargetTable = job.TargetTable,
            PartitionColumn = job.PartitionColumn,
            PartitionStrategy = job.PartitionStrategy,
            Partitions = job.Partitions,
            Parallelism = settings.Parallelism,
            BatchSize = settings.BatchSize
        };

        store.Save(checkpoint);
    }

    private async Task ReconcileCheckpointAsync(
        CopySettings settings,
        CopyCheckpoint checkpoint,
        string sourceQualified,
        string targetQualified,
        string partitionQuoted,
        CopyCheckpointStore store,
        CancellationToken cancellationToken)
    {
        var toReconcile = checkpoint.Plan.Ranges
            .Where(r => r.Status is PartitionStatus.InProgress or PartitionStatus.Failed && r.ExpectedRows > 0)
            .ToList();

        var changed = false;
        foreach (var entry in checkpoint.Plan.Ranges)
        {
            if (entry.Status == PartitionStatus.Completed)
                continue;

            if (entry.ExpectedRows == 0)
            {
                entry.Status = PartitionStatus.Completed;
                entry.RowsCopied = 0;
                entry.CompletedUtc = DateTimeOffset.UtcNow;
                changed = true;
            }
        }

        if (toReconcile.Count == 0)
        {
            if (changed)
                store.Save(checkpoint);
            return;
        }

        AnsiConsole.MarkupLine(
            $"[grey]Reconciling[/] {toReconcile.Count} in-progress partition(s) (count verification)...");

        var token = await tokenCache.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);
        var semaphore = new SemaphoreSlim(Math.Min(4, settings.Parallelism));

        var tasks = toReconcile.Select(async entry =>
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var targetCount = await GetRangeCountAsync(
                    settings.TargetServer, settings.TargetDatabase, targetQualified, partitionQuoted,
                    entry.Lo, entry.Hi, token, settings.TargetPort, readOnlyIntent: false,
                    settings.TargetTrustServerCertificate, cancellationToken).ConfigureAwait(false);

                if (targetCount != entry.ExpectedRows)
                    return false;

                var sourceCount = await GetRangeCountAsync(
                    settings.SourceServer, settings.SourceDatabase, sourceQualified, partitionQuoted,
                    entry.Lo, entry.Hi, token, settings.SourcePort, settings.SourceReadOnly,
                    settings.SourceTrustServerCertificate, cancellationToken).ConfigureAwait(false);

                if (sourceCount != targetCount)
                    return false;

                entry.Status = PartitionStatus.Completed;
                entry.RowsCopied = targetCount;
                entry.CompletedUtc = DateTimeOffset.UtcNow;
                AnsiConsole.MarkupLine(
                    $"[grey]Partition P{entry.Index} already complete[/] ({targetCount:N0} rows)");
                return true;
            }
            finally
            {
                semaphore.Release();
            }
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        if (results.Any(r => r))
            changed = true;

        if (changed)
            store.Save(checkpoint);
    }

    private async Task<bool> ExecutePartitionWithRetryAsync(
        CopySettings settings,
        CopyCheckpoint checkpoint,
        CopyCheckpointStore store,
        PartitionPlanEntry entry,
        string sourceQualified,
        string targetQualified,
        string partitionQuoted,
        CopyProgressReporter progress,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            entry.Attempts++;
            entry.Status = PartitionStatus.InProgress;
            store.Save(checkpoint);

            try
            {
                await CommitPartitionAsync(
                    settings, entry, sourceQualified, targetQualified, partitionQuoted, progress, cancellationToken)
                    .ConfigureAwait(false);

                entry.Status = PartitionStatus.Completed;
                entry.CompletedUtc = DateTimeOffset.UtcNow;
                store.Save(checkpoint);
                return true;
            }
            catch (Exception ex) when (SqlTransient.IsTransient(ex))
            {
                entry.Status = PartitionStatus.Failed;
                store.Save(checkpoint);

                if (entry.Attempts > settings.MaxRetries)
                {
                    AnsiConsole.MarkupLine(
                        $"[red]Partition P{entry.Index} failed after {settings.MaxRetries} retries:[/] {Markup.Escape(ex.Message)}");
                    return false;
                }

                AnsiConsole.MarkupLine(
                    $"[yellow]Transient error on P{entry.Index} (attempt {entry.Attempts}/{settings.MaxRetries}):[/] {Markup.Escape(ex.Message)}");
                await SqlTransient.DelayBeforeRetryAsync(entry.Attempts, settings.RetryBaseDelayMs, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                entry.Status = PartitionStatus.Failed;
                store.Save(checkpoint);
                AnsiConsole.MarkupLine(
                    $"[red]Partition P{entry.Index} failed:[/] {Markup.Escape(ex.Message)}");
                return false;
            }
        }
    }

    private async Task CommitPartitionAsync(
        CopySettings settings,
        PartitionPlanEntry entry,
        string sourceQualified,
        string targetQualified,
        string partitionQuoted,
        CopyProgressReporter progress,
        CancellationToken cancellationToken)
    {
        if (entry.ExpectedRows == 0)
        {
            progress.Complete(entry.Index, 0);
            return;
        }

        var token = await tokenCache.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);

        await DeletePartitionSliceAsync(
            settings.TargetServer, settings.TargetDatabase, targetQualified, partitionQuoted,
            entry.Lo, entry.Hi, token, settings.TargetPort, settings.TargetTrustServerCertificate, cancellationToken)
            .ConfigureAwait(false);

        await CopyPartitionAsync(
            settings.SourceServer, settings.SourceDatabase, sourceQualified,
            settings.TargetServer, settings.TargetDatabase, targetQualified,
            partitionQuoted, entry.ToIdRange(), settings.BatchSize, token,
            settings.SourceReadOnly, settings.SourcePort, settings.TargetPort,
            settings.SourceTrustServerCertificate, settings.TargetTrustServerCertificate,
            progress, cancellationToken, useTableLock: false, finalizeProgress: false).ConfigureAwait(false);

        var sourceCount = await GetRangeCountAsync(
            settings.SourceServer, settings.SourceDatabase, sourceQualified, partitionQuoted,
            entry.Lo, entry.Hi, token, settings.SourcePort, settings.SourceReadOnly,
            settings.SourceTrustServerCertificate, cancellationToken).ConfigureAwait(false);

        var targetCount = await GetRangeCountAsync(
            settings.TargetServer, settings.TargetDatabase, targetQualified, partitionQuoted,
            entry.Lo, entry.Hi, token, settings.TargetPort, readOnlyIntent: false,
            settings.TargetTrustServerCertificate, cancellationToken).ConfigureAwait(false);

        if (sourceCount != targetCount || sourceCount != entry.ExpectedRows)
        {
            throw new InvalidOperationException(
                $"Partition P{entry.Index} verification failed: source={sourceCount}, target={targetCount}, expected={entry.ExpectedRows}.");
        }

        entry.RowsCopied = targetCount;
        progress.Complete(entry.Index, targetCount);
    }

    private static void WriteDoneFromCheckpoint(CopyCheckpoint checkpoint)
    {
        var total = checkpoint.Plan.Ranges.Sum(r => r.RowsCopied);
        AnsiConsole.MarkupLine($"[green]Done.[/] {total:N0} rows copied.");
    }
}
