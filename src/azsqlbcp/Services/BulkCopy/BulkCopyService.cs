using AzSqlBcp.Commands;
using AzSqlBcp.Services;
using Spectre.Console;

namespace AzSqlBcp.Services.BulkCopy;

public sealed partial class BulkCopyService(TokenCache tokenCache)
{
    private async Task CopyLegacyAsync(CopySettings settings, CancellationToken cancellationToken)
    {
        var sourceQualified = QuoteTable(settings.SourceTable);
        var targetQualified = QuoteTable(settings.TargetTable);

        var token = await tokenCache.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);

        AnsiConsole.MarkupLine($"[grey]Truncating[/] {Markup.Escape(targetQualified)}...");
        await TruncateAsync(
                settings.TargetServer, settings.TargetDatabase, targetQualified, token, settings.TargetPort,
                settings.TargetTrustServerCertificate, cancellationToken)
            .ConfigureAwait(false);
        AnsiConsole.MarkupLine("[grey]Truncate complete.[/]");

        if (settings.NoPartitionColumn)
        {
            await CopyWithoutPartitionAsync(
                settings.SourceServer, settings.SourceDatabase, sourceQualified,
                settings.TargetServer, settings.TargetDatabase, targetQualified,
                settings.BatchSize, token, settings.SourceReadOnly, settings.SourcePort, settings.TargetPort,
                settings.SourceTrustServerCertificate, settings.TargetTrustServerCertificate, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        var partitionQuoted = QuoteIdent(settings.PartitionColumn!);
        AnsiConsole.MarkupLine($"[grey]Reading bounds/count for[/] {Markup.Escape(sourceQualified)}...");
        var (minId, maxId, totalRowCount) = await GetBoundsAsync(
            settings.SourceServer, settings.SourceDatabase, sourceQualified, partitionQuoted, token,
            settings.SourcePort, settings.SourceReadOnly, settings.SourceTrustServerCertificate, cancellationToken)
            .ConfigureAwait(false);

        if (minId is null || maxId is null || totalRowCount == 0)
        {
            AnsiConsole.MarkupLine("[yellow]Source table is empty. Truncate complete; nothing to copy.[/]");
            return;
        }

        var ranges = BuildRanges(minId.Value, maxId.Value, settings.Parallelism);
        AnsiConsole.MarkupLine(
            $"[grey]Copying[/] {Markup.Escape(sourceQualified)} [grey]->[/] {Markup.Escape(targetQualified)} " +
            $"[grey]({ranges.Count} partitions, {totalRowCount:N0} rows, ids {minId}..{maxId}, batch {settings.BatchSize:N0})[/]");

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = settings.Parallelism,
            CancellationToken = cancellationToken
        };

        CopyProgressReporter? progress = null;

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
                progress = new CopyProgressReporter(ctx, ranges, totalRowCount, minId.Value, maxId.Value);

                await Parallel.ForEachAsync(ranges, options, async (range, ct) =>
                {
                    var workerToken = await tokenCache.GetAccessTokenAsync(ct).ConfigureAwait(false);
                    await CopyPartitionAsync(
                        settings.SourceServer, settings.SourceDatabase, sourceQualified,
                        settings.TargetServer, settings.TargetDatabase, targetQualified,
                        partitionQuoted, range, settings.BatchSize, workerToken, settings.SourceReadOnly,
                        settings.SourcePort, settings.TargetPort, settings.SourceTrustServerCertificate,
                        settings.TargetTrustServerCertificate, progress, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }).ConfigureAwait(false);

        WriteDone(progress);
    }

    private async Task CopyWithoutPartitionAsync(
        string sourceServer,
        string sourceDatabase,
        string sourceQualified,
        string targetServer,
        string targetDatabase,
        string targetQualified,
        int batchSize,
        string token,
        bool sourceReadOnly,
        int sourcePort,
        int targetPort,
        bool sourceTrustServerCertificate,
        bool targetTrustServerCertificate,
        CancellationToken cancellationToken)
    {
        AnsiConsole.MarkupLine($"[grey]Counting rows in[/] {Markup.Escape(sourceQualified)}...");
        var totalRowCount = await GetCountAsync(
            sourceServer, sourceDatabase, sourceQualified, token, sourcePort, sourceReadOnly,
            sourceTrustServerCertificate, cancellationToken)
            .ConfigureAwait(false);

        if (totalRowCount == 0)
        {
            AnsiConsole.MarkupLine("[yellow]Source table is empty. Truncate complete; nothing to copy.[/]");
            return;
        }

        AnsiConsole.MarkupLine(
            $"[grey]Copying[/] {Markup.Escape(sourceQualified)} [grey]->[/] {Markup.Escape(targetQualified)} " +
            $"[grey](1 stream, {totalRowCount:N0} rows, batch {batchSize:N0})[/]");

        var ranges = new List<IdRange> { new(0, 0, 0) };
        CopyProgressReporter? progress = null;

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
                progress = new CopyProgressReporter(ctx, ranges, totalRowCount, minId: 0, maxId: 0);

                var workerToken = await tokenCache.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);
                await CopyPartitionAsync(
                    sourceServer, sourceDatabase, sourceQualified,
                    targetServer, targetDatabase, targetQualified,
                    partitionQuoted: null, range: null, batchSize, workerToken, sourceReadOnly, sourcePort, targetPort,
                    sourceTrustServerCertificate, targetTrustServerCertificate, progress, cancellationToken)
                    .ConfigureAwait(false);
            }).ConfigureAwait(false);

        WriteDone(progress);
    }

    private static void WriteDone(CopyProgressReporter? progress)
    {
        var total = progress?.TotalRows ?? 0;
        var elapsed = progress?.ElapsedSeconds ?? 1;
        var avgRate = total / elapsed;
        AnsiConsole.MarkupLine($"[green]Done.[/] {total:N0} rows @ {avgRate:N0} rows/s");
    }
}
