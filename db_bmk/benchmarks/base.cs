using BenchmarkDotNet.Attributes;

namespace db_bmk.benchmarks;

public abstract class Base
{
    protected const int MaxConcurrencyLevel = 12;

    [Params(10, 100, 1000)]
    public int NumInserts { get; set; } = 10;

    protected int NumQueries => NumInserts / 2;

    protected async Task MainInsertsLoopAsync(Func<int, int, Task> insert)
    {
        var tasks = new List<Task>();
        var maxCount = (int) Math.Ceiling((double)NumInserts / MaxConcurrencyLevel);
        var k = 0;

        while (k < NumInserts)
        {
            var inc = maxCount;

            if (k + maxCount > NumInserts)
                inc = NumInserts - k;

            tasks.Add(insert(k, inc));
            k += inc;
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    protected async Task<List<object?>> MainReadLoopAsync(Func<int, Task<object?>> read)
    {
        var tasks = new List<Task<object?>>();
        var maxCount = (int) Math.Ceiling((double)NumQueries / MaxConcurrencyLevel);
        var k = 0;

        while (k < NumQueries)
        {
            var inc = maxCount;

            if (k + maxCount > NumQueries)
                inc = NumQueries - k;

            tasks.Add(read(k));
            k += inc;
        }

        var result = await Task.WhenAll(tasks).ConfigureAwait(false);
        return result.ToList();
    }
}