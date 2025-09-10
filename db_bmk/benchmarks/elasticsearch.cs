using BenchmarkDotNet.Attributes;
using Elastic.Clients.Elasticsearch;

namespace db_bmk.benchmarks;

internal record ElasticKeyValueModel(string id, int value, string name);

[MemoryDiagnoser]
public class Elasticsearch : Base
{
    private ElasticsearchClient _client;
    private const string IndexName = "ch-bmk";
    
    [GlobalSetup(Targets = new[] { nameof(EvalInsertAsync), nameof(EvalBulkInsertAsync)})]
    public void SetupInsert()
    {
        _client = new ElasticsearchClient(new Uri("http://127.0.0.1:9200"));
        
        var indexExistsResponse = _client.Indices.Exists(IndexName);
        if (indexExistsResponse.Exists) _client.Indices.Delete(IndexName);
        
        _client.Indices.Create<ElasticKeyValueModel>(IndexName, i => i
            .Mappings(m => m
                .Properties(p => p
                    .Keyword(x => x.id)
                    .Text(x => x.name)
                    .IntegerNumber(x => x.value)
                )
            )
        );
    }
    
    [GlobalSetup(Targets = [nameof(EvalQueryAsync)])]
    public void SetupRead()
    {
        _client = new ElasticsearchClient(new Uri("http://127.0.0.1:9200"));
    }
    
    [GlobalCleanup]
    public void Cleanup() { }
    
    [Benchmark]
    [BenchmarkCategory("insert")]
    public async Task EvalInsertAsync() => await MainInsertsLoopAsync(Insert);

    [Benchmark]
    [BenchmarkCategory("insert")]
    public async Task EvalBulkInsertAsync() => await MainInsertsLoopAsync(BulkInsert);

    [Benchmark]
    [BenchmarkCategory("read")]
    public async Task<object> EvalQueryAsync() => await MainReadLoopAsync(Read);
    
    private async Task Insert(int i, int count)
    {
        foreach (var c in Enumerable.Range(i, count))
        {
            var model = new ElasticKeyValueModel(Guid.NewGuid().ToString(), c, string.Empty);
            await _client.IndexAsync(model, x => x.Index(IndexName)).ConfigureAwait(false);
        }
    }
    
    private async Task BulkInsert(int i, int count)
    {
        var models = Enumerable.Range(i, count).Select(c => new ElasticKeyValueModel(Guid.NewGuid().ToString(), c, string.Empty));
        await _client.BulkAsync(x => x.Index(IndexName).IndexMany(models));
    }
    
    private async Task<object> Read(int i)
    {
        var response = await _client.SearchAsync<ElasticKeyValueModel>(s => s
            .Indices(new []{ IndexName })
            .Query(q => q
                .Term(t => t
                    .Field(f => f.value)
                    .Value(i)
                )
            )
        );
        
        return response.Documents.Count == 0 
            ? throw new InvalidOperationException($"Value {i} not found.") 
            : response.Documents.First();
    }
}