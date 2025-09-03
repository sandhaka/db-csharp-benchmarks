using BenchmarkDotNet.Attributes;
using MongoDB.Bson;
using MongoDB.Driver;

namespace db_bmk.benchmarks;

[MemoryDiagnoser]
public class Mongo : Base
{
    private IMongoCollection<BsonDocument> _collection;
    private IMongoClient _client;

    [GlobalSetup(Targets = new[] { nameof(EvalInsertAsync), nameof(EvalBulkInsertAsync)})]
    public void Setup()
    {
        var settings = new MongoClientSettings
        {
            Server = new MongoServerAddress("127.0.0.1", 27017),
            Credential = MongoCredential.CreateCredential("admin", "root", "example")
        };

        _client = new MongoClient(settings);

        _client.GetDatabase("benchmark").DropCollection("keyvaluecollection");

        _collection = _client.GetDatabase("benchmark")
            .GetCollection<BsonDocument>(
                "keyvaluecollection"
            );

         _collection.Indexes.CreateOne(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("value")
            )
        );
    }

    [GlobalCleanup(Targets = new[] {nameof(EvalQueryAsync)})]
    public void Cleanup()
    {
        _client.GetDatabase("benchmark").DropCollection("keyvaluecollection");
        _client.Dispose();
    }

    [Benchmark]
    [BenchmarkCategory("insert")]
    public async Task EvalInsertAsync() => await MainInsertsLoopAsync(Insert);

    [Benchmark]
    [BenchmarkCategory("insert")]
    public async Task EvalBulkInsertAsync() => await MainInsertsLoopAsync(BulkInsert);

    [Benchmark]
    [BenchmarkCategory("read")]
    public async Task<object?> EvalQueryAsync() => await MainReadLoopAsync(Read);

    private async Task Insert(int i, int count)
    {
        foreach (var c in Enumerable.Range(i, count))
        {
            var document = new BsonDocument
            {
                ["name"] = string.Empty,
                ["value"] = c
            };
            await _collection.InsertOneAsync(document).ConfigureAwait(false);
        }
    }

    private async Task BulkInsert(int i, int count)
    {
        var documents = Enumerable.Range(i, count).Select(c => new BsonDocument
        {
            ["name"] = string.Empty,
            ["value"] = c
        });

        await _collection.InsertManyAsync(documents);
    }

    private async Task<object?> Read(int i)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("value", i);
        var cursor = await _collection.FindAsync(filter).ConfigureAwait(false);
        var document = cursor.FirstOrDefault();
        if (document != null)
        {
            var name = document["name"].AsString;
            var value = document["value"].AsInt32;
            return new { name, value };
        }
        throw new InvalidOperationException($"Value {i} not found");
    }
}