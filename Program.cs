using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Streams;
using Orleans.Streams.Core;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureHostConfiguration(configHost =>
{
    configHost.AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Logging:LogLevel:Orleans"] = "Warning",
        ["Logging:LogLevel:Orleans.Streaming.EventHubs.EventHubQueueCache"] = "Debug"
    });
});

builder.UseOrleans(static siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage("PubSubStore");
    siloBuilder.AddEventHubStreams(Constants.StreamProviderName, (ISiloEventHubStreamConfigurator configurator) =>
    {
        configurator.ConfigureEventHub(builder => builder.Configure(options =>
        {
            options.ConfigureEventHubConnection(
                Constants.EventHubConnectionString,
                Constants.EHPath,
                Constants.EHConsumerGroup);
        }));
        configurator.UseAzureTableCheckpointer(
            builder => builder.Configure(options =>
            {
                options.ConfigureTableServiceClient(Constants.DataConnectionString);
                options.PersistInterval = TimeSpan.FromSeconds(1);
            }));

        configurator.ConfigureCacheEviction(cacheEvictionOptions =>
        {
            cacheEvictionOptions.Configure(options =>
            {
                options.DataMaxAgeInCache = TimeSpan.FromSeconds(5);
                options.DataMinTimeInCache = TimeSpan.FromSeconds(3);
                options.MetadataMinTimeInCache = TimeSpan.FromSeconds(1000);
            });
        });

        configurator.ConfigurePullingAgent(pullingAgentOptions =>
        {
            pullingAgentOptions.Configure(opt =>
            {
                opt.StreamInactivityPeriod = TimeSpan.FromSeconds(5);  // Stream will be deactivated
            });
        });
    });

    siloBuilder.Configure<GrainCollectionOptions>(options =>
    {
        options.CollectionQuantum = TimeSpan.FromSeconds(3);
        options.CollectionAge = TimeSpan.FromSeconds(1000); // Grain will not be deactivated
    });
});

using var host = builder.Build();
await host.StartAsync().ConfigureAwait(false);

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var consumerGrain = grainFactory.GetGrain<IConsumerGrain>(Guid.Empty);

var producerGrain = grainFactory.GetGrain<IEventProducerTestGrain>(Guid.Empty);

Console.WriteLine("Program: Producing item 1");
await producerGrain.Produce(1);

// Wait for the cached data to expire
await Task.Delay(TimeSpan.FromSeconds(10));
// Produce a few unrelated items to make sure the cache is purged
await grainFactory.GetGrain<IOtherEventProducerTestGrain>(Guid.NewGuid()).Produce(0);
await grainFactory.GetGrain<IOtherEventProducerTestGrain>(Guid.NewGuid()).Produce(0);
await grainFactory.GetGrain<IOtherEventProducerTestGrain>(Guid.NewGuid()).Produce(0);
await grainFactory.GetGrain<IOtherEventProducerTestGrain>(Guid.NewGuid()).Produce(0);
await grainFactory.GetGrain<IOtherEventProducerTestGrain>(Guid.NewGuid()).Produce(0);
await grainFactory.GetGrain<IOtherEventProducerTestGrain>(Guid.NewGuid()).Produce(0);
// Wait to make sure the cache is purged
await Task.Delay(TimeSpan.FromSeconds(10));

Console.WriteLine("Program: Producing item 2 and 3");
await producerGrain.Produce(2);
await producerGrain.Produce(3);
// Observe the console. Only item 3 is delivered to the consumer
await Task.Delay(TimeSpan.FromSeconds(5));

await host.StopAsync();
Console.WriteLine("Press any key to exit...");
Console.ReadKey();

public interface IEventProducerTestGrain : IGrainWithGuidKey
{
    Task Produce(int item);
}

public interface IOtherEventProducerTestGrain : IGrainWithGuidKey
{
    Task Produce(int item);
}

public sealed class EventProducerTestGrain : Grain, IEventProducerTestGrain
{
    private IAsyncStream<int>? _stream;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamId = StreamId.Create(Constants.NamespaceName, GrainContext.GrainId.GetGuidKey());
        _stream = this.GetStreamProvider(Constants.StreamProviderName).GetStream<int>(streamId);

        return base.OnActivateAsync(cancellationToken);
    }

    public async Task Produce(int item)
    {
        await _stream!.OnNextAsync(item);
    }
}

public sealed class OtherEventProducerTestGrain : Grain, IOtherEventProducerTestGrain
{
    private IAsyncStream<int>? _stream;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamId = StreamId.Create("ns-2", GrainContext.GrainId.GetGuidKey());
        _stream = this.GetStreamProvider(Constants.StreamProviderName).GetStream<int>(streamId);

        return base.OnActivateAsync(cancellationToken);
    }

    public async Task Produce(int item)
    {
        await _stream!.OnNextAsync(item);
    }
}

public interface IConsumerGrain : IGrainWithGuidKey
{
}

public interface IOtherConsumerGrain : IGrainWithGuidKey
{
}

[ImplicitStreamSubscription(Constants.NamespaceName)]
public class ConsumerGrain : Grain, IConsumerGrain, IStreamSubscriptionObserver
{
    private readonly LoggerObserver _observer;

    public ConsumerGrain()
    {
        _observer = new LoggerObserver();
    }

    public async Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
    {
        await handleFactory.Create<int>().ResumeAsync(observer: _observer);
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("ConsumerGrain OnActivateAsync");
        return Task.CompletedTask;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        Console.WriteLine($"ConsumerGrain OnDeactivateAsync: Reason: {reason}");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Class that will log streaming events
    /// </summary>
    private class LoggerObserver : IAsyncObserver<int>
    {
        public Task OnCompletedAsync() => Task.CompletedTask;

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine($"ConsumerGrain OnErrorAsync: Ex: {ex}");
            return Task.CompletedTask;
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            Console.WriteLine($"ConsumerGrain OnNextAsync: Item: {item}, Token = {token}");
            return Task.CompletedTask;
        }
    }
}

[ImplicitStreamSubscription("ns-2")]
public class OtherConsumerGrain : Grain, IOtherConsumerGrain, IStreamSubscriptionObserver
{
    public async Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
    {
        await handleFactory.Create<int>().ResumeAsync(observer: new NullObserver());
    }

    private class NullObserver : IAsyncObserver<int>
    {
        public Task OnCompletedAsync() => Task.CompletedTask;
        public Task OnErrorAsync(Exception ex)
        {
            return Task.CompletedTask;
        }
        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            return Task.CompletedTask;
        }
    }
}

public static class Constants
{
    public const string StreamProviderName = "StreamProvider-1";
    public const string NamespaceName = "ns-1";
    public const string EHConsumerGroup = "cg1";
    public const string EHPath = "eh1";

    public const string DataConnectionString = "UseDevelopmentStorage=true;";
    public const string EventHubConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
}