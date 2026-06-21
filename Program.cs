using Elsa.Extensions;
// 💡 Elsa v3 전용 핵심 네임스페이스
using Elsa.Workflows;
using Elsa.Workflows.Activities;
using Elsa.Workflows.Runtime;
using Elsa.Workflows.Runtime.Activities;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

using Orleans;
using Orleans.Hosting;

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ProductionSagaArchitectureV3
{
    // ========================================================================
    // LAYER 1: CORE DOMAIN CLUSTER 
    // ========================================================================
    public interface IOrderGrain : IGrainWithGuidKey
    {
        Task SubmitOrderAsync(string item, decimal price);
    }

    public class OrderState
    {
        public string Item { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public string Status { get; set; } = "None";
    }

    public class OrderGrain : Grain<OrderState>, IOrderGrain
    {
        public async Task SubmitOrderAsync(string item, decimal price)
        {
            if (price <= 0) throw new ArgumentException("주문 금액이 올바르지 않습니다.");
            if (State.Status == "Submitted") throw new InvalidOperationException("이미 처리 중인 주문입니다.");

            State.Item = item;
            State.Price = price;
            State.Status = "Submitted";
            await WriteStateAsync();

            Console.WriteLine($"\n⚡ [Core Orleans] 주문 검증 완료 및 상태 저장 완료! (ID: {this.GetPrimaryKey()})");

            await using var nats = new NatsClient("nats://localhost:4222");
            var js = nats.CreateJetStreamContext();

            var eventPayload = $"{{\"OrderId\":\"{this.GetPrimaryKey()}\", \"Item\":\"{item}\", \"Price\":{price}}}";
            await js.PublishAsync("order.submitted", Encoding.UTF8.GetBytes(eventPayload));
            Console.WriteLine("📢 [Core Orleans] 도메인 이벤트 'order.submitted' NATS로 쏘아 올림.");
        }
    }

    // ========================================================================
    // LAYER 2: SAGA WORKFLOW ORCHESTRATOR 
    // ========================================================================
    public class OrderSagaWorkflow : WorkflowBase
    {
        protected override void Build(IWorkflowBuilder builder)
        {
            builder.Root = new Sequence
            {
                Activities =
                {
                    // 1. NATS가 던져줄 'OnOrderSubmitted' 이벤트(시그널)가 올 때까지 자원을 쓰지 않고 대기
                    new Event("OnOrderSubmitted") { CanStartWorkflow = true }, 

                    new Inline(context => Console.WriteLine("🏁 [Elsa v3 Saga] 'OnOrderSubmitted' 이벤트 감지. 외부 PG 결제 연동 시작.")),

                    new Inline(context => Console.WriteLine("💸 [Elsa v3 외부연동] 3rd Party 외주 결제 대행사 API 호출 및 승인 완료.")),

                    new Inline(async context =>
                    {
                        await using (var nats = new NatsClient("nats://localhost:4222"))
                        {
                            var js = nats.CreateJetStreamContext();
                            Console.WriteLine("🔀 [Elsa v3 Saga] NATS 연결 및 스트림 상태 확인 중...");
                            await js.CreateStreamAsync(new StreamConfig("COMMAND_STREAM_V2", new[] { "command.>" }));
                            var commandPayload = "{\"PointsToAward\":100}";
                            var payloadBytes = Encoding.UTF8.GetBytes(commandPayload);
                            Console.WriteLine("🔀 [Elsa v3 Saga] 하위 시스템 처리를 위해 NATS로 'command.add.point' 명령 발행.");
        
                            // 3. 서버가 확실하게 저장할 때까지 동기적으로 끝까지 대기
                            var ack = await js.PublishAsync("command.add.point", payloadBytes).ConfigureAwait(false);

                            Console.WriteLine($"✅ [NATS 서버 응답 수신 완료] Stream: {ack.Stream}, Value: {ack.Value}");
                        }

                    }),

                    new Inline(context => Console.WriteLine("🎉 [Elsa v3 Saga] 모든 사가 오케스트레이션 여정 정상 종료!"))
                }
            };
        }
    }

    // ========================================================================
    // LAYER 3: INTEGRATION WORKER LAYER 
    // ========================================================================
    public interface IUserPointGrain : IGrainWithGuidKey
    {
        Task AwardPointsAsync(int points);
    }

    public class UserPointGrain : Grain, IUserPointGrain
    {
        public Task AwardPointsAsync(int points)
        {
            Console.WriteLine($"🎁 [Worker Orleans] UserPointGrain ({this.GetPrimaryKey()}): 포인트 {points}점 적립 완료!");
            return Task.CompletedTask;
        }
    }

    // ========================================================================
    // LAYER 4: NATS INFRASTRUCTURE CONSUMERS 
    // ========================================================================
    public class NatsDomainEventConsumer : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        public NatsDomainEventConsumer(IServiceProvider serviceProvider) => _serviceProvider = serviceProvider;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await using var nats = new NatsClient("nats://localhost:4222");
            var js = nats.CreateJetStreamContext();

            await js.CreateStreamAsync(new StreamConfig("DOMAIN_STREAM", new[] { "order.*" }), stoppingToken);
            var consumer = await js.CreateOrUpdateConsumerAsync("DOMAIN_STREAM", new ConsumerConfig("elsa-event-consumer") { AckPolicy = ConsumerConfigAckPolicy.Explicit }, stoppingToken);

            Console.WriteLine("🤖 [NATS 인프라] v3 이벤트 컨슈머 가동 중... (Target: order.*)");

            try
            {
                await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: stoppingToken))
                {
                    string payload = Encoding.UTF8.GetString(msg.Data);
                    using var scope = _serviceProvider.CreateScope();

                    var eventPublisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
                    await eventPublisher.PublishAsync("OnOrderSubmitted", cancellationToken: stoppingToken);

                    await msg.AckAsync(cancellationToken: stoppingToken);


                }
            }
            catch (OperationCanceledException) { }
        }
    }

    public class NatsSagaCommandConsumer : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        public NatsSagaCommandConsumer(IServiceProvider serviceProvider) => _serviceProvider = serviceProvider;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await using var nats = new NatsClient("nats://localhost:4222");
            var js = nats.CreateJetStreamContext();

            // 1. 스트림 상태 동기화
            await js.CreateStreamAsync(new StreamConfig("COMMAND_STREAM_V2", new[] { "command.>" }), stoppingToken);

            //var consumer = await js.CreateOrUpdateConsumerAsync(
            //    "COMMAND_STREAM_V2",
            //    new ConsumerConfig("orleans-command-consumer-v2") { AckPolicy = ConsumerConfigAckPolicy.Explicit },
            //    stoppingToken
            //);

            var consumer = await js.CreateConsumerAsync("COMMAND_STREAM_V2", new ConsumerConfig("orleans-command-consumer-v2") { AckPolicy = ConsumerConfigAckPolicy.Explicit },
                stoppingToken
            );
            Console.WriteLine("🤖 [NATS 인프라] 사가 커맨드 컨슈머 가동 중... (Target: command.>)");

            try
            {
                await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: stoppingToken))
                {
                    Console.WriteLine($"\n📥 [NATS 커맨드 컨슈머] ----> 메시지 수신 성공!");
                    Console.WriteLine($"   - Subject : {msg.Subject}");
                    Console.WriteLine($"   - Payload : {Encoding.UTF8.GetString(msg.Data)}");

                    using var scope = _serviceProvider.CreateScope();
                    var grainFactory = scope.ServiceProvider.GetRequiredService<IGrainFactory>();

                    var workerGrain = grainFactory.GetGrain<IUserPointGrain>(Guid.NewGuid());
                    await workerGrain.AwardPointsAsync(100);

                    // 처리 완료 응답
                    await msg.AckAsync(cancellationToken: stoppingToken);
                    Console.WriteLine("✅ [NATS 커맨드 컨슈머] 비즈니스 연동 및 Ack 전송 완료.\n");
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [NATS 커맨드 컨슈머 에러] {ex.Message}");
            }
        }
    }
    // ========================================================================
    // LAYER 5: HOST
    // ========================================================================
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "The Perfect Elsa v3 Event-Driven Saga Architecture";

            var host = Host.CreateDefaultBuilder(args)
            .UseOrleans(siloBuilder =>
            {
                siloBuilder.UseLocalhostClustering();

                siloBuilder.AddMemoryGrainStorageAsDefault();
            })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddElsa(elsa =>
                    {
                        elsa.UseWorkflowRuntime();
                        elsa.AddWorkflow<OrderSagaWorkflow>();
                    });

                    services.AddHostedService<NatsDomainEventConsumer>();
                    services.AddHostedService<NatsSagaCommandConsumer>();
                })
                .Build();

            await host.StartAsync();

            using var scope = host.Services.CreateScope();
            var grainFactory = scope.ServiceProvider.GetRequiredService<IGrainFactory>();

            var testOrderId = Guid.NewGuid();
            var orderGrain = grainFactory.GetGrain<IOrderGrain>(testOrderId);

            Console.WriteLine("\n🚀 [Client] 사용자가 주문 요청을 보냈습니다.");
            await orderGrain.SubmitOrderAsync("클린 소프트웨어 아키텍처 v3", 42000);

            Console.WriteLine("\n[System] 엔터를 누르면 종료됩니다.\n");
            Console.ReadLine();

            await host.StopAsync();
        }
    }
}