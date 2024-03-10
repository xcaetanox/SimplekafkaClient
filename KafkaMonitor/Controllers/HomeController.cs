using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace KafkaMonitor.Controllers
{
    public class HomeController : Controller
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;
        private readonly ILogger<HomeController> _logger;



        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
            // Configurações do produtor Kafka
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            // Configurações do consumidor Kafka
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "meu-grupo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> SendMessage(string message)
        {
            try
            {
                // Criar um produtor Kafka
                using (var producer = new ProducerBuilder<string, string>(_producerConfig).Build())
                {
                    // Gerar uma chave aleatória
                    string key = Guid.NewGuid().ToString();

                    // Enviar mensagem para o tópico "meu-topico" com a chave aleatória
                   await producer.ProduceAsync("meu-topico", new Message<string, string> { Key = key, Value = message });
                }
                return View("Index");
            }
            catch (Exception ex)
            {
                // Lidar com erros
                return StatusCode(500, $"Erro ao enviar mensagem: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult ConsumeMessage()
        {
            try
            {
                List<string> messages = new List<string>();

                // Criar um consumidor Kafka
                using (var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
                {
                    // Subscrever o tópico "meu-topico"
                    consumer.Subscribe("meu-topico");

                    CancellationTokenSource cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) => {
                        e.Cancel = true; // prevent the process from terminating.
                        cts.Cancel();
                    };

                    // Loop de leitura de mensagens
                    while (true)
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        if (consumeResult != null)
                        {
                            // Adicionar mensagem à lista
                            messages.Add(consumeResult.Value);
                            consumer.Commit(consumeResult);
                            return Json(new { messages });
                        }
                    }
                }

                // Retornar a lista de mensagens como JSON
                
            }
            catch (Exception ex)
            {
                // Lidar com erros
                return StatusCode(500, $"Erro ao consumir mensagem: {ex.Message}");
            }
        }
    }
}
