using ConsumerINFluxDb.Services;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

namespace ConsumerINFluxDb
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to Consumer");

            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
            };

            var connection = factory.CreateConnection();

            using var channel = connection.CreateModel();

            channel.QueueDeclare("booking-test04", durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);


            var consumer = new EventingBasicConsumer(channel);
            var service = new InfluxDBService();
            consumer.Received += (model, eventArgs) =>
            {
                var body = eventArgs.Body.ToArray();

                var symbol = JsonSerializer.Deserialize<SymbolQuote>(body);

                Console.WriteLine($"Symbol Id: {symbol.SymbolId} Symbol Name: {symbol.SymbolName} Bid {symbol.Bid} Ask {symbol.Ask}");
                service.Write(write =>
                {
                    DateTime original = DateTime.Now; // Current date and time

                    DateTime truncated = new DateTime(
                        original.Year,
                        original.Month,
                        original.Day,
                        original.Hour,
                        original.Minute,
                        0 // Set seconds to 0
                    );

                    //var point = PointData.Measurement($"Symbols-{truncated}")
                    var point = PointData.Measurement($"Symbols-{truncated}")
                        .Tag("Id", symbol.SymbolId.ToString())
                        .Field("symbolObject", JsonSerializer.Serialize(symbol))
                        //.Field("BId", symbol.Bid)
                        //.Field("ASK", symbol.Ask)
                        //.Field("Digits", symbol.Digits)
                        .Timestamp(DateTime.Now, WritePrecision.Ms);

                    write.WritePoint(point, "Symbols", "organization");
                    channel.BasicAck(deliveryTag: eventArgs.DeliveryTag, multiple: false);
                    Console.WriteLine($"Symbol has been added{symbol.SymbolId}");


                });
            };

            channel.BasicConsume(queue: "booking-test04", autoAck: false, consumer: consumer);

            Console.ReadKey();
        }
    }
}
