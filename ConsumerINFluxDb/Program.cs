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

            channel.QueueDeclare("booking-test02", durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);


            var consumer = new EventingBasicConsumer(channel);
            var service = new InfluxDBService();
            consumer.Received += (model, eventArgs) =>
            {
                var body = eventArgs.Body.ToArray();

                var booking = JsonSerializer.Deserialize<Booking>(body);

                Console.WriteLine($"Passenger Id: {booking.Id} Passenger Name: {booking.PassengerName}");
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

                    var point = PointData.Measurement($"bookings-{truncated}")
                        .Tag("PassengerName", booking.PassengerName)
                        .Field("Id", booking.Id)
                        .Timestamp(DateTime.Now, WritePrecision.Ms);

                    write.WritePoint(point, "NewBucket", "organization");
                    channel.BasicAck(deliveryTag: eventArgs.DeliveryTag, multiple: false);
                    Console.WriteLine($"passanger has been added{booking.Id}");


                });
            };

            channel.BasicConsume(queue: "booking-test02", autoAck: false, consumer: consumer);

            Console.ReadKey();
        }
    }
}
