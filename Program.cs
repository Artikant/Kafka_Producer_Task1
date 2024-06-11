using Confluent.Kafka;
using KafkaProducer;
using Newtonsoft.Json;
using System;
using System.Threading;

namespace KafkaProducer
{
	public class VehicleData
	{
		public int Id { get; set; }
		public string? AreaName { get; set; }
		public decimal Latitude { get; set; }
		public decimal Longitude { get; set; }
		public string? RegistrationNo { get; set; }
		public decimal Speed { get; set; }
		public int? AQI { get; set; }
		public bool? IsWorking { get; set; }
		public string? TransportationType { get; set; }
	}
}

class Program
{
	static void Main(string[] args)
	{
		var config = new ProducerConfig
		{
			BootstrapServers = "localhost:9092"
		};

		using var producer = new ProducerBuilder<string, string>(config).Build();
        var locations = new List<(string AreaName, decimal Latitude, decimal Longitude)>
            {
                ("Mumbai, Maharashtra", 19.0760m, 72.8777m),
                ("Delhi", 28.7041m, 77.1025m),
                ("Bangalore, Karnataka", 12.9716m, 77.5946m),
                ("Hyderabad, Telangana", 17.3850m, 78.4867m),
                ("Ahmedabad, Gujarat", 23.0225m, 72.5714m),
                ("Chennai, Tamil Nadu", 13.0827m, 80.2707m),
                ("Kolkata, West Bengal", 22.5726m, 88.3639m),
                ("Surat, Gujarat", 21.1702m, 72.8311m),
                ("Pune, Maharashtra", 18.5204m, 73.8567m),
                ("Jaipur, Rajasthan", 26.9124m, 75.7873m)
            };

        Random random = new Random();

        Func<(decimal Latitude, decimal Longitude), (decimal, decimal)> generateLocation = (coords) =>
        {
            decimal newLat = coords.Latitude + (decimal)(random.NextDouble() * 0.0001 - 0.00005);
            decimal newLon = coords.Longitude + (decimal)(random.NextDouble() * 0.0001 - 0.00005);
            return (newLat, newLon);
        };

        while (true)
        {
            var location = locations[random.Next(locations.Count)];
            var vehicle = new VehicleData
            {
                Id = random.Next(1, 1000),
                AreaName = location.AreaName,
                Latitude = location.Latitude,
                Longitude = location.Longitude,
                RegistrationNo = $"TS{random.Next(10, 99)}AB{random.Next(1000, 9999)}",
                Speed = random.Next(20, 60),
                AQI = random.Next(0, 501),
                IsWorking = random.Next(0, 2) == 1,
                TransportationType = random.Next(0, 2) == 0 ? "Bus" : "Truck"
            };
            (vehicle.Latitude, vehicle.Longitude) = generateLocation((vehicle.Latitude, vehicle.Longitude));
            var messageValue = JsonConvert.SerializeObject(vehicle);
            producer.Produce("vehicledataitem", new Message<string, string> { Key = null, Value = messageValue });
            Console.WriteLine("Message sent: " + messageValue);
            Thread.Sleep(5000);
        }
    }
}
