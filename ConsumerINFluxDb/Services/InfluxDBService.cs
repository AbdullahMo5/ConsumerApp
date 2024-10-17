using InfluxDB.Client;

namespace ConsumerINFluxDb.Services
{
    public class InfluxDBService
    {
        private readonly string _token;
        public InfluxDBService()
        {
            _token = "sus_eC43ySDFAiOeiHA2VVws7cISrpuBLQh-PzY1Cnmk7kZO8o2SE61QXqdgVtjn1nKNWYQLUMwLnk3aCigQJQ==";
        }

        public void Write(Action<WriteApi> action)
        {
            //using var client = InfluxDBClientFactory.Create("http://localhost:8086", _token);
            using var client = new InfluxDBClient("http://localhost:8086", _token);
            using var write = client.GetWriteApi();
            action(write);
        }

        public async Task<T> QueryAsync<T>(Func<QueryApi, Task<T>> action)
        {
            using var client = new InfluxDBClient("http://localhost:8086", _token);
            var query = client.GetQueryApi();
            return await action(query);
        }
    }
}
