namespace ConsumerINFluxDb
{
    public class SymbolQuote
    {
        public int SymbolId { get; set; }
        public string SymbolName { get; set; }
        public decimal Bid { get; set; }
        public decimal Ask { get; set; }
        public int Digits { get; set; }
    }
}
