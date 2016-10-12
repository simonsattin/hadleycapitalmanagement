using HadleyCapitalManagement.Norgate;
using HadleyCapitalManagement.Quandl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement
{
    class Program
    {
        static void Main(string[] args)
        {
            //var processor = new NorgateDataProcessor();
            //processor.Run();

            var quandlDownloader = new QuandlPriceHistoryDownloader();
            quandlDownloader.Run();

            Console.ReadKey();
        }
    }
}
