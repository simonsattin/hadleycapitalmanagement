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
            var downloader = new QuandlPriceHistoryDownloader();
            downloader.Run();

            Console.ReadKey();
        }
    }
}
