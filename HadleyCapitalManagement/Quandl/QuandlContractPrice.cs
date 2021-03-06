﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.Quandl
{
    public class QuandlContractPrice
    {
        public int Id { get; set; }
        public int ContractId { get; set; }
        public DateTime Date { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
        public int OpenInterest { get; set; }
        
        public QuandlContract Contract { get; set; }
    }
}
