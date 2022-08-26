using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models
{
    public class Diag_result<T> //модель для данных результата диагностирования 
    {
        //информация о локомотиве
        public bool ERR { get; set; } //тип
        public string ERR_Message { get; set; } //серия
                                                //список объектов класса Т
        public List<T> Table = new List<T>();
    }
}
