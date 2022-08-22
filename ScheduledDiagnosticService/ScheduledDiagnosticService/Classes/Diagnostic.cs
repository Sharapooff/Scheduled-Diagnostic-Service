using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Classes
{
    using System.Data.SqlClient;
    using System.IO;
    using System.Threading.Tasks;

    internal class Diagnostic
    {
        public string Algoritm_id { get; set; }
        public string _section_id { get; set; }
        public string _tablica { get; set; }
        public string dat_ot { get; set; }
        public string dat_do { get; set; }
        public string Seriya { get; set; }
        public string Number { get; set; }
        public string Section { get; set; }
        public string Report_PDF_file_path { get; set; } //путь к файлу отчету PDF

        //конструктор по умолчанию
        public Diagnostic()
        { }
        //конструктор с параметрами 
        public Diagnostic(string Algorinm_id, string Section_id, string Tablica, string Start_Date, string End_Date, string Seriya, string Number, string Section)
        {
            this.Algoritm_id = Algorinm_id;
            this._section_id = Section_id;
            this._tablica = Tablica;
            this.dat_ot = Start_Date;
            this.dat_do = End_Date;
            this.Seriya = Seriya;
            this.Number = Number;
            this.Section = Section;
            this.Report_PDF_file_path = "";

        }



    }
}
