using Classes;
using Models;
using Nancy.Json;

using ScheduledDiagnosticService.Context;
using ScheduledDiagnosticService.Models.DataBase;
using Microsoft.Extensions.Configuration;

namespace ScheduledDiagnosticService
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }
        bool StartService = false;

        private void button1_Click(object sender, EventArgs e)
        {           
            if (!StartService)
            {
                // запустить таймер
                try
                {
                    StartService = true;
                    timer1.Interval = Convert.ToInt32(textBox1.Text);
                    timer1.Start();
                    button1.Text = "StopService";
                    outToLog("========= Сервис запущен: " + DateTime.Now.ToString() + " ========", Color.Black);
                }
                catch (Exception ex)
                {
                    outToLog(ex.Message, Color.Red);
                }
            }
            else
            {
                // остановить таймер
                try
                {                    
                    StartService = false;
                    timer1.Stop();
                    button1.Text = "StartService";
                    outToLog("========= Сервис остановлен: " + DateTime.Now.ToString() + " ========", Color.Black);
                }
                catch (Exception ex)
                {
                   outToLog(ex.Message, Color.Red);
                }
            }
        }

        private async void timer1_Tick(object sender, EventArgs e)
        {
            try
            {
                // 1) по таймеру (раз в сутки) считать из БД список секций лдокомотивов и алгоритмов диагностирования для них;
                // 2) для каждой скекции из списка, считанных из БД, запустить диагностирование;
                // 3) результаты необходимо записать в БД для последующего использования через вызов API;

                var _algoritms = "*2-0*2-1*2-2*2-3*3-1*4-1*4-2*5-1*5-2*5-3*6-1*";
                var _section_id = "759";
                var _tablica = "TE25KM_HZM";
                var _dtStart = DateTime.Now.AddMonths(-1).ToString("yyyy.MM.dd");
                var _dtStop = DateTime.Now.ToString("yyyy.MM.dd");
                var _seriya = "2ТЭ25КМ";
                var _number = "446";
                var _section = "А";

                outToLog("Запуск алгоритмов диагностики по расписанию: " + DateTime.Now.ToString(), Color.Black);

                Diagnostic DIA = new Diagnostic(_algoritms, _section_id, _tablica, _dtStart, _dtStop, _seriya, _number, _section); //объект класса диагностики   
                DIA.Notify += outToLog;
                Report_Diagnostic_Models rezultDiagnostic = await DIA.GetDiagnosticAsync(); // rezultDiagnostic = await Task.Run(() => DIA.GetDiagnostic());
                Result _result = await DIA.SaveResultDiagnosticAsync(rezultDiagnostic);
                //выполнить диагностику и записать результат в переменную rezultDiagnostic
                //if (rezultDiagnostic.ERR == false) //если объект результата диагностики без ошибок
                //{
                //    //rezultDiagnostic.Report_PDF_file_path = DIA.Create_Report_PDF(rezultDiagnostic, Server.MapPath("/"));//создаем отчет возвращает имя файла при успешном создании или ERR  
                //    //DIA.ExcelReportCreate(rezultDiagnostic, Server.MapPath("/REPORTS/"));
                //}
                ////сериализуем, получаем из объекта-результата строку json
                //var serializer = new JavaScriptSerializer();//Создаем объект сериализации            
                //var serializedRezultDiagnostic = serializer.Serialize(rezultDiagnostic);//получаем строку json сериализуя некий объек            
                ////var deserializedResult = serializer.Deserialize<Report_Models.Report_Diagnostic_Models>(serializedRezultDiagnostic); //получаем объект, десериализуя строку json (наш параметр json_param)
                //////return Json(serializedRezultDiagnostic, JsonRequestBehavior.AllowGet);
                //outToLog(_number + "!!");

                //logRichTextBox.Invoke((Action)delegate { logRichTextBox.AppendText("\r\n" + "!!!"); });

                outToLog(_seriya + _number + _section + " Done: " + DateTime.Now.ToString(), Color.Green);
            }
            catch (System.Exception ex)
            {
                outToLog(ex.Message, Color.Red);
            }
            finally
            {
                //button1.Text = "StopService";
            }
        }
        /// <summary>
        /// Ввод только цифр в поле периода диагностирования
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void textBox1_KeyPress(object sender, KeyPressEventArgs e)
        {
            char number = e.KeyChar;

            if (!Char.IsDigit(number))
            {
                e.Handled = true;
            }
        }
        /// <summary>
        /// Логирование в logRichTextBox
        /// </summary>
        /// <param name="output"></param>
        void outToLog(string output, Color color)
        {
            logRichTextBox.Invoke((Action)delegate
            {

                logRichTextBox.SelectionColor = color;
                if (!string.IsNullOrWhiteSpace(logRichTextBox.Text))
                {
                    logRichTextBox.AppendText("\r\n" + output);
                }
                else
                {
                    logRichTextBox.AppendText(output);
                }
                logRichTextBox.ScrollToCaret();

            });
        }

        private void button2_Click(object sender, EventArgs e)
        {
            var builder = new ConfigurationBuilder();
            // установка пути к текущему каталогу
            builder.SetBasePath(Directory.GetCurrentDirectory());
            // получаем конфигурацию из файла appsettings.json
            builder.AddJsonFile("appsettings.json");
            // создаем конфигурацию
            var config = builder.Build();
            // получаем строку подключения
            string connectionString = config.GetConnectionString("DiagServiceConnection");
            using (DiagServiceContext db = new DiagServiceContext(connectionString))
            {
                bool isCreated = db.Database.EnsureCreated();
                if (isCreated) Console.WriteLine("База данных была создана");
                else Console.WriteLine("База данных уже существует");


                Section section1 = new Section { Notation = "2ТЭ25КМ", RefID = 333 };
                Section section2 = new Section { Notation = "3ТЭ25КМ", RefID = 334 };
                db.Sections.AddRange(section1, section2);
                db.SaveChanges();
                var sections = db.Sections.ToList();
                logRichTextBox.AppendText("\r\n" + "Список объектов:");
                foreach (Section s in sections)
                {
                    logRichTextBox.AppendText("\r\n" + $"{s.Id} - {s.Notation}");
                }
                //UserProfile profile1 = new UserProfile { Age = 22, Name = "Tom", User = user1 };
                //UserProfile profile2 = new UserProfile { Age = 27, Name = "Alice", User = user2 };
                //db.UserProfiles.AddRange(profile1, profile2);
            }
        }
    }
}