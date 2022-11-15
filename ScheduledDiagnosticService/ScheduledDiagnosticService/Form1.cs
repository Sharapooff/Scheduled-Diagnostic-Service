using Classes;
using Models;
using Nancy.Json;

using ScheduledDiagnosticService.Context;
using ScheduledDiagnosticService.Models.DataBase;
using Microsoft.Extensions.Configuration;
using ScheduledDiagnosticService.Classes;

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
                    var _sections = db.Sections.ToList();
                    string _s_sectionId = "";
                    string _s_algoritms = "";

                    foreach (Section s in _sections)
                    {
                        //outToLog("\r\n" + s.Notation, Color.Black);
                        _s_algoritms = "";
                        foreach (TimeTable tt in s.TimeTables)
                        {
                            _s_algoritms += tt.Algoritm?.Notation;
                        }
                        _s_algoritms = _s_algoritms.Replace("**", "*");
                        //logRichTextBox.AppendText(_s_algoritms);
                        if (s.RefID.HasValue)
                        {
                            _s_sectionId = ((int)s.RefID).ToString();
                        }

                        try
                        {
                            // 1) по таймеру (раз в сутки) считать из БД список секций лдокомотивов и алгоритмов диагностирования для них;
                            // 2) для каждой скекции из списка, считанных из БД, запустить диагностирование;
                            // 3) результаты необходимо записать в БД для последующего использования через вызов API;

                            outToLog("Запуск алгоритмов диагностики по расписанию: " + s.Notation + " (" + DateTime.Now.ToString() + ")", Color.Black);
                            Diagnostic DIA = new Diagnostic(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString, _s_sectionId, _s_algoritms, DateTime.Now.AddMonths(-1).ToString("yyyy.MM.dd"), DateTime.Now.ToString("yyyy.MM.dd")); //объект класса диагностики   
                            DIA.Notify += outToLog;
                            Report_Diagnostic_Models rezultDiagnostic = await DIA.GetDiagnosticAsync(); // rezultDiagnostic = await Task.Run(() => DIA.GetDiagnostic());                            
                            Result _result = await new SaveDiagnosticResult().SaveResultDiagnosticAsync(rezultDiagnostic, connectionString, Convert.ToInt32(_s_sectionId));
                            //outToLog(DIA.Seriya + DIA.Number + DIA.Section + " Done: " + DateTime.Now.ToString(), Color.Green);
                        }
                        catch (System.Exception ex)
                        {
                            outToLog(ex.Message, Color.Red);
                        }
                        finally
                        {

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                outToLog(ex.Message, Color.Red);
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

        void logToTxt(string output, Color color)
        {
            //запись лог файла
            System.IO.File.AppendAllText(@$"D:\Sharapooff\LogDiagServices\LOG.ini", "\r\n" + output);
        }
       

        private async void Form1_Shown(object sender, EventArgs e)
        {
            try
            {
                if (File.Exists(@$"D:\Sharapooff\LogDiagServices\LOG.ini")) { File.Delete(@$"D:\Sharapooff\LogDiagServices\LOG.ini"); }

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
                    var _sections = db.Sections.ToList();
                    string _s_sectionId = "";
                    string _s_algoritms = "";

                    foreach (Section s in _sections)
                    {
                        //outToLog("\r\n" + s.Notation, Color.Black);
                        _s_algoritms = "";
                        foreach (TimeTable tt in s.TimeTables)
                        {
                            _s_algoritms += tt.Algoritm?.Notation;
                        }
                        _s_algoritms = _s_algoritms.Replace("**", "*");
                        //logRichTextBox.AppendText(_s_algoritms);
                        if (s.RefID.HasValue)
                        {
                            _s_sectionId = ((int)s.RefID).ToString();
                        }

                        try
                        {
                            outToLog("Запуск алгоритмов диагностики по расписанию: " + s.Notation + " (" + DateTime.Now.ToString() + ")", Color.Black);
                            logToTxt("Запуск алгоритмов диагностики по расписанию: " + s.Notation + " (" + DateTime.Now.ToString() + ")", Color.Black);
                            Diagnostic DIA = new Diagnostic(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString, _s_sectionId, _s_algoritms, DateTime.Now.AddMonths(-1).ToString("yyyy.MM.dd"), DateTime.Now.ToString("yyyy.MM.dd")); //объект класса диагностики   
                            DIA.Notify += outToLog;
                            DIA.Notify += logToTxt;
                            Report_Diagnostic_Models rezultDiagnostic = await DIA.GetDiagnosticAsync(); // rezultDiagnostic = await Task.Run(() => DIA.GetDiagnostic());                            
                            Result _result = await new SaveDiagnosticResult().SaveResultDiagnosticAsync(rezultDiagnostic, connectionString, Convert.ToInt32(_s_sectionId));
                            //outToLog(DIA.Seriya + DIA.Number + DIA.Section + " Done: " + DateTime.Now.ToString(), Color.Green);
                        }
                        catch (System.Exception ex)
                        {
                            outToLog(ex.Message, Color.Red);
                            logToTxt(ex.Message, Color.Red);
                        }
                        finally
                        {

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                outToLog(ex.Message, Color.Red);
                logToTxt(ex.Message, Color.Red);
            }
            finally
            { 
                Application.Exit();
            }
        }
    }
}