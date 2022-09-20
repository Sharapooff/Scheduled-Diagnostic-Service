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
                // ��������� ������
                try
                {
                    StartService = true;
                    timer1.Interval = Convert.ToInt32(textBox1.Text);
                    timer1.Start();
                    button1.Text = "StopService";
                    outToLog("========= ������ �������: " + DateTime.Now.ToString() + " ========", Color.Black);
                }
                catch (Exception ex)
                {
                    outToLog(ex.Message, Color.Red);
                }
            }
            else
            {
                // ���������� ������
                try
                {                    
                    StartService = false;
                    timer1.Stop();
                    button1.Text = "StartService";
                    outToLog("========= ������ ����������: " + DateTime.Now.ToString() + " ========", Color.Black);
                }
                catch (Exception ex)
                {
                   outToLog(ex.Message, Color.Red);
                }
            }
        }

        private async void timer1_Tick(object sender, EventArgs e)
        {
            //
        }
        /// <summary>
        /// ���� ������ ���� � ���� ������� ����������������
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
        /// ����������� � logRichTextBox
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

        private async void button2_Click(object sender, EventArgs e)
        {
            try
            {
                var builder = new ConfigurationBuilder();
                // ��������� ���� � �������� ��������
                builder.SetBasePath(Directory.GetCurrentDirectory());
                // �������� ������������ �� ����� appsettings.json
                builder.AddJsonFile("appsettings.json");
                // ������� ������������
                var config = builder.Build();
                // �������� ������ �����������
                string connectionString = config.GetConnectionString("DiagServiceConnection");
                using (DiagServiceContext db = new DiagServiceContext(connectionString))
                {
                    var _sections = db.Sections.ToList();
                    string _s_sectionId = "";
                    string _s_algoritms = "";

                    foreach (Section s in _sections)
                    {
                        outToLog("\r\n" + s.Notation, Color.Black);
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
                            // 1) �� ������� (��� � �����) ������� �� �� ������ ������ ������������ � ���������� ���������������� ��� ���;
                            // 2) ��� ������ ������� �� ������, ��������� �� ��, ��������� ����������������;
                            // 3) ���������� ���������� �������� � �� ��� ������������ ������������� ����� ����� API;

                            outToLog("������ ���������� ����������� �� ����������: " + DateTime.Now.ToString(), Color.Black);

                            Diagnostic DIA = new Diagnostic(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString, _s_sectionId, _s_algoritms, DateTime.Now.AddMonths(-1).ToString("yyyy.MM.dd"), DateTime.Now.ToString("yyyy.MM.dd")); //������ ������ �����������   
                            DIA.Notify += outToLog;
                            Report_Diagnostic_Models rezultDiagnostic = await DIA.GetDiagnosticAsync(); // rezultDiagnostic = await Task.Run(() => DIA.GetDiagnostic());
                            Result _result = await DIA.SaveResultDiagnosticAsync(rezultDiagnostic);
                            //��������� ����������� � �������� ��������� � ���������� rezultDiagnostic
                            //if (rezultDiagnostic.ERR == false) //���� ������ ���������� ����������� ��� ������
                            //{
                            //    //rezultDiagnostic.Report_PDF_file_path = DIA.Create_Report_PDF(rezultDiagnostic, Server.MapPath("/"));//������� ����� ���������� ��� ����� ��� �������� �������� ��� ERR  
                            //    //DIA.ExcelReportCreate(rezultDiagnostic, Server.MapPath("/REPORTS/"));
                            //}
                            ////�����������, �������� �� �������-���������� ������ json
                            //var serializer = new JavaScriptSerializer();//������� ������ ������������            
                            //var serializedRezultDiagnostic = serializer.Serialize(rezultDiagnostic);//�������� ������ json ���������� ����� �����            
                            ////var deserializedResult = serializer.Deserialize<Report_Models.Report_Diagnostic_Models>(serializedRezultDiagnostic); //�������� ������, ������������ ������ json (��� �������� json_param)
                            //////return Json(serializedRezultDiagnostic, JsonRequestBehavior.AllowGet);
                            //outToLog(_number + "!!");

                            //logRichTextBox.Invoke((Action)delegate { logRichTextBox.AppendText("\r\n" + "!!!"); });

                            outToLog(DIA.Seriya + DIA.Number + DIA.Section + " Done: " + DateTime.Now.ToString(), Color.Green);
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

                    //UserProfile profile1 = new UserProfile { Age = 22, Name = "Tom", User = user1 };
                    //UserProfile profile2 = new UserProfile { Age = 27, Name = "Alice", User = user2 };
                    //db.UserProfiles.AddRange(profile1, profile2);
                }
            }
            catch(Exception ex)
            {
                outToLog(ex.Message, Color.Red);
            }

        }
    }
}