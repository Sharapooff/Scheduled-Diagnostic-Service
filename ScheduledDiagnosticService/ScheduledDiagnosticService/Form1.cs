using Classes;
using Models;
using Nancy.Json;

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
            try
            {
                // 1) �� ������� (��� � �����) ������� �� �� ������ ������ ������������ � ���������� ���������������� ��� ���;
                // 2) ��� ������ ������� �� ������, ��������� �� ��, ��������� ����������������;
                // 3) ���������� ���������� �������� � �� ��� ������������ ������������� ����� ����� API;

                var _algoritms = "*2-0*2-1*2-2*2-3*3-1*4-1*4-2*5-1*5-2*5-3*6-1*";
                var _section_id = "759";
                var _tablica = "TE25KM_HZM";
                var _dtStart = DateTime.Now.AddMonths(-1).ToString("yyyy.MM.dd");
                var _dtStop = DateTime.Now.ToString("yyyy.MM.dd");
                var _seriya = "2��25��";
                var _number = "446";
                var _section = "�";

                outToLog("������ ���������� ����������� �� ����������: " + DateTime.Now.ToString(), Color.Black);

                Diagnostic DIA = new Diagnostic(_algoritms, _section_id, _tablica, _dtStart, _dtStop, _seriya, _number, _section); //������ ������ �����������   
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

    }
}