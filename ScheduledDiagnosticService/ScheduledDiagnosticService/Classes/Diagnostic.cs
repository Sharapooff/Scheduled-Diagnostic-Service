using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Models;

namespace Classes
{
    using System.Data.SqlClient;
    using System.IO;
    using System.Threading.Tasks;

    public delegate void DiagnosticHandler(string message, Color color);
    internal class Diagnostic
    {
        //поля
        public event DiagnosticHandler? Notify;              // 1.Определение события (уведомление)
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
        //методы
        public Report_Diagnostic_Models GetDiagnostic()
        {
            Report_Diagnostic_Models _reportModel = new Report_Diagnostic_Models();
            //в объекте, который будет возвращаться как результат, нужно указать те же поля, что в классе Diagnostic
            _reportModel.Algorithms_list = this.Algoritm_id;
            _reportModel.Section_Name = this.Section;
            _reportModel.Number = this.Number;
            _reportModel.Seriya = this.Seriya;
            _reportModel.StringData_start = this.dat_ot;
            _reportModel.StringData_end = this.dat_do;
            _reportModel.Tipe = this._tablica;
            _reportModel.ERR = false;
            _reportModel.ERR_message = "";
            /*
             * здесь вызываем все выбранные методы диагностирования
             * и заполняем модель отчета сразу или после окончания 
             * работы всех методов (в отдельных потоках) из их результатов
            */
            try
            {
                //создаем под каждый алгоритм свою задачу
                Task<Diag_result<Tabels_Models.Tab_1_1>> task_1_1 = new Task<Diag_result<Tabels_Models.Tab_1_1>>(Algoritm_1_1);
                Task<Diag_result<Tabels_Models.Tab_1_2>> task_1_2 = new Task<Diag_result<Tabels_Models.Tab_1_2>>(Algoritm_1_2);

                Task<List<Tabels_Models.GroupSmsModel>> task_2_0 = new Task<List<Tabels_Models.GroupSmsModel>>(Algoritm_2_0);//Alarms messages
                Task<Diag_result<Tabels_Models.Tab_2_1>> task_2_1 = new Task<Diag_result<Tabels_Models.Tab_2_1>>(Algoritm_2_1);
                Task<Diag_result<Tabels_Models.Tab_2_2>> task_2_2 = new Task<Diag_result<Tabels_Models.Tab_2_2>>(Algoritm_2_2);
                Task<Diag_result<Tabels_Models.Tab_2_3>> task_2_3 = new Task<Diag_result<Tabels_Models.Tab_2_3>>(Algoritm_2_3);
                Task<Diag_result<Tabels_Models.Tab_3_1>> task_3_1 = new Task<Diag_result<Tabels_Models.Tab_3_1>>(Algoritm_3_1);
                Task<Diag_result<Tabels_Models.Tab_3_1_1>> task_3_1_1 = new Task<Diag_result<Tabels_Models.Tab_3_1_1>>(Algoritm_3_1_1);
                Task<Diag_result<Tabels_Models.Tab_4_1>> task_4_1 = new Task<Diag_result<Tabels_Models.Tab_4_1>>(Algoritm_4_1);
                Task<Diag_result<Tabels_Models.Tab_4_2>> task_4_2 = new Task<Diag_result<Tabels_Models.Tab_4_2>>(Algoritm_4_2);
                Task<Diag_result<Tabels_Models.Tab_5_1>> task_5_1 = new Task<Diag_result<Tabels_Models.Tab_5_1>>(Algoritm_5_1);
                Task<Diag_result<Tabels_Models.Tab_5_1_1>> task_5_1_1 = new Task<Diag_result<Tabels_Models.Tab_5_1_1>>(Algoritm_5_1_1);
                Task<Diag_result<Tabels_Models.Tab_5_2>> task_5_2 = new Task<Diag_result<Tabels_Models.Tab_5_2>>(Algoritm_5_2);
                Task<Diag_result<Tabels_Models.Tab_5_3>> task_5_3 = new Task<Diag_result<Tabels_Models.Tab_5_3>>(Algoritm_5_3);
                Task<Diag_result<Tabels_Models.Tab_5_3_1>> task_5_3_1 = new Task<Diag_result<Tabels_Models.Tab_5_3_1>>(Algoritm_5_3_1);
                Task<Diag_result<Tabels_Models.Tab_6_1>> task_6_1 = new Task<Diag_result<Tabels_Models.Tab_6_1>>(Algoritm_6_1);
                //.. и так далее

                //запускаем потоки только выбранных алгоритмов. Если алгоритм предполагает возвращение 2 таблиц, то дочернюю запускаем отдельным процессом, для которого создаем отдельный метод
                if (Algoritm_id.IndexOf("*1-1*") > -1) { task_1_1.Start(); };
                if (Algoritm_id.IndexOf("*1-2*") > -1) { task_1_2.Start(); };

                if (Algoritm_id.IndexOf("*2-0*") > -1) { task_2_0.Start(); };//Alarms messages
                if (Algoritm_id.IndexOf("*2-1*") > -1) { task_2_1.Start(); };
                if (Algoritm_id.IndexOf("*2-2*") > -1) { task_2_2.Start(); };
                if (Algoritm_id.IndexOf("*2-3*") > -1) { task_2_3.Start(); };
                if (Algoritm_id.IndexOf("*3-1*") > -1) { task_3_1.Start(); task_3_1_1.Start(); };
                if (Algoritm_id.IndexOf("*4-1*") > -1) { task_4_1.Start(); };
                if (Algoritm_id.IndexOf("*4-2*") > -1) { task_4_2.Start(); };
                if (Algoritm_id.IndexOf("*5-1*") > -1) { task_5_1.Start(); task_5_1_1.Start(); };
                if (Algoritm_id.IndexOf("*5-2*") > -1) { task_5_2.Start(); };
                if (Algoritm_id.IndexOf("*5-3*") > -1) { task_5_3.Start(); task_5_3_1.Start(); };
                if (Algoritm_id.IndexOf("*6-1*") > -1) { task_6_1.Start(); };
                //.. и так далее

                //ловим результаты всех запущенных потоков (основных и дочерних)
                if (Algoritm_id.IndexOf("*1-1*") > -1) { _reportModel.Table_1_1 = task_1_1.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*1-2*") > -1) { _reportModel.Table_1_2 = task_1_2.Result; } // ожидаем получение результата

                if (Algoritm_id.IndexOf("*2-0*") > -1) { _reportModel.AlarmMessege = task_2_0.Result; } // ожидаем получение результата Alarms messages
                if (Algoritm_id.IndexOf("*2-1*") > -1) { _reportModel.Table_2_1 = task_2_1.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*2-2*") > -1) { _reportModel.Table_2_2 = task_2_2.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*2-3*") > -1) { _reportModel.Table_2_3 = task_2_3.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*3-1*") > -1) { _reportModel.Table_3_1 = task_3_1.Result; _reportModel.Table_3_1_1 = task_3_1_1.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*4-1*") > -1) { _reportModel.Table_4_1 = task_4_1.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*4-2*") > -1) { _reportModel.Table_4_2 = task_4_2.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*5-1*") > -1) { _reportModel.Table_5_1 = task_5_1.Result; _reportModel.Table_5_1_1 = task_5_1_1.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*5-2*") > -1) { _reportModel.Table_5_2 = task_5_2.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*5-3*") > -1) { _reportModel.Table_5_3 = task_5_3.Result; _reportModel.Table_5_3_1 = task_5_3_1.Result; } // ожидаем получение результата
                if (Algoritm_id.IndexOf("*6-1*") > -1) { _reportModel.Table_6_1 = task_6_1.Result; } // ожидаем получение результата
                //.. и так далее


                //если все команды отработали без ошибок в конце устанавливаем флаг отсутствия ошибки
                _reportModel.ERR = false;
                Notify?.Invoke(Seriya + Number + Section + " - алгоритмы отраболтали без ошибок", Color.Green);   // Вызов события 
            }
            catch (Exception ex)
            {
                //если при выполнении команд возникли шибоки, устанавливаем флаг ошибки
                _reportModel.ERR = true;
                _reportModel.ERR_message = ex.Message;
                Notify?.Invoke(Seriya + Number + Section + " - ошибка алгоритма алгоритмы: " + ex.Message, Color.Red);   // Вызов события 
            }
            return (_reportModel);
        }
        //
        // определение асинхронного метода
        async public Task<Report_Diagnostic_Models> GetDiagnosticAsync()
        {
            Report_Diagnostic_Models _reportModel = await Task.Run(() => GetDiagnostic());                // выполняется асинхронно
            return _reportModel;
        }
        async public Task<Result> SaveResultDiagnosticAsync(Report_Diagnostic_Models ResultDiagnostic)//, DBCotext dbCotext)
        {
            Result _result = new Result { ERR = false, ERR_Message = ""};

            return (_result);
        }
        
        //События _________________________________________________________________________________________


        //Алгоритмы диагностирования _________________________________________________________________________________________
        public Diag_result<Tabels_Models.Tab_1_1> Algoritm_1_1()
        {
            Diag_result<Tabels_Models.Tab_1_1> t_1_1 = new Diag_result<Tabels_Models.Tab_1_1>();
            try
            {
                string _SQL = "";
                int ColCil = 0;
                //dat
                string g = "", m = "", d = "", dat_ot = "", dat_do = "";
                g = this.dat_ot.Remove(4, 6);
                m = this.dat_ot.Remove(0, 5).Remove(2, 3);
                d = this.dat_ot.Remove(0, 8);
                dat_ot = g + "-" + m + "-" + d;
                g = this.dat_do.Remove(4, 6);
                m = this.dat_do.Remove(0, 5).Remove(2, 3);
                d = this.dat_do.Remove(0, 8);
                dat_do = g + "-" + m + "-" + d;
                if (_tablica == "2TE25A_01") { ColCil = 12; } else { ColCil = 16; }
                //запрос формируем тут что бы не передавать через url 
                string pPKM = "", pREJ = "",
                    pA1 = "", pA2 = "", pA3 = "", pA4 = "", pA5 = "", pA6 = "", pA7 = "", pA8 = "",
                    pB1 = "", pB2 = "", pB3 = "", pB4 = "", pB5 = "", pB6 = "", pB7 = "", pB8 = "";
                //параметры запроса
                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; pREJ = "Analog_99";
                        pB1 = "Analog_80"; pB2 = "Analog_81"; pB3 = "Analog_82"; pB4 = "Analog_83"; pB5 = "Analog_84"; pB6 = "Analog_85"; pB7 = "Analog_86"; pB8 = "Analog_87";
                        pA1 = "Analog_89"; pA2 = "Analog_90"; pA3 = "Analog_91"; pA4 = "Analog_92"; pA5 = "Analog_93"; pA6 = "Analog_94"; pA7 = "Analog_95"; pA8 = "Analog_96";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; pREJ = "Analog_99";
                        pB1 = "Analog_80"; pB2 = "Analog_81"; pB3 = "Analog_82"; pB4 = "Analog_83"; pB5 = "Analog_84"; pB6 = "Analog_85"; pB7 = "Analog_86"; pB8 = "Analog_87";
                        pA1 = "Analog_89"; pA2 = "Analog_90"; pA3 = "Analog_91"; pA4 = "Analog_92"; pA5 = "Analog_93"; pA6 = "Analog_94"; pA7 = "Analog_95"; pA8 = "Analog_96";
                        break;
                    case "MSU_BS215":
                        pPKM = "Analog_173"; pREJ = "Analog_174";
                        pA1 = "Analog_128"; pA2 = "Analog_127"; pA3 = "Analog_126"; pA4 = "Analog_125"; pA5 = "Analog_124"; pA6 = "Analog_123"; pA7 = "Analog_122"; pA8 = "Analog_121";
                        pB1 = "Analog_137"; pB2 = "Analog_136"; pB3 = "Analog_135"; pB4 = "Analog_134"; pB5 = "Analog_133"; pB6 = "Analog_132"; pB7 = "Analog_131"; pB8 = "Analog_130";
                        break;
                    case "2TE116U_01":
                        pPKM = "Analog_93"; pREJ = "Analog_94";
                        pA1 = "Analog_62"; pA2 = "Analog_61"; pA3 = "Analog_60"; pA4 = "Analog_59"; pA5 = "Analog_58"; pA6 = "Analog_57"; pA7 = "Analog_56"; pA8 = "Analog_55";
                        pB1 = "Analog_71"; pB2 = "Analog_70"; pB3 = "Analog_69"; pB4 = "Analog_68"; pB5 = "Analog_67"; pB6 = "Analog_66"; pB7 = "Analog_65"; pB8 = "Analog_64";
                        break;
                    case "3TE116U_01":
                        pPKM = "Analog_93"; pREJ = "Analog_94";
                        pA1 = "Analog_62"; pA2 = "Analog_61"; pA3 = "Analog_60"; pA4 = "Analog_59"; pA5 = "Analog_58"; pA6 = "Analog_57"; pA7 = "Analog_56"; pA8 = "Analog_55";
                        pB1 = "Analog_71"; pB2 = "Analog_70"; pB3 = "Analog_69"; pB4 = "Analog_68"; pB5 = "Analog_67"; pB6 = "Analog_66"; pB7 = "Analog_65"; pB8 = "Analog_64";
                        break;
                    case "2TE25A_01":
                        pPKM = "Analog_107"; pREJ = "Analog_106";
                        pA1 = "Analog_97"; pA2 = "Analog_98"; pA3 = "Analog_99"; pA4 = "Analog_100"; pA5 = "Analog_101"; pA6 = "Analog_102";
                        pB1 = "Analog_88"; pB2 = "Analog_89"; pB3 = "Analog_90"; pB4 = "Analog_91"; pB5 = "Analog_92"; pB6 = "Analog_93";
                        break;
                    default:
                        break;
                }
                //запрос            
                if (_tablica == "2TE25A_01")
                {
                    _SQL = "SELECT [SectionID], [MeasDateTime], [" + pPKM + "], [" + pREJ + "], [" + pA1 + "], [" + pA2 + "], [" + pA3 + "], [" + pA4 + "], [" + pA5 + "], [" + pA6 + "], [" + pB1 + "], [" + pB2 +
                    "], [" + pB3 + "], [" + pB4 + "], [" + pB5 + "], [" + pB6 + "] FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                    "WHERE [SectionID]=" + _section_id + " and [" + pPKM + "]>=13 and [" + pPKM + "]<16 and [" + pREJ + "]=5 and MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_do + " 23:59:59', 102) " +
                    "ORDER BY MeasDateTime";
                }
                else
                {
                    _SQL = "SELECT [SectionID], [MeasDateTime], [" + pPKM + "], [" + pREJ + "], [" + pA1 + "], [" + pA2 + "], [" + pA3 + "], [" + pA4 + "], [" + pA5 + "], [" + pA6 + "], [" + pA7 + "], [" + pA8 + "], [" + pB1 + "], [" + pB2 +
                    "], [" + pB3 + "], [" + pB4 + "], [" + pB5 + "], [" + pB6 + "], [" + pB7 + "], [" + pB8 + "] FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                    "WHERE [SectionID]=" + _section_id + " and [" + pPKM + "]>=13 and [" + pPKM + "]<16 and [" + pREJ + "]=5 and MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_do + " 23:59:59', 102) " +
                    "ORDER BY MeasDateTime";
                }
                SqlConnection CoNn = new SqlConnection();
                CoNn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString;
                CoNn.OpenAsync();
                SqlCommand cmd3 = CoNn.CreateCommand();
                cmd3.CommandText = _SQL;
                SqlDataReader reader3 = cmd3.ExecuteReader();
                List<Zapis_t_cil> SPISOK = new List<Zapis_t_cil>();
                //string ar1 = _p1; string ar2 = _p2; string ar3 = _p3; string ar4 = _p4; string ar5 = _p5; string ar6 = _p6;
                long i = 0;
                Zapis_t_cil zap = new Zapis_t_cil(); zap.Cil = new string[17];
                if (_tablica == "2TE25A_01")
                {
                    while (reader3.Read())
                    {
                        //подумать что сделать с парсингом dateTime для ускорения обработки json
                        i++;
                        // DateTime.TryParse(reader3["MeasDateTime"].ToString(), out date);
                        //if ((i > 1) && (date_copy != date))
                        {
                            zap.Cil = (string[])zap.Cil.Clone();//сщздаем копию массива для каждого набора температур

                            zap.DAT = reader3["MeasDateTime"].ToString();//date.ToString("yyyy-MM-dd HH:mm:ss");// + ", " + reader3[ar1].ToString().Replace(",", ".") + ", " + reader3[ar2].ToString().Replace(",", ".") + ", " + reader3[ar3].ToString().Replace(",", ".") + ", " + reader3[ar4].ToString().Replace(",", ".") + ", " + reader3[ar5].ToString().Replace(",", ".") + ", " + reader3[ar6].ToString().Replace(",", ".") + "\n ";
                            zap.PKM = reader3[pPKM].ToString();//ПКМD:\РЕЗЕРВНЫЕ КОПИИ ПРОЕКТОВ\WebApplication1\WebApplication1\Content\
                            zap.REJIM = reader3[pREJ].ToString();//РЕЖИМ
                            zap.Cil[1] = reader3[pA1].ToString();//А1
                            zap.Cil[2] = reader3[pA2].ToString();//А2
                            zap.Cil[3] = reader3[pA3].ToString();//А3
                            zap.Cil[4] = reader3[pA4].ToString();//А4
                            zap.Cil[5] = reader3[pA5].ToString();//А5
                            zap.Cil[6] = reader3[pA6].ToString();//А6

                            zap.Cil[7] = reader3[pB1].ToString();//В1
                            zap.Cil[8] = reader3[pB2].ToString();//В2
                            zap.Cil[9] = reader3[pB3].ToString();//В3
                            zap.Cil[10] = reader3[pB4].ToString();//В4
                            zap.Cil[11] = reader3[pB5].ToString();//В5
                            zap.Cil[12] = reader3[pB6].ToString();//В6                            
                        }
                        SPISOK.Add(zap);
                        //date_copy = date;
                    }
                }
                else
                {
                    while (reader3.Read())
                    {
                        i++;
                        {
                            zap.Cil = (string[])zap.Cil.Clone();//создаем копию массива для каждого набора температур

                            zap.DAT = reader3["MeasDateTime"].ToString();//date.ToString("yyyy-MM-dd HH:mm:ss");// + ", " + reader3[ar1].ToString().Replace(",", ".") + ", " + reader3[ar2].ToString().Replace(",", ".") + ", " + reader3[ar3].ToString().Replace(",", ".") + ", " + reader3[ar4].ToString().Replace(",", ".") + ", " + reader3[ar5].ToString().Replace(",", ".") + ", " + reader3[ar6].ToString().Replace(",", ".") + "\n ";
                            zap.PKM = reader3[pPKM].ToString();//ПКМ
                            zap.REJIM = reader3[pREJ].ToString();//РЕЖИМ
                            zap.Cil[1] = reader3[pA1].ToString();//А1
                            zap.Cil[2] = reader3[pA2].ToString();//А2
                            zap.Cil[3] = reader3[pA3].ToString();//А3
                            zap.Cil[4] = reader3[pA4].ToString();//А4
                            zap.Cil[5] = reader3[pA5].ToString();//А5
                            zap.Cil[6] = reader3[pA6].ToString();//А6
                            zap.Cil[7] = reader3[pA7].ToString();//А7
                            zap.Cil[8] = reader3[pA8].ToString();//А8

                            zap.Cil[9] = reader3[pB1].ToString();//В1
                            zap.Cil[10] = reader3[pB2].ToString();//В2
                            zap.Cil[11] = reader3[pB3].ToString();//В3
                            zap.Cil[12] = reader3[pB4].ToString();//В4
                            zap.Cil[13] = reader3[pB5].ToString();//В5
                            zap.Cil[14] = reader3[pB6].ToString();//В6
                            zap.Cil[15] = reader3[pB7].ToString();//В7
                            zap.Cil[16] = reader3[pB8].ToString();//В8                   
                        }
                        SPISOK.Add(zap);
                        //date_copy = date;
                    }
                }

                CoNn.Close();
                i = 0;

                //разбираем полученые из запроса данные
                Int32 kol_dat = 0;//количество разных дат
                Int32 kol_zap_za_daty = 0, kol_zap_za_daty_13 = 0, kol_zap_za_daty_14 = 0, kol_zap_za_daty_15 = 0;//количество записей за одну дату
                                                                                                                  //средние значения 13-15пкм(1-3, 1-16) и среднее по всем цилиндрам(i,17)
                long[,] sredniezn = new long[3 + 1, 20 + 1];
                //кумулятивные суммы(13)
                KumSum KSum_13 = new KumSum(); KSum_13.Cil = new long[17];//запись таблицы
                List<KumSum> ListKSum_13 = new List<KumSum>();//таблица
                KumSum KSum_0_13 = new KumSum(); KSum_0_13.Cil = new long[17];//
                                                                              //кумулятивные суммы(14)
                KumSum KSum_14 = new KumSum(); KSum_14.Cil = new long[17];//запись таблицы
                List<KumSum> ListKSum_14 = new List<KumSum>();//таблица
                KumSum KSum_0_14 = new KumSum(); KSum_0_14.Cil = new long[17];//
                                                                              //кумулятивные суммы(15)
                KumSum KSum_15 = new KumSum(); KSum_15.Cil = new long[17];//запись таблицы
                List<KumSum> ListKSum_15 = new List<KumSum>();//таблица
                KumSum KSum_0_15 = new KumSum(); KSum_0_15.Cil = new long[17];//
                                                                              //результирующие данные                
                List<Tabl> Tabl_1 = new List<Tabl>();//таблица
                Tabl zTabl_1 = new Tabl();//запись таблицы
                                          //пустая начальная запись
                Zapis_t_cil last_zapis = new Zapis_t_cil()
                {
                    DAT = "00.00.0000 00:00:00"
                };

                foreach (Zapis_t_cil zapis in SPISOK)
                {
                    if (zapis.DAT.Remove(9) != last_zapis.DAT.Remove(9))
                    {
                        //13 ПКМ
                        if (kol_zap_za_daty_13 >= 100)
                        {
                            for (int j = 1; j <= (ColCil / 2); j++)//средняя Т по всем цилиндрам
                            {
                                sredniezn[1, j] = sredniezn[1, j] / kol_zap_za_daty_13; if (sredniezn[1, j] > 620) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Предельное превышение температур выпускных газов по цилиндру А" + j.ToString() + " (13ПКМ)"; Tabl_1.Add(zTabl_1); };
                                sredniezn[1, 17] += sredniezn[1, j];
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)//средняя Т по всем цилиндрам
                            {
                                sredniezn[1, j] = sredniezn[1, j] / kol_zap_za_daty_13; if (sredniezn[1, j] > 620) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Предельное превышение температур выпускных газов по цилиндру B" + (j - (ColCil / 2)).ToString() + " (13ПКМ)"; Tabl_1.Add(zTabl_1); };
                                sredniezn[1, 17] += sredniezn[1, j];
                            }
                            //средняя Т по всем цилиндрам
                            sredniezn[1, 17] = sredniezn[1, 17] / ColCil;
                            //превышение от среднего значения
                            for (int j = 1; j <= (ColCil / 2); j++)
                            {
                                if ((sredniezn[1, j] - sredniezn[1, 17]) > 50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Превышение температуры выпускных газов цилиндра А" + j.ToString() + " от среднего значения выше нормы (13ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)
                            {
                                if ((sredniezn[1, j] - sredniezn[1, 17]) > 50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Превышение температуры выпускных газов цилиндра В" + (j - (ColCil / 2)).ToString() + " от среднего значения выше нормы (13ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            //снижение от среднего значения
                            for (int j = 1; j <= (ColCil / 2); j++)
                            {
                                if ((sredniezn[1, j] - sredniezn[1, 17]) < -50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Температура выпускных газов цилиндра А" + j.ToString() + " от среднего значения ниже нормы (13ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)
                            {
                                if ((sredniezn[1, j] - sredniezn[1, 17]) < -50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Температура выпускных газов цилиндра В" + (j - (ColCil / 2)).ToString() + " от среднего значения ниже нормы (13ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            //кумулятивные суммы
                            if (KSum_0_13.Cil[1] == 0)//если первые суммы за текущую дату, то пишим их в стартовую расчетную сумму
                            {
                                KSum_0_13.dat = last_zapis.DAT.Remove(10);
                                for (int j = 1; j <= ColCil; j++)
                                {
                                    KSum_0_13.Cil[j] = sredniezn[1, j];
                                }
                            }
                            //последующие суммы считаем как разность между текущей и стартовой расчетной
                            KSum_13.Cil = (long[])KSum_13.Cil.Clone();//создаем копию массива
                            KSum_13.dat = last_zapis.DAT.Remove(10);
                            for (int j = 1; j <= ColCil; j++)
                            {
                                KSum_13.Cil[j] = KSum_13.Cil[j] + (sredniezn[1, j] - KSum_0_13.Cil[j]);
                            }
                            //добавляем кумулятивную сумму за текущую дату в список кумсум
                            ListKSum_13.Add(KSum_13);

                        }
                        //14 ПКМ
                        if (kol_zap_za_daty_14 >= 100)
                        {
                            for (int j = 1; j <= (ColCil / 2); j++)//средняя Т по всем цилиндрам
                            {
                                sredniezn[2, j] = sredniezn[2, j] / kol_zap_za_daty_14; if (sredniezn[2, j] > 620) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Предельное превышение температур выпускных газов по цилиндру А" + j.ToString() + " (14ПКМ)"; Tabl_1.Add(zTabl_1); };
                                sredniezn[2, 17] += sredniezn[2, j];
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)//средняя Т по всем цилиндрам
                            {
                                sredniezn[2, j] = sredniezn[2, j] / kol_zap_za_daty_14; if (sredniezn[2, j] > 620) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Предельное превышение температур выпускных газов по цилиндру B" + (j - (ColCil / 2)).ToString() + " (14ПКМ)"; Tabl_1.Add(zTabl_1); };
                                sredniezn[2, 17] += sredniezn[2, j];
                            }
                            //средняя Т по всем цилиндрам
                            sredniezn[2, 17] = sredniezn[2, 17] / ColCil;
                            //превышение от среднего значения
                            for (int j = 1; j <= (ColCil / 2); j++)
                            {
                                if ((sredniezn[2, j] - sredniezn[2, 17]) > 50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Превышение температуры выпускных газов цилиндра А" + j.ToString() + " от среднего значения выше нормы (14ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)
                            {
                                if ((sredniezn[2, j] - sredniezn[2, 17]) > 50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Превышение температуры выпускных газов цилиндра В" + (j - (ColCil / 2)).ToString() + " от среднего значения выше нормы (14ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            //снижение от среднего значения
                            for (int j = 1; j <= (ColCil / 2); j++)
                            {
                                if ((sredniezn[2, j] - sredniezn[2, 17]) < -50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Температура выпускных газов цилиндра А" + j.ToString() + " от среднего значения ниже нормы (14ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)
                            {
                                if ((sredniezn[2, j] - sredniezn[2, 17]) < -50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Температура выпускных газов цилиндра В" + (j - (ColCil / 2)).ToString() + " от среднего значения ниже нормы (14ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            //кумулятивные суммы
                            if (KSum_0_14.Cil[1] == 0)//если первые суммы за текущую дату, то пишим их в стартовую расчетную сумму
                            {
                                KSum_0_14.dat = last_zapis.DAT.Remove(10);
                                for (int j = 1; j <= ColCil; j++)
                                {
                                    KSum_0_14.Cil[j] = sredniezn[2, j];
                                }
                            }
                            //последующие суммы считаем как разность между текущей и стартовой расчетной
                            KSum_14.Cil = (long[])KSum_14.Cil.Clone();//создаем копию массива
                            KSum_14.dat = last_zapis.DAT.Remove(10);
                            for (int j = 1; j <= ColCil; j++)
                            {
                                KSum_14.Cil[j] = KSum_14.Cil[j] + (sredniezn[2, j] - KSum_0_14.Cil[j]);
                            }
                            //добавляем кумулятивную сумму за текущую дату в список кумсум
                            ListKSum_14.Add(KSum_14);

                        }
                        //15 ПКМ
                        if (kol_zap_za_daty_15 >= 100)
                        {
                            for (int j = 1; j <= (ColCil / 2); j++)//средняя Т по всем цилиндрам
                            {
                                sredniezn[3, j] = sredniezn[3, j] / kol_zap_za_daty_15; if (sredniezn[3, j] > 620) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Предельное превышение температур выпускных газов по цилиндру А" + j.ToString() + " (15ПКМ)"; Tabl_1.Add(zTabl_1); };
                                sredniezn[3, 17] += sredniezn[3, j];
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)//средняя Т по всем цилиндрам
                            {
                                sredniezn[3, j] = sredniezn[3, j] / kol_zap_za_daty_15; if (sredniezn[3, j] > 620) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Предельное превышение температур выпускных газов по цилиндру B" + (j - (ColCil / 2)).ToString() + " (15ПКМ)"; Tabl_1.Add(zTabl_1); };
                                sredniezn[3, 17] += sredniezn[3, j];
                            }
                            //средняя Т по всем цилиндрам
                            sredniezn[3, 17] = sredniezn[3, 17] / ColCil;
                            //превышение от среднего значения
                            for (int j = 1; j <= (ColCil / 2); j++)
                            {
                                if ((sredniezn[3, j] - sredniezn[3, 17]) > 50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Превышение температуры выпускных газов цилиндра А" + j.ToString() + " от среднего значения выше нормы (15ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)
                            {
                                if ((sredniezn[3, j] - sredniezn[3, 17]) > 50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Превышение температуры выпускных газов цилиндра В" + (j - (ColCil / 2)).ToString() + " от среднего значения выше нормы (15ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            //снижение от среднего значения
                            for (int j = 1; j <= (ColCil / 2); j++)
                            {
                                if ((sredniezn[3, j] - sredniezn[3, 17]) < -50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Температура выпускных газов цилиндра А" + j.ToString() + " от среднего значения ниже нормы (15ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            for (int j = (ColCil / 2) + 1; j <= ColCil; j++)
                            {
                                if ((sredniezn[3, j] - sredniezn[3, 17]) < -50) { zTabl_1.datatime = last_zapis.DAT.Remove(10); zTabl_1.sms = "Температура выпускных газов цилиндра В" + (j - (ColCil / 2)).ToString() + " от среднего значения ниже нормы (15ПКМ)"; Tabl_1.Add(zTabl_1); }
                            }
                            //кумулятивные суммы
                            if (KSum_0_15.Cil[1] == 0)//если первые суммы за текущую дату, то пишим их в стартовую расчетную сумму
                            {
                                KSum_0_15.dat = last_zapis.DAT.Remove(10);
                                for (int j = 1; j <= ColCil; j++)
                                {
                                    KSum_0_15.Cil[j] = sredniezn[3, j];
                                }
                            }
                            //последующие суммы считаем как разность между текущей и стартовой расчетной
                            KSum_15.Cil = (long[])KSum_15.Cil.Clone();//создаем копию массива
                            KSum_15.dat = last_zapis.DAT.Remove(10);
                            for (int j = 1; j <= ColCil; j++)
                            {
                                KSum_15.Cil[j] = KSum_15.Cil[j] + (sredniezn[3, j] - KSum_0_15.Cil[j]);
                            }
                            //добавляем кумулятивную сумму за текущую дату в список кумсум
                            ListKSum_15.Add(KSum_15);
                        }
                        kol_dat++;//количество дней
                        last_zapis.DAT = zapis.DAT;//
                                                   //обнуляем суммы
                        kol_zap_za_daty = 0; kol_zap_za_daty_13 = 0; kol_zap_za_daty_14 = 0; kol_zap_za_daty_15 = 0;
                    }
                    else//если даты совпали
                    {
                        if (zapis.PKM == "13")
                        {
                            for (int j = 1; j <= ColCil; j++)
                            {
                                sredniezn[1, j] += Convert.ToInt32(zapis.Cil[j]);
                            }

                            kol_zap_za_daty_13++;
                        }
                        else
                        {
                            if (zapis.PKM == "14")
                            {
                                for (int j = 1; j <= ColCil; j++)
                                {
                                    sredniezn[2, j] += Convert.ToInt32(zapis.Cil[j]);
                                }

                                kol_zap_za_daty_14++;
                            }
                            else
                            {
                                if (zapis.PKM == "15")
                                {
                                    for (int j = 1; j <= ColCil; j++)
                                    {
                                        sredniezn[3, j] += Convert.ToInt32(zapis.Cil[j]);
                                    }

                                    kol_zap_za_daty_15++;
                                }
                            }
                        }
                        kol_zap_za_daty++;
                    }
                    //повторить расчет средних!
                }

                //заполняем массив выходных данных
                DateTime date;

                if (_tablica == "2TE25A_01")
                {
                    foreach (KumSum zs in ListKSum_13)
                    {
                        DateTime.TryParse(zs.dat, out date);
                        t_1_1.Table.Add(new Tabels_Models.Tab_1_1(date.ToString("yyyy-MM-dd"), "13", zs.Cil[1].ToString().Replace(",", "."), zs.Cil[2].ToString().Replace(",", "."), zs.Cil[3].ToString().Replace(",", "."), zs.Cil[4].ToString().Replace(",", "."), zs.Cil[5].ToString().Replace(",", "."), zs.Cil[6].ToString().Replace(",", "."), "", "", zs.Cil[7].ToString().Replace(",", "."), zs.Cil[8].ToString().Replace(",", "."), zs.Cil[9].ToString().Replace(",", "."), zs.Cil[10].ToString().Replace(",", "."), zs.Cil[11].ToString().Replace(",", "."), zs.Cil[12].ToString().Replace(",", "."), "", ""));
                    }
                    foreach (KumSum zs in ListKSum_14)
                    {
                        DateTime.TryParse(zs.dat, out date);
                        t_1_1.Table.Add(new Tabels_Models.Tab_1_1(date.ToString("yyyy-MM-dd"), "14", zs.Cil[1].ToString().Replace(",", "."), zs.Cil[2].ToString().Replace(",", "."), zs.Cil[3].ToString().Replace(",", "."), zs.Cil[4].ToString().Replace(",", "."), zs.Cil[5].ToString().Replace(",", "."), zs.Cil[6].ToString().Replace(",", "."), "", "", zs.Cil[7].ToString().Replace(",", "."), zs.Cil[8].ToString().Replace(",", "."), zs.Cil[9].ToString().Replace(",", "."), zs.Cil[10].ToString().Replace(",", "."), zs.Cil[11].ToString().Replace(",", "."), zs.Cil[12].ToString().Replace(",", "."), "", ""));

                    }
                    foreach (KumSum zs in ListKSum_15)
                    {
                        DateTime.TryParse(zs.dat, out date);
                        t_1_1.Table.Add(new Tabels_Models.Tab_1_1(date.ToString("yyyy-MM-dd"), "15", zs.Cil[1].ToString().Replace(",", "."), zs.Cil[2].ToString().Replace(",", "."), zs.Cil[3].ToString().Replace(",", "."), zs.Cil[4].ToString().Replace(",", "."), zs.Cil[5].ToString().Replace(",", "."), zs.Cil[6].ToString().Replace(",", "."), "", "", zs.Cil[7].ToString().Replace(",", "."), zs.Cil[8].ToString().Replace(",", "."), zs.Cil[9].ToString().Replace(",", "."), zs.Cil[10].ToString().Replace(",", "."), zs.Cil[11].ToString().Replace(",", "."), zs.Cil[12].ToString().Replace(",", "."), "", ""));
                    }

                }
                else
                {
                    foreach (KumSum zs in ListKSum_13)
                    {
                        DateTime.TryParse(zs.dat, out date);
                        t_1_1.Table.Add(new Tabels_Models.Tab_1_1(date.ToString("yyyy-MM-dd"), "13", zs.Cil[1].ToString().Replace(",", "."), zs.Cil[2].ToString().Replace(",", "."), zs.Cil[3].ToString().Replace(",", "."), zs.Cil[4].ToString().Replace(",", "."), zs.Cil[5].ToString().Replace(",", "."), zs.Cil[6].ToString().Replace(",", "."), zs.Cil[7].ToString().Replace(",", "."), zs.Cil[8].ToString().Replace(",", "."), zs.Cil[9].ToString().Replace(",", "."), zs.Cil[10].ToString().Replace(",", "."), zs.Cil[11].ToString().Replace(",", "."), zs.Cil[12].ToString().Replace(",", "."), zs.Cil[13].ToString().Replace(",", "."), zs.Cil[14].ToString().Replace(",", "."), zs.Cil[15].ToString().Replace(",", "."), zs.Cil[16].ToString().Replace(",", ".")));
                    }
                    foreach (KumSum zs in ListKSum_14)
                    {
                        DateTime.TryParse(zs.dat, out date);
                        t_1_1.Table.Add(new Tabels_Models.Tab_1_1(date.ToString("yyyy-MM-dd"), "14", zs.Cil[1].ToString().Replace(",", "."), zs.Cil[2].ToString().Replace(",", "."), zs.Cil[3].ToString().Replace(",", "."), zs.Cil[4].ToString().Replace(",", "."), zs.Cil[5].ToString().Replace(",", "."), zs.Cil[6].ToString().Replace(",", "."), zs.Cil[7].ToString().Replace(",", "."), zs.Cil[8].ToString().Replace(",", "."), zs.Cil[9].ToString().Replace(",", "."), zs.Cil[10].ToString().Replace(",", "."), zs.Cil[11].ToString().Replace(",", "."), zs.Cil[12].ToString().Replace(",", "."), zs.Cil[13].ToString().Replace(",", "."), zs.Cil[14].ToString().Replace(",", "."), zs.Cil[15].ToString().Replace(",", "."), zs.Cil[16].ToString().Replace(",", ".")));
                    }
                    foreach (KumSum zs in ListKSum_15)
                    {
                        DateTime.TryParse(zs.dat, out date);
                        t_1_1.Table.Add(new Tabels_Models.Tab_1_1(date.ToString("yyyy-MM-dd"), "15", zs.Cil[1].ToString().Replace(",", "."), zs.Cil[2].ToString().Replace(",", "."), zs.Cil[3].ToString().Replace(",", "."), zs.Cil[4].ToString().Replace(",", "."), zs.Cil[5].ToString().Replace(",", "."), zs.Cil[6].ToString().Replace(",", "."), zs.Cil[7].ToString().Replace(",", "."), zs.Cil[8].ToString().Replace(",", "."), zs.Cil[9].ToString().Replace(",", "."), zs.Cil[10].ToString().Replace(",", "."), zs.Cil[11].ToString().Replace(",", "."), zs.Cil[12].ToString().Replace(",", "."), zs.Cil[13].ToString().Replace(",", "."), zs.Cil[14].ToString().Replace(",", "."), zs.Cil[15].ToString().Replace(",", "."), zs.Cil[16].ToString().Replace(",", ".")));
                    }

                }

                foreach (Tabl zt in Tabl_1)
                {
                    t_1_1.Table.Add(new Tabels_Models.Tab_1_1(zt.datatime.ToString(), zt.sms));
                }

                //значения коэфициентов наклона для каждого цилиндра                     
                double[] koefn_13 = new double[17];//массив с коэфициентами наклона
                double[] koefn_14 = new double[17];//массив с коэфициентами наклона
                double[] koefn_15 = new double[17];//массив с коэфициентами наклона
                if (ListKSum_13.Count > 9)
                {
                    for (int i2 = 1; i2 <= ColCil; i2++)//по всем цилиндрам
                    {
                        int col = 0;
                        long sumpr = 0;
                        long sum = 0;
                        foreach (KumSum zks in ListKSum_13)//все кумсуммы текущего цилиндра
                        {
                            col++;
                            sumpr += col * zks.Cil[i2];
                            sum += zks.Cil[i2];
                        }
                        koefn_13[i2] = (Double)((10 * sumpr) - (55 * sum)) / (Double)825;
                    }
                }
                if (ListKSum_14.Count > 9)
                {
                    for (int i2 = 1; i2 <= ColCil; i2++)//по всем цилиндрам
                    {
                        int col = 0;
                        long sumpr = 0;
                        long sum = 0;
                        foreach (KumSum zks in ListKSum_14)//все кумсуммы текущего цилиндра
                        {
                            col++;
                            sumpr += col * zks.Cil[i2];
                            sum += zks.Cil[i2];
                        }
                        koefn_14[i2] = (Double)((10 * sumpr) - (55 * sum)) / (Double)825;
                    }
                }
                if (ListKSum_15.Count > 9)
                {
                    for (int i2 = 1; i2 <= ColCil; i2++)//по всем цилиндрам
                    {
                        int col = 0;
                        long sumpr = 0;
                        long sum = 0;
                        foreach (KumSum zks in ListKSum_15)//все кумсуммы текущего цилиндра
                        {
                            col++;
                            sumpr += col * zks.Cil[i2];
                            sum += zks.Cil[i2];
                        }
                        koefn_15[i2] = (Double)((10 * sumpr) - (55 * sum)) / (Double)825;
                    }
                }

                //коэффициенты кидаем в 
                List<string> _coef = new List<string>();

                for (int j = 1; j <= ColCil; j++)
                {
                    _coef.Add(Math.Round(koefn_13[j], 1).ToString());
                }
                t_1_1.Table.Add(new Tabels_Models.Tab_1_1("13", _coef));
                _coef.Clear();
                for (int j = 1; j <= ColCil; j++)
                {
                    _coef.Add(Math.Round(koefn_14[j], 1).ToString());
                }
                t_1_1.Table.Add(new Tabels_Models.Tab_1_1("14", _coef));
                _coef.Clear();
                for (int j = 1; j <= ColCil; j++)
                {
                    _coef.Add(Math.Round(koefn_15[j], 1).ToString());
                }
                t_1_1.Table.Add(new Tabels_Models.Tab_1_1("15", _coef));
                _coef.Clear();
                //в самом конце сброс флага наличия ошибки
                t_1_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_1_1.ERR = true;
                t_1_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_1_1);

        }
        public Diag_result<Tabels_Models.Tab_1_2> Algoritm_1_2()
        {
            Diag_result<Tabels_Models.Tab_1_2> t_1_2 = new Diag_result<Tabels_Models.Tab_1_2>();
            try
            {
                string pPKM = "", T_mas = "", ChvKV = "", Regim = "", _SQL = "";//для алгоритма по температуре масла

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; T_mas = "Analog_28"; ChvKV = "Analog_130"; Regim = "Analog_99";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; T_mas = "Analog_77"; ChvKV = "Analog_127"; Regim = "Analog_99";
                        break;
                    case "MSU_BS215":
                        pPKM = "Analog_173"; T_mas = "Analog_148"; ChvKV = "Analog_55"; Regim = "Analog_174";
                        break;
                    case "2TE116U_01":
                        pPKM = "Analog_93"; T_mas = "Analog_76"; ChvKV = "Analog_101"; Regim = "Analog_94";
                        break;
                    case "3TE116U_01":
                        pPKM = "Analog_93"; T_mas = "Analog_76"; ChvKV = "Analog_101"; Regim = "Analog_94";
                        break;
                    case "2TE25A_01":
                        pPKM = "Analog_107"; T_mas = "Analog_73"; ChvKV = "Analog_121"; Regim = "Analog_106";
                        break;
                    default:
                        break;
                }

                //выбор первой строки в таблице инцидента
                _SQL = "with cte as (select ROW_NUMBER() over (order by MeasDateTime) as rn, * from " +
                        "(SELECT[SectionID], [MeasDateTime],[" + pPKM + "], [" + T_mas + "], [" + ChvKV + "], [" + Regim + "] " +
                        "FROM[diag_lcm].[Res].[_" + _tablica + " ] ) as t1 " +
                        "WHERE[SectionID] = " + _section_id + " and [" + ChvKV + "]>340 and [" + T_mas + "]>88 and [" + pPKM + "]>5 and [" + Regim + "]=4  " +
                        "and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_do + " 23:59:59', 102))) " +
                        "select TOP 1 d1.MeasDateTime,d1.[SectionID],d1.[" + pPKM + "], d1.[" + T_mas + "], d1.[" + ChvKV + "], d1.[" + Regim + "], " +
                        "datediff(SECOND, d1.MeasDateTime, d2.MeasDateTime) as int_sec " +
                        "from cte d1 " +
                        "join cte d2 on d2.rn=d1.rn-1";

                SqlConnection CoNn = new SqlConnection();
                CoNn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString;
                CoNn.Open();
                SqlCommand cmd3 = CoNn.CreateCommand();
                cmd3.CommandTimeout = 600; //увеличение время выполнения запроса сек
                cmd3.CommandText = _SQL;
                //	int kol_count = cmd3.ExecuteScalar();
                SqlDataReader reader3 = cmd3.ExecuteReader();

                List<Zapis_t_mas> SPISOK = new List<Zapis_t_mas>();
                long i = 0;
                Int32 dat_count = 0;
                Zapis_t_mas zap_Tm = new Zapis_t_mas();

                while (reader3.Read())
                {
                    //подумать что сделать с парсингом dateTime для ускорения обработки json
                    i++;
                    {
                        //zap.Cil = (string[])zap.Cil.Clone();//сщздаем копию массива для каждого набора температур

                        zap_Tm.DAT = reader3["MeasDateTime"].ToString();//date.ToString("yyyy-MM-dd HH:mm:ss");// + ", " + reader3[ar1].ToString().Replace(",", ".") + ", " + reader3[ar2].ToString().Replace(",", ".") + ", " + reader3[ar3].ToString().Replace(",", ".") + ", " + reader3[ar4].ToString().Replace(",", ".") + ", " + reader3[ar5].ToString().Replace(",", ".") + ", " + reader3[ar6].ToString().Replace(",", ".") + "\n ";
                                                                        //		zap.PKM = reader3[pPKM].ToString();//ПКМD:\РЕЗЕРВНЫЕ КОПИИ ПРОЕКТОВ\WebApplication1\WebApplication1\Content\
                        zap_Tm.T_mas = reader3[T_mas].ToString();//Т масла
                        zap_Tm.ChvKV = reader3[ChvKV].ToString();//Частота вращения КВ
                        zap_Tm.PKM = reader3[pPKM].ToString();//ПКМ
                        zap_Tm.Regim = reader3[Regim].ToString();//Режим работы тепловоза
                    }
                    SPISOK.Add(zap_Tm);//количество заптсей
                    dat_count++;
                }
                CoNn.Close();

                i = 0;

                //выбор инцидентов
                _SQL = "with cte as (select ROW_NUMBER() over (order by MeasDateTime) as rn, * from " +
                        "(SELECT[SectionID], [MeasDateTime],[" + pPKM + "], [" + T_mas + "], [" + ChvKV + "], [" + Regim + "] " +
                        "FROM[diag_lcm].[Res].[_" + _tablica + " ] ) as t1 " +
                        "WHERE[SectionID] =  " + _section_id + " and [" + ChvKV + "]>340 and [" + T_mas + "]>88 and [" + pPKM + "]>5 and [" + Regim + "]=4 " +
                        "and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_do + " 23:59:59', 102))) " +
                        "select d1.MeasDateTime, d1.[SectionID], d1.[" + pPKM + "], d1.[" + T_mas + "], d1.[" + ChvKV + "], d1.[" + Regim + "], " +
                        "datediff(SECOND, d1.MeasDateTime, d2.MeasDateTime) as int_sec " +
                        "from cte d1 " +
                        "join cte d2 on d2.rn= d1.rn - 1 " +
                        "where datediff(MINUTE, d1.MeasDateTime, d2.MeasDateTime)<-10 ";

                //SqlConnection CoNn = new SqlConnection();
                CoNn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString;
                CoNn.Open();
                SqlCommand cmd4 = CoNn.CreateCommand();
                cmd4.CommandTimeout = 600; //увеличение время выполнения запроса сек
                cmd4.CommandText = _SQL;
                SqlDataReader reader4 = cmd4.ExecuteReader();

                while (reader4.Read())
                {
                    //подумать что сделать с парсингом dateTime для ускорения обработки json
                    i++;
                    {
                        zap_Tm.DAT = reader4["MeasDateTime"].ToString();//date.ToString("yyyy-MM-dd HH:mm:ss");// + ", " + reader3[ar1].ToString().Replace(",", ".") + ", " + reader3[ar2].ToString().Replace(",", ".") + ", " + reader3[ar3].ToString().Replace(",", ".") + ", " + reader3[ar4].ToString().Replace(",", ".") + ", " + reader3[ar5].ToString().Replace(",", ".") + ", " + reader3[ar6].ToString().Replace(",", ".") + "\n ";
                                                                        //		zap.PKM = reader3[pPKM].ToString();//ПКМD:\РЕЗЕРВНЫЕ КОПИИ ПРОЕКТОВ\WebApplication1\WebApplication1\Content\
                        zap_Tm.T_mas = reader4[T_mas].ToString();//Т воды
                        zap_Tm.ChvKV = reader4[ChvKV].ToString();//Частота вращения КВ
                        zap_Tm.PKM = reader4[pPKM].ToString();//ПКМ
                        zap_Tm.Regim = reader4[Regim].ToString();//Режим работы тепловоза

                    }
                    SPISOK.Add(zap_Tm);//количество записей
                    dat_count++;
                }

                List<Tabl_mas> Tabl_3 = new List<Tabl_mas>();//таблица
                Tabl_mas mTabl_3 = new Tabl_mas();//запись таблицы
                                                  //						  //пустая начальная запись
                Zapis_t_mas last_zapis = new Zapis_t_mas();

                last_zapis.DAT = "00.00.0000 00:00:00";

                int j = 1;

                DateTime date;
                foreach (Zapis_t_mas zapis in SPISOK)
                {
                    mTabl_3.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    mTabl_3.sms = "Температура масла превышена";
                    mTabl_3.T_mas = zapis.T_mas;
                    Tabl_3.Add(mTabl_3);
                    j++;
                    t_1_2.Table.Add(new Tabels_Models.Tab_1_2(date.ToString("yyyy-MM-dd HH-mm-ss"), mTabl_3.sms, zapis.T_mas));
                }
                CoNn.Close();

                //в самом конце сброс флага наличия ошибки
                t_1_2.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_1_2.ERR = true;
                t_1_2.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_1_2);
        }
        public List<Tabels_Models.GroupSmsModel> Algoritm_2_0()
        {
            List<Tabels_Models.GroupSmsModel> GroupSms = new List<Tabels_Models.GroupSmsModel>();
            try
            {
                //строка запроса                                                                            CONVERT(DATETIME, '" + dat_ot_new + " 23:59:59', 102)
                //сортировка по типу сообщений
                //string _SQL_sms = "SELECT * FROM [diag_lcm].[Res].GetMessages(" + _section_id + ", CONVERT(DATETIME,'" + dat_ot + " 00:00:00', 102),  CONVERT(DATETIME,'" + dat_do + "  23:59:59', 102)) ORDER BY mess_type";
                //сортировка по количеству повторений сообщений
                string _SQL_sms = "SELECT * FROM [diag_lcm].[Res].GetMessages(" + _section_id + ", CONVERT(DATETIME,'" + dat_ot + " 00:00:00', 102),  CONVERT(DATETIME,'" + dat_do + "  23:59:59', 102)) ORDER BY quantity DESC";
                SqlConnection CoNn = new SqlConnection();
                CoNn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString;
                CoNn.Open();
                SqlCommand cmd3 = CoNn.CreateCommand();
                cmd3.CommandTimeout = 600;
                cmd3.CommandText = _SQL_sms;
                SqlDataReader reader3 = cmd3.ExecuteReader();
                string s = "";
                while (reader3.Read())
                {
                    s = reader3["mpsu_mess"].ToString();
                    if (s != "Cообщение не распознано: 354")
                    {
                        //вторым запросом подтягиваем имя типа вместо числового типа (подумать и переделать в процедуре в один запрос)
                        SqlConnection CoNn_2 = new SqlConnection();
                        CoNn_2.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString;
                        CoNn_2.Open();
                        SqlCommand cmd_2 = CoNn_2.CreateCommand();
                        cmd_2.CommandTimeout = 600; //увеличение время выполнения запроса сек
                        cmd_2.CommandText = "SELECT [ID],[Name] FROM [diag_lcm].[Config].[AlMessTypes] WHERE[ID] = " + reader3["mess_type"].ToString();
                        SqlDataReader reader_2 = cmd_2.ExecuteReader();
                        //список дат
                        List<DateTime> Mess_Dates = new List<DateTime>();
                        while (reader_2.Read())
                        {
                            ////здесь по коду каждого найденого сообщения формируем запрос на все даты, когда это собщение встречалось
                            ////ДОДЕЛАТЬ ДЛЯ ДРУГИХ СЕРИЙ res._TE25KM_HZM res
                            //SqlConnection CoNn_3 = new SqlConnection();
                            //CoNn_3.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString;
                            //CoNn_3.Open();
                            //SqlCommand cmd_3 = CoNn_3.CreateCommand();
                            //cmd_3.CommandTimeout = 600; //увеличение время выполнения запроса сек
                            //cmd_3.CommandText = "SELECT res.MeasDateTime, res.Mpsu_Mess, res.Analog_1 from res._TE25KM_HZM res " +
                            //    "left JOIN Config.AlertMessages msg on res.Mpsu_Mess = msg.Message " +
                            //    "where msg.id = " + reader3["id"].ToString() + " AND res.MeasDateTime > CONVERT(DATETIME,'" + dat_ot + " 00:00:00', 102) AND res.MeasDateTime < CONVERT(DATETIME,'" + dat_do + "  23:59:59', 102) " +
                            //    "and res.SectionID = " + _section_id +
                            //    " order by res.MeasDateTime";
                            //SqlDataReader reader_3 = cmd_3.ExecuteReader();
                            ////читаем даты
                            //while (reader_3.Read())
                            //{
                            //    Mess_Dates.Add(Convert.ToDateTime(reader_3["MeasDateTime"]));
                            //}
                            //заполнение модели для отчета                            
                            GroupSms.Add(new Tabels_Models.GroupSmsModel(reader3["mpsu_mess"].ToString(), reader_2["Name"].ToString(), Convert.ToInt32(reader3["quantity"]), Mess_Dates));
                        }
                        CoNn_2.Close();
                    }
                }
                CoNn.Close();
                //в самом конце сброс флага наличия ошибки
                //t_2_0.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                //t_2_0.ERR = true;
                //t_2_0.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (GroupSms);
        } //сообщения

        //шаблон алгоритма диагностированния
        public Diag_result<Tabels_Models.Tab_2_1> Algoritm_0_0()
        {
            Diag_result<Tabels_Models.Tab_2_1> t_2_1 = new Diag_result<Tabels_Models.Tab_2_1>();
            try
            {
                //выполнение алгоритма
                //...............
                //сюда копируем алгоритм из старой версии
                //...............
                //заполнение результата. Заполнение построчно, каждую строку таблицы-результата               
                t_2_1.Table.Add(new Tabels_Models.Tab_2_1());//!!! исправить с использованием конструктора с параметрами !!!
                //в самом конце сброс флага наличия ошибки
                t_2_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_2_1.ERR = true;
                t_2_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_2_1);
        }

        public Diag_result<Tabels_Models.Tab_2_1> Algoritm_2_1()
        {
            Diag_result<Tabels_Models.Tab_2_1> t_2_1 = new Diag_result<Tabels_Models.Tab_2_1>();
            try
            {
                DateTime date_beg, date_end;

                string PTG_spr_zapis = "", data_zapis = "", soob_zapis = "", pkm_zapis = "";

                string PTG_spr = "0";

                string pPKM = "", PTG = "", ChvKV = "", Regim = "", Boks = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; PTG = "Analog_101"; ChvKV = "Analog_130"; Regim = "Analog_99"; Boks = "Analog_116";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; PTG = "Analog_101"; ChvKV = "Analog_127"; Regim = "Analog_99"; Boks = "Analog_116";
                        break;

                    default:
                        break;
                }

                string year2, month2, day2;
                string dat_ot_new2, dat_do_new2;

                dat_ot_new2 = dat_ot;
                dat_do_new2 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new2 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new2 + " 23:59:59");

                List<Zapis_Moschnost_diz> SPISOK = new List<Zapis_Moschnost_diz>();
                long i = 0;
                //Int32 dat_count = 0;
                Zapis_Moschnost_diz zap_md = new Zapis_Moschnost_diz();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime, [" + pPKM + "], [" + PTG + "], [" + ChvKV + "], [" + Regim + "],[" + Boks + "]) as ( " +
                           " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + PTG + "], [" + ChvKV + "], [" + Regim + "],[" + Boks + "]" +
                           "FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                           " WHERE([SectionID]= " + _section_id + ") and ([" + ChvKV + "]>340 ) " +
                           " and ([" + PTG + "] between 1 and 1540) " +
                           " and ([" + pPKM + "] between 13 and 15) and ([" + Boks + "]=0) " +
                           " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new2 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new2 + " 23:59:59', 102))), " +
                           " groups AS( " +
                           " SELECT " +
                           " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                           " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + pPKM + "], [" + PTG + "], [" + ChvKV + "], [" + Regim + "], [" + Boks + "] " +
                           " FROM dates d " +
                           "             ) , " +
                           " logic as ( " +
                           " SELECT " +
                           " COUNT(*) AS consecutiveDates," +
                           " MIN(DateTime) AS minDate," +
                           " MAX(DateTime) AS maxDate, " +
                           " MAX(" + PTG + ") as PTG, " +
                           " AVG(" + ChvKV + ") as ChVK, " +
                           " AVG(" + pPKM + ") as PKM, " +
                           " AVG(" + Boks + ") as Boks, " +
                           " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec" +
                           " FROM   groups " +
                           " GROUP BY grp " +
                           " ) " +
                           " Select  top 1 * " +
                           "  from logic " +
                           " where int_sec>30  " +
                           " ORDER BY 1 DESC  ";

                    Connection.Open();
                    SqlDataReader reader12 = Command.ExecuteReader();
                    //  cmd12.CommandTimeout = 600; //увеличение время выполнения запроса сек

                    zap_md.soob1 = "";

                    while ((reader12.Read()) && (reader12["minDate"].ToString() != ""))
                    {
                        zap_md.PKM = reader12["PKM"].ToString();
                        //zap_md.DAT = Convert.ToDateTime(reader12["MaxDate"].ToString()).ToShortDateString();//дата
                        zap_md.DAT = reader12["MinDate"].ToString();//дата
                        zap_md.ChvKV = reader12["ChVK"].ToString();
                        zap_md.PTG = reader12["PTG"].ToString();
                        zap_md.soob1 = "Мала мощность ДГУ";

                        SPISOK.Add(zap_md);//количество записей

                        zap_md.soob1 = "";
                    }

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year2 = date_beg.ToShortDateString().Remove(0, 6);
                    month2 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day2 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new2 = year2 + "." + month2 + "." + day2; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                List<Tabl_md> Tabl_9 = new List<Tabl_md>();//таблица
                Tabl_md mdTabl_9 = new Tabl_md();//запись таблицы
                                                 //пустая начальная запись
                Zapis_Moschnost_diz last_zapis = new Zapis_Moschnost_diz();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Moschnost_diz zapis in SPISOK)
                {
                    mdTabl_9.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    mdTabl_9.znach = zapis.PTG;
                    mdTabl_9.sms = zapis.soob1;
                    mdTabl_9.pkm = zapis.PKM;

                    if (mdTabl_9.pkm == "13")
                    { mdTabl_9.PTG_norm = "1750..2050"; }       ///////добавить

                    if (mdTabl_9.pkm == "14")
                    { mdTabl_9.PTG_norm = "1980..2150"; }       ///////добавить

                    if (mdTabl_9.pkm == "15")
                    { mdTabl_9.PTG_norm = "2160..2260"; }       ///////добавить

                    if (Convert.ToDouble(zapis.PTG) > Convert.ToDouble(PTG_spr))
                    {
                        PTG_spr_zapis = zapis.PTG;
                        PTG_spr = PTG_spr_zapis;
                        data_zapis = zapis.DAT;
                        soob_zapis = zapis.soob1;
                        pkm_zapis = zapis.PKM;
                    }

                    Tabl_9.Add(mdTabl_9);
                    j++;

                    #region Данные для отчета (таблица 2_1)
                    t_2_1.Table.Add(new Tabels_Models.Tab_2_1(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.PTG, mdTabl_9.PTG_norm, zapis.soob1));
                    #endregion

                }

                t_2_1.ERR = false;

            }   //try

            catch (Exception ex)//если же возникла ошибка
            {
                t_2_1.ERR = true;
                t_2_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }

            return (t_2_1);
        }
        public Diag_result<Tabels_Models.Tab_2_2> Algoritm_2_2()
        {
            Diag_result<Tabels_Models.Tab_2_2> t_2_2 = new Diag_result<Tabels_Models.Tab_2_2>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string Pm_vh_d_spr_zapis = "", Pm_vih_2nas_zapis = "", Tm_vih_d_spr_zapis = "", data_zapis = "", pkm_zapis = "";
                string Pm_vh_d_spr_zapis1 = "", Pm_vih_2nas1_zapis = "", Tm_vih_d_spr_zapis1 = "", data_zapis1 = "", pkm_zapis1 = "";

                string Pm_vh_d_spr = "0";
                string Pm_vh_d_spr1;

                string pPKM = "", Pm_vh_d = "", ChvKV = "", Tm_vih_d = "", Pm_vih_2nas = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Pm_vh_d = "Analog_122"; Pm_vih_2nas = "Analog_10"; ChvKV = "Analog_130"; Tm_vih_d = "Analog_75";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Pm_vh_d = "Analog_124"; Pm_vih_2nas = "Analog_10"; ChvKV = "Analog_127"; Tm_vih_d = "Analog_75";
                        break;

                    default:
                        break;
                }
                string nul_stroka = "-";

                string year3, month3, day3;
                string dat_ot_new3, dat_do_new3;

                dat_ot_new3 = dat_ot;
                dat_do_new3 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new3 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new3 + " 23:59:59");

                List<Zapis_Pmasla_MS> SPISOK = new List<Zapis_Pmasla_MS>();
                long i = 0;
                Zapis_Pmasla_MS zap_Pm_ms = new Zapis_Pmasla_MS();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = " with dates(DateTime, [" + pPKM + "], [" + Pm_vh_d + "], [" + Pm_vih_2nas + "], [" + ChvKV + "], [" + Tm_vih_d + "]) as ( " +
                            " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Pm_vh_d + "], [" + Pm_vih_2nas + "], [" + ChvKV + "], [" + Tm_vih_d + "] " +
                            " FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                            " WHERE([SectionID]= " + _section_id + ") and ([" + ChvKV + "]>340 ) " +
                            " and ([" + pPKM + "]>=13) " +
                            " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new3 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new3 + " 23:59:59', 102))) ,  " +
                            " groups AS( " +
                            " SELECT " +
                            " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                            " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + pPKM + "], [" + Pm_vh_d + "], [" + Pm_vih_2nas + "], [" + ChvKV + "], [" + Tm_vih_d + "] " +
                            " FROM dates d " +
                              "         ) , " +
                              " Logic as " +
                              "  ( " +
                            " SELECT " +
                            " COUNT(*) AS consecutiveDates, " +
                            " MIN(DateTime) AS minDate, " +
                            " MAX(DateTime) AS maxDate, " +
                            " MAX(" + pPKM + ") as PKM,  " +
                            " AVG(" + ChvKV + ") as ChVK,  " +
                            " AVG(" + Pm_vh_d + ") as Pm_vh_d,  " +
                            " AVG(" + Pm_vih_2nas + ") as Pm_vih_2nas," +
                            " AVG(" + Tm_vih_d + ") as Tm_vih_diz, " +
                            " AVG((" + Pm_vih_2nas + ")-(" + Pm_vh_d + ")) as deltaP, " +
                            " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                            " FROM   groups " +
                            " GROUP BY grp " +
                            "	) " +
                            "Select top 1 * " +
                            " from logic " +
                            " where int_sec > 30 " +
                            "ORDER BY 10 DESC, 5 DESC ";

                    Connection.Open();
                    SqlDataReader reader13 = Command.ExecuteReader();

                    zap_Pm_ms.soob1 = "";

                    while (reader13.Read())
                    {
                        zap_Pm_ms.PKM = reader13["PKM"].ToString();
                        zap_Pm_ms.DAT = reader13["MinDate"].ToString();//дата
                        zap_Pm_ms.ChvKV = reader13["ChVK"].ToString();
                        zap_Pm_ms.Pm_vh_d = Convert.ToString(Math.Round(Convert.ToDouble(reader13["Pm_vh_d"].ToString()), 1));
                        zap_Pm_ms.Tm_vih_d = Convert.ToString(Math.Round(Convert.ToDouble(reader13["Tm_vih_diz"].ToString()), 1));
                        zap_Pm_ms.Pm_vih_2nas = Convert.ToString(Math.Round(Convert.ToDouble(reader13["Pm_vih_2nas"].ToString()), 1));
                        zap_Pm_ms.deltaP = Convert.ToString(Math.Round(Convert.ToDouble(reader13["deltaP"].ToString()), 1));
                        zap_Pm_ms.soob1 = "";

                        SPISOK.Add(zap_Pm_ms);//количество записей

                        zap_Pm_ms.soob1 = "";
                    }

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year3 = date_beg.ToShortDateString().Remove(0, 6);
                    month3 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day3 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new3 = year3 + "." + month3 + "." + day3; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                List<Tabl_Pm_ms> Tabl_10 = new List<Tabl_Pm_ms>();//таблица
                Tabl_Pm_ms Pm_msTabl_10 = new Tabl_Pm_ms();//запись таблицы
                                                           //пустая начальная запись
                Zapis_Pmasla_MS last_zapis = new Zapis_Pmasla_MS();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Pmasla_MS zapis in SPISOK)
                {
                    Pm_msTabl_10.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    Pm_msTabl_10.PKM = zapis.PKM;
                    Pm_msTabl_10.Pm_vh_d = zapis.Pm_vh_d;
                    Pm_msTabl_10.deltaP = zapis.deltaP;
                    Pm_msTabl_10.Tm_vih_d = zapis.Tm_vih_d;


                    if (Convert.ToDouble(zapis.Pm_vh_d) > Convert.ToDouble(Pm_vh_d_spr))                //данные для отчета-шаблона  
                    {
                        ////добавить столбец zapis.deltaP           !!!!!!!
                        Pm_vh_d_spr_zapis = zapis.Pm_vh_d;
                        Pm_vh_d_spr = Pm_vh_d_spr_zapis;
                        Tm_vih_d_spr_zapis = zapis.Tm_vih_d;
                        data_zapis = zapis.DAT;
                        pkm_zapis = zapis.PKM;
                    }

                    Tabl_10.Add(Pm_msTabl_10);
                    j++;

                    #region Данные для отчета (таблица 2_2)
                    t_2_2.Table.Add(new Tabels_Models.Tab_2_2(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.Pm_vh_d, zapis.deltaP, zapis.Tm_vih_d));
                    #endregion

                }

                string year4, month4, day4;
                string dat_ot_new4, dat_do_new4;

                dat_ot_new4 = dat_ot;
                dat_do_new4 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new4 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new4 + " 23:59:59");

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = " with dates(DateTime, [" + pPKM + "], [" + Pm_vh_d + "], [" + ChvKV + "], [" + Tm_vih_d + "]) as ( " +
                            " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Pm_vh_d + "], [" + ChvKV + "], [" + Tm_vih_d + "] " +
                            " FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                            " WHERE([SectionID]= " + _section_id + ") and ([" + ChvKV + "]>340 )and ([" + pPKM + "] between 0 and 1) and  [" + Tm_vih_d + "]>65 " +
                            " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new4 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new4 + " 23:59:59', 102))) ,  " +
                            " groups AS( " +
                            " SELECT " +
                            " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                            " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + pPKM + "], [" + Pm_vh_d + "], [" + ChvKV + "], [" + Tm_vih_d + "] " +
                            " FROM dates d " +
                            "           ),  " +
                            " logic as " +
                            "       (  " +
                            " SELECT " +
                            " COUNT(*) AS consecutiveDates, " +
                            " MIN(DateTime) AS minDate, " +
                            " MAX(DateTime) AS maxDate, " +
                            " MAX(" + pPKM + ") as PKM,  " +
                            " AVG(" + Pm_vh_d + ") as Pm_vh_d,  " +
                            " AVG(" + ChvKV + ") as ChVK,  " +
                            " AVG(" + Tm_vih_d + ") as Tm_vih_diz, " +
                            " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                            " FROM   groups " +
                            " GROUP BY grp " +
                            "		 ) " +
                            "Select top 1 *  " +
                            " from logic " +
                            "where int_sec > 30" +
                            "ORDER BY 8 DESC ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader13 = Command.ExecuteReader();

                    zap_Pm_ms.soob2 = "";

                    while (reader13.Read())
                    {
                        //i++;

                        zap_Pm_ms.PKM = reader13["PKM"].ToString();
                        zap_Pm_ms.DAT = Convert.ToDateTime(reader13["MaxDate"].ToString()).ToShortDateString();//дата
                        zap_Pm_ms.ChvKV = reader13["ChVK"].ToString();
                        zap_Pm_ms.Pm_vh_d = Convert.ToString(Math.Round(Convert.ToDouble(reader13["Pm_vh_d"].ToString()), 1));
                        zap_Pm_ms.Tm_vih_d = Convert.ToString(Math.Round(Convert.ToDouble(reader13["Tm_vih_diz"].ToString()), 0));
                        zap_Pm_ms.soob2 = "";

                        SPISOK.Add(zap_Pm_ms);//количество записей

                        // dat_count++;
                        zap_Pm_ms.soob1 = "";
                        Pm_vh_d_spr1 = zap_Pm_ms.Pm_vh_d;           //для отчета взять реальное значение для сравнения
                    }

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year4 = date_beg.ToShortDateString().Remove(0, 6);
                    month4 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day4 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new4 = year4 + "." + month4 + "." + day4; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                Pm_vh_d_spr1 = zap_Pm_ms.Pm_vh_d;

                foreach (Zapis_Pmasla_MS zapis in SPISOK)
                {
                    Pm_msTabl_10.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    Pm_msTabl_10.PKM = zapis.PKM;
                    Pm_msTabl_10.Pm_vh_d = zapis.Pm_vh_d;
                    if (Pm_msTabl_10.Pm_vh_d == "1.7")
                    {
                        int a = 3;
                    }
                    Pm_msTabl_10.Tm_vih_d = zapis.Tm_vih_d;

                    if (Convert.ToDouble(zapis.Pm_vh_d) < Convert.ToDouble(Pm_vh_d_spr1))                //данные для отчета-шаблона  
                    {
                        Pm_vh_d_spr_zapis1 = zapis.Pm_vh_d;
                        Pm_vh_d_spr1 = Pm_vh_d_spr_zapis;
                        Tm_vih_d_spr_zapis1 = zapis.Tm_vih_d;
                        data_zapis1 = zapis.DAT;
                        pkm_zapis1 = zapis.PKM;
                    }

                    Tabl_10.Add(Pm_msTabl_10);
                    j++;

                    #region Данные для отчета (таблица 2_2)
                    t_2_2.Table.Add(new Tabels_Models.Tab_2_2(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.Pm_vh_d, zapis.deltaP, zapis.Tm_vih_d));
                    //t_2_2.Table.Add(new Tabels_Models.Tab_2_2(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.Pm_vh_d, nul_stroka, zapis.Tm_vih_d));
                    #endregion
                }

                //...............
                //в самом конце сброс флага наличия ошибки
                t_2_2.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_2_2.ERR = true;
                t_2_2.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_2_2);
        }
        public Diag_result<Tabels_Models.Tab_2_3> Algoritm_2_3()
        {
            Diag_result<Tabels_Models.Tab_2_3> t_2_3 = new Diag_result<Tabels_Models.Tab_2_3>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string LC_spr_zapis = "", LC1_spr_zapis = "", LC2_spr_zapis = "", LC3_spr_zapis = "", LC4_spr_zapis = "", LC5_spr_zapis = "", LC6_spr_zapis = "";
                string LC7_spr_zapis = "", LC8_spr_zapis = "", LP_spr_zapis = "", LP1_spr_zapis = "", LP2_spr_zapis = "", LP3_spr_zapis = "", LP4_spr_zapis = "";
                string LP5_spr_zapis = "", LP6_spr_zapis = "", LP7_spr_zapis = "", LP8_spr_zapis = "", data_zapis = "", pkm_zapis = "";

                string LC_spr = "0";

                string pPKM = "", ChvKV = "", LC = "", LC1 = "", LC2 = "", LC3 = "", LC4 = "", LC5 = "", LC6 = "", LC7 = "", LC8 = "";
                string PC = "", PC1 = "", PC2 = "", PC3 = "", PC4 = "", PC5 = "", PC6 = "", PC7 = "", PC8 = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; ChvKV = "Analog_130"; LC1 = "Analog_80"; LC2 = "Analog_81"; LC3 = "Analog_82"; LC4 = "Analog_83"; LC5 = "Analog_84";
                        LC6 = "Analog_85"; LC7 = "Analog_86"; LC8 = "Analog_87"; LC = "Analog_88";
                        PC1 = "Analog_89"; PC2 = "Analog_90"; PC3 = "Analog_91"; PC4 = "Analog_92"; PC5 = "Analog_93";
                        PC6 = "Analog_94"; PC7 = "Analog_95"; PC8 = "Analog_96"; PC = "Analog_97";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; ChvKV = "Analog_127"; LC1 = "Analog_80"; LC2 = "Analog_81"; LC3 = "Analog_82"; LC4 = "Analog_83"; LC5 = "Analog_84";
                        LC6 = "Analog_85"; LC7 = "Analog_86"; LC8 = "Analog_87"; LC = "Analog_88";
                        PC1 = "Analog_89"; PC2 = "Analog_90"; PC3 = "Analog_91"; PC4 = "Analog_92"; PC5 = "Analog_93";
                        PC6 = "Analog_94"; PC7 = "Analog_95"; PC8 = "Analog_96"; PC = "Analog_97";
                        break;

                    default:
                        break;
                }

                double sum_sredn;

                string year5, month5, day5;
                string dat_ot_new5, dat_do_new5;

                dat_ot_new5 = dat_ot;
                dat_do_new5 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new5 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new5 + " 23:59:59");

                List<Zapis_TCil> SPISOK = new List<Zapis_TCil>();
                long i = 0;
                Zapis_TCil zap_TCil = new Zapis_TCil();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime, [" + pPKM + "], [" + ChvKV + "], [" + LC1 + "], [" + LC2 + "], [" + LC3 + "], [" + LC4 + "], [" + LC5 + "], [" + LC6 + "], [" + LC7 + "], " +
                                            "[" + LC8 + "], [" + LC + "], [" + PC1 + "],[" + PC2 + "], [" + PC3 + "], [" + PC4 + "], [" + PC5 + "],[" + PC6 + "], [" + PC7 + "], [" + PC8 + "], [" + PC + "]) as ( " +
                                            "select DISTINCT CAST(MeasDateTime as DateTime), [" + pPKM + "], [" + ChvKV + "], [" + LC1 + "], [" + LC2 + "], [" + LC3 + "], [" + LC4 + "], [" + LC5 + "], [" + LC6 + "], [" + LC7 + "], " +
                                            "[" + LC8 + "], [" + LC + "], [" + PC1 + "],[" + PC2 + "], [" + PC3 + "], [" + PC4 + "], [" + PC5 + "],[" + PC6 + "], [" + PC7 + "], [" + PC8 + "], [" + PC + "] " +
                                            "FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                                           " WHERE([SectionID]= " + _section_id + ") and ([" + ChvKV + "]>340) and ([" + pPKM + "]>8  ) and  " +
                                            "(MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new5 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new5 + " 23:59:59', 102))), " +
                                            "groups AS( " +
                                            "SELECT " +
                                            "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                            "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,[" + pPKM + "], [" + ChvKV + "], [" + LC1 + "], " +
                                            " [" + LC2 + "], [" + LC3 + "], [" + LC4 + "], [" + LC5 + "], [" + LC6 + "], [" + LC7 + "],[" + LC8 + "], [" + LC + "], " +
                                            " [" + PC1 + "],[" + PC2 + "], [" + PC3 + "], [" + PC4 + "], [" + PC5 + "],[" + PC6 + "], [" + PC7 + "], [" + PC8 + "], [" + PC + "] " +
                                            "   FROM dates d " +
                                            "        ), " +
                                            "logic as " +
                                            "   ( " +
                                            "SELECT " +
                                            "COUNT(*) AS consecutiveDates, " +
                                            "MIN(DateTime) AS minDate, " +
                                            "MAX(DateTime) AS maxDate, " +
                                            "MAX([" + pPKM + "]) as PKM, " +
                                            "AVG([" + ChvKV + "]) as ChVK, " +
                                            "AVG([" + LC1 + "]) as TL1Cil, " +
                                            "AVG([" + LC2 + "]) as TL2Cil, " +
                                            "AVG([" + LC3 + "]) as TL3Cil, " +
                                            "AVG([" + LC4 + "]) as TL4Cil, " +
                                            "AVG([" + LC5 + "]) as TL5Cil, " +
                                            "AVG([" + LC6 + "]) as TL6Cil, " +
                                            "AVG([" + LC7 + "]) as TL7Cil, " +
                                            "AVG([" + LC8 + "]) as TL8Cil, " +
                                            "AVG([" + LC + "]) as TLCil, " +
                                            "AVG([" + PC1 + "]) as TP1Cil, " +
                                            "AVG([" + PC2 + "]) as TP2Cil, " +
                                            "AVG([" + PC3 + "]) as TP3Cil, " +
                                            "AVG([" + PC4 + "]) as TP4Cil, " +
                                            "AVG([" + PC5 + "]) as TP5Cil, " +
                                            "AVG([" + PC6 + "]) as TP6Cil, " +
                                            "AVG([" + PC7 + "]) as TP7Cil, " +
                                            "AVG([" + PC8 + "]) as TP8Cil, " +
                                            "AVG([" + PC + "]) as TPCil, " +
                                            "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                            "    FROM   groups " +
                                            "GROUP BY grp " +
                                            "	) " +
                                            " Select top 1 * " +
                                            " from logic " +
                                            " where int_sec > 30 " +
                                            "ORDER BY 24 DESC ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader14 = Command.ExecuteReader();

                    while (reader14.Read())
                    {
                        zap_TCil.PKM = reader14["PKM"].ToString();
                        zap_TCil.DAT = reader14["MinDate"].ToString();//дата
                        zap_TCil.CilL = reader14["TLCil"].ToString();
                        zap_TCil.CilL1 = reader14["TL1Cil"].ToString();
                        zap_TCil.CilL2 = reader14["TL2Cil"].ToString();
                        zap_TCil.CilL3 = reader14["TL3Cil"].ToString();
                        zap_TCil.CilL4 = reader14["TL4Cil"].ToString();
                        zap_TCil.CilL5 = reader14["TL5Cil"].ToString();
                        zap_TCil.CilL6 = reader14["TL6Cil"].ToString();
                        zap_TCil.CilL7 = reader14["TL7Cil"].ToString();
                        zap_TCil.CilL8 = reader14["TL8Cil"].ToString();
                        zap_TCil.CilP = reader14["TPCil"].ToString();
                        zap_TCil.CilP1 = reader14["TP1Cil"].ToString();
                        zap_TCil.CilP2 = reader14["TP2Cil"].ToString();
                        zap_TCil.CilP3 = reader14["TP3Cil"].ToString();
                        zap_TCil.CilP4 = reader14["TP4Cil"].ToString();
                        zap_TCil.CilP5 = reader14["TP5Cil"].ToString();
                        zap_TCil.CilP6 = reader14["TP6Cil"].ToString();
                        zap_TCil.CilP7 = reader14["TP7Cil"].ToString();
                        zap_TCil.CilP8 = reader14["TP8Cil"].ToString();

                        sum_sredn = Math.Round((Convert.ToDouble(zap_TCil.CilL1) + Convert.ToDouble(zap_TCil.CilL2) + Convert.ToDouble(zap_TCil.CilL3) + Convert.ToDouble(zap_TCil.CilL4) + Convert.ToDouble(zap_TCil.CilL5) + Convert.ToDouble(zap_TCil.CilL6) + Convert.ToDouble(zap_TCil.CilL7) + Convert.ToDouble(zap_TCil.CilL8) + Convert.ToDouble(zap_TCil.CilP1) + Convert.ToDouble(zap_TCil.CilP2) + Convert.ToDouble(zap_TCil.CilP3) + Convert.ToDouble(zap_TCil.CilP4) + Convert.ToDouble(zap_TCil.CilP5) + Convert.ToDouble(zap_TCil.CilP6) + Convert.ToDouble(zap_TCil.CilP7) + Convert.ToDouble(zap_TCil.CilP8)) / 16);

                        zap_TCil.delta_cil_L1 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL1)));
                        zap_TCil.delta_cil_L2 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL2)));
                        zap_TCil.delta_cil_L3 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL3)));
                        zap_TCil.delta_cil_L4 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL4)));
                        zap_TCil.delta_cil_L5 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL5)));
                        zap_TCil.delta_cil_L6 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL6)));
                        zap_TCil.delta_cil_L7 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL7)));
                        zap_TCil.delta_cil_L8 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilL8)));

                        zap_TCil.delta_cil_P1 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP1)));
                        zap_TCil.delta_cil_P2 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP2)));
                        zap_TCil.delta_cil_P3 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP3)));
                        zap_TCil.delta_cil_P4 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP4)));
                        zap_TCil.delta_cil_P5 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP5)));
                        zap_TCil.delta_cil_P6 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP6)));
                        zap_TCil.delta_cil_P7 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP7)));
                        zap_TCil.delta_cil_P8 = Convert.ToString(Math.Abs(sum_sredn - Convert.ToDouble(zap_TCil.CilP8)));

                        zap_TCil.delta_cil = Convert.ToString(sum_sredn);

                        SPISOK.Add(zap_TCil);//количество записей
                    }

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year5 = date_beg.ToShortDateString().Remove(0, 6);
                    month5 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day5 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new5 = year5 + "." + month5 + "." + day5; // переводим в формат yyyymmdd для sql

                    Connection.Close();
                } //while (date_beg < date_end)


                List<Tabl_TCil> Tabl_11 = new List<Tabl_TCil>();//таблица
                Tabl_TCil tcTabl_11 = new Tabl_TCil();//запись таблицы
                                                      //пустая начальная запись
                Zapis_TCil last_zapis = new Zapis_TCil();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_TCil zapis in SPISOK)
                {
                    tcTabl_11.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    tcTabl_11.PKM = zapis.PKM;
                    tcTabl_11.CilL = zapis.CilL;
                    tcTabl_11.CilL1 = zapis.CilL1;
                    tcTabl_11.CilL2 = zapis.CilL2;
                    tcTabl_11.CilL3 = zapis.CilL3;
                    tcTabl_11.CilL4 = zapis.CilL4;
                    tcTabl_11.CilL5 = zapis.CilL5;
                    tcTabl_11.CilL6 = zapis.CilL6;
                    tcTabl_11.CilL7 = zapis.CilL7;
                    tcTabl_11.CilL8 = zapis.CilL8;

                    tcTabl_11.CilP = zapis.CilP;
                    tcTabl_11.CilP1 = zapis.CilP1;
                    tcTabl_11.CilP2 = zapis.CilP2;
                    tcTabl_11.CilP3 = zapis.CilP3;
                    tcTabl_11.CilP4 = zapis.CilP4;
                    tcTabl_11.CilP5 = zapis.CilP5;
                    tcTabl_11.CilP6 = zapis.CilP6;
                    tcTabl_11.CilP7 = zapis.CilP7;
                    tcTabl_11.CilP8 = zapis.CilP8;

                    tcTabl_11.delta_cil = zapis.delta_cil;      //////добавить
                    tcTabl_11.delta_cil_L1 = zapis.delta_cil_L1;
                    tcTabl_11.delta_cil_L2 = zapis.delta_cil_L2;
                    tcTabl_11.delta_cil_L3 = zapis.delta_cil_L3;
                    tcTabl_11.delta_cil_L4 = zapis.delta_cil_L4;
                    tcTabl_11.delta_cil_L5 = zapis.delta_cil_L5;
                    tcTabl_11.delta_cil_L6 = zapis.delta_cil_L6;
                    tcTabl_11.delta_cil_L7 = zapis.delta_cil_L7;
                    tcTabl_11.delta_cil_L8 = zapis.delta_cil_L8;

                    tcTabl_11.delta_cil_P1 = zapis.delta_cil_P1;
                    tcTabl_11.delta_cil_P2 = zapis.delta_cil_P2;
                    tcTabl_11.delta_cil_P3 = zapis.delta_cil_P3;
                    tcTabl_11.delta_cil_P4 = zapis.delta_cil_P4;
                    tcTabl_11.delta_cil_P5 = zapis.delta_cil_P5;
                    tcTabl_11.delta_cil_P6 = zapis.delta_cil_P6;
                    tcTabl_11.delta_cil_P7 = zapis.delta_cil_P7;
                    tcTabl_11.delta_cil_P8 = zapis.delta_cil_P8;

                    if (Convert.ToDouble(zapis.CilL) > Convert.ToDouble(LC_spr))                //данные для отчета-шаблона  
                    {
                        LC_spr_zapis = zapis.CilL;
                        LC_spr = LC_spr_zapis;
                        LC1_spr_zapis = zapis.CilL1;
                        LC2_spr_zapis = zapis.CilL2;
                        LC3_spr_zapis = zapis.CilL3;
                        LC4_spr_zapis = zapis.CilL4;
                        LC5_spr_zapis = zapis.CilL5;
                        LC6_spr_zapis = zapis.CilL6;
                        LC7_spr_zapis = zapis.CilL7;
                        LC8_spr_zapis = zapis.CilL8;
                        LP_spr_zapis = zapis.CilP;
                        LP1_spr_zapis = zapis.CilP1;
                        LP2_spr_zapis = zapis.CilP2;
                        LP3_spr_zapis = zapis.CilP3;
                        LP4_spr_zapis = zapis.CilP4;
                        LP5_spr_zapis = zapis.CilP5;
                        LP6_spr_zapis = zapis.CilP6;
                        LP7_spr_zapis = zapis.CilP7;
                        LP8_spr_zapis = zapis.CilP8;
                        data_zapis = zapis.DAT;
                        pkm_zapis = zapis.PKM;
                    }
                    Tabl_11.Add(tcTabl_11);

                    j++;

                    #region Данные для отчета (таблица 2_3)
                    t_2_3.Table.Add(new Tabels_Models.Tab_2_3(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM,
                    zapis.CilL, zapis.CilL1, zapis.delta_cil_L1, zapis.CilL2, zapis.delta_cil_L2, zapis.CilL3, zapis.delta_cil_L3, zapis.CilL4, zapis.delta_cil_L4, zapis.CilL5, zapis.delta_cil_L5, zapis.CilL6, zapis.delta_cil_L6, zapis.CilL7, zapis.delta_cil_L7, zapis.CilL8, zapis.delta_cil_L8,
                    zapis.CilP, zapis.CilP1, zapis.delta_cil_P1, zapis.CilP2, zapis.delta_cil_P2, zapis.CilP3, zapis.delta_cil_P3, zapis.CilP4, zapis.delta_cil_P4, zapis.CilP5, zapis.delta_cil_P5, zapis.CilP6, zapis.delta_cil_P6, zapis.CilP7, zapis.delta_cil_P7, zapis.CilP8, zapis.delta_cil_P8,
                    zapis.delta_cil));
                    #endregion
                }
                //...............
                //в самом конце сброс флага наличия ошибки
                t_2_3.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_2_3.ERR = true;
                t_2_3.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_2_3);
        }
        public Diag_result<Tabels_Models.Tab_3_1> Algoritm_3_1()
        {
            Diag_result<Tabels_Models.Tab_3_1> t_3_1 = new Diag_result<Tabels_Models.Tab_3_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string Tv_vih_diz_spr_zapis = "", Tv_hol_kont_spr_zapis = "", Tokr_sr_spr_zapis = "", vremia_zapis = "", data_zapis = "", pkm_zapis = "";
                string Tv_vih_diz_spr_zapis1 = "", Tv_hol_kont_spr_zapis1 = "", Tokr_sr_spr_zapis1 = "", vremia_zapis1 = "", data_zapis1 = "", pkm_zapis1 = "";

                string Tv_vih_diz_spr = "0";
                string Tv_vih_diz_spr1 = "0";

                string pPKM = "", Tv_vih_diz = "", Tv_hol_kont = "", Tokr_sr = "", PTG = "";
                string MB1 = "", MB2 = "", MB3 = "", MB4 = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Tv_vih_diz = "Analog_29"; Tv_hol_kont = "Analog_76"; Tokr_sr = "Analog_72"; PTG = "Analog_101";
                        MB1 = "DiscrIn_81"; MB2 = "DiscrIn_82"; MB3 = "DiscrIn_83"; MB4 = "DiscrIn_84";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Tv_vih_diz = "Analog_29"; Tv_hol_kont = "Analog_76"; Tokr_sr = "Analog_72"; PTG = "Analog_101";
                        MB1 = "DiscrIn_81"; MB2 = "DiscrIn_82"; MB3 = "DiscrIn_83"; MB4 = "DiscrIn_84";
                        break;

                    default:
                        break;
                }

                string year6, month6, day6, year7, month7, day7;
                string dat_ot_new6, dat_do_new6, dat_ot_new7, dat_do_new7;

                string YYYY, MM, DD, hh, mm, ss;
                string YYYY1, MM1, DD1, hh1, mm1, ss1;

                String minDate, maxDate, minDate_dop, maxDate_dop;
                String deltaDate, pkm_str;
                String Tv_v_diz_str, Tv_hol_kont_str, Tokr_sr_str;

                DateTime minDate1, maxDate1, minDate2, maxDate2;

                double deltaDate_mm, deltaDate_ss;
                double delta_Tv_v_diz, delta_Tv_hol_kont, delta_Tokr_sr;

                dat_ot_new6 = dat_ot;
                dat_do_new6 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new6 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new6 + " 23:59:59");

                pkm_str = "0";  //условно
                deltaDate = "0"; //условно
                minDate = dat_ot_new6 + " 00:00:00"; //условно
                maxDate = dat_do_new6 + " 23:59:59"; //условно

                int dannie = 0;     //проверка на наличие данных в основном/первом запросе

                List<Zapis_Holod_ustr> SPISOK = new List<Zapis_Holod_ustr>();
                List<Zapis_Holod_ustr> SPISOK1 = new List<Zapis_Holod_ustr>();
                long i = 0;
                Zapis_Holod_ustr zap_HUstr = new Zapis_Holod_ustr();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "], [" + Tv_vih_diz + "], [" + Tv_hol_kont + "], [" + Tokr_sr + "],[" + MB1 + "], [" + MB2 + "], [" + MB3 + "], [" + MB4 + "]) as(  " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Tv_vih_diz + "], [" + Tv_hol_kont + "], [" + Tokr_sr + "],[" + MB1 + "], [" + MB2 + "], [" + MB3 + "], [" + MB4 + "] " +
                                          " FROM[diag_lcm].[Res].[_" + _tablica + "]" +
                                          " WHERE ([SectionID]= " + _section_id + ") and([" + pPKM + "]= 8 or[" + pPKM + "]= 9 or[" + pPKM + "]= 10 or[" + pPKM + "]= 11 or[" + pPKM + "]= 12 or[" + pPKM + "]= 13 or[" + pPKM + "]= 14 or[" + pPKM + "]= 15) " +
                                          " and([" + MB1 + "]= 0 and [" + MB2 + "] = 0 and [" + MB3 + "] = 0 and [" + MB4 + "] = 0) " +
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new6 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new6 + " 23:59:59', 102))), " +
                                          "   groups AS( " +
                                          "   SELECT " +
                                          "   ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "   dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + MB1 + "], [" + MB2 + "], [" + MB3 + "], [" + MB4 + "], " +
                                          "   [" + pPKM + "], [" + Tv_vih_diz + "], [" + Tv_hol_kont + "], [" + Tokr_sr + "] " +
                                          "      FROM dates d " +
                                          "             ),  " +
                                          " logic as ( " +
                                          "  SELECT  " +
                                          "  COUNT(*) AS consecutiveDates, " +
                                          "  MIN(DateTime) AS minDate, " +
                                          "  MAX(DateTime) AS maxDate, " +
                                          "  ([" + pPKM + "]) as PKM,  " +
                                          "  MAX([" + Tv_vih_diz + "]) as Tv_vih_diz_max,  " +
                                          "  MIN([" + Tv_vih_diz + "]) as Tv_vih_diz_min, " +
                                          "  MAX([" + Tv_hol_kont + "]) as Tv_hol_kont_max, " +
                                          "  MIN([" + Tv_hol_kont + "]) as Tv_hol_kont_min, " +
                                          "  MAX([" + Tokr_sr + "]) as Tokr_vozd_max, " +
                                          "  MIN([" + Tokr_sr + "]) as Tokr_vozd_min, " +
                                          "  datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          "  FROM   groups " +
                                          "GROUP BY grp,[" + pPKM + "] " +
                                          "		 )" +
                                          " Select top 1 *" +
                                          " from logic" +
                                          " where int_sec > 30" +
                                          "ORDER BY 4 DESC,1 DESC";

                    Connection.Open();
                    SqlDataReader reader15 = Command.ExecuteReader();

                    if (reader15.HasRows) //проверка на содержание одной или несколько строк, есть ли данные
                    {
                        dannie = 1;
                    }
                    else
                    {
                        dannie = 0;
                    }

                    delta_Tv_v_diz = 0;
                    delta_Tv_hol_kont = 0;
                    delta_Tokr_sr = 0;

                    while (reader15.Read())           //  разбор даты и время самого продолжительно участка для следующего запроса
                    {
                        minDate1 = Convert.ToDateTime(reader15["MinDate"].ToString());
                        minDate = minDate1.ToShortDateString() + " " + minDate1.ToLongTimeString();
                        if (minDate1.ToLongTimeString().Length == 7)
                        {
                            minDate = minDate1.ToShortDateString() + " 0" + minDate1.ToLongTimeString();
                        }
                        //minDate = minDate1.Year.ToString("yyyy")+"-"+ minDate1.Month.ToString()+ "-" + minDate1.Day.ToString()+ " " + minDate1.Hour.ToString()+ ":" + minDate1.Minute.ToString()+ ":" + minDate1.Second.ToString();
                        maxDate1 = Convert.ToDateTime(reader15["MaxDate"].ToString());
                        maxDate = maxDate1.ToShortDateString() + " " + maxDate1.ToLongTimeString();
                        if (maxDate1.ToLongTimeString().Length == 7)
                        {
                            maxDate = maxDate1.ToShortDateString() + " 0" + maxDate1.ToLongTimeString();
                        }

                        deltaDate = reader15["int_sec"].ToString(); //разница времени в сек
                        if (Convert.ToDouble(deltaDate) > 60)
                        {
                            deltaDate_mm = Math.Truncate(Convert.ToDouble(deltaDate) / 60);  //выделение целого числа из деления-минуты
                            deltaDate_ss = Convert.ToDouble(deltaDate) - (deltaDate_mm * 60);
                            deltaDate = Convert.ToString(deltaDate_mm + " мин. " + deltaDate_ss + " сек.");
                        }
                        else
                        {
                            deltaDate = reader15["int_sec"].ToString() + " сек.";
                        }
                        pkm_str = reader15["PKM"].ToString();

                        YYYY = minDate.ToString().Remove(0, 6).Remove(4);
                        MM = minDate.ToString().Remove(0, 3).Remove(2);
                        DD = minDate.ToString().Remove(2);
                        hh = " " + minDate.ToString().Remove(0, 11).Remove(2);
                        mm = minDate.ToString().Remove(0, 14).Remove(2);
                        ss = minDate.ToString().Remove(0, minDate.Length - 2);

                        minDate = YYYY + "-" + MM + "-" + DD + hh + ":" + mm + ":" + ss;        // переводим в формат yyyymmdd hhmmss для sql

                        YYYY1 = maxDate.ToString().Remove(0, 6).Remove(4);
                        MM1 = maxDate.ToString().Remove(0, 3).Remove(2);
                        DD1 = maxDate.ToString().Remove(2);
                        hh1 = " " + maxDate.ToString().Remove(0, 11).Remove(2);
                        mm1 = maxDate.ToString().Remove(0, 14).Remove(2);
                        ss1 = maxDate.ToString().Remove(0, maxDate.Length - 2);

                        maxDate = YYYY1 + "-" + MM1 + "-" + DD1 + hh1 + ":" + mm1 + ":" + ss1;  // переводим в формат yyyymmdd hhmmss для sql
                    }

                    Connection.Close();

                    if (dannie == 1) //проверка на содержание одной или несколько строк, есть ли данные
                    {
                        Command.CommandText = " with SRC as ( " +
                                           "SELECT top 1[MeasDateTime],[" + Tv_vih_diz + "] as a2,[" + Tv_hol_kont + "] as b2,[" + Tokr_sr + "] as c2,[" + PTG + "] as PTG2 " +
                                           " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                           " WHERE ([SectionID]= " + _section_id + ")  and ([" + pPKM + "]=" + pkm_str + ") and ([" + MB1 + "]= 0 and [" + MB2 + "]= 0 and [" + MB3 + "]= 0 and [" + MB4 + "]= 0) " +
                                           " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + minDate + "' , 102) AND CONVERT(DATETIME, '" + maxDate + "', 102)) " +
                                           " order by MeasDateTime desc     " +
                                           "              ) " +
                                           " SELECT* FROM SRC " +
                                           "   UNION " +
                                           " SELECT top 1 [MeasDateTime],[" + Tv_vih_diz + "] as a1,[" + Tv_hol_kont + "] as b1,[" + Tokr_sr + "] as c1,[" + PTG + "] as PTG1 " +
                                           " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                           " WHERE ([SectionID]= " + _section_id + ")  and ([" + pPKM + "]=" + pkm_str + ") and ([" + MB1 + "]= 0 and [" + MB2 + "]= 0 and [" + MB3 + "]= 0 and [" + MB4 + "]= 0) " +
                                           " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + minDate + "' , 102) AND CONVERT(DATETIME, '" + maxDate + "', 102)) ";
                        Connection.Open();
                        SqlDataReader reader16 = Command.ExecuteReader();

                        int stroka = 0;

                        while (reader16.Read())
                        {
                            stroka++;

                            Tv_v_diz_str = reader16["a2"].ToString();
                            Tv_hol_kont_str = reader16["b2"].ToString();
                            Tokr_sr_str = reader16["c2"].ToString();

                            zap_HUstr.DAT = reader16["MeasDateTime"].ToString();//дата
                            zap_HUstr.PKM = pkm_str;

                            zap_HUstr.delta_Tv_v_diz = Convert.ToString(Math.Round(Math.Abs(delta_Tv_v_diz - Math.Round(Convert.ToDouble(Tv_v_diz_str), 1)), 1));
                            zap_HUstr.delta_Tv_hol_kont = Convert.ToString(Math.Abs(delta_Tv_hol_kont - Convert.ToDouble(Tv_hol_kont_str)));
                            //zap_HUstr.delta_Tokr_sr = Convert.ToString(Math.Abs(delta_Tokr_sr - Convert.ToDouble(Tokr_sr_str)));
                            zap_HUstr.PTG = reader16["PTG2"].ToString();
                            zap_HUstr.Tokr_sr = Tokr_sr_str;

                            zap_HUstr.delta_Time_str = deltaDate;

                            delta_Tv_v_diz = Math.Abs(delta_Tv_v_diz - Math.Round(Convert.ToDouble(Tv_v_diz_str), 1));
                            delta_Tv_hol_kont = Math.Abs(delta_Tv_hol_kont - Convert.ToDouble(Tv_hol_kont_str));
                            //delta_Tokr_sr = Math.Abs(delta_Tokr_sr - Convert.ToDouble(Tokr_sr_str));

                            if (stroka == 2)
                            {
                                SPISOK.Add(zap_HUstr);//количество записей
                            }

                        }
                    }       //if (dannie == 1)
                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year6 = date_beg.ToShortDateString().Remove(0, 6);
                    month6 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day6 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new6 = year6 + "." + month6 + "." + day6; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                List<Tabl_HolodUstr> Tabl_12 = new List<Tabl_HolodUstr>();//таблица
                Tabl_HolodUstr huTabl_12 = new Tabl_HolodUstr();//запись таблицы
                                                                //пустая начальная запись
                Zapis_Holod_ustr last_zapis = new Zapis_Holod_ustr();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Holod_ustr zapis in SPISOK)
                {
                    huTabl_12.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    huTabl_12.PKM = zapis.PKM;
                    huTabl_12.Tv_vih_diz = zapis.delta_Tv_v_diz;
                    huTabl_12.Tv_hol_kont = zapis.delta_Tv_hol_kont;
                    huTabl_12.Tokr_sr = zapis.Tokr_sr;
                    huTabl_12.PTG = zapis.PTG;
                    huTabl_12.vremia = zapis.delta_Time_str;

                    if (Convert.ToDouble(zapis.delta_Tv_v_diz) > Convert.ToDouble(Tv_vih_diz_spr))                //данные для отчета-шаблона  
                    {
                        //!!!!!! добавить столбец мощность PTG
                        Tv_vih_diz_spr_zapis = zapis.delta_Tv_v_diz;
                        Tv_vih_diz_spr = Tv_vih_diz_spr_zapis;
                        Tv_hol_kont_spr_zapis = zapis.delta_Tv_hol_kont;
                        Tokr_sr_spr_zapis = zapis.Tokr_sr;
                        vremia_zapis = zapis.delta_Time_str;
                        data_zapis = zapis.DAT;
                        pkm_zapis = zapis.PKM;
                    }

                    Tabl_12.Add(huTabl_12);

                    j++;

                    #region Данные для отчета (таблица 3_1)
                    t_3_1.Table.Add(new Tabels_Models.Tab_3_1(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.PKM, zapis.PTG, zapis.delta_Tv_v_diz, zapis.delta_Tv_hol_kont, zapis.Tokr_sr, zapis.delta_Time_str));
                    #endregion
                }
                //...............

                //в самом конце сброс флага наличия ошибки
                t_3_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_3_1.ERR = true;
                t_3_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_3_1);
        }
        public Diag_result<Tabels_Models.Tab_3_1_1> Algoritm_3_1_1()
        {
            Diag_result<Tabels_Models.Tab_3_1_1> t_3_1_1 = new Diag_result<Tabels_Models.Tab_3_1_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string Tv_vih_diz_spr_zapis1 = "", Tv_hol_kont_spr_zapis1 = "", Tokr_sr_spr_zapis1 = "", vremia_zapis1 = "", data_zapis1 = "", pkm_zapis1 = "";

                string Tv_vih_diz_spr1 = "0";

                string pPKM = "", Tv_vih_diz = "", Tv_hol_kont = "", Tokr_sr = "", PTG = "";
                string MB1 = "", MB2 = "", MB3 = "", MB4 = "";

                List<Zapis_Holod_ustr> SPISOK = new List<Zapis_Holod_ustr>();
                List<Zapis_Holod_ustr> SPISOK1 = new List<Zapis_Holod_ustr>();
                long i = 0;
                Zapis_Holod_ustr zap_HUstr = new Zapis_Holod_ustr();

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Tv_vih_diz = "Analog_29"; Tv_hol_kont = "Analog_76"; Tokr_sr = "Analog_72"; PTG = "Analog_101";
                        MB1 = "DiscrIn_81"; MB2 = "DiscrIn_82"; MB3 = "DiscrIn_83"; MB4 = "DiscrIn_84";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Tv_vih_diz = "Analog_29"; Tv_hol_kont = "Analog_76"; Tokr_sr = "Analog_72"; PTG = "Analog_101";
                        MB1 = "DiscrIn_81"; MB2 = "DiscrIn_82"; MB3 = "DiscrIn_83"; MB4 = "DiscrIn_84";
                        break;

                    default:
                        break;
                }

                string year7, month7, day7;
                string dat_ot_new7, dat_do_new7;

                string YYYY, MM, DD, hh, mm, ss;
                string YYYY1, MM1, DD1, hh1, mm1, ss1;

                String minDate_dop, maxDate_dop;
                String deltaDate, pkm_str;
                String Tv_v_diz_str, Tv_hol_kont_str, Tokr_sr_str;

                DateTime minDate2, maxDate2;

                double deltaDate_mm, deltaDate_ss;
                double delta_Tv_v_diz, delta_Tv_hol_kont, delta_Tokr_sr;

                dat_ot_new7 = dat_ot;
                dat_do_new7 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new7 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new7 + " 23:59:59");

                pkm_str = "0";  //условно
                deltaDate = "0"; //условно

                minDate_dop = dat_ot_new7 + " 00:00:00"; //условно
                maxDate_dop = dat_do_new7 + " 23:59:59"; //условно

                int dannie = 0;     //проверка на наличие данных в основном/первом запросе

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "], [" + Tv_vih_diz + "], [" + Tv_hol_kont + "], [" + Tokr_sr + "],[" + MB1 + "], [" + MB2 + "], [" + MB3 + "], [" + MB4 + "]) as(  " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Tv_vih_diz + "], [" + Tv_hol_kont + "], [" + Tokr_sr + "],[" + MB1 + "], [" + MB2 + "], [" + MB3 + "], [" + MB4 + "] " +
                                          " FROM[diag_lcm].[Res].[_" + _tablica + "]" +
                                          " WHERE ([SectionID]= " + _section_id + ") and([" + pPKM + "]= 8 or[" + pPKM + "]= 9 or[" + pPKM + "]= 10 or[" + pPKM + "]= 11 or[" + pPKM + "]= 12 or[" + pPKM + "]= 13 or[" + pPKM + "]= 14 or[" + pPKM + "]= 15) " +
                                          " and([" + MB1 + "]= 1 and [" + MB2 + "] = 1 and [" + MB3 + "] = 1 and [" + MB4 + "] = 1) " +
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new7 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new7 + " 23:59:59', 102))), " +
                                          "   groups AS( " +
                                          "   SELECT " +
                                          "   ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "   dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + MB1 + "], [" + MB2 + "], [" + MB3 + "], [" + MB4 + "], " +
                                          "   [" + pPKM + "], [" + Tv_vih_diz + "], [" + Tv_hol_kont + "], [" + Tokr_sr + "] " +
                                          "      FROM dates d " +
                                          "             ),  " +
                                          " logic as ( " +
                                          "  SELECT  " +
                                          "  COUNT(*) AS consecutiveDates, " +
                                          "  MIN(DateTime) AS minDate, " +
                                          "  MAX(DateTime) AS maxDate, " +
                                          "  ([" + pPKM + "]) as PKM,  " +
                                          "  datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          "  FROM   groups " +
                                          "GROUP BY grp,[" + pPKM + "] " +
                                          "		 )" +
                                          " Select top 1 *" +
                                          " from logic" +
                                          " where int_sec > 30" +
                                          "ORDER BY 4 DESC,1 DESC";
                    Connection.Open();
                    SqlDataReader reader17 = Command.ExecuteReader();

                    if (reader17.HasRows) //проверка на содержание одной или несколько строк, есть ли данные
                    {
                        dannie = 1;
                    }
                    else
                    {
                        dannie = 0;
                    }

                    delta_Tv_v_diz = 0;
                    delta_Tv_hol_kont = 0;
                    delta_Tokr_sr = 0;

                    while (reader17.Read())           //  разбор даты и время самого продолжительно участка для следующего запроса
                    {
                        minDate2 = Convert.ToDateTime(reader17["MinDate"].ToString());
                        minDate_dop = minDate2.ToShortDateString() + " " + minDate2.ToLongTimeString();
                        if (minDate2.ToLongTimeString().Length == 7)
                        {
                            minDate_dop = minDate2.ToShortDateString() + " 0" + minDate2.ToLongTimeString();
                        }

                        maxDate2 = Convert.ToDateTime(reader17["MaxDate"].ToString());
                        maxDate_dop = maxDate2.ToShortDateString() + " " + maxDate2.ToLongTimeString();
                        if (maxDate2.ToLongTimeString().Length == 7)
                        {
                            maxDate_dop = maxDate2.ToShortDateString() + " 0" + maxDate2.ToLongTimeString();
                        }

                        deltaDate = reader17["int_sec"].ToString(); //разница времени в сек
                        if (Convert.ToDouble(deltaDate) > 60)
                        {
                            deltaDate_mm = Math.Truncate(Convert.ToDouble(deltaDate) / 60);  //выделение целого числа из деления-минуты
                            deltaDate_ss = Convert.ToDouble(deltaDate) - (deltaDate_mm * 60);
                            deltaDate = Convert.ToString(deltaDate_mm + " мин. " + deltaDate_ss + " сек.");
                        }
                        else
                        {
                            deltaDate = reader17["int_sec"].ToString() + " сек.";
                        }
                        pkm_str = reader17["PKM"].ToString();

                        YYYY = minDate_dop.ToString().Remove(0, 6).Remove(4);
                        MM = minDate_dop.ToString().Remove(0, 3).Remove(2);
                        DD = minDate_dop.ToString().Remove(2);
                        hh = " " + minDate_dop.ToString().Remove(0, 11).Remove(2);
                        mm = minDate_dop.ToString().Remove(0, 14).Remove(2);
                        ss = minDate_dop.ToString().Remove(0, minDate_dop.Length - 2);

                        minDate_dop = YYYY + "-" + MM + "-" + DD + hh + ":" + mm + ":" + ss;        // переводим в формат yyyymmdd hhmmss для sql

                        YYYY1 = maxDate_dop.ToString().Remove(0, 6).Remove(4);
                        MM1 = maxDate_dop.ToString().Remove(0, 3).Remove(2);
                        DD1 = maxDate_dop.ToString().Remove(2);
                        hh1 = " " + maxDate_dop.ToString().Remove(0, 11).Remove(2);
                        mm1 = maxDate_dop.ToString().Remove(0, 14).Remove(2);
                        ss1 = maxDate_dop.ToString().Remove(0, maxDate_dop.Length - 2);

                        maxDate_dop = YYYY1 + "-" + MM1 + "-" + DD1 + hh1 + ":" + mm1 + ":" + ss1;  // переводим в формат yyyymmdd hhmmss для sql
                    }

                    Connection.Close();

                    if (dannie == 1) //проверка на содержание одной или несколько строк, есть ли данные
                    {
                        Command.CommandText = " with SRC as ( " +
                                               "SELECT top 1[MeasDateTime],[" + Tv_vih_diz + "] as a2,[" + Tv_hol_kont + "] as b2,[" + Tokr_sr + "] as c2,[" + PTG + "] as PTG2 " +
                                               " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                               " WHERE ([SectionID]= " + _section_id + ")  and ([" + pPKM + "]=" + pkm_str + ") and ([" + MB1 + "]= 1 and [" + MB2 + "]= 1 and [" + MB3 + "]= 1 and [" + MB4 + "]= 1) " +
                                               " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + minDate_dop + "' , 102) AND CONVERT(DATETIME, '" + maxDate_dop + "', 102)) " +
                                               " order by MeasDateTime desc     " +
                                               "              ) " +
                                               " SELECT* FROM SRC " +
                                               "   UNION " +
                                               " SELECT top 1 [MeasDateTime],[" + Tv_vih_diz + "] as a1,[" + Tv_hol_kont + "] as b1,[" + Tokr_sr + "] as c1,[" + PTG + "] as PTG1 " +
                                               " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                               " WHERE ([SectionID]= " + _section_id + ")  and ([" + pPKM + "]=" + pkm_str + ") and ([" + MB1 + "]= 1 and [" + MB2 + "]= 1 and [" + MB3 + "]= 1 and [" + MB4 + "]= 1) " +
                                               " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + minDate_dop + "' , 102) AND CONVERT(DATETIME, '" + maxDate_dop + "', 102)) ";
                        Connection.Open();
                        SqlDataReader reader18 = Command.ExecuteReader();

                        int stroka = 0;

                        while (reader18.Read())
                        {
                            stroka++;

                            Tv_v_diz_str = reader18["a2"].ToString();
                            Tv_hol_kont_str = reader18["b2"].ToString();
                            Tokr_sr_str = reader18["c2"].ToString();

                            zap_HUstr.DAT = reader18["MeasDateTime"].ToString();//дата
                            zap_HUstr.PKM = pkm_str;

                            zap_HUstr.delta_Tv_v_diz = Convert.ToString(Math.Round(Math.Abs(delta_Tv_v_diz - Math.Round(Convert.ToDouble(Tv_v_diz_str), 1)), 1));
                            zap_HUstr.delta_Tv_hol_kont = Convert.ToString(Math.Abs(delta_Tv_hol_kont - Convert.ToDouble(Tv_hol_kont_str)));
                            zap_HUstr.delta_Time_str = deltaDate;
                            zap_HUstr.PTG = reader18["PTG2"].ToString();
                            zap_HUstr.Tokr_sr = Tokr_sr_str;

                            delta_Tv_v_diz = Math.Abs(delta_Tv_v_diz - Math.Round(Convert.ToDouble(Tv_v_diz_str), 1));
                            delta_Tv_hol_kont = Math.Abs(delta_Tv_hol_kont - Convert.ToDouble(Tv_hol_kont_str));

                            if (stroka == 2)
                            {
                                SPISOK1.Add(zap_HUstr);//количество записей
                            }
                        }
                    } //if (dannie==1) 

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year7 = date_beg.ToShortDateString().Remove(0, 6);
                    month7 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day7 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new7 = year7 + "." + month7 + "." + day7; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                List<Tabl_HolodUstr> Tabl_13 = new List<Tabl_HolodUstr>();//таблица
                Tabl_HolodUstr huTabl_13 = new Tabl_HolodUstr();//запись таблицы

                DateTime date;

                int j = 0;

                foreach (Zapis_Holod_ustr zapis in SPISOK1)
                {
                    huTabl_13.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    huTabl_13.PKM = zapis.PKM;
                    huTabl_13.Tv_vih_diz = zapis.delta_Tv_v_diz;
                    huTabl_13.Tv_hol_kont = zapis.delta_Tv_hol_kont;
                    huTabl_13.Tokr_sr = zapis.Tokr_sr;
                    huTabl_13.PTG = zapis.PTG;
                    huTabl_13.vremia = zapis.delta_Time_str;

                    if (Convert.ToDouble(zapis.delta_Tv_v_diz) > Convert.ToDouble(Tv_vih_diz_spr1))                //данные для отчета-шаблона  
                    {
                        Tv_vih_diz_spr_zapis1 = zapis.delta_Tv_v_diz;
                        Tv_vih_diz_spr1 = Tv_vih_diz_spr_zapis1;
                        Tv_hol_kont_spr_zapis1 = zapis.delta_Tv_hol_kont;
                        Tokr_sr_spr_zapis1 = Tokr_sr;
                        vremia_zapis1 = zapis.delta_Time_str;
                        data_zapis1 = zapis.DAT;
                        pkm_zapis1 = zapis.PKM;
                    }

                    Tabl_13.Add(huTabl_13);

                    j++;

                    #region Данные для отчета (таблица 3_1)
                    t_3_1_1.Table.Add(new Tabels_Models.Tab_3_1_1(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.PTG, zapis.delta_Tv_v_diz, zapis.delta_Tv_hol_kont, zapis.Tokr_sr, zapis.delta_Time_str));
                    #endregion
                }
                //...............

                //в самом конце сброс флага наличия ошибки
                t_3_1_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_3_1_1.ERR = true;
                t_3_1_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_3_1_1);
        }
        public Diag_result<Tabels_Models.Tab_4_1> Algoritm_4_1()
        {
            Diag_result<Tabels_Models.Tab_4_1> t_4_1 = new Diag_result<Tabels_Models.Tab_4_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string FTOT_spr_zapis = "", TNVD_spr_zapis = "", ChvKV_spr_zapis = "", data_zapis = "", pkm_zapis = "";

                string FTOT_spr = "0";

                string pPKM = "", ChvKV = "", FTOT = "", TNVD = "", KMN = "";


                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; ChvKV = "Analog_130"; FTOT = "Analog_11"; TNVD = "Analog_12"; KMN = "DiscrIn_54";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; ChvKV = "Analog_127"; FTOT = "Analog_11"; TNVD = "Analog_12"; KMN = "DiscrIn_54";
                        break;

                    default:
                        break;
                }

                string year8, month8, day8;
                string dat_ot_new8, dat_do_new8;

                dat_ot_new8 = dat_ot;
                dat_do_new8 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new8 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new8 + " 23:59:59");

                List<Zapis_FTOT_TNVD> SPISOK = new List<Zapis_FTOT_TNVD>();
                long i = 0;
                Zapis_FTOT_TNVD zap_FT = new Zapis_FTOT_TNVD();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    //ФТОТ/ТНВД при мин оборотах 
                    Command.CommandText = "with dates(DateTime, [" + pPKM + "], [" + FTOT + "], [" + TNVD + "], [" + ChvKV + "], [" + KMN + "]) as ( " +
                                          "select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + FTOT + "], [" + TNVD + "], [" + ChvKV + "], [" + KMN + "] " +
                                          "FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                                          "WHERE([SectionID]=" + _section_id + ") and ([" + ChvKV + "] between 345 and 355) and ([" + KMN + "]= 0) " +
                                          "and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new8 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new8 + " 23:59:59', 102))) ,  " +
                                          "groups AS( " +
                                          "SELECT " +
                                          "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,  [" + pPKM + "], [" + FTOT + "], [" + TNVD + "], [" + ChvKV + "], [" + KMN + "] " +
                                          "FROM dates d " +
                                          "         ),  " +
                                          "logic as " +
                                          "     (" +
                                          "SELECT " +
                                          "COUNT(*) AS consecutiveDates, " +
                                          "MIN(DateTime) AS minDate, " +
                                          "MAX(DateTime) AS maxDate, " +
                                          "ROUND(AVG([" + FTOT + "]),1) as FTOT,   " +
                                          "ROUND(AVG([" + TNVD + "]),1) as TNVD, " +
                                          "AVG([" + ChvKV + "]) as ChVKV, " +
                                          "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          " FROM   groups " +
                                          " GROUP BY grp, Analog_100 " +
                                          " 	) " +
                                          " Select top 1 * " +
                                          " from logic " +
                                          " where int_sec > 30 " +
                                          " ORDER BY 6 ASC,7 DESC ";

                    Connection.Open();
                    SqlDataReader reader19 = Command.ExecuteReader();

                    while (reader19.Read())
                    {
                        zap_FT.DAT = reader19["MaxDate"].ToString();//дата
                        zap_FT.ChVKV = reader19["ChVKV"].ToString();
                        zap_FT.FTOT = reader19["FTOT"].ToString();
                        zap_FT.TNVD = reader19["TNVD"].ToString();

                        SPISOK.Add(zap_FT);//количество записей
                    }

                    Connection.Close();

                    //ФТОТ/ТНВД при макс оборотах 
                    Command.CommandText = "with dates(DateTime, [" + pPKM + "], [" + FTOT + "], [" + TNVD + "], [" + ChvKV + "], [" + KMN + "]) as ( " +
                                          "select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + FTOT + "], [" + TNVD + "], [" + ChvKV + "], [" + KMN + "] " +
                                          "FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                                          "WHERE([SectionID]=" + _section_id + ") and ([" + ChvKV + "] between 500 and 1050) and ([" + KMN + "]= 0) " +
                                          "and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new8 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new8 + " 23:59:59', 102))) ,  " +
                                          "groups AS( " +
                                          "SELECT " +
                                          "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,  [" + pPKM + "], [" + FTOT + "], [" + TNVD + "], [" + ChvKV + "], [" + KMN + "] " +
                                          "FROM dates d " +
                                          "         ),    " +
                                          "logic as " +
                                          " (" +
                                          "SELECT " +
                                          "COUNT(*) AS consecutiveDates, " +
                                          "MIN(DateTime) AS minDate, " +
                                          "MAX(DateTime) AS maxDate, " +
                                          "ROUND(AVG([" + FTOT + "]),1) as FTOT,   " +
                                          "ROUND(AVG([" + TNVD + "]),1) as TNVD, " +
                                          "AVG([" + ChvKV + "]) as ChVKV, " +
                                          "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          " FROM   groups " +
                                          "GROUP BY grp, Analog_100 " +
                                          "		 ) " +
                                          " Select top 1 * " +
                                          " from logic " +
                                          " where int_sec > 30 " +
                                          "ORDER BY 6 DESC,7 DESC ";
                    Connection.Open();
                    SqlDataReader reader20 = Command.ExecuteReader();

                    while (reader20.Read())
                    {
                        zap_FT.DAT = reader20["MaxDate"].ToString();//дата
                        zap_FT.ChVKV = reader20["ChVKV"].ToString();
                        zap_FT.FTOT = reader20["FTOT"].ToString();
                        zap_FT.TNVD = reader20["TNVD"].ToString();

                        SPISOK.Add(zap_FT);//количество записей
                    }
                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year8 = date_beg.ToShortDateString().Remove(0, 6);
                    month8 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day8 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new8 = year8 + "." + month8 + "." + day8; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)

                List<Tabl_FtotTnvd> Tabl_14 = new List<Tabl_FtotTnvd>();//таблица
                Tabl_FtotTnvd FTTabl_14 = new Tabl_FtotTnvd();//запись таблицы
                                                              //пустая начальная запись
                Zapis_FTOT_TNVD last_zapis = new Zapis_FTOT_TNVD();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_FTOT_TNVD zapis in SPISOK)
                {
                    FTTabl_14.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    FTTabl_14.FTOT = zapis.FTOT;
                    FTTabl_14.TNVD = zapis.TNVD;
                    FTTabl_14.ChVKV = zapis.ChVKV;

                    if (Convert.ToDouble(zapis.FTOT) > Convert.ToDouble(FTOT_spr))                //данные для отчета-шаблона  
                    {
                        //Убрать столбик ФТОТ
                        FTOT_spr_zapis = zapis.FTOT;
                        FTOT_spr = FTOT_spr_zapis;
                        TNVD_spr_zapis = zapis.TNVD;
                        ChvKV_spr_zapis = zapis.ChVKV;
                        data_zapis = zapis.DAT;
                    }

                    Tabl_14.Add(FTTabl_14);
                    j++;

                    #region Данные для отчета (таблица 4_1)
                    t_4_1.Table.Add(new Tabels_Models.Tab_4_1(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.TNVD, zapis.ChVKV));
                    #endregion
                }
                //...............

                //в самом конце сброс флага наличия ошибки
                t_4_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_4_1.ERR = true;
                t_4_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_4_1);
        }
        public Diag_result<Tabels_Models.Tab_4_2> Algoritm_4_2()
        {
            Diag_result<Tabels_Models.Tab_4_2> t_4_2 = new Diag_result<Tabels_Models.Tab_4_2>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string Rashod_spr_zapis = "", pPKM_spr_zapis = "", vremia_spr_zapis = "", data_zapis = "", pkm_zapis = "", PTG = "";

                string Rashod_spr = "0";

                string pPKM = "", Massa = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Massa = "Analog_157"; PTG = "Analog_101";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Massa = "Analog_139"; PTG = "Analog_101";
                        break;

                    default:
                        break;
                }

                string year9, month9, day9;
                string dat_ot_new9, dat_do_new9;

                string YYYY2, MM2, DD2, hh2, mm2, ss2;  //для minDate
                string YYYY3, MM3, DD3, hh3, mm3, ss3;  //для maxDate

                String minDate, maxDate, minDate_dop, maxDate_dop;
                String deltaDate, pkm_str;
                String Massa_str;

                DateTime minDate3, maxDate3;

                double deltaDate_mm1, deltaDate_ss1;
                double delta_Massa;
                double rashod, delta_vr;

                dat_ot_new9 = dat_ot;
                dat_do_new9 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new9 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new9 + " 23:59:59");

                delta_vr = 1; //условно 1, так как используется в расчете расхода в знаменателе
                pkm_str = "0";  //условно
                deltaDate = "0"; //условно
                minDate = dat_ot_new9 + " 00:00:00"; //условно
                maxDate = dat_do_new9 + " 23:59:59"; //условно

                int dannie = 0;     //проверка на наличие данных в основном/первом запросе

                List<Zapis_Rashod_topl> SPISOK = new List<Zapis_Rashod_topl>();
                long i = 0;
                Zapis_Rashod_topl zap_RashT = new Zapis_Rashod_topl();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = " with dates(DateTime, [" + pPKM + "], [" + Massa + "], [" + PTG + "]) as (  " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime), [" + pPKM + "], [" + Massa + "], [" + PTG + "] " +
                                          " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                          " WHERE ([SectionID]= " + _section_id + ") and ([" + pPKM + "]= 12 or [" + pPKM + "]= 13 or [" + pPKM + "]= 14 or [" + pPKM + "]= 15) and " +
                                          " (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new9 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new9 + " 23:59:59', 102))), " +
                                          " groups AS( " +
                                          " SELECT " +
                                          " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + pPKM + "], [" + Massa + "], [" + PTG + "] " +
                                          " FROM dates d " +
                                           "         )   , " +
                                           " logic AS ( " +
                                           " SELECT  " +
                                            " MIN(DateTime) AS minDate, " +
                                            " MAX(DateTime) AS maxDate, " +
                                            " ([" + pPKM + "]) as PKM, " +
                                            " AVG([" + PTG + "]) as PTG, " +
                                            " AVG([" + Massa + "]) as Massa, " +
                                            " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                            " FROM   groups " +
                                            " GROUP BY grp, [" + pPKM + "] " +
                                            " 		 ) " +
                                            " Select top 1 * " +
                                             " from logic " +
                                             " where int_sec>120  " +
                                             " ORDER BY 6 DESC, 4 DESC  ";
                    Connection.Open();
                    SqlDataReader reader21 = Command.ExecuteReader();

                    if (reader21.HasRows) //проверка на содержание одной или несколько строк, есть ли данные
                    {
                        dannie = 1;
                    }
                    else
                    {
                        dannie = 0;
                    }

                    delta_Massa = 0;
                    rashod = 0;

                    while (reader21.Read())           //  разбор даты и время самого продолжительно участка для следующего запроса
                    {
                        minDate3 = Convert.ToDateTime(reader21["MinDate"].ToString());
                        minDate = minDate3.ToShortDateString() + " " + minDate3.ToLongTimeString();
                        if (minDate3.ToLongTimeString().Length == 7)
                        {
                            minDate = minDate3.ToShortDateString() + " 0" + minDate3.ToLongTimeString();
                        }
                        maxDate3 = Convert.ToDateTime(reader21["MaxDate"].ToString());
                        maxDate = maxDate3.ToShortDateString() + " " + maxDate3.ToLongTimeString();
                        if (maxDate3.ToLongTimeString().Length == 7)
                        {
                            maxDate = maxDate3.ToShortDateString() + " 0" + maxDate3.ToLongTimeString();
                        }

                        deltaDate = reader21["int_sec"].ToString(); //разница времени в сек

                        delta_vr = Convert.ToDouble(deltaDate); //для расчета расхода кг/мин

                        if (Convert.ToDouble(deltaDate) > 60)
                        {
                            deltaDate_mm1 = Math.Truncate(Convert.ToDouble(deltaDate) / 60);  //выделение целого числа из деления-минуты
                            deltaDate_ss1 = Convert.ToDouble(deltaDate) - (deltaDate_mm1 * 60);
                            deltaDate = Convert.ToString(deltaDate_mm1 + " мин. " + deltaDate_ss1 + " сек.");
                        }
                        else
                        {
                            deltaDate = reader21["int_sec"].ToString() + " сек.";
                        }

                        pkm_str = reader21["PKM"].ToString();

                        YYYY2 = minDate.ToString().Remove(0, 6).Remove(4);
                        MM2 = minDate.ToString().Remove(0, 3).Remove(2);
                        DD2 = minDate.ToString().Remove(2);
                        hh2 = " " + minDate.ToString().Remove(0, 11).Remove(2);
                        mm2 = minDate.ToString().Remove(0, 14).Remove(2);
                        ss2 = minDate.ToString().Remove(0, minDate.Length - 2);

                        minDate = YYYY2 + "-" + MM2 + "-" + DD2 + hh2 + ":" + mm2 + ":" + ss2;        // переводим в формат yyyymmdd hhmmss для sql

                        YYYY3 = maxDate.ToString().Remove(0, 6).Remove(4);
                        MM3 = maxDate.ToString().Remove(0, 3).Remove(2);
                        DD3 = maxDate.ToString().Remove(2);
                        hh3 = " " + maxDate.ToString().Remove(0, 11).Remove(2);
                        mm3 = maxDate.ToString().Remove(0, 14).Remove(2);
                        ss3 = maxDate.ToString().Remove(0, maxDate.Length - 2);

                        maxDate = YYYY3 + "-" + MM3 + "-" + DD3 + hh3 + ":" + mm3 + ":" + ss3;  // переводим в формат yyyymmdd hhmmss для sql
                    }
                    Connection.Close();

                    if (dannie == 1) //проверка на содержание одной или несколько строк, есть ли данные
                    {
                        Command.CommandText = " with  SRC as ( " +
                                              " SELECT top 1[MeasDateTime],[" + pPKM + "] as a2,[" + Massa + "] as b2,[" + PTG + "] as PTG2 " +
                                              " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                              " WHERE [SectionID]= " + _section_id + "  and [" + pPKM + "]=" + pkm_str + " " +
                                              " and MeasDateTime BETWEEN CONVERT(DATETIME, '" + minDate + "' , 102) AND CONVERT(DATETIME, '" + maxDate + "', 102) " +
                                              " order by MeasDateTime desc    " +
                                              "             ) " +
                                              " SELECT* FROM SRC " +
                                              " UNION " +
                                              " SELECT top 1 [MeasDateTime],[" + pPKM + "] as a1,[" + Massa + "] as b1,[" + PTG + "] as PTG1 " +
                                              " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                              " WHERE [SectionID]= " + _section_id + "  and [" + pPKM + "]=" + pkm_str + " " +
                                              " and MeasDateTime BETWEEN CONVERT(DATETIME, '" + minDate + "' , 102) AND CONVERT(DATETIME, '" + maxDate + "', 102) ";
                        Connection.Open();
                        SqlDataReader reader22 = Command.ExecuteReader();

                        int stroka = 0;

                        while (reader22.Read())
                        {
                            stroka++;

                            Massa_str = reader22["b2"].ToString();

                            //zap_RashT.DAT = Convert.ToDateTime(reader22["MeasDateTime"].ToString()).ToShortDateString();//дата
                            zap_RashT.DAT = reader22["MeasDateTime"].ToString();//дата
                            zap_RashT.PKM = pkm_str;
                            zap_RashT.PTG = reader22["PTG2"].ToString();//дата

                            zap_RashT.delta_massa = Convert.ToString(Math.Abs(delta_Massa) - Convert.ToDouble(Massa_str));

                            zap_RashT.vremia = deltaDate;

                            delta_Massa = Math.Abs(delta_Massa - Convert.ToDouble(Massa_str));


                            if (stroka == 2)
                            {
                                rashod = Math.Round(delta_Massa * 60 / delta_vr, 1);
                                zap_RashT.rashod = Convert.ToString(rashod);

                                SPISOK.Add(zap_RashT);//количество записей
                            }
                        }
                    }       //if (dannie == 1)

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year9 = date_beg.ToShortDateString().Remove(0, 6);
                    month9 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day9 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new9 = year9 + "." + month9 + "." + day9; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                List<Tabl_RashT> Tabl_15 = new List<Tabl_RashT>();//таблица
                Tabl_RashT RtTabl_15 = new Tabl_RashT();//запись таблицы
                                                        //пустая начальная запись
                Zapis_Rashod_topl last_zapis = new Zapis_Rashod_topl();

                last_zapis.DAT = "00.00.0000 00:00:00";

                int j = 1;

                DateTime date;

                foreach (Zapis_Rashod_topl zapis in SPISOK)
                {
                    RtTabl_15.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    RtTabl_15.PKM = zapis.PKM;
                    RtTabl_15.PTG = zapis.PTG;   //добавить столбец в pdf
                    RtTabl_15.rashod = zapis.rashod;
                    RtTabl_15.Vremia = zapis.vremia;

                    if (Convert.ToDouble(zapis.rashod) > Convert.ToDouble(Rashod_spr))                //данные для отчета-шаблона  
                    {
                        //!!!! добавить столбик мощность дизеля  
                        Rashod_spr_zapis = zapis.rashod;
                        Rashod_spr = Rashod_spr_zapis;
                        vremia_spr_zapis = zapis.vremia;
                        pPKM_spr_zapis = zapis.PKM;
                        data_zapis = zapis.DAT;
                    }

                    Tabl_15.Add(RtTabl_15);

                    j++;

                    #region Данные для отчета (таблица 4_2)
                    t_4_2.Table.Add(new Tabels_Models.Tab_4_2(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.rashod, zapis.PTG, zapis.vremia));
                    #endregion
                }

                //в самом конце сброс флага наличия ошибки
                t_4_2.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_4_2.ERR = true;
                t_4_2.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_4_2);
        }
        public Diag_result<Tabels_Models.Tab_5_1> Algoritm_5_1()
        {
            Diag_result<Tabels_Models.Tab_5_1> t_5_1 = new Diag_result<Tabels_Models.Tab_5_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string I1_spr_zapis = "", I2_spr_zapis = "", I3_spr_zapis = "", I4_spr_zapis = "", I5_spr_zapis = "", I6_spr_zapis = "", PKM_spr_zapis = "", data_zapis = "";
                string PKM_zapis = "", U_spr_zapis = "";

                string I1_spr = "0";
                string U_spr = "0";

                string pPKM = "", Ited1 = "", Ited2 = "", Ited3 = "", Ited4 = "", Ited5 = "", Ited6 = "", UTG = "", ChVKV = "";
                string KP1 = "", KP2 = "", KP3 = "", KP4 = "", KP5 = "", KP6 = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Ited1 = "Analog_31"; Ited2 = "Analog_32"; Ited3 = "Analog_33"; Ited4 = "Analog_34"; Ited5 = "Analog_35"; Ited6 = "Analog_36";
                        KP1 = "DiscrIn_35"; KP2 = "DiscrIn_36"; KP3 = "DiscrIn_37"; KP4 = "DiscrIn_38"; KP5 = "DiscrIn_39"; KP6 = "DiscrIn_40";
                        UTG = "Analog_37"; ChVKV = "Analog_127";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Ited1 = "Analog_31"; Ited2 = "Analog_32"; Ited3 = "Analog_33"; Ited4 = "Analog_34"; Ited5 = "Analog_35"; Ited6 = "Analog_36";
                        KP1 = "DiscrIn_35"; KP2 = "DiscrIn_36"; KP3 = "DiscrIn_37"; KP4 = "DiscrIn_38"; KP5 = "DiscrIn_39"; KP6 = "DiscrIn_40";
                        UTG = "Analog_37"; ChVKV = "Analog_127";
                        break;

                    default:
                        break;
                }

                string year10, month10, day10, year11, month11, day11;
                string dat_ot_new10, dat_do_new10, dat_ot_new11, dat_do_new11;

                dat_ot_new10 = dat_ot;
                dat_do_new10 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new10 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new10 + " 23:59:59");

                List<Zapis_Ited> SPISOK = new List<Zapis_Ited>();
                List<Zapis_Ited> SPISOK1 = new List<Zapis_Ited>();
                long i = 0;
                Zapis_Ited zap_It = new Zapis_Ited();

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + ChVKV + "], " +
                                          " [" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "]) as ( " +
                                          "select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "], " +
                                          " [" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "],[" + ChVKV + "] " +
                                          "FROM [diag_lcm].[Res].[_" + _tablica + "]" +
                                          "WHERE [SectionID]= " + _section_id + " and [" + ChVKV + "]>350 and [" + pPKM + "]>1   " +
                                          "and ([" + Ited1 + "]>1200 or[" + Ited2 + "]>1200 or[" + Ited3 + "]>1200 or[" + Ited4 + "]>1200 or[" + Ited5 + "]>1200 or[" + Ited6 + "]>1200) " +
                                          "and ([" + KP1 + "]=1 or[" + KP2 + "]=1 or[" + KP3 + "]=1 or[" + KP4 + "]=1 or[" + KP5 + "]=1 or[" + KP6 + "]=1) " +
                                          "and(MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new10 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new10 + " 23:59:59', 102))), " +
                                          "groups AS( " +
                                          "SELECT " +
                                          "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "], " +
                                          " [" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + ChVKV + "],[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "] " +
                                          "FROM dates d " +
                                          "          ) " +
                                          "SELECT  " +
                                          " top 1 COUNT(*) AS consecutiveDates, " +
                                          "([" + pPKM + "]) as PKM, " +
                                          "MIN(DateTime) AS minDate, " +
                                          "MAX(DateTime) AS maxDate, " +
                                          "Round(AVG( [" + Ited1 + "]),0) as Ited1,  " +
                                          "Round(AVG( [" + Ited2 + "]),0) as Ited2,  " +
                                          "Round(AVG( [" + Ited3 + "]),0) as Ited3,  " +
                                          "Round(AVG( [" + Ited4 + "]),0) as Ited4,  " +
                                          "Round(AVG( [" + Ited5 + "]),0) as Ited5,  " +
                                          "Round(AVG( [" + Ited6 + "]),0) as Ited6,  " +
                                          "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          "FROM   groups " +
                                          "GROUP BY grp,[" + pPKM + "] " +
                                          "ORDER BY 1 DESC, 2 DESC   ";
                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader23 = Command.ExecuteReader();

                    while (reader23.Read())           //  разбор даты и время самого продолжительно участка для следующего запроса
                    {
                        zap_It.DAT = reader23["MinDate"].ToString();//дата
                        zap_It.PKM = reader23["PKM"].ToString();
                        zap_It.Ited1 = reader23["Ited1"].ToString();
                        zap_It.Ited2 = reader23["Ited2"].ToString();
                        zap_It.Ited3 = reader23["Ited3"].ToString();
                        zap_It.Ited4 = reader23["Ited4"].ToString();
                        zap_It.Ited5 = reader23["Ited5"].ToString();
                        zap_It.Ited6 = reader23["Ited6"].ToString();

                        SPISOK.Add(zap_It);//количество записей
                    }
                    Connection.Close();

                    // I> 1150A 2 min
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + ChVKV + "], " +
                                          "[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "] ) as ( " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "]," +
                                          "[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "],[" + ChVKV + "] " +
                                          " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                          " WHERE [SectionID]= " + _section_id + " and [" + ChVKV + "]>350 and [" + pPKM + "]>1   " +
                                          " and([" + Ited1 + "]>1150 or[" + Ited2 + "]>1150 or[" + Ited3 + "]>1150 or[" + Ited4 + "]>1150 or[" + Ited5 + "]>1150 or[" + Ited6 + "]>1150) " +
                                          "and([" + KP1 + "]=1 or[" + KP2 + "]=1 or[" + KP3 + "]=1 or[" + KP4 + "]=1 or[" + KP5 + "]=1 or[" + KP6 + "]=1) " +
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new10 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new10 + " 23:59:59', 102))), " +
                                          " groups AS( " +
                                           " SELECT " +
                                            " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                            " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp,  DateTime,[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "], " +
                                            " [" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + ChVKV + "],[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "] " +
                                            " FROM dates d " +
                                            "        ) , " +
                                            " logic AS( " +
                                            " SELECT " +
                                            " ([" + pPKM + "]) as PKM, " +
                                            " MIN(DateTime) AS minDate, " +
                                            " MAX(DateTime) AS maxDate, " +
                                            " Round(AVG([" + Ited1 + "]),0) as Ited1, " +
                                            " Round(AVG([" + Ited2 + "]),0) as Ited2, " +
                                            " Round(AVG([" + Ited3 + "]),0) as Ited3, " +
                                            " Round(AVG([" + Ited4 + "]),0) as Ited4, " +
                                            " Round(AVG([" + Ited5 + "]),0) as Ited5, " +
                                            " Round(AVG([" + Ited6 + "]),0) as Ited6, " +
                                            " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                            " FROM   groups " +
                                            " GROUP BY [" + pPKM + "], grp " +
                                            "        ) " +
                                            " Select top 1 * " +
                                            " from logic " +
                                            " where int_sec>120   " +    //120
                                            "ORDER BY int_sec desc,1 DESC, 2 DESC ";
                    Connection.Open();
                    SqlDataReader reader24 = Command.ExecuteReader();

                    while (reader24.Read())           //  разбор даты и время самого продолжительно участка для следующего запроса
                    {
                        zap_It.DAT = reader24["MinDate"].ToString();//дата
                        zap_It.PKM = reader24["PKM"].ToString();
                        zap_It.Ited1 = reader24["Ited1"].ToString() + "  >2 мин.";
                        zap_It.Ited2 = reader24["Ited2"].ToString() + "  >2 мин.";
                        zap_It.Ited3 = reader24["Ited3"].ToString() + "  >2 мин.";
                        zap_It.Ited4 = reader24["Ited4"].ToString() + "  >2 мин.";
                        zap_It.Ited5 = reader24["Ited5"].ToString() + "  >2 мин.";
                        zap_It.Ited6 = reader24["Ited6"].ToString() + "  >2 мин.";

                        SPISOK.Add(zap_It);//количество записей
                    }
                    Connection.Close();

                    // I> 900A 5 min        
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + ChVKV + "], " +
                                          " [" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "]) as ( " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "], " +
                                          " [" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "],[" + ChVKV + "] " +
                                          " FROM [diag_lcm].[Res].[_" + _tablica + "] " +
                                          " WHERE[SectionID]= " + _section_id + " and [" + ChVKV + "]>350 and [" + pPKM + "]>1  " +
                                          " and([" + Ited1 + "]>900 or[" + Ited2 + "]>900 or[" + Ited3 + "]>900 or[" + Ited4 + "]>900 or[" + Ited5 + "]>900 or[" + Ited6 + "]>900) " +
                                          "and([" + KP1 + "]=1 or[" + KP2 + "]=1 or[" + KP3 + "]=1 or[" + KP4 + "]=1 or[" + KP5 + "]=1 or[" + KP6 + "]=1) " +
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new10 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new10 + " 23:59:59', 102))), " +
                                          " groups AS( " +
                                           " SELECT " +
                                            " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                            " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp,  DateTime,[" + pPKM + "], [" + Ited1 + "],[" + Ited2 + "], " +
                                            " [" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + ChVKV + "],[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "] " +
                                            " FROM dates d " +
                                            "        ) , " +
                                            " logic AS( " +
                                            " SELECT " +
                                            " ([" + pPKM + "]) as PKM, " +
                                            " MIN(DateTime) AS minDate, " +
                                            " MAX(DateTime) AS maxDate, " +
                                            " Round(AVG([" + Ited1 + "]),0) as Ited1, " +
                                            " Round(AVG([" + Ited2 + "]),0) as Ited2, " +
                                            " Round(AVG([" + Ited3 + "]),0) as Ited3, " +
                                            " Round(AVG([" + Ited4 + "]),0) as Ited4, " +
                                            " Round(AVG([" + Ited5 + "]),0) as Ited5, " +
                                            " Round(AVG([" + Ited6 + "]),0) as Ited6, " +
                                            " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                            " FROM   groups " +
                                            " GROUP BY [" + pPKM + "], grp " +
                                            "        ) " +
                                            " Select top 1 * " +
                                            " from logic " +
                                            " where int_sec>300   " +
                                            "ORDER BY int_sec desc,1 DESC, 2 DESC ";
                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader25 = Command.ExecuteReader();

                    while (reader25.Read())           //  разбор даты и время самого продолжительно участка для следующего запроса
                    {
                        zap_It.DAT = reader25["MinDate"].ToString();//дата
                        zap_It.PKM = reader25["PKM"].ToString();
                        zap_It.Ited1 = reader25["Ited1"].ToString() + "  >5 мин.";
                        zap_It.Ited2 = reader25["Ited2"].ToString() + "  >5 мин.";
                        zap_It.Ited3 = reader25["Ited3"].ToString() + "  >5 мин.";
                        zap_It.Ited4 = reader25["Ited4"].ToString() + "  >5 мин.";
                        zap_It.Ited5 = reader25["Ited5"].ToString() + "  >5 мин.";
                        zap_It.Ited6 = reader25["Ited6"].ToString() + "  >5 мин.";

                        SPISOK.Add(zap_It);//количество записей
                    }
                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year10 = date_beg.ToShortDateString().Remove(0, 6);
                    month10 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day10 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new10 = year10 + "." + month10 + "." + day10; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)

                List<Tabl_It> Tabl_16 = new List<Tabl_It>();//таблица
                Tabl_It ItTabl_16 = new Tabl_It();//запись таблицы
                                                  //пустая начальная запись
                Zapis_Ited last_zapis = new Zapis_Ited();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Ited zapis in SPISOK)
                {
                    if (zapis.Ited1.Length > 3)
                    {
                        ItTabl_16.I1 = zapis.Ited1.Remove(3);
                    }
                    else
                    {
                        ItTabl_16.I1 = zapis.Ited1;
                    }

                    ItTabl_16.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    ItTabl_16.PKM = zapis.PKM;
                    ItTabl_16.I2 = zapis.Ited2;
                    ItTabl_16.I3 = zapis.Ited3;
                    ItTabl_16.I4 = zapis.Ited4;
                    ItTabl_16.I5 = zapis.Ited5;
                    ItTabl_16.I6 = zapis.Ited6;

                    if (Convert.ToDouble(ItTabl_16.I1) > Convert.ToDouble(I1_spr))                //данные для отчета-шаблона  !!!!!Проверить
                    {
                        I1_spr_zapis = ItTabl_16.I1;
                        I1_spr = I1_spr_zapis;
                        I2_spr_zapis = zapis.Ited2;
                        I3_spr_zapis = zapis.Ited3;
                        I4_spr_zapis = zapis.Ited4;
                        I5_spr_zapis = zapis.Ited5;
                        I6_spr_zapis = zapis.Ited6;
                        PKM_spr_zapis = zapis.PKM;
                        data_zapis = zapis.DAT;
                    }

                    Tabl_16.Add(ItTabl_16);

                    j++;

                    #region Данные для отчета (таблица 5_1)
                    t_5_1.Table.Add(new Tabels_Models.Tab_5_1(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.Ited1, zapis.Ited2, zapis.Ited3, zapis.Ited4, zapis.Ited5, zapis.Ited6));
                    #endregion
                }

                //заполнение результата. Заполнение построчно, каждую строку таблицы-результата               
                t_5_1.ERR = false;
                // t_5_1_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_5_1.ERR = true;
                t_5_1.ERR_Message = ex.Message;

                // t_5_1_1.ERR = true;
                // t_5_1_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_5_1);
            // return (t_5_1_1);
        }
        public Diag_result<Tabels_Models.Tab_5_1_1> Algoritm_5_1_1()
        {
            Diag_result<Tabels_Models.Tab_5_1_1> t_5_1_1 = new Diag_result<Tabels_Models.Tab_5_1_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string PKM_spr_zapis = "", data_zapis = "";
                string PKM_zapis = "", U_spr_zapis = "";

                string U_spr = "0";

                string pPKM = "", UTG = "", ChVKV = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; UTG = "Analog_37"; ChVKV = "Analog_127";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; UTG = "Analog_37"; ChVKV = "Analog_127";
                        break;

                    default:
                        break;
                }

                string year11, month11, day11;
                string dat_ot_new10, dat_do_new10, dat_ot_new11, dat_do_new11;

                dat_ot_new11 = dat_ot;
                dat_do_new11 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new11 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new11 + " 23:59:59");

                // List<Zapis_Ited> SPISOK = new List<Zapis_Ited>();
                List<Zapis_Ited> SPISOK1 = new List<Zapis_Ited>();
                long i = 0;
                Zapis_Ited zap_It = new Zapis_Ited();

                dat_ot_new11 = dat_ot;
                dat_do_new11 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new11 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new11 + " 23:59:59");

                while (date_beg < date_end)
                {
                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = " with dates(DateTime,[" + pPKM + "], [" + UTG + "], [" + ChVKV + "]) as ( " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "], [" + UTG + "], [" + ChVKV + "] " +
                                          " FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                                          "  WHERE([SectionID]= " + _section_id + ") and ([" + ChVKV + "]>340 ) and ([" + UTG + "]>810 ) " +    //700 для отладки
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new11 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new11 + " 23:59:59', 102))), " +
                                          " groups AS( " +
                                          " SELECT " +
                                          " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,[" + pPKM + "], [" + UTG + "], [" + ChVKV + "] " +
                                           " FROM dates d " +
                                           " ) " +
                                           " SELECT  " +
                                           " top 1 COUNT(*) AS consecutiveDates, " +
                                           " ([" + pPKM + "]) as PKM, " +
                                           " MIN(DateTime) AS minDate, " +
                                           " MAX(DateTime) AS maxDate, " +
                                           " Round(AVG([" + UTG + "]), 0) as Utg, " +
                                           " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                           " FROM   groups " +
                                           " GROUP BY Analog_100, grp " +
                                           " ORDER BY 1 DESC, 2 DESC ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader26 = Command.ExecuteReader();

                    while (reader26.Read())
                    {

                        zap_It.PKM = reader26["PKM"].ToString();
                        zap_It.DAT = reader26["MinDate"].ToString();//дата
                        zap_It.U = reader26["Utg"].ToString();

                        SPISOK1.Add(zap_It);//количество записей
                    }

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year11 = date_beg.ToShortDateString().Remove(0, 6);
                    month11 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day11 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new11 = year11 + "." + month11 + "." + day11; // переводим в формат yyyymmdd для sql

                    Connection.Close();

                } //while (date_beg < date_end)

                List<Tabl_It> Tabl_16 = new List<Tabl_It>();//таблица
                Tabl_It ItTabl_16 = new Tabl_It();//запись таблицы
                                                  //пустая начальная запись
                Zapis_Ited last_zapis = new Zapis_Ited();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Ited zapis in SPISOK1)
                {
                    ItTabl_16.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    ItTabl_16.PKM = zapis.PKM;
                    ItTabl_16.U = zapis.U;

                    if (Convert.ToDouble(zapis.U) > Convert.ToDouble(U_spr))                //данные для отчета-шаблона  
                    {
                        U_spr_zapis = zapis.U;
                        U_spr = U_spr_zapis;
                        PKM_spr_zapis = zapis.PKM;
                        data_zapis = zapis.DAT;
                    }

                    Tabl_16.Add(ItTabl_16);
                    j++;

                    #region Данные для отчета (таблица 5_1)
                    t_5_1_1.Table.Add(new Tabels_Models.Tab_5_1_1(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.PKM, zapis.U));
                    #endregion
                }

                //заполнение результата. Заполнение построчно, каждую строку таблицы-результата               
                t_5_1_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_5_1_1.ERR = true;
                t_5_1_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_5_1_1);
        }  //превышение напряжения Алгоритм 5-1 *добавила табл*
        public Diag_result<Tabels_Models.Tab_5_2> Algoritm_5_2()
        {
            Diag_result<Tabels_Models.Tab_5_2> t_5_2 = new Diag_result<Tabels_Models.Tab_5_2>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string KP1_spr_zapis = "", KP2_spr_zapis = "", KP3_spr_zapis = "", KP4_spr_zapis = "", KP5_spr_zapis = "", KP6_spr_zapis = "", PKM_spr_zapis = "", data_zapis = "";
                string KP1_spr = "0";
                string pPKM = "", ChvKV = "", KP1 = "", KP2 = "", KP3 = "", KP4 = "", KP5 = "", KP6 = "", Vtepl = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; ChvKV = "Analog_130"; KP1 = "Analog_54"; KP2 = "Analog_55"; KP3 = "Analog_56"; KP4 = "Analog_57"; KP5 = "Analog_58"; KP6 = "Analog_59"; Vtepl = "Analog_112";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; ChvKV = "Analog_127"; KP1 = "Analog_54"; KP2 = "Analog_55"; KP3 = "Analog_56"; KP4 = "Analog_57"; KP5 = "Analog_58"; KP6 = "Analog_59"; Vtepl = "Analog_112";
                        break;

                    default:
                        break;
                }

                string year12, month12, day12;
                string dat_ot_new12, dat_do_new12;

                dat_ot_new12 = dat_ot;
                dat_do_new12 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new12 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new12 + " 23:59:59");

                double usk1, usk2, usk3, usk4, usk5, usk6;

                List<Zapis_Uscor_KP> SPISOK = new List<Zapis_Uscor_KP>();
                long i = 0;
                Zapis_Uscor_KP zap_UKP = new Zapis_Uscor_KP();

                while (date_beg < date_end)
                {
                    usk1 = 0;
                    usk2 = 0;
                    usk3 = 0;
                    usk4 = 0;
                    usk5 = 0;
                    usk6 = 0;

                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    //ФТОТ/ТНВД при мин оборотах 
                    Command.CommandText = " with dates(DateTime,[" + pPKM + "],[" + ChvKV + "],[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "],[" + Vtepl + "]) as ( " +
                                        " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "],[" + ChvKV + "],[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "],[" + Vtepl + "] " +
                                        " FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                                        " WHERE([SectionID] = " + _section_id + ") and [" + ChvKV + "]>350 and [" + pPKM + "]>0 and [" + Vtepl + "]>10  " +
                                        " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new12 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new12 + " 23:59:59', 102))) , " +
                                        " groups AS( " +
                                        " SELECT " +
                                        " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                        " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,[" + pPKM + "],[" + ChvKV + "],[" + KP1 + "],[" + KP2 + "],[" + KP3 + "],[" + KP4 + "],[" + KP5 + "],[" + KP6 + "],[" + Vtepl + "] " +
                                        " FROM dates d " +
                                        "          ), " +
                                        " logic as( " +
                                        " SELECT " +
                                        " ([" + pPKM + "]) as PKM, " +
                                        " MIN(DateTime) AS minDate, " +
                                        " MAX(DateTime) AS maxDate, " +
                                        " MIN([" + KP1 + "]) as minKP1, " +
                                        " MAX([" + KP1 + "]) as maxKP1, " +
                                        " MIN([" + KP2 + "]) as minKP2, " +
                                        " MAX([" + KP2 + "]) as maxKP2, " +
                                        " MIN([" + KP3 + "]) as minKP3, " +
                                        " MAX([" + KP3 + "]) as maxKP3, " +
                                        " MIN([" + KP4 + "]) as minKP4, " +
                                        " MAX([" + KP4 + "]) as maxKP4, " +
                                        " MIN([" + KP5 + "]) as minKP5, " +
                                        " MAX([" + KP5 + "]) as maxKP5, " +
                                        " MIN([" + KP6 + "]) as minKP6, " +
                                        " MAX([" + KP6 + "]) as maxKP6, " +
                                        " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                        " FROM   groups " +
                                        " GROUP BY [" + pPKM + "], grp   " +
                                        "        ) " +
                                        " Select top 1 " +
                                        " PKM, minDate, maxDate, int_sec, minKP1, maxKP1, maxKP1-minKP1 as deltaKP1, minKP2, maxKP2, maxKP2-minKP2 as deltaKP2, " +
                                        " minKP3, maxKP3,  maxKP3-minKP3 as deltaKP3, minKP4, maxKP4,  maxKP4-minKP4 as deltaKP4, minKP5, maxKP5,  maxKP5-minKP5 as deltaKP5, " +
                                        " minKP6, maxKP6,  maxKP6-minKP6 as deltaKP6 " +
                                        " from logic " +
                                        " where int_sec between 2 and 5 " +
                                        " ORDER BY  deltaKP1 desc, deltaKP2 desc, deltaKP3 desc, deltaKP4 desc, deltaKP5 desc ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader27 = Command.ExecuteReader();

                    while (reader27.Read())
                    {
                        zap_UKP.DAT = reader27["MinDate"].ToString();//дата
                        zap_UKP.PKM = reader27["PKM"].ToString();

                        usk1 = Convert.ToDouble(reader27["deltaKP1"].ToString()) / Convert.ToDouble(reader27["int_sec"].ToString());
                        zap_UKP.uscorKP1 = Convert.ToString(Math.Round(usk1, 1));

                        usk2 = Convert.ToDouble(reader27["deltaKP2"].ToString()) / Convert.ToDouble(reader27["int_sec"].ToString());
                        zap_UKP.uscorKP2 = Convert.ToString(Math.Round(usk2, 1));

                        usk3 = Convert.ToDouble(reader27["deltaKP3"].ToString()) / Convert.ToDouble(reader27["int_sec"].ToString());
                        zap_UKP.uscorKP3 = Convert.ToString(Math.Round(usk3, 1));

                        usk4 = Convert.ToDouble(reader27["deltaKP4"].ToString()) / Convert.ToDouble(reader27["int_sec"].ToString());
                        zap_UKP.uscorKP4 = Convert.ToString(Math.Round(usk4, 1));

                        usk5 = Convert.ToDouble(reader27["deltaKP5"].ToString()) / Convert.ToDouble(reader27["int_sec"].ToString());
                        zap_UKP.uscorKP5 = Convert.ToString(Math.Round(usk5, 1));

                        usk6 = Convert.ToDouble(reader27["deltaKP6"].ToString()) / Convert.ToDouble(reader27["int_sec"].ToString());
                        zap_UKP.uscorKP6 = Convert.ToString(Math.Round(usk6, 1));

                        SPISOK.Add(zap_UKP);//количество записей
                    }

                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year12 = date_beg.ToShortDateString().Remove(0, 6);
                    month12 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day12 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new12 = year12 + "." + month12 + "." + day12; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)

                List<Tabl_UscKP> Tabl_17 = new List<Tabl_UscKP>();//таблица
                Tabl_UscKP uKPTabl_17 = new Tabl_UscKP();//запись таблицы
                                                         //пустая начальная запись
                Zapis_Uscor_KP last_zapis = new Zapis_Uscor_KP();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Uscor_KP zapis in SPISOK)
                {
                    uKPTabl_17.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    uKPTabl_17.PKM = zapis.PKM;
                    uKPTabl_17.Usc1 = zapis.uscorKP1;
                    uKPTabl_17.Usc2 = zapis.uscorKP2;
                    uKPTabl_17.Usc3 = zapis.uscorKP3;
                    uKPTabl_17.Usc4 = zapis.uscorKP4;
                    uKPTabl_17.Usc5 = zapis.uscorKP5;
                    uKPTabl_17.Usc6 = zapis.uscorKP6;

                    if (Convert.ToDouble(zapis.uscorKP1) > Convert.ToDouble(KP1_spr))                //данные для отчета-шаблона  
                    {
                        KP1_spr_zapis = zapis.uscorKP1;
                        KP1_spr = KP1_spr_zapis;
                        KP2_spr_zapis = zapis.uscorKP2;
                        KP3_spr_zapis = zapis.uscorKP3;
                        KP4_spr_zapis = zapis.uscorKP4;
                        KP5_spr_zapis = zapis.uscorKP5;
                        KP6_spr_zapis = zapis.uscorKP6;
                        PKM_spr_zapis = zapis.PKM;
                        data_zapis = zapis.DAT;
                    }

                    Tabl_17.Add(uKPTabl_17);
                    j++;

                    //заполнение результата. Заполнение построчно, каждую строку таблицы-результата
                    #region Данные для отчета (таблица 4_3)
                    t_5_2.Table.Add(new Tabels_Models.Tab_5_2(date.ToString("yyyy-MM-dd HH-mm-ss"), zapis.PKM, zapis.uscorKP1, zapis.uscorKP2, zapis.uscorKP3, zapis.uscorKP4, zapis.uscorKP5, zapis.uscorKP6));
                    #endregion
                }

                //в самом конце сброс флага наличия ошибки
                t_5_2.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_5_2.ERR = true;
                t_5_2.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_5_2);
        }
        public Diag_result<Tabels_Models.Tab_5_3> Algoritm_5_3()
        {
            Diag_result<Tabels_Models.Tab_5_3> t_5_3 = new Diag_result<Tabels_Models.Tab_5_3>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string Ited1_spr_zapis = "", Ited2_spr_zapis = "", Ited3_spr_zapis = "", Ited4_spr_zapis = "", Ited5_spr_zapis = "", Ited6_spr_zapis = "", data_zapis = "";
                string Ited1_spr = "0";
                string pPKM = "", Ited1 = "", Ited2 = "", Ited3 = "", Ited4 = "", Ited5 = "", Ited6 = "", ChVKV = "", KSH1 = "", KSH2 = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Ited1 = "Analog_31"; Ited2 = "Analog_32"; Ited3 = "Analog_33"; Ited4 = "Analog_34"; Ited5 = "Analog_35"; Ited6 = "Analog_36";
                        ChVKV = "Analog_130"; KSH1 = "DiscrIn_33"; KSH2 = "DiscrIn_34";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Ited1 = "Analog_31"; Ited2 = "Analog_32"; Ited3 = "Analog_33"; Ited4 = "Analog_34"; Ited5 = "Analog_35"; Ited6 = "Analog_36";
                        ChVKV = "Analog_127"; KSH1 = "DiscrIn_33"; KSH2 = "DiscrIn_34";
                        break;
                    default:
                        break;
                }

                string year13, month13, day13;
                string dat_ot_new13, dat_do_new13;

                dat_ot_new13 = dat_ot;
                dat_do_new13 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new13 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new13 + " 23:59:59");

                double raspr1, raspr2, raspr3, raspr4, raspr5, raspr6;

                double Iraspr_min, Iraspr_max;

                List<Zapis_Raspred_I> SPISOK = new List<Zapis_Raspred_I>();
                List<Zapis_Raspred_I> SPISOK1 = new List<Zapis_Raspred_I>();
                long i = 0;
                Zapis_Raspred_I zap_RasprI = new Zapis_Raspred_I();

                while (date_beg < date_end)
                {
                    raspr1 = 0;
                    raspr2 = 0;
                    raspr3 = 0;
                    raspr4 = 0;
                    raspr5 = 0;
                    raspr6 = 0;

                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "],[" + ChVKV + "],[" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + KSH1 + "],[" + KSH2 + "]) as (" +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "],[" + ChVKV + "],[" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + KSH1 + "],[" + KSH2 + "] " +
                                          " FROM [diag_lcm].[Res].[_" + _tablica + "]  " +
                                          "WHERE ([SectionID] = " + _section_id + ") and [" + ChVKV + "]>350  and [" + KSH1 + "]=0 and [" + KSH2 + "]=0 " +
                                          " and([" + Ited1 + "]>100 or [" + Ited2 + "]>100 or [" + Ited3 + "]>100 or [" + Ited4 + "]>100 or [" + Ited5 + "]>100 or [" + Ited6 + "]>100 ) " +
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new13 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new13 + " 23:59:59', 102)) ) ,  " +
                                          "groups AS( " +
                                          "SELECT " +
                                          "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,[" + pPKM + "],[" + ChVKV + "],[" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + KSH1 + "],[" + KSH2 + "] " +
                                          "FROM dates d " +
                                          " ) ,       " +
                                          "logic as" +
                                          "       (" +
                                          "SELECT " +
                                          "top 1 COUNT(*) AS consecutiveDates, " +
                                          "MIN(DateTime) AS minDate, " +
                                          "MAX(DateTime) AS maxDate, " +
                                          "Round(AVG([" + Ited1 + "]),1)as I1, " +
                                          "Round(AVG([" + Ited2 + "]),1)as I2, " +
                                          "Round(AVG([" + Ited3 + "]),1)as I3, " +
                                          "Round(AVG([" + Ited4 + "]),1)as I4,  " +
                                          "Round(AVG([" + Ited5 + "]),1)as I5,  " +
                                          "Round(AVG([" + Ited6 + "]),1)as I6, " +
                                          "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          "FROM groups " +
                                          "where[" + Ited1 + "]>100 and[" + Ited2 + "]>100 and[" + Ited3 + "]>100 and[" + Ited4 + "]>100 and[" + Ited5 + "]>100 and[" + Ited6 + "]> 100 " +
                                          "GROUP BY grp " +
                                          "		   ) " +
                                          "Select top 1 *  " +
                                          " from logic  " +
                                          " where int_sec > 30  " +
                                          "ORDER BY int_sec desc ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader28 = Command.ExecuteReader();

                    while ((reader28.Read()) && (reader28["MinDate"].ToString() != ""))
                    {
                        zap_RasprI.DAT = reader28["MinDate"].ToString();//дата.

                        Iraspr_min = Convert.ToDouble(reader28["I1"].ToString());

                        if (Iraspr_min > Convert.ToDouble(reader28["I2"].ToString()))
                        {
                            Iraspr_min = Convert.ToDouble(reader28["I2"].ToString());
                        }

                        if (Iraspr_min > Convert.ToDouble(reader28["I3"].ToString()))
                        {
                            Iraspr_min = Convert.ToDouble(reader28["I3"].ToString());
                        }

                        if (Iraspr_min > Convert.ToDouble(reader28["I4"].ToString()))
                        {
                            Iraspr_min = Convert.ToDouble(reader28["I4"].ToString());
                        }

                        if (Iraspr_min > Convert.ToDouble(reader28["I5"].ToString()))
                        {
                            Iraspr_min = Convert.ToDouble(reader28["I5"].ToString());
                        }

                        if (Iraspr_min > Convert.ToDouble(reader28["I6"].ToString()))
                        {
                            Iraspr_min = Convert.ToDouble(reader28["I6"].ToString());
                        }


                        Iraspr_max = Convert.ToDouble(reader28["I1"].ToString());

                        if (Iraspr_max < Convert.ToDouble(reader28["I2"].ToString()))
                        {
                            Iraspr_max = Convert.ToDouble(reader28["I2"].ToString());
                        }

                        if (Iraspr_max < Convert.ToDouble(reader28["I3"].ToString()))
                        {
                            Iraspr_max = Convert.ToDouble(reader28["I3"].ToString());
                        }

                        if (Iraspr_max < Convert.ToDouble(reader28["I4"].ToString()))
                        {
                            Iraspr_max = Convert.ToDouble(reader28["I4"].ToString());
                        }

                        if (Iraspr_max < Convert.ToDouble(reader28["I5"].ToString()))
                        {
                            Iraspr_max = Convert.ToDouble(reader28["I5"].ToString());
                        }

                        if (Iraspr_max < Convert.ToDouble(reader28["I6"].ToString()))
                        {
                            Iraspr_max = Convert.ToDouble(reader28["I6"].ToString());
                        }

                        raspr1 = Math.Round(Iraspr_max / Iraspr_min, 3);     //!!!!!!Изменила

                        if (raspr1 > 1.12)
                        {
                            zap_RasprI.razbrosI1_8 = Convert.ToString(raspr1);
                            SPISOK.Add(zap_RasprI);//количество записей
                        }
                    }
                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year13 = date_beg.ToShortDateString().Remove(0, 6);
                    month13 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day13 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new13 = year13 + "." + month13 + "." + day13; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)

                List<Tabl_rasprI> Tabl_18 = new List<Tabl_rasprI>();//таблица
                Tabl_rasprI rITabl_18 = new Tabl_rasprI();//запись таблицы
                                                          //пустая начальная запись
                Zapis_Raspred_I last_zapis = new Zapis_Raspred_I();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Raspred_I zapis in SPISOK)
                {
                    rITabl_18.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    rITabl_18.I1_8 = zapis.razbrosI1_8;

                    if (Convert.ToDouble(zapis.razbrosI1_8) > Convert.ToDouble(Ited1_spr))                //данные для отчета-шаблона  
                    {
                        Ited1_spr_zapis = zapis.razbrosI1_8;
                        Ited1_spr = Ited1_spr_zapis;
                        Ited2_spr_zapis = zapis.razbrosI2_8;
                        Ited3_spr_zapis = zapis.razbrosI3_8;
                        Ited4_spr_zapis = zapis.razbrosI4_8;
                        Ited5_spr_zapis = zapis.razbrosI5_8;
                        Ited6_spr_zapis = zapis.razbrosI6_8;
                        data_zapis = zapis.DAT;
                        //после изменения таблицы выводим одно значение вместо 6
                    }
                    Tabl_18.Add(rITabl_18);
                    j++;

                    #region Данные для отчета (таблица 5_3)
                    t_5_3.Table.Add(new Tabels_Models.Tab_5_3(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.razbrosI1_8));
                    #endregion
                }
                //в самом конце сброс флага наличия ошибки
                t_5_3.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_5_3.ERR = true;
                t_5_3.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_5_3);
        }
        public Diag_result<Tabels_Models.Tab_5_3_1> Algoritm_5_3_1()
        {
            Diag_result<Tabels_Models.Tab_5_3_1> t_5_3_1 = new Diag_result<Tabels_Models.Tab_5_3_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;
                string Ited1_12_spr_zapis = "", Ited2_12_spr_zapis = "", Ited3_12_spr_zapis = "", Ited4_12_spr_zapis = "", Ited5_12_spr_zapis = "", Ited6_12_spr_zapis = "", data_zapis_12 = "";
                string Ited1_12_spr = "0";
                string pPKM = "", Ited1 = "", Ited2 = "", Ited3 = "", Ited4 = "", Ited5 = "", Ited6 = "", ChVKV = "", KSH1 = "", KSH2 = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        pPKM = "Analog_100"; Ited1 = "Analog_31"; Ited2 = "Analog_32"; Ited3 = "Analog_33"; Ited4 = "Analog_34"; Ited5 = "Analog_35"; Ited6 = "Analog_36";
                        ChVKV = "Analog_130"; KSH1 = "DiscrIn_33"; KSH2 = "DiscrIn_34";
                        break;
                    case "TE25KM_HZM":
                        pPKM = "Analog_100"; Ited1 = "Analog_31"; Ited2 = "Analog_32"; Ited3 = "Analog_33"; Ited4 = "Analog_34"; Ited5 = "Analog_35"; Ited6 = "Analog_36";
                        ChVKV = "Analog_127"; KSH1 = "DiscrIn_33"; KSH2 = "DiscrIn_34";
                        break;

                    default:
                        break;
                }

                double raspr1, raspr2, raspr3, raspr4, raspr5, raspr6;
                List<Zapis_Raspred_I> SPISOK1 = new List<Zapis_Raspred_I>();
                long i = 0;
                Zapis_Raspred_I zap_RasprI = new Zapis_Raspred_I();

                string year14, month14, day14;
                string dat_ot_new14, dat_do_new14;

                dat_ot_new14 = dat_ot;
                dat_do_new14 = dat_do;

                double Iraspr_min1, Iraspr_max1;

                date_beg = Convert.ToDateTime(dat_ot_new14 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new14 + " 23:59:59");

                List<Tabl_rasprI> Tabl_18 = new List<Tabl_rasprI>();//таблица
                Tabl_rasprI rITabl_18 = new Tabl_rasprI();//запись таблицы
                                                          //пустая начальная запись
                Zapis_Raspred_I last_zapis = new Zapis_Raspred_I();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                while (date_beg < date_end)
                {
                    raspr1 = 0;
                    raspr2 = 0;
                    raspr3 = 0;
                    raspr4 = 0;
                    raspr5 = 0;
                    raspr6 = 0;

                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime,[" + pPKM + "],[" + ChVKV + "],[" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + KSH1 + "],[" + KSH2 + "]) as (" +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + pPKM + "],[" + ChVKV + "],[" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + KSH1 + "],[" + KSH2 + "] " +
                                          " FROM [diag_lcm].[Res].[_" + _tablica + "]  " +
                                          "WHERE ([SectionID] = " + _section_id + ") and [" + ChVKV + "]>350  and [" + KSH1 + "]<>0 " +
                                          " and([" + Ited1 + "]>100 or [" + Ited2 + "]>100 or [" + Ited3 + "]>100 or [" + Ited4 + "]>100 or [" + Ited5 + "]>100 or [" + Ited6 + "]>100 ) " +
                                          " and (MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new14 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new14 + " 23:59:59', 102)) ) ,  " +
                                          "groups AS( " +
                                          "SELECT " +
                                          "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime,[" + pPKM + "],[" + ChVKV + "],[" + Ited1 + "],[" + Ited2 + "],[" + Ited3 + "],[" + Ited4 + "],[" + Ited5 + "],[" + Ited6 + "],[" + KSH1 + "],[" + KSH2 + "] " +
                                          "FROM dates d " +
                                          " ) ,       " +
                                          "logic as" +
                                          "       (" +
                                          "SELECT " +
                                          "top 1 COUNT(*) AS consecutiveDates, " +
                                          "MIN(DateTime) AS minDate, " +
                                          "MAX(DateTime) AS maxDate, " +
                                          "Round(AVG([" + Ited1 + "]),1)as I1, " +
                                          "Round(AVG([" + Ited2 + "]),1)as I2, " +
                                          "Round(AVG([" + Ited3 + "]),1)as I3, " +
                                          "Round(AVG([" + Ited4 + "]),1)as I4,  " +
                                          "Round(AVG([" + Ited5 + "]),1)as I5,  " +
                                          "Round(AVG([" + Ited6 + "]),1)as I6, " +
                                          "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          "FROM groups " +
                                          "where[" + Ited1 + "]>100 and[" + Ited2 + "]>100 and[" + Ited3 + "]>100 and[" + Ited4 + "]>100 and[" + Ited5 + "]>100 and[" + Ited6 + "]> 100 " +
                                          "GROUP BY grp " +
                                          "		   ) " +
                                          "Select top 1 *  " +
                                          " from logic  " +
                                          " where int_sec > 30  " +
                                          "ORDER BY int_sec desc ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader29 = Command.ExecuteReader();

                    while ((reader29.Read()) && (reader29["MinDate"].ToString() != ""))
                    {
                        zap_RasprI.DAT = reader29["MinDate"].ToString();//дата.

                        zap_RasprI.DAT = reader29["MinDate"].ToString();//дата.

                        Iraspr_min1 = Convert.ToDouble(reader29["I1"].ToString());

                        if (Iraspr_min1 > Convert.ToDouble(reader29["I2"].ToString()))
                        {
                            Iraspr_min1 = Convert.ToDouble(reader29["I2"].ToString());
                        }

                        if (Iraspr_min1 > Convert.ToDouble(reader29["I3"].ToString()))
                        {
                            Iraspr_min1 = Convert.ToDouble(reader29["I3"].ToString());
                        }

                        if (Iraspr_min1 > Convert.ToDouble(reader29["I4"].ToString()))
                        {
                            Iraspr_min1 = Convert.ToDouble(reader29["I4"].ToString());
                        }

                        if (Iraspr_min1 > Convert.ToDouble(reader29["I5"].ToString()))
                        {
                            Iraspr_min1 = Convert.ToDouble(reader29["I5"].ToString());
                        }

                        if (Iraspr_min1 > Convert.ToDouble(reader29["I6"].ToString()))
                        {
                            Iraspr_min1 = Convert.ToDouble(reader29["I6"].ToString());
                        }


                        Iraspr_max1 = Convert.ToDouble(reader29["I1"].ToString());

                        if (Iraspr_max1 < Convert.ToDouble(reader29["I2"].ToString()))
                        {
                            Iraspr_max1 = Convert.ToDouble(reader29["I2"].ToString());
                        }

                        if (Iraspr_max1 < Convert.ToDouble(reader29["I3"].ToString()))
                        {
                            Iraspr_max1 = Convert.ToDouble(reader29["I3"].ToString());
                        }

                        if (Iraspr_max1 < Convert.ToDouble(reader29["I4"].ToString()))
                        {
                            Iraspr_max1 = Convert.ToDouble(reader29["I4"].ToString());
                        }

                        if (Iraspr_max1 < Convert.ToDouble(reader29["I5"].ToString()))
                        {
                            Iraspr_max1 = Convert.ToDouble(reader29["I5"].ToString());
                        }

                        if (Iraspr_max1 < Convert.ToDouble(reader29["I6"].ToString()))
                        {
                            Iraspr_max1 = Convert.ToDouble(reader29["I6"].ToString());
                        }

                        raspr2 = Math.Round(Iraspr_max1 / Iraspr_min1, 3);      //!!!!!!Изменила

                        if (raspr2 > 1.2)
                        {
                            zap_RasprI.razbrosI1_12 = Convert.ToString(raspr1);
                            SPISOK1.Add(zap_RasprI);//количество записей
                        }
                    }
                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year14 = date_beg.ToShortDateString().Remove(0, 6);
                    month14 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day14 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new14 = year14 + "." + month14 + "." + day14; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)

                last_zapis.DAT = "00.00.0000 00:00:00";

                j = 1;

                foreach (Zapis_Raspred_I zapis in SPISOK1)
                {
                    rITabl_18.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);
                    rITabl_18.I1_12 = zapis.razbrosI1_12;       //!!!!!!Изменила
                                                                //rITabl_18.I2_12 = zapis.razbrosI2_12;
                                                                //rITabl_18.I3_12 = zapis.razbrosI3_12;
                                                                //rITabl_18.I4_12 = zapis.razbrosI4_12;
                                                                //rITabl_18.I5_12 = zapis.razbrosI5_12;
                                                                //rITabl_18.I6_12 = zapis.razbrosI6_12;

                    if (Convert.ToDouble(zapis.razbrosI1_12) > Convert.ToDouble(Ited1_12_spr))                //данные для отчета-шаблона  
                    {
                        Ited1_12_spr_zapis = zapis.razbrosI1_12;
                        Ited1_12_spr = Ited1_12_spr_zapis;
                        Ited2_12_spr_zapis = zapis.razbrosI2_12;
                        Ited3_12_spr_zapis = zapis.razbrosI3_12;
                        Ited4_12_spr_zapis = zapis.razbrosI4_12;
                        Ited5_12_spr_zapis = zapis.razbrosI5_12;
                        Ited6_12_spr_zapis = zapis.razbrosI6_12;
                        data_zapis_12 = zapis.DAT;
                        //после изменения таблицы выводим одно значение вместо 6
                    }

                    Tabl_18.Add(rITabl_18);
                    j++;

                    #region Данные для отчета (таблица 5_3_1)
                    t_5_3_1.Table.Add(new Tabels_Models.Tab_5_3_1(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.razbrosI1_12));
                    #endregion
                }
                //в самом конце сброс флага наличия ошибки
                t_5_3_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_5_3_1.ERR = true;
                t_5_3_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_5_3_1);
        }   //нарушение токораспределения Алгоритм 5-3 *добавила табл*

        //щелочная никель-кадмиевой аккумуляторная батарея
        public Diag_result<Tabels_Models.Tab_6_1> Algoritm_6_1()
        {
            Diag_result<Tabels_Models.Tab_6_1> t_6_1 = new Diag_result<Tabels_Models.Tab_6_1>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string I_SG = "", U_AB = "", Regim = "", T_okr_sr = "", T_hol_sp = "";

                switch (_tablica)
                {
                    case "TE25KM_MSU":
                        I_SG = "Analog_47"; U_AB = "Analog_42"; Regim = "Analog_99"; T_okr_sr = "Analog_72"; T_hol_sp = "Analog_98";
                        break;
                    case "TE25KM_HZM":
                        I_SG = "Analog_47"; U_AB = "Analog_42"; Regim = "Analog_99"; T_okr_sr = "Analog_72"; T_hol_sp = "Analog_98";
                        break;
                    default:
                        break;
                }

                string year15, month15, day15;
                string dat_ot_new15, dat_do_new15;

                dat_ot_new15 = dat_ot;
                dat_do_new15 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new15 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new15 + " 23:59:59");

                double I_SG_sum, U_Akk_sum, I_kv_sum, pr_IU, T_OkrSr, T_HolSp, kol_izm;
                double R, E, C_nom, C, T_vozd, E_procent;

                List<Zapis_Akkum_batar> SPISOK = new List<Zapis_Akkum_batar>();
                //List<Zapis_Akkum_batar> SPISOK1 = new List<Zapis_Akkum_batar>();
                long i = 0;
                Zapis_Akkum_batar zap_Akk_batar = new Zapis_Akkum_batar();

                while (date_beg < date_end)
                {
                    C = 0;

                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime, [" + I_SG + "], [" + U_AB + "], [" + Regim + "], [" + T_okr_sr + "], [" + T_hol_sp + "]) as ( " +
                                          "select DISTINCT CAST(MeasDateTime as DateTime),[" + I_SG + "], [" + U_AB + "], [" + Regim + "], [" + T_okr_sr + "], [" + T_hol_sp + "] " +
                                          "FROM[diag_lcm].[Res].[_" + _tablica + "] " +
                                          "WHERE([SectionID]=" + _section_id + " and [" + Regim + "]= 2  and [" + I_SG + "] between 100 and 1500 " +
                                          "and MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new15 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new15 + " 23:59:59', 102)) ) " +
                                          "  ,       " +
                                          "groups AS " +
                                          "( " +
                                          "SELECT " +
                                          "ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          "dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + I_SG + "], [" + U_AB + "], [" + Regim + "], [" + T_okr_sr + "], [" + T_hol_sp + "] " +
                                          "FROM dates d " +
                                          " ) , " +
                                          "tabl as( " +
                                          "SELECT " +
                                          "MIN(DateTime) AS minDate, " +
                                          "MAX(DateTime) AS maxDate, " +
                                          "SUM([" + I_SG + "]) as I_SG_sum, " +
                                          "SUM( [" + U_AB + "]) as U_Akk_sum,  " +
                                          "SUM(Convert(Float, [" + I_SG + "]) * Convert(Float, [" + I_SG + "])) as I_kv_sum,  " +
                                          "SUM(Convert(Float, [" + I_SG + "]) * Convert(Float, [" + U_AB + "])) as pr_IU,  " +
                                          "SUM([" + T_okr_sr + "]) as T_OkrSr, " +
                                          "SUM([" + T_hol_sp + "]) as T_HolSp, " +
                                          "COUNT(" + I_SG + ") as kol_zap, " +
                                          "datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          "FROM   groups " +
                                          "GROUP BY grp " +
                                          " ) " +
                                          "select* " +
                                          "from tabl " +
                                          "where int_sec>=3	 " +
                                          "order by  2,3 desc ";

                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader30 = Command.ExecuteReader();

                    while (reader30.Read())
                    //   {***}
                    {
                        zap_Akk_batar.DAT = reader30["minDate"].ToString();//дата
                        zap_Akk_batar.I_SG_sum = reader30["I_SG_sum"].ToString();
                        zap_Akk_batar.U_AB_sum = reader30["U_Akk_sum"].ToString();
                        zap_Akk_batar.I_kv_sum = reader30["I_kv_sum"].ToString();
                        zap_Akk_batar.pr_IU = reader30["pr_IU"].ToString();
                        zap_Akk_batar.T_OkrSr = reader30["T_OkrSr"].ToString();
                        //zap_Akk_batar.T_HolSp = reader30["T_HolSp"].ToString();
                        zap_Akk_batar.kol_izm = reader30["kol_zap"].ToString();

                        I_SG_sum = Convert.ToDouble(zap_Akk_batar.I_SG_sum);
                        U_Akk_sum = Convert.ToDouble(zap_Akk_batar.U_AB_sum);
                        I_kv_sum = Convert.ToDouble(zap_Akk_batar.I_kv_sum);
                        pr_IU = Convert.ToDouble(zap_Akk_batar.pr_IU);
                        T_OkrSr = Convert.ToDouble(zap_Akk_batar.T_OkrSr);
                        //T_HolSp = Convert.ToDouble(zap_Akk_batar.T_HolSp);
                        kol_izm = Convert.ToDouble(zap_Akk_batar.kol_izm);

                        if ((kol_izm * I_kv_sum - I_SG_sum * I_SG_sum) == 0)
                        {
                            break;
                        }
                        //рассчеты для вывода
                        R = Math.Round((U_Akk_sum * I_SG_sum - kol_izm * pr_IU) / (kol_izm * I_kv_sum - I_SG_sum * I_SG_sum), 3);
                        E = Math.Round((U_Akk_sum + R * I_SG_sum) / kol_izm, 1);

                        C_nom = Math.Abs(Math.Round(100 - ((R - 0.024) / 0.024) * 250, 1));
                        //C = Math.Abs(Math.Round(100 - ((R - 0.024) / 0.024) * 250, 1));
                        T_vozd = Math.Round((T_OkrSr / kol_izm), 1);

                        if (T_vozd < 15)
                        {
                            C_nom = Math.Round(C_nom + C_nom * 0.005 * (15 - T_vozd), 1);
                        }

                        if (T_vozd > 35)
                        {
                            C_nom = Math.Round(C_nom + C_nom * 0.01 * (T_vozd - 35), 1);
                        }

                        E_procent = Math.Round(((E - 72) / (97.92 - 72)) * 100);

                        zap_Akk_batar.R = Convert.ToString(R);
                        zap_Akk_batar.E = Convert.ToString(E);
                        zap_Akk_batar.C_nom = Convert.ToString(C_nom);
                        //if (C > 0)
                        //{
                        // zap_Akk_batar.C = Convert.ToString(C); 
                        //}

                        zap_Akk_batar.T_vozd = Convert.ToString(T_vozd);
                        // zap_Akk_batar.E_procent = Convert.ToString(E_procent);

                        SPISOK.Add(zap_Akk_batar);//количество записей

                    }//  {***}

                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year15 = date_beg.ToShortDateString().Remove(0, 6);
                    month15 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day15 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new15 = year15 + "." + month15 + "." + day15; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)   {***}

                List<Tabl_Akk_bat> Tabl_19 = new List<Tabl_Akk_bat>();//таблица
                Tabl_Akk_bat AkBTabl_19 = new Tabl_Akk_bat();//запись таблицы
                                                             //пустая начальная запись
                Zapis_Akkum_batar last_zapis = new Zapis_Akkum_batar();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Akkum_batar zapis in SPISOK)
                {
                    AkBTabl_19.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);

                    AkBTabl_19.R = zapis.R;
                    AkBTabl_19.E = zapis.E;
                    AkBTabl_19.C = zapis.C_nom;
                    AkBTabl_19.Temp = zapis.T_vozd;
                    //AkBTabl_19.E_proc = zapis.E_procent;


                    //if (Convert.ToDouble(zapis.razbrosI1_8) > Convert.ToDouble(Ited1_spr))                //данные для отчета-шаблона  
                    //{
                    //    Ited1_spr_zapis = zapis.razbrosI1_8;
                    //    Ited1_spr = Ited1_spr_zapis;
                    //    Ited2_spr_zapis = zapis.razbrosI2_8;
                    //    Ited3_spr_zapis = zapis.razbrosI3_8;
                    //    Ited4_spr_zapis = zapis.razbrosI4_8;
                    //    Ited5_spr_zapis = zapis.razbrosI5_8;
                    //    Ited6_spr_zapis = zapis.razbrosI6_8;
                    //    data_zapis = zapis.DAT;
                    //после изменения таблицы выводим одно значение вместо 6
                    //}
                    Tabl_19.Add(AkBTabl_19);
                    j++;

                    //}   //if znamenatel !=0
                    #region Данные для отчета (таблица 5_3)
                    //t_6_1.Table.Add(new Tabels_Models.Tab_6_1(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.T_vozd, zapis.R, zapis.E, zapis.C, zapis.E_procent));
                    t_6_1.Table.Add(new Tabels_Models.Tab_6_1(date.ToString("yyyy.MM.dd  HH:mm:ss"), zapis.R, zapis.C_nom, zapis.E, zapis.T_vozd));
                    #endregion
                }
                //в самом конце сброс флага наличия ошибки
                t_6_1.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_6_1.ERR = true;
                t_6_1.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_6_1);
        }
        //кислотная аккумуляторная батарея БС
        public Diag_result<Tabels_Models.Tab_1_8> Algoritm_1_8()
        {
            Diag_result<Tabels_Models.Tab_1_8> t_1_8 = new Diag_result<Tabels_Models.Tab_1_8>();
            try
            {
                //выполнение алгоритма
                //...............
                DateTime date_beg, date_end;

                string I_SG = "", U_AB = "", Regim = "", T_okr_sr = "", KD_on = "", KD_off = "";

                switch (_tablica)
                {
                    case "MSU_BS215":
                        I_SG = "Analog_32"; U_AB = "Analog_35"; Regim = "Analog_174"; T_okr_sr = "Analog_145";
                        KD_on = "DiscrIn_18"; KD_off = "DiscrOut_17";
                        break;
                    default:
                        break;
                }

                string year16, month16, day16;
                string dat_ot_new16, dat_do_new16;

                dat_ot_new16 = dat_ot;
                dat_do_new16 = dat_do;

                date_beg = Convert.ToDateTime(dat_ot_new16 + " 00:00:00");
                date_end = Convert.ToDateTime(dat_do_new16 + " 23:59:59");

                double I_SG_sum, U_Akk_sum, I_kv_sum, pr_IU, T_OkrSr, kol_izm;
                double K, C, C_ab, T_vozd;

                List<Zapis_Akkum_batar_BS> SPISOK = new List<Zapis_Akkum_batar_BS>();
                long i = 0;
                Zapis_Akkum_batar_BS zap_Akk_batar_BS = new Zapis_Akkum_batar_BS();

                while (date_beg < date_end)
                {
                    C = 0;

                    SqlConnection Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["lcmConnection"].ConnectionString);
                    SqlCommand Command = Connection.CreateCommand();
                    Command.CommandText = "with dates(DateTime,[" + I_SG + "], [" + U_AB + "], [" + Regim + "], [" + T_okr_sr + "],  [" + KD_on + "], [" + KD_off + "]) as ( " +
                                          " select DISTINCT CAST(MeasDateTime as DateTime),[" + I_SG + "], [" + U_AB + "], [" + Regim + "], [" + T_okr_sr + "],  [" + KD_on + "], [" + KD_off + "] " +
                                          " FROM[diag_lcm].[Res].[_" + _tablica + "]" +
                                          " WHERE([SectionID]= " + _section_id + " and [" + Regim + "]= 2  and [" + I_SG + "] between 100 and 1500 " +
                                          " and MeasDateTime BETWEEN CONVERT(DATETIME, '" + dat_ot_new16 + " 00:00:00', 102) AND CONVERT(DATETIME, '" + dat_ot_new16 + " 23:59:59', 102)) ) " +
                                          "  ,       " +
                                          " groups AS " +
                                          "  ( " +
                                          " SELECT " +
                                          " ROW_NUMBER() OVER (ORDER BY DateTime) AS rn, " +
                                          " dateadd(second, -ROW_NUMBER() OVER(ORDER BY DateTime), DateTime) AS grp, DateTime, [" + I_SG + "], [" + U_AB + "], [" + Regim + "], [" + T_okr_sr + "],  [" + KD_on + "], [" + KD_off + "] " +
                                          " FROM dates d " +
                                          " ) , " +
                                          " tabl as( " +
                                          " SELECT " +
                                          " MIN(DateTime) AS minDate, " +
                                          " MAX(DateTime) AS maxDate, " +
                                          " SUM([" + I_SG + "]) as I_SG_sum, " +
                                          " SUM([" + U_AB + "]) as U_Akk_sum,  " +
                                          " SUM(Convert(Float, [" + I_SG + "]) * Convert(Float, [" + I_SG + "])) as I_kv_sum,  " +
                                          " SUM(Convert(Float, [" + I_SG + "]) * Convert(Float, [" + U_AB + "])) as pr_IU,  " +
                                          " SUM([" + T_okr_sr + "]) as T_OkrSr, " +
                                          " datediff(SECOND, MIN(DateTime), MAX(DateTime)) as int_sec " +
                                          " FROM   groups " +
                                          " GROUP BY grp " +
                                          " ) " +
                                          " select* " +
                                          " from tabl " +
                                          " where int_sec>=2 " +
                                          " order by  2,3 desc ";
                    Connection.Open();
                    Command.CommandTimeout = 600; //увеличение время выполнения запроса сек
                    SqlDataReader reader31 = Command.ExecuteReader();

                    while (reader31.Read())
                    //   {***}
                    {
                        zap_Akk_batar_BS.DAT = reader31["minDate"].ToString();//дата
                        zap_Akk_batar_BS.I_SG_sum = reader31["I_SG_sum"].ToString();
                        zap_Akk_batar_BS.U_AB_sum = reader31["U_Akk_sum"].ToString();
                        zap_Akk_batar_BS.I_kv_sum = reader31["I_kv_sum"].ToString();
                        zap_Akk_batar_BS.pr_IU = reader31["pr_IU"].ToString();
                        zap_Akk_batar_BS.T_OkrSr = reader31["T_OkrSr"].ToString();
                        zap_Akk_batar_BS.kol_izm = reader31["int_sec"].ToString();

                        I_SG_sum = Convert.ToDouble(zap_Akk_batar_BS.I_SG_sum);
                        U_Akk_sum = Convert.ToDouble(zap_Akk_batar_BS.U_AB_sum);
                        I_kv_sum = Convert.ToDouble(zap_Akk_batar_BS.I_kv_sum);
                        pr_IU = Convert.ToDouble(zap_Akk_batar_BS.pr_IU);
                        T_OkrSr = Convert.ToDouble(zap_Akk_batar_BS.T_OkrSr);
                        kol_izm = Convert.ToDouble(zap_Akk_batar_BS.kol_izm) + 1;

                        //рассчеты для вывода
                        K = Math.Round((U_Akk_sum * I_SG_sum - kol_izm * pr_IU) / (kol_izm * I_kv_sum - I_SG_sum * I_SG_sum), 3);
                        //  E = Math.Round((U_Akk_sum + R * I_SG_sum) / kol_izm, 3);

                        C = Math.Round(50 + 50 * ((0.0312 - K) / (0.0312 - 0.0232)));
                        //C = Math.Abs(Math.Round(100 - ((R - 0.024) / 0.024) * 250, 1));
                        T_vozd = T_OkrSr / kol_izm;

                        C_ab = Math.Round(C / (1 + 0.01 * (T_vozd - 20)));

                        zap_Akk_batar_BS.K = Convert.ToString(K);
                        zap_Akk_batar_BS.C = Convert.ToString(C);
                        zap_Akk_batar_BS.C_ab = Convert.ToString(C_ab);

                        zap_Akk_batar_BS.T_vozd = Convert.ToString(T_vozd);
                        //     zap_Akk_batar.E_procent = Convert.ToString(E_procent);

                        SPISOK.Add(zap_Akk_batar_BS);//количество записей

                    }//  {***}

                    Connection.Close();

                    date_beg = date_beg.AddDays(1); //прибавляем сутки для sql

                    year16 = date_beg.ToShortDateString().Remove(0, 6);
                    month16 = date_beg.ToShortDateString().Remove(0, 3).Remove(2);
                    day16 = date_beg.ToShortDateString().Remove(2);
                    dat_ot_new16 = year16 + "." + month16 + "." + day16; // переводим в формат yyyymmdd для sql

                } //while (date_beg < date_end)   {***}

                List<Tabl_Akk_bat_BS> Tabl_20 = new List<Tabl_Akk_bat_BS>();//таблица
                Tabl_Akk_bat_BS AkBTabl_20 = new Tabl_Akk_bat_BS();//запись таблицы
                                                                   //пустая начальная запись
                Zapis_Akkum_batar_BS last_zapis = new Zapis_Akkum_batar_BS();

                last_zapis.DAT = "00.00.0000 00:00:00";

                DateTime date;

                int j = 1;

                foreach (Zapis_Akkum_batar_BS zapis in SPISOK)
                {
                    AkBTabl_20.datatime = zapis.DAT;
                    DateTime.TryParse(zapis.DAT, out date);

                    AkBTabl_20.K = zapis.K;
                    AkBTabl_20.C = zapis.C;
                    AkBTabl_20.C_ab = zapis.C_ab;
                    AkBTabl_20.Temp = zapis.T_vozd;



                    //if (Convert.ToDouble(zapis.razbrosI1_8) > Convert.ToDouble(Ited1_spr))                //данные для отчета-шаблона  
                    //{
                    //    Ited1_spr_zapis = zapis.razbrosI1_8;
                    //    Ited1_spr = Ited1_spr_zapis;
                    //    Ited2_spr_zapis = zapis.razbrosI2_8;
                    //    Ited3_spr_zapis = zapis.razbrosI3_8;
                    //    Ited4_spr_zapis = zapis.razbrosI4_8;
                    //    Ited5_spr_zapis = zapis.razbrosI5_8;
                    //    Ited6_spr_zapis = zapis.razbrosI6_8;
                    //    data_zapis = zapis.DAT;
                    //после изменения таблицы выводим одно значение вместо 6
                    //}
                    Tabl_20.Add(AkBTabl_20);
                    j++;

                    #region Данные для отчета (таблица 5_3)
                    //t_6_1.Table.Add(new Tabels_Models.Tab_6_1(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.T_vozd, zapis.R, zapis.E, zapis.C_nom, zapis.C, zapis.E_procent));
                    t_1_8.Table.Add(new Tabels_Models.Tab_1_8(date.ToString("yyyy-MM-dd  HH-mm-ss"), zapis.T_vozd, zapis.K, zapis.C, zapis.C_ab));
                    #endregion
                }
                //в самом конце сброс флага наличия ошибки
                t_1_8.ERR = false;
            }
            catch (Exception ex)//если же возникла ошибка
            {
                t_1_8.ERR = true;
                t_1_8.ERR_Message = ex.Message;
            }
            finally //в любом случае 
            {
                //можно например логировать событие    
            }
            return (t_1_8);
        }
        //кислотная аккумуляторная батарея БС


        //добавить алгоритмы 1-7
        public void Algoritm_1_3()
        {

        }

    }
}
