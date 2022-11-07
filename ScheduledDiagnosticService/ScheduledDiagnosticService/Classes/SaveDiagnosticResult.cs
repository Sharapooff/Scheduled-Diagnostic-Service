using Microsoft.EntityFrameworkCore;
using Models;
using Nancy.Json;
using ScheduledDiagnosticService.Context;
using ScheduledDiagnosticService.Models.DataBase;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ScheduledDiagnosticService.Classes
{
    public class SaveDiagnosticResult
    {
        async public Task<Result> SaveResultDiagnosticAsync(Report_Diagnostic_Models ResultDiagnostic, string ConnectionString, int sectionId)//, DBCotext dbCotext)
        {
            if (ResultDiagnostic.ERR == true) return (new Result { ERR = ResultDiagnostic.ERR, ERR_Message = ResultDiagnostic.ERR_message });

            Result _result = new Result { ERR = false, ERR_Message = "" };
            using (DiagServiceContext db = new DiagServiceContext(ConnectionString))
            {
                var algoritms = await db.Algoritms.ToListAsync();
                DateTime _DiagDT = DateTime.Now;
                foreach (Algoritm a in algoritms)
                {
                    JavaScriptSerializer serializer = new();//Создаем объект сериализации
                    Incident incident = new(); //объект инцидента
                    incident.DiagDT = _DiagDT;
                    foreach (var s in from p in db.Sections where p.RefID == sectionId select p.Id)
                        incident.SectionId = s;
                    switch (a.Notation)
                    {
                        case "*1-1*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_1);
                            break;
                        case "*1-2*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_2);
                            break;
                        case "*1-3*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_3);
                            break;
                        case "*1-4*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_4);
                            break;
                        case "*1-5*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_5);
                            break;
                        case "*1-6*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_6);
                            break;
                        case "*1-7*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_7);
                            break;
                        case "*1-8*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_1_8);
                            break;
                        case "*2-0*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.AlarmMessege);
                            break;
                        case "*2-1*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_2_1);
                            break;
                        case "*2-2*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_2_2);
                            break;
                        case "*2-3*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_2_3);
                            break;
                        case "*3-1*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_3_1);
                            break;
                        case "*4-1*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_4_1);
                            break;
                        case "*4-2*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_4_2);
                            break;
                        case "*5-1*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_5_1);
                            break;
                        case "*5-2*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_5_2);
                            break;
                        case "*5-3*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_5_3);
                            break;
                        case "*6-1*":
                            incident.AlgoritmId = a.Id;
                            incident.DiagResult = serializer.Serialize(ResultDiagnostic.Table_6_1);
                            break;
                        default:
                            break;
                    }

                    await db.Incidents.AddAsync(incident);
                    await db.SaveChangesAsync();
                    //Notify?.Invoke("\r\n" + "Сохранение в БД выполнено успешно.", Color.Green);

                }
            }
            return (_result);
        }
    }
}
