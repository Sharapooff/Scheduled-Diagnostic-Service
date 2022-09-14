using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.ComponentModel.DataAnnotations;


namespace ScheduledDiagnosticService.Models.DataBase
{
    public class Section
    {
        [Required]
        [Key]
        public int Id { get; set; }
        //-----------------------------------

        public string? Notation { get; set; }
        //-----------------------------------

        public virtual List<Incident> Incidents { get; set; } = new(); // навигационное свойство
        public virtual List<TimeTable> TimeTables { get; set; } = new(); // навигационное свойство

        //Атрибут Required указывает, что данное свойство обязательно для установки, то есть будет иметь определение NOT NULL в БД,
        //даже если оно представляет nullable-тип
    }
}
