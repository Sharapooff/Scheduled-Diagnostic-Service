using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;

namespace ScheduledDiagnosticService.Models.DataBase
{
    [Index("DiagDT")] // add  index
    public class Incident
    {
        [Required]
        [Key]
        public int Id { get; set; }
        //-----------------------------------

        [Required]
        public DateTime? DiagDT { get; set; }
        //-----------------------------------

        [Required]
        public int AlgoritmId { get; set; } // внешний ключ  
        //-----------------------------------

        [Required]
        public int SectionId { get; set; } // внешний ключ    
        //-----------------------------------

        [Required]
        public string? DiagResult { get; set; }
        //-----------------------------------


        [Required]
        public virtual Section? Section { get; set; } // навигационное свойство
        [Required]
        public virtual Algoritm? Algoritm { get; set; } // навигационное свойство
    }
}
