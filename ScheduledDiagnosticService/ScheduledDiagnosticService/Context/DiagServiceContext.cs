using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.EntityFrameworkCore;
using ScheduledDiagnosticService.Models.DataBase;

namespace ScheduledDiagnosticService.Context
{
    internal class DiagServiceContext : DbContext
    {
        public string connectionString;
        //readonly StreamWriter logStream = new StreamWriter("mylog.txt", true);

        //Datasets_______________________________________________________
        //public DbSet<Sections> Sections { get; set; };
        //public DbSet<Sections> Sections { get; set; } = null!;
        public DbSet<Section> Sections => Set<Section>();
        public DbSet<Incident> Incidents => Set<Incident>();
        public DbSet<Algoritm> Algoritms => Set<Algoritm>();
        public DbSet<TypeAlgorithm> TypeAlgorithms => Set<TypeAlgorithm>();
        public DbSet<TimeTable> TimeTables => Set<TimeTable>();


        //EnsureCreated_______________________________________________________
        public DiagServiceContext(string connectionString)
        {
            this.connectionString = connectionString;   //получаем извне строку подключения
            Database.EnsureCreated();
        }
        //SetConfiguring_______________________________________________________
        //OnConfiguring_______________________________________________________
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            //optionsBuilder.UseSqlServer(@"...");
            optionsBuilder
                .UseLazyLoadingProxies()    // подключение lazy loading
                .UseSqlServer(connectionString);
            
            //log to console
            //.LogTo(Console.WriteLine);
            //log to Output
            //.LogTo(message => System.Diagnostics.Debug.WriteLine(message));
            //log to file
            //.LogTo(logStream.WriteLine);
        }
        //или в конструкторе_______________________________________________________
        //public DiagServiceContext(DbContextOptions<DiagServiceContext> options) : base(options)
        //{
        //    Database.EnsureCreatedAsync();
        //}
        //public override void Dispose()
        //{
        //    base.Dispose();
        //    logStream.Dispose();
        //}

        //public override async ValueTask DisposeAsync()
        //{
        //    await base.DisposeAsync();
        //    await logStream.DisposeAsync();
        //}


    }
}
