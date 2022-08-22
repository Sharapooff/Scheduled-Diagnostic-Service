

namespace ScheduledDiagnosticService
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            // запустить таймер
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            // 1) по таймеру (раз в сутки) считать из БД список секций лдокомотивов и алгоритмов диагностирования для них;
            // 2) для каждой скекции из списка, считанных из БД, запустить диагностирование через вызов API;
            // 3) результаты необходимо записать в БД;


        }
    }
}