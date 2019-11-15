using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace DataAccessTest
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            DbOpearte Opearte = new DbOpearte(comboBox1.SelectedItem.ToString());           
            MessageBox.Show(string.Format("表总共{0}条数据",Opearte.SelectDt()));
        }

        private void button2_Click(object sender, EventArgs e)
        {
            DbOpearte Opearte = new DbOpearte(comboBox1.SelectedText);
            MessageBox.Show(string.Format("DataSet总共{0}个表", Opearte.SelectDataSet()));
        }

        private void button3_Click(object sender, EventArgs e)
        {
            DbOpearte Opearte = new DbOpearte(comboBox1.SelectedText);
            MessageBox.Show(string.Format("Update影响{0}条数", Opearte.Update()));
        }

        private void button4_Click(object sender, EventArgs e)
        {
            DbOpearte Opearte = new DbOpearte(comboBox1.SelectedText);
            MessageBox.Show(string.Format("Update影响{0}条数", Opearte.UpdateWithPara()));
        }

        private void button5_Click(object sender, EventArgs e)
        {
            DbOpearte Opearte = new DbOpearte(comboBox1.SelectedText);
            Opearte.RecordUpdate();
        }

        
    }
}
