namespace DataAccessTest
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.button1 = new System.Windows.Forms.Button();
            this.button2 = new System.Windows.Forms.Button();
            this.button3 = new System.Windows.Forms.Button();
            this.button4 = new System.Windows.Forms.Button();
            this.button5 = new System.Windows.Forms.Button();
            this.comboBox1 = new System.Windows.Forms.ComboBox();
            this.label1 = new System.Windows.Forms.Label();
            this.button6 = new System.Windows.Forms.Button();
            this.GetTbByProcedure = new System.Windows.Forms.Button();
            this.ExecuteProcedure = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(73, 113);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(136, 23);
            this.button1.TabIndex = 0;
            this.button1.Text = "查询返回DataTable";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(73, 153);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(136, 23);
            this.button2.TabIndex = 1;
            this.button2.Text = "查询返回DataSet";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(73, 188);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(136, 23);
            this.button3.TabIndex = 2;
            this.button3.Text = "更新表";
            this.button3.UseVisualStyleBackColor = true;
            this.button3.Click += new System.EventHandler(this.button3_Click);
            // 
            // button4
            // 
            this.button4.Location = new System.Drawing.Point(73, 227);
            this.button4.Name = "button4";
            this.button4.Size = new System.Drawing.Size(136, 23);
            this.button4.TabIndex = 3;
            this.button4.Text = "更新表带参数";
            this.button4.UseVisualStyleBackColor = true;
            this.button4.Click += new System.EventHandler(this.button4_Click);
            // 
            // button5
            // 
            this.button5.Location = new System.Drawing.Point(73, 275);
            this.button5.Name = "button5";
            this.button5.Size = new System.Drawing.Size(136, 23);
            this.button5.TabIndex = 4;
            this.button5.Text = "RecordUpdate";
            this.button5.UseVisualStyleBackColor = true;
            this.button5.Click += new System.EventHandler(this.button5_Click);
            // 
            // comboBox1
            // 
            this.comboBox1.FormattingEnabled = true;
            this.comboBox1.Items.AddRange(new object[] {
            "Oracle",
            "SqlServer"});
            this.comboBox1.Location = new System.Drawing.Point(81, 72);
            this.comboBox1.Name = "comboBox1";
            this.comboBox1.Size = new System.Drawing.Size(121, 20);
            this.comboBox1.TabIndex = 5;
            this.comboBox1.Text = "选择数据库";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(91, 31);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(89, 12);
            this.label1.TabIndex = 6;
            this.label1.Text = "数据库操作实例";
            // 
            // button6
            // 
            this.button6.Location = new System.Drawing.Point(73, 336);
            this.button6.Name = "button6";
            this.button6.Size = new System.Drawing.Size(136, 23);
            this.button6.TabIndex = 7;
            this.button6.Text = "RecordUpdate写emlog（有id）";
            this.button6.UseVisualStyleBackColor = true;
            this.button6.Click += new System.EventHandler(this.button6_Click);
            // 
            // GetTbByProcedure
            // 
            this.GetTbByProcedure.Location = new System.Drawing.Point(40, 382);
            this.GetTbByProcedure.Name = "GetTbByProcedure";
            this.GetTbByProcedure.Size = new System.Drawing.Size(206, 23);
            this.GetTbByProcedure.TabIndex = 8;
            this.GetTbByProcedure.Text = "执行存储过程返回dataTable";
            this.GetTbByProcedure.UseVisualStyleBackColor = true;
            this.GetTbByProcedure.Click += new System.EventHandler(this.GetTbByProcedure_Click);
            // 
            // ExecuteProcedure
            // 
            this.ExecuteProcedure.Location = new System.Drawing.Point(73, 448);
            this.ExecuteProcedure.Name = "ExecuteProcedure";
            this.ExecuteProcedure.Size = new System.Drawing.Size(136, 23);
            this.ExecuteProcedure.TabIndex = 9;
            this.ExecuteProcedure.Text = "执行存储过程,只返回是否成功";
            this.ExecuteProcedure.UseVisualStyleBackColor = true;
            this.ExecuteProcedure.Click += new System.EventHandler(this.ExecuteProcedure_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(351, 606);
            this.Controls.Add(this.ExecuteProcedure);
            this.Controls.Add(this.GetTbByProcedure);
            this.Controls.Add(this.button6);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.comboBox1);
            this.Controls.Add(this.button5);
            this.Controls.Add(this.button4);
            this.Controls.Add(this.button3);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.button1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Button button3;
        private System.Windows.Forms.Button button4;
        private System.Windows.Forms.Button button5;
        private System.Windows.Forms.ComboBox comboBox1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button button6;
        private System.Windows.Forms.Button GetTbByProcedure;
        private System.Windows.Forms.Button ExecuteProcedure;
    }
}