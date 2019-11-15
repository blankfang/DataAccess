using System;
using System.Collections.Generic;

using System.Text;
using System.Data;
using System.Xml;
using System.ComponentModel;
using System.IO;

namespace DataAccess
{
    /// <summary>
    /// 数据记录对象，描述一条数据行记录
    /// </summary>
    public class Record
    {
        #region 字段、属性
        internal event ExceptionHandler OnException;
        internal event ExceptionExHandler OnExceptionEx;
        internal event ExceptionXPHandler OnExceptionXP;
        internal event MessageHandler OnProgress;

        private XmlDocument _FieldValueChangedLog;

        /// <summary>
        /// 字值变化日志，如果是新记录则返回空（null）
        /// </summary>
        public XmlDocument FieldValueChangedLog
        {
            get
            {
                if (_IsNewRecord)
                    return null;
                _FieldValueChangedLog = new XmlDocument();
                foreach (Field _Field in FieldCollection)
                {
                    if (_Field != null && _Field.OldValue != null && !_Field.Value.Equals(_Field.OldValue))
                    {
                        if (_Field.FieldType == FieldType.DateTime)
                        {
                            if (_Field.OldValue.ToString() == _Field.Value.ToString())
                                continue;
                        }
                        XmlElement xeFieldValueChangedLogs = _FieldValueChangedLog["FieldValueChangedLogs"];
                        if (xeFieldValueChangedLogs == null)
                        {
                            xeFieldValueChangedLogs = _FieldValueChangedLog.CreateElement("FieldValueChangedLogs");
                            _FieldValueChangedLog.AppendChild(xeFieldValueChangedLogs);
                        }
                        XmlElement xeLog = _FieldValueChangedLog.CreateElement("Log");
                        XmlAttribute xaName = _FieldValueChangedLog.CreateAttribute("FieldName");
                        xaName.InnerText = _Field.Name;
                        xeLog.Attributes.Append(xaName);

                        XmlAttribute xaNewValue = _FieldValueChangedLog.CreateAttribute("NewValue");
                        xaNewValue.InnerText = _Field.Value.ToString();
                        xeLog.Attributes.Append(xaNewValue);

                        XmlAttribute xaOldValue = _FieldValueChangedLog.CreateAttribute("OldValue");
                        xaOldValue.InnerText = _Field.OldValue == null ? "Null" : _Field.OldValue.ToString();
                        xeLog.Attributes.Append(xaOldValue);

                        xeFieldValueChangedLogs.AppendChild(xeLog);
                    }
                }

                return _FieldValueChangedLog;
            }
        }
        private const int ERROR = -1;
        /// <summary>
        /// 记录对应的数据表名
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// 记录对应的数据表名Id
        /// </summary>
        public string TableId { get; set; }
        /// <summary>
        /// 记录对应的数据表名emLgdbId
        /// </summary>
        public string emLgdbId { get; set; }

        //public DBType dbType { get; set; }
        /// <summary>
        /// 记录对应的数据库连接对象
        /// </summary>
        internal Connection Connection { get; set; }

        /// <summary>
        /// 处于集合中时的索引值
        /// </summary>
        public int Index { get; set; }
        public string keyVuale { get; set; }
        /// <summary>
        /// 操作类型1:插入有效记录，2：插入无效记录,3：更新有效记录，4：更新无效记录
        /// </summary>
        public int doflag;

        /// <summary>
        /// 记录字段集合
        /// </summary>
        private FieldCollection _fieldcollection = new FieldCollection();
        public FieldCollection FieldCollection
        {
            get
            {
                return _fieldcollection;
            }
            set
            {
                _fieldcollection = value;
            }
        }

        private bool _IsNewRecord = true;
        private bool IsNewRecord
        {
            set
            {
                _IsNewRecord = value;
                foreach (Field f in this.FieldCollection)
                {
                    f.IsNew = _IsNewRecord;
                }
            }
        }
        #endregion

        #region 构造

        /// <summary>
        /// 数据录入系统用构造方法
        /// </summary>
        /// <param name="TableName">表名</param>
        /// <param name="IsNewRecord">是否新记录</param>
        public Record(string TableName, bool IsNewRecord)
        {
            this.TableName = TableName;
            this._IsNewRecord = IsNewRecord;
            this.Index = 0;
        }

        /// <summary>
        /// Record构造方法
        /// </summary>
        /// <param name="TableName">表名</param>
        public Record(string TableName)
        {
            this.TableName = TableName;
            this.Index = 0;
        }
        /// <summary>
        /// 为反序列化准备
        /// </summary>
        private Record()
        { }
        #endregion

        #region 方法

        /// <summary>
        /// 添加一个字段对象
        /// </summary>
        /// <param name="field"></param>
        public void AddField(Field field)
        {
            this._fieldcollection.Add(field);
        }

        /// <summary>
        /// 添加一个字段对象
        /// </summary>
        /// <param name="FieldName">字段名</param>
        /// <param name="FieldValue">字段值</param>
        /// <param name="isKey">是否主键</param>
        public void AddField(string FieldName, string FieldValue, bool isKey)
        {
            Field field = new Field();
            field.IsNew = this._IsNewRecord;
            field.Name = FieldName;
            field.Value = FieldValue;
            field.IsKey = isKey;
            _fieldcollection.Add(field);
        }

        /// <summary>
        /// 添加一个非主键字段对象
        /// </summary>
        /// <param name="FieldName">字段名</param>
        /// <param name="FieldValue">字段值</param>
        public void AddField(string FieldName, string FieldValue)
        {
            Field field = new Field();
            field.IsNew = this._IsNewRecord;
            field.Name = FieldName;
            field.Value = FieldValue;
            field.IsKey = false;
            _fieldcollection.Add(field);
        }

        public void AddField(string FieldName, string FieldValue, FieldType FieldType)
        {
            Field field = new Field();
            field.IsNew = this._IsNewRecord;
            field.Name = FieldName;
            field.Value = FieldValue;
            field.IsKey = false;
            field.FieldType = FieldType;
            _fieldcollection.Add(field);
        }

        public void AddField(string FieldName, string FieldValue, FieldType FieldType, bool isKey)
        {
            Field field = new Field();
            field.IsNew = this._IsNewRecord;
            field.Name = FieldName;
            field.Value = FieldValue;
            field.IsKey = isKey;
            field.FieldType = FieldType;
            _fieldcollection.Add(field);
        }
        public void AddField(string FieldName, object FieldValue, FieldType FieldType, bool isKey)
        {
            Field field = new Field();
            field.IsNew = this._IsNewRecord;
            field.Name = FieldName;
            field.Value = FieldValue;
            field.IsKey = isKey;
            field.FieldType = FieldType;
            _fieldcollection.Add(field);
        }
        public void AddField(string FieldName, object FieldValue, FieldType FieldType, bool isKey, string OldFieldName)
        {
            Field field = new Field();
            field.IsNew = this._IsNewRecord;
            field.Name = FieldName;
            field.Value = FieldValue;
            field.IsKey = isKey;
            field.FieldType = FieldType;
            field.oldName = OldFieldName;
            _fieldcollection.Add(field);
        }
        /// <summary>
        /// 记录对象更新
        /// </summary>
        /// <returns></returns>
        internal int Update()
        {
            try
            {
                if (this.Connection == null)
                {
                    OnException(@"请为数据记录指定数据库连接对象。");
                    return ERROR;
                }
                List<string> pName = new List<string>();
                List<object> pValue = new List<object>();

                string sql = "";
                if (this.Connection.ConnectionType == DBType.SQLServer)
                    sql = new SQLGeneral().General(this, ExecuteMode.Update);
                else
                    sql = new SQLGeneral().General(this, ExecuteMode.Update, out pName, out pValue);

                Command cmd = new Command(this.Connection);
                cmd.OnException += new ExceptionHandler(cmd_OnException);
                cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);

                doflag = 2;
                return cmd.Execute(sql, pName.ToArray(), pValue.ToArray());
            }
            catch (Exception ex)
            {
                OnException(ex.ToString());
                return ERROR;
            }

        }

        void cmd_OnExceptionEx(Exception Exception)
        {
            if (OnExceptionEx != null)
            {
                OnExceptionEx(Exception);
            }
        }

        void cmd_OnException(string ExceptionMessage)
        {
            if (OnException != null)
            {
                OnException(ExceptionMessage);
            }
            if (OnExceptionXP != null)
            {
                OnExceptionXP(this, new EventArgs() { StackMessage = "", ExceptionMessage = ExceptionMessage });
            }
        }

        /// <summary>
        /// 插入新的记录
        /// </summary>
        /// <returns></returns>
        internal int Insert()
        {
            try
            {
                if (this.Connection == null)
                {
                    OnException(@"请为数据记录指定数据库连接对象。");
                    return ERROR;
                }
                List<string> pName = new List<string>();
                List<object> pValue = new List<object>();
                string sql = "";
                if (this.Connection.ConnectionType == DBType.SQLServer)
                    sql = new SQLGeneral().General(this, ExecuteMode.Insert);
                else
                    sql = new SQLGeneral().General(this, ExecuteMode.Insert, out pName, out pValue);
                Command cmd = new Command(this.Connection);
                cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
                cmd.OnException += new ExceptionHandler(cmd_OnException);
                doflag = 1;
                this._IsNewRecord = false;
                object[] values = pValue.ToArray();
                string vals = string.Empty;
                foreach (object obj in values)
                {
                    vals += string.Format(@"'{0}',", obj.ToString());
                }
                //File.WriteAllText(@"log.log", sql + vals);
                return cmd.Execute(sql, pName.ToArray(), pValue.ToArray());
            }
            catch (Exception ex)
            {
                OnException(ex.ToString());
                return ERROR;
            }
        }

        /// <summary>
        /// 删除记录对象
        /// </summary>
        /// <returns></returns>
        internal int Delete()
        {
            try
            {
                if (this.Connection == null)
                {
                    OnException(@"请为数据记录指定数据库连接对象。");
                    return ERROR;
                }
                List<string> pName = new List<string>();
                List<object> pValue = new List<object>();
                string sql = "";
                if (this.Connection.ConnectionType == DBType.SQLServer)
                    sql = new SQLGeneral().General(this, ExecuteMode.Delete);
                else
                    sql = new SQLGeneral().General(this, ExecuteMode.Delete, out pName, out pValue);
                Command cmd = new Command(this.Connection);
                cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
                cmd.OnException += new ExceptionHandler(cmd_OnException);
                doflag = 0;
                return cmd.Execute(sql, pName.ToArray(), pValue.ToArray());
            }
            catch (Exception ex)
            {
                OnException(ex.ToString());
                return ERROR;
            }
        }

        internal int InsertOrUpdate()
        {
            try
            {
                if (this.Connection == null)
                {
                    OnException(@"请为数据记录指定数据库连接对象。");
                    return ERROR;
                }

                string sqlChk = new SQLGeneral().General(this, ExecuteMode.Check);
                //OnException(@"检查主键sql。" + sqlChk);
                Command cmd = new Command(this.Connection);
                cmd.OnException += new ExceptionHandler(cmd_OnException);
                cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
                DataTable dtCount = cmd.GetDataTable(sqlChk);
                if (dtCount == null)
                {
                    return ERROR;
                }
                bool bUpdate = Convert.ToBoolean(dtCount.Rows[0][0]);
                List<string> pName = new List<string>();
                List<object> pValue = new List<object>();
                string sqlExecute = string.Empty;
                if (bUpdate)
                {
                    doflag = 3;
                    if (this.Connection.ConnectionType == DBType.SQLServer)
                        sqlExecute = new SQLGeneral().General(this, ExecuteMode.Update);
                    else
                        sqlExecute = new SQLGeneral().General(this, ExecuteMode.Update, out pName, out pValue);
                }
                else
                {
                    doflag = 1;
                    if ((DBType)this.Connection.ConnectionType == DBType.SQLServer)
                        sqlExecute = new SQLGeneral().General(this, ExecuteMode.Insert);
                    else
                        sqlExecute = new SQLGeneral().General(this, ExecuteMode.Insert, out pName, out pValue);

                    this._IsNewRecord = false;
                }
                return cmd.Execute(sqlExecute, pName.ToArray(), pValue.ToArray());
            }
            catch (Exception ex)
            {
                OnException(ex.ToString());
                return ERROR;
            }
        }
        /// <summary>
        /// 执行数据记录对象的操作
        /// </summary>
        /// <param name="execMode">操作模式选项：Insert,Update,Delete,InsertOrUpdate</param>
        /// <returns></returns>
        internal int Process(ExecuteMode execMode)
        {
            keyVuale = SetKeyValue();
            int rVal = 0;
            switch (execMode)
            {
                case ExecuteMode.Insert:
                    rVal = Insert();
                    break;
                case ExecuteMode.Update:
                    rVal = Update();
                    break;
                case ExecuteMode.InsertOrUpdate:
                    rVal = InsertOrUpdate();
                    break;
                case ExecuteMode.Delete:
                    rVal = Delete();
                    break;
            }
            return rVal;
        }
        /// <summary>
        /// 写log表
        /// </summary>
        /// <returns></returns>
        internal int AddLog(string emlogtable)
        {
            try
            {
                if (this.Connection == null)
                {
                    OnException(@"请为数据记录指定数据库连接对象。");
                    return ERROR;
                }
                string logTableName = this.TableName;
                string[] arr = this.TableName.Split('.');
                if (arr.Length > 1)
                    logTableName = arr[1];

                int eisdel = 0;

                if (this._fieldcollection["EISDEL"] != null && this._fieldcollection["EISDEL"].Value != null)
                    eisdel = int.Parse(this._fieldcollection["EISDEL"].Value.ToString());
                if (eisdel == 1)
                {
                    if (doflag != 0)
                        doflag = doflag + 1;
                }
                string sql = " insert into  {0}(tablename,recordid,emlogicdbid,tableid,edoflag) values('" + logTableName + "','" + this.GetEID() + "','" + this.emLgdbId + "','" + this.TableId + "','" + doflag + "')";

                if (this.Connection.ConnectionType == DBType.SQLServer)
                    sql = string.Format(sql, emlogtable);
                else
                    sql = string.Format(sql, emlogtable);

                Command cmd = new Command(this.Connection);
                cmd.OnException += new ExceptionHandler(cmd_OnException);
                cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
                return cmd.Execute(sql);
            }
            catch (Exception ex)
            {
                OnException(ex.ToString());
                return ERROR;
            }
        }
        public string GetEID()
        {

            if (this._fieldcollection["EID"] != null && this._fieldcollection["EID"].Value != null)
                return this._fieldcollection["EID"].Value.ToString();

            string sql = "select eid from  " + this.TableName + " where " + this.SetKeyValue();

            Command cmd = new Command(this.Connection);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            DataTable dt = cmd.GetDataTable(sql);

            if (dt != null && dt.Rows.Count > 0)
            {
                return dt.Rows[0][0].ToString();
            }
            else
            {
                OnException("没有找到EID");
                return "";
            }


        }
        /// <summary>
        /// 产生新eid
        /// </summary>
        /// <returns></returns>
        public string CreateEID()
        {

            string sql = " Select gm.seq_eid.nextval from dual";
            Command cmd = new Command(this.Connection);
            cmd.OnException += new ExceptionHandler(cmd_OnException);

            object eid = cmd.ExcuteScalar(sql);
            return eid.ToString();
        }


        public string SetKeyValue()
        {
            return new SQLGeneral().General(this);
        }
        public string SetOldKeyValue()
        {
            return new SQLGeneral().GeneralOld(this);
        }
        public string SetOldKeyValue(DBType dbType)
        {
            return new SQLGeneral().GeneralOld(this, dbType);
        }
        #region 序列化&反序列化

        /// <summary>
        /// 生成序列化XML文档对象
        /// </summary>
        /// <returns>XmlDocument</returns>
        public XmlDocument Serialization()
        {
            XmlDocument _Records = new XmlDocument();
            XmlElement xeRecords = _Records.CreateElement("Records");
            XmlElement xeRecord = this.Serialization(_Records);
            xeRecords.AppendChild(xeRecord);
            _Records.AppendChild(xeRecords);
            return _Records;
        }

        /// <summary>
        /// 序列化方法
        /// </summary>
        /// <returns></returns>
        public XmlElement Serialization(XmlDocument _Records)
        {
            XmlElement _Record = _Records.CreateElement("Record");

            XmlAttribute xaName = _Records.CreateAttribute("TableName");
            xaName.InnerText = this.TableName;
            _Record.Attributes.Append(xaName);

            XmlAttribute xaTableId = _Records.CreateAttribute("TableId");
            xaTableId.InnerText = this.TableId.ToString();
            _Record.Attributes.Append(xaTableId);

            XmlAttribute xaEmLgdbId = _Records.CreateAttribute("EmLgdbId");
            xaEmLgdbId.InnerText = this.emLgdbId.ToString();
            _Record.Attributes.Append(xaEmLgdbId);

            //XmlAttribute xaDBType = _Records.CreateAttribute("DBType");
            //xaDBType.InnerText = this.dbType.ToString();
            //_Record.Attributes.Append(xaDBType);

            XmlAttribute xaIndex = _Records.CreateAttribute("Index");
            xaIndex.InnerText = this.Index.ToString();
            _Record.Attributes.Append(xaIndex);

            XmlAttribute xaKeyVuale = _Records.CreateAttribute("KeyVuale");
            xaKeyVuale.InnerText = this.keyVuale.ToString();
            _Record.Attributes.Append(xaKeyVuale);

            XmlAttribute xaDoFlag = _Records.CreateAttribute("DoFlag");
            xaDoFlag.InnerText = this.doflag.ToString();
            _Record.Attributes.Append(xaDoFlag);

            if (this.Connection != null)
            {
                XmlElement xeConnection = this.Connection.Serialization(_Records);
                _Record.AppendChild(xeConnection);
            }

            XmlElement xeFields = _Records.CreateElement("Fields");
            _Record.AppendChild(xeFields);
            foreach (Field _Field in this.FieldCollection)
            {
                XmlElement xeField = _Field.Serialization(_Records);
                xeFields.AppendChild(xeField);
            }
            return _Record;
        }

        /// <summary>
        /// 反序列化方法
        /// </summary>
        /// <param name="_Record">XmlElement对象</param>
        /// <returns>Record</returns>
        public static Record Deserialize(XmlElement _Record)
        {
            try
            {
                Record tempRecord = new Record();

                XmlAttribute xaName = _Record.Attributes["TableName"];
                if (xaName != null)
                    tempRecord.TableName = xaName.InnerText;

                XmlAttribute xaTableId = _Record.Attributes["TableId"];
                if (xaTableId != null)
                    tempRecord.TableId = xaTableId.InnerText;

                XmlAttribute xaEmLgdbId = _Record.Attributes["EmLgdbId"];
                if (xaEmLgdbId != null)
                    tempRecord.emLgdbId = xaEmLgdbId.InnerText;

                //XmlAttribute xaDBType = _Record.Attributes["DBType"];
                //if (xaDBType != null)
                //    tempRecord.dbType = (DBType)Enum.Parse(typeof(DBType), xaDBType.InnerText);

                XmlAttribute xaIndex = _Record.Attributes["Index"];
                if (xaIndex != null)
                    tempRecord.Index = int.Parse(xaIndex.InnerText);

                XmlAttribute xaKeyVuale = _Record.Attributes["KeyVuale"];
                if (xaKeyVuale != null)
                    tempRecord.keyVuale = xaKeyVuale.InnerText;

                XmlAttribute xaDoFlag = _Record.Attributes["DoFlag"];
                if (xaDoFlag != null)
                    tempRecord.doflag = int.Parse(xaDoFlag.InnerText);

                XmlElement xeConnection = _Record["Connection"];
                if (xeConnection != null)
                    tempRecord.Connection = Connection.Deserialize(xeConnection); ;

                XmlElement xeFields = _Record["Fields"];
                foreach (XmlElement xeField in xeFields.ChildNodes)
                {
                    tempRecord.AddField(Field.Deserialize(xeField));
                }

                return tempRecord;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("字段反序列化失败，错误信息：{0}", ex.Message));
            }
        }

        #endregion

        #endregion
    }
}
