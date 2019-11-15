using System;
using System.Collections.Generic;

using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Collections;

namespace DataAccess
{
    /// <summary>
    /// 数据库访问入口
    /// </summary>
    public class DBAccess
    {
        #region 异常处理、执行消息
        /// <summary>
        /// 捕获异常，触发异常事件
        /// </summary>
        public event ExceptionHandler OnException;
        /// <summary>
        /// 捕获异常，触发异常事件，返回原始异常信息
        /// </summary>
        public event ExceptionExHandler OnExceptionEx;
        /// <summary>
        /// 捕获异常，触发异常事件。提供更丰富的异常信息
        /// </summary>
        public event ExceptionXPHandler OnExceptionXP;
        /// <summary>
        /// 捕获执行消息，触发执行事件
        /// </summary>
        public event MessageHandler OnProgress;

        public event RecordHandler OnProgressOK;

        #endregion

        #region 字段
        /// <summary>
        /// 数据库连接对象
        /// </summary>
        private Connection _conn = null;

        private int _commandTimeout = 30;
        /// <summary>
        /// 语句执行超时时间设置。默认30秒；
        /// </summary>
        public int CommandTimeOut
        {
            get
            {
                return _commandTimeout;
            }
            set
            {
                _commandTimeout = value;
            }
        }
        public bool IsTran
        {
            set 
            {
                if (value)
                {
                    
                    this.BeginTransaction();
                }
              
            }
        }
        private string _emlogtable = @"gm.emlog";
        /// <summary>
        /// 记录EMLog的表名称
        /// </summary>
        public string EMLogTable
        {
            get
            {
                return _emlogtable;
            }
            set
            {
                _emlogtable = value;
            }
        }

        #endregion

        #region 构造函数
        /// <summary>
        /// 数据库访问构造器
        /// </summary>
        /// <param name="DataSource">数据库所在计算机IP地址或名称；数据库类型为Oracle时该项留空。</param>
        /// <param name="DataBase">数据库名称</param>
        /// <param name="UserID">用户名</param>
        /// <param name="Password">密码</param>
        /// <param name="ConnectionType">数据库类型</param>
        public DBAccess(string DataSource, string DataBase, string UserID, string Password, DBType ConnectionType)
        {
            this._conn = new Connection(DataSource, DataBase, UserID, Password, ConnectionType);
        }
        /// <summary>
        /// 数据库访问构造器
        /// </summary>
        /// <param name="ConnectionString">数据库连接字符串。</param>
        /// <param name="ConnectionType">数据库类型。Oracle 或 SQLServer</param>
        public DBAccess(string ConnectionString, DBType ConnectionType)
        {
            this._conn = new Connection(ConnectionString, ConnectionType);            
        }
        #region 事务
        public void BeginTransaction()
        {
            if (this._conn.mCon.State == ConnectionState.Closed)
                this._conn.mCon.Open();
            this._conn.BeginTransaction();
        }
        public void Commit()
        {
            this._conn.Commit();
        }
        public void Rollback()
        {
            this._conn.Rollback();
        }
        #endregion
        /// <summary>
        /// 数据库访问构造器
        /// </summary>
        /// <param name="DataSource">数据库所在计算机IP地址或名称；数据库类型为Oracle时该项留空。</param>
        /// <param name="DataBase">数据库名称</param>
        /// <param name="UserID">用户名</param>
        /// <param name="Password">密码</param>
        /// <param name="ConnectTimeout">连接超时；单位：秒</param>
        /// <param name="ConnectionType">数据库类型</param>
        public DBAccess(string DataSource, string DataBase, string UserID, string Password, int ConnectTimeout, DBType ConnectionType)
        {
            this._conn = new Connection(DataSource, DataBase, UserID, Password, ConnectionType);
            this._conn.ConnectTimeout = ConnectTimeout;
        }
        #endregion

        #region 公开方法
        /// <summary>
        /// 执行指定的SQL查询语句
        /// </summary>
        /// <param name="sql">SQL 语句</param>
        /// <returns>受影响行数</returns>
        public int Execute(string sql)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.Execute(sql);
        }


        /// <summary>
        /// 执行带参数的SQL查询语句
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="ParamNames">参数名数组</param>
        /// <param name="ParamValues">参数值数组</param>
        /// <returns>受影响行数</returns>
        public int Execute(string sql, string[] ParamNames, object[] ParamValues)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.Execute(sql, ParamNames, ParamValues);
        }
        /// <summary>
        /// 执行存储过程，带返回参数的
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public Hashtable RunProcedure(string sql, IDataParameter[] parameters)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.RunProcedure(sql, parameters);
        }

        /// <summary>
        /// 执行存储过程，返回DataTable
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataTable GetTbByProcedure(string sql, IDataParameter[] parameters)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.GetTbByProcedure(sql, parameters);
        }
        /// <summary>
        /// 执行存储过程，返回执行是否成功
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public bool ExecuteProcedure(string sql, IDataParameter[] parameters)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.ExecuteProcedure(sql, parameters);
        }
        /// <summary>
        /// 事务方式
        /// </summary>
        /// <param name="sqls">多个sql语句</param>
        /// <returns></returns>
        public bool ExecuteTrans(string[] sqls)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.ExecuteTrans(sqls);
        }

        /// <summary>
        /// 事务方式
        /// </summary>
        /// <param name="sqlContents"></param>
        /// <returns></returns>
        public bool ExecuteTrans(List<SqlContent> sqlContents)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx+=new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.ExecuteTrans(sqlContents);
        }

        /// <summary>
        /// 执行SQL语句，返回第一行第一列数据
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>第一行第一列数据</returns>
        public object ExecuteScalar(string sql)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.ExcuteScalar(sql);
        }
        /// <summary>
        /// 提供对Record对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="record">数据记录对象</param>
        /// <param name="execMode">插入、更新、删除、插入或更新</param>
        /// <returns></returns>
        public int ProcessRecord(Record record, ExecuteMode execMode)
        {
            record.OnException += new ExceptionHandler(record_OnException);
            record.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            record.OnExceptionXP += new ExceptionXPHandler(record_OnExceptionXP);

            if (OnProgress != null)
            {
                OnProgress(string.Format(@"{2} 处理一条数据记录。记录关联表：{0}；操作：{1}", record.TableName, execMode.ToString(), DateTime.Now));
            }
            //record.dbType = this._conn.ConnectionType;
            record.Connection = this._conn;
            return record.Process(execMode);
        }
        /// <summary>
        /// 提供对Record对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="record">数据记录对象</param>
        /// <param name="execMode">插入、更新、删除、插入或更新</param>
        /// <param name="isAddLog">是否添加日志</param>
        /// <returns></returns>
        public int ProcessRecord(Record record, ExecuteMode execMode, bool isAddLog)
        {
            record.OnException += new ExceptionHandler(record_OnException);
            record.OnExceptionXP += new ExceptionXPHandler(record_OnExceptionXP);
            record.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);

            if (OnProgress != null)
            {
                OnProgress(string.Format(@"{2} 处理一条数据记录。记录关联表：{0}；操作：{1}", record.TableName, execMode.ToString(), DateTime.Now));
            }
            record.Connection = this._conn;
            DateTime dt1 = DateTime.Now;
            int i = record.Process(execMode);
            if (i != -1)
            {
                
//#if debug
                 DateTime dt2 = DateTime.Now;
                 TimeSpan ts = dt2 - dt1;
                
                if (OnProgress != null)
                {
                    OnProgress(string.Format(@"{0} 处理一条数据记录。记录关联表：{1}；操作花费时间{2}", DateTime.Now, record.TableName, ts.TotalMilliseconds));
                }


                dt1 = DateTime.Now;
//#endif
                if (isAddLog)
                    i = record.AddLog(this._emlogtable);
#if debug
                dt2 = DateTime.Now;

                ts = dt2 - dt1;
                if (OnProgress != null)
                {
                    OnProgress(string.Format(@"{0} 处理一条数据记录。记录关联表：{1}；写入log花费时间{2}", DateTime.Now, record.TableName, ts.TotalMilliseconds));
                }
#endif
                return i;
            }

            return -1;
        }
        /// <summary>
        /// 提供对Record对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="record">数据记录对象</param>
        /// <param name="execMode">插入、更新、删除、插入或更新</param>
        /// <param name="isAddLog">是否添加日志</param>
        /// <param name="isAddLog">是否生成新id</param>
        /// <returns></returns>
        public int ProcessRecord(Record record, ExecuteMode execMode, bool isAddLog, bool isNewID)
        {
            record.OnException += new ExceptionHandler(record_OnException);
            record.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            record.OnExceptionXP += new ExceptionXPHandler(record_OnExceptionXP);
            if (isNewID)
            {
                if (record.FieldCollection["EID"] == null || record.FieldCollection["EID"].Value == null)
                {
                    string eid = record.CreateEID();
                    if (record.FieldCollection["EID"] == null)
                        record.FieldCollection["EID"].Value = eid;
                    else
                        record.FieldCollection.Add(new Field("EID", eid));
                }
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"{2} 处理一条数据记录。记录关联表：{0}；操作：{1}", record.TableName, execMode.ToString(), DateTime.Now));
            }
            record.Connection = this._conn;
            DateTime dt1 = DateTime.Now;
            int i = record.Process(execMode);
            if (record.Process(execMode) != -1)
            {
                DateTime dt2 = DateTime.Now;
#if debug
                 DateTime dt2 = DateTime.Now;
                 TimeSpan ts = dt2 - dt1;
                
                if (OnProgress != null)
                {
                    OnProgress(string.Format(@"{0} 处理一条数据记录。记录关联表：{1}；操作花费时间{2}", DateTime.Now, record.TableName, ts.TotalMilliseconds));
                }
#endif

                dt1 = DateTime.Now;
                if (isAddLog)
                    i = record.AddLog(this._emlogtable);
                dt2 = DateTime.Now;
#if debug
                ts = dt2 - dt1;
                if (OnProgress != null)
                {
                    OnProgress(string.Format(@"{0} 处理一条数据记录。记录关联表：{1}；写入log花费时间{2}", DateTime.Now, record.TableName, ts.TotalMilliseconds));
                }
#endif
                return i;
            }

            return -1;
        }
        void record_OnExceptionXP(object sender, EventArgs EventArgs)
        {
            if (OnExceptionXP != null)
            {
                OnExceptionXP(sender, EventArgs);
            }
        }

        void record_OnException(string ExceptionMessage)
        {
            if (OnException != null)
            {
                OnException(ExceptionMessage);
            }
        }

        /// <summary>
        /// 提供对RecordCollection对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="recCollection"></param>
        /// <param name="execMode"></param>
        /// <returns>返回成功行数</returns>
        public int ProcessRecord(RecordCollection recCollection, ExecuteMode execMode)
        {
            int count = 0;
            try
            {

                foreach (Record record in recCollection)
                {
                    if (ProcessRecord(record, execMode) != -1)
                    {
                        //addLog(record);
                        count++;
                    }
                    else
                    {
                        if (this._conn.mTransaction != null)
                        {
                            this._conn.mTransaction.Rollback();
                            return 0;
                        }
                            
                    }
                }
               if (this._conn.mTransaction != null)
               {
                   this._conn.mTransaction.Commit();
                   return  count;
               }
            }
            finally
            {
                this.CloseConn();
 
            }
            return count;
        }
        /// <summary>
        /// 提供对RecordCollection对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="recCollection"></param>
        /// <param name="execMode"></param>
        /// <returns>返回成功行数</returns>
        public int ProcessRecord(RecordCollection recCollection, ExecuteMode execMode, string type)
        {
            int count = 0;
            foreach (Record record in recCollection)
            {
                if (ProcessRecord(record, execMode) != -1)
                {
                    //addLog(record);
                    if (type == "err")
                        //cmd_OnProgressOK((string)record.FieldCollection["EID"].Value);
                        cmd_OnProgressOK(record.SetOldKeyValue());
                    count++;
                }
                else
                {
                     if(this._conn.mTransaction!=null)
                         throw new Exception("更新有错");
                }
            }
            return count;
        }
        /// <summary>
        /// 提供对RecordCollection对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="recCollection"></param>
        /// <param name="execMode"></param>
        /// <param name="type">正常同步，处理错误同步</param>
        /// <param name="dbType">源记录的数据库类型</param>
        /// <returns></returns>
        public int ProcessRecord(RecordCollection recCollection, ExecuteMode execMode, string type,DBType dbType)
        {
            int count = 0;
            foreach (Record record in recCollection)
            {
                if (ProcessRecord(record, execMode) != -1)
                {
                    
                    if (type == "err")
                        cmd_OnProgressOK(record.SetOldKeyValue(dbType));
                    count++;
                }
                else
                {
                    if (this._conn.mTransaction != null)
                        throw new Exception("更新有错");
                }
            }
            return count;
        }
        /// <summary>
        /// 提供对RecordCollection对象的操做。插入、更新、删除、插入或更新
        /// </summary>
        /// <param name="recCollection">RecordCollection对象集</param>
        /// <param name="execMode">插入、更新、删除、插入或更新</param>
        /// <param name="type">err：处理错误记录</param>
        /// <param name="isAddLog">是否写入emlog</param>
        /// <returns>返回成功行数</returns>
        public int ProcessRecord(RecordCollection recCollection, ExecuteMode execMode, string type, bool isAddLog,bool isSub)
        {
            int count = 0;
            try
            {
                foreach (Record record in recCollection)
                {
                    if (ProcessRecord(record, execMode, isAddLog) != -1)
                    {
                        //addLog(record);
                        if (type == "err" && !isSub)
                            cmd_OnProgressOK(record.SetOldKeyValue());

                        count++;
                    }
                    else
                    {
                        if (this._conn.mTransaction != null)
                        {
                            this._conn.mTransaction.Rollback();
                            count = -1;
                            return count;
                        }
                    }
                }
                if (this._conn.mTransaction != null)
                {
                    this._conn.mTransaction.Commit();
                    return count;
                }
            }
            finally
            {
                this.CloseConn();

            }
            return count;
            
        }

        /// <summary>
        /// 执行SQL语句，返回DataTable对象；；遇到错误返回null。
        /// </summary>
        /// <param name="sql">查询语句</param>
        /// <returns></returns>
        public DataTable GetDataTable(string sql)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.GetDataTable(sql);
        }
        public void CloseConn()
        {
            if (this._conn.mCon != null)
                this._conn.mCon.Close();

        }
        void cmd_OnProgress(string Message)
        {
            if (OnProgress != null)
            {
                OnProgress(Message);
            }
        }
        void cmd_OnProgressOK(string record)
        {
            if (OnProgressOK != null)
            {
                OnProgressOK(record);
            }
        }
        void cmd_OnException(string ExceptionMessage)
        {
            if (OnException != null)
            {
                OnException(ExceptionMessage);
            }
        }
        void cmd_OnExceptionEx(Exception Exception)
        {
            if (OnExceptionEx != null)
            {
                OnExceptionEx(Exception);
            }
        }
        public DataSet GetDataSet(string sql, string[] ParamNames, object[] ParamValues)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            return cmd.GetDataTableP(sql, ParamNames, ParamValues);
        }


        /// <summary>
        /// 执行SQL语句，返回DataSet对象；遇到错误返回null。
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataSet GetDataSet(string sql)
        {
            Command cmd = new Command(this._conn);
            cmd.OnException += new ExceptionHandler(cmd_OnException);
            cmd.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            cmd.OnProgress += new MessageHandler(cmd_OnProgress);
            cmd.CommandTimeout = this._commandTimeout;
            return cmd.GetDataSet(sql);
        }
        /// <summary>
        /// 记录Record对象的表名，表id，eid到emlog表
        /// </summary>
        /// <param name="record">数据记录对象</param>        
        /// <returns></returns>
        public int addLog(Record record)
        {
            record.OnException += new ExceptionHandler(record_OnException);
            record.OnExceptionEx += new ExceptionExHandler(cmd_OnExceptionEx);
            record.OnExceptionXP += new ExceptionXPHandler(record_OnExceptionXP);

            if (OnProgress != null)
            {
                OnProgress(string.Format(@"{0} 。记录表：{0}的log", DateTime.Now, record.TableName));
            }
            record.Connection = this._conn;
            return record.AddLog(this._emlogtable);
        }
        #endregion

        #region 数据库连接测试
        public bool Connect()
        {
            return this._conn.Connect();
        }
        #endregion
    }
}
