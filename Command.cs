using System;
using System.Collections.Generic;

using System.Text;
using System.Data.SqlClient;
using System.Data.OracleClient;
using System.Data;
using System.Collections;
using System.Data.Common;

namespace DataAccess
{
    /// <summary>
    /// Command对象，实现对SqlCommand和OracleCommand的简单封装
    /// </summary>
    class Command
    {
        #region 字段、属性
        internal event ExceptionHandler OnException;
        internal event ExceptionExHandler OnExceptionEx;
        internal event MessageHandler OnProgress;

        private const int CLOB_MIN_LENGTH = 2000;
        private const int CLOB_MAX_LENGTH = 4000;
        /// <summary>
        /// 数据库类型；SQLServer，Oracle
        /// </summary>
        private DBType _dbType = DBType.Oracle;

        internal int CommandTimeout { get; set; }

        /// <summary>
        /// 要执行的SQL
        /// </summary>
        internal string CommandText { get; set; }
        /// <summary>
        /// 数据库连接对象
        /// </summary>
        private Connection _connection = null;

        private const int ERROR = -1;
        #endregion

        #region 构造
        public Command(Connection connection)
        {
            this._dbType = connection.ConnectionType;
            this._connection = connection;
        }
        #endregion

        #region 执行SQL语句
        /// <summary>
        /// 执行指定的SQL语句，并返回受影响函数
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        internal int Execute(string sql)
        {
            int effectRows = 0;
            try
            {
                OpenConn();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        effectRows = OracleCommandExecute(sql);
                        break;
                    case DBType.SQLServer:
                        effectRows = SqlCommandExecute(sql);
                        break;
                }
            }
           finally
            {
                CloseConn();    
            }
            return effectRows;
        }

        /// <summary>
        /// 返回首行首列值
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        internal object ExcuteScalar(string sql)
        {
            object returnval = null;
            try
            {
                OpenConn();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        returnval = OracleCommandExecuteScalar(sql);
                        break;
                    case DBType.SQLServer:
                        returnval = SqlCommandExecuteScalar(sql);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return returnval;
        }

        private object SqlCommandExecuteScalar(string sql)
        {
            object returnval = null;            
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            try
            {
                returnval = cmd.ExecuteScalar();
            }
            catch (SqlException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return ERROR;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            finally
            {
                cmd.Dispose();
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return returnval;
        }
        private object OracleCommandExecuteScalar(string sql)
        {
            object returnval = null;

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;

            try
            {
                returnval = cmd.ExecuteScalar();
            }
            catch (OracleException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return ERROR;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            finally
            {
                cmd.Dispose();               
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return returnval;
        }

        /// <summary>
        /// 针对SQLServer数据库执行SQL查询语句
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private int SqlCommandExecute(string sql)
        {
            int effectRows = 0;

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            try
            {
                effectRows = cmd.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return ERROR;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            finally
            {
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sql, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }

        /// <summary>
        /// 针对Oracle数据库执行SQL查询语句
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private int OracleCommandExecute(string sql)
        {
            int effectRows = 0;

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            try
            {
                effectRows = cmd.ExecuteNonQuery();
            }
            catch (OracleException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return ERROR;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            finally
            {
                cmd.Dispose();                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sql, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }
        #endregion

        #region 执行带参数的SQL语句
        internal int Execute(string sql, string[] ParamNames, object[] ParamValues)
        {

            int effectRows = 0;
            try
            {
                OpenConn();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        effectRows = OracleCommandExecute(sql, ParamNames, ParamValues);
                        break;
                    case DBType.SQLServer:
                        effectRows = SqlCommandExecute(sql, ParamNames, ParamValues);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return effectRows;
        }
        private int SqlCommandExecute(string sql, string[] ParamNames, object[] ParamValues)
        {
            if (ParamNames.Length != ParamValues.Length)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"传入的参数名称数组长度和参数对应值数据长度不一致。数据库连接：{0}；待执行SQL：{1}", this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            int effectRows = 0;

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;

            for (int i = 0; i < ParamNames.Length; i++)
            {
                cmd.Parameters.Add(new SqlParameter(ParamNames[i], ParamValues[i]));
            }
            try
            {
                effectRows = cmd.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"数据库连接错误。连接字符串：{0}；待执行SQL：{1}", this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return ERROR;
            }
            catch
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"数据库连接错误。连接字符串：{0}；待执行SQL：{1}", this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            finally
            {
                cmd.Dispose();
               
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sql, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }
        private int OracleCommandExecute(string sql, string[] ParamNames, object[] ParamValues)
        {
            string sql1 = sql;
            if (ParamNames.Length != ParamValues.Length)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"传入的参数名称数组长度和参数对应值数据长度不一致。数据库连接：{0}；待执行SQL：{1}", this._connection.ConnectionString, sql));
                }
                return ERROR;
            }
            int effectRows = 0;

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            
            for (int i = 0; i < ParamNames.Length; i++)
            {
                OracleParameter op = new OracleParameter(ParamNames[i], ParamValues[i]);
                //cmd.Parameters.Add(new OracleParameter(ParamNames[i], ParamValues[i]));

                if (ParamValues[i] is byte[])
                {
                    op.OracleType = OracleType.Blob;
                }
                if (ParamValues[i] is System.String)
                {
                    int length = ParamValues[i].ToString().Length;
                    if (length >= CLOB_MIN_LENGTH)
                        op.OracleType = OracleType.Clob;

                    sql1 = sql1.Replace(":" + ParamNames[i].ToString(), "'" + ParamValues[i].ToString() + "'");
                }
                if (ParamValues[i] is System.DateTime)
                    sql1 = sql1.Replace(":" + ParamNames[i].ToString(), "to_date('" + Convert.ToDateTime(ParamValues[i]).ToString("yyyy-MM-dd HH:mm:ss") + "','yyyy-MM-dd hh24:mi:ss')");
                //else if(ParamValues[i] is System.DateTime)

                cmd.Parameters.Add(op);
            }
            try
            {
                effectRows = cmd.ExecuteNonQuery();
            }
            catch (OracleException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql1));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return ERROR;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sql1));
                }
                return ERROR;
            }
            finally
            {
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sql, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }
        #endregion

        #region 查询并返回DataTable对象
        /// <summary>
        /// 通过指定的SQL查询语句，返回相应的DataTable对象结果集；如果失败，返回null;
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        internal DataTable GetDataTable(string sql)
        {
            DataTable dt = null;
            try
            {
                OpenConn();
                switch (this._connection.ConnectionType)
                {
                    case DBType.SQLServer:
                        dt = SqlGetDataTable(sql);
                        break;
                    case DBType.Oracle:
                        dt = OracleGetDataTable(sql);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return dt;
        }
        #region 工具函数
        private DbDataAdapter NewDataAdapter(DbCommand cmd)
        {
            if (_dbType == DBType.SQLServer)
            {
                return new SqlDataAdapter((SqlCommand)cmd);
            }
            else if (_dbType == DBType.Oracle)
            {
                return new OracleDataAdapter((OracleCommand)cmd);
            }
            else
            {
                string err = string.Format("GetAdapter():不支持的数据库类型({0})", _dbType);
                throw new Exception(err);
            }
        }
        #endregion
        /// <summary>
        /// 针对SQLServer数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private DataTable SqlGetDataTable(string sql)
        {
            DataTable dt = new DataTable();
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;

            DbDataAdapter adapter = NewDataAdapter(cmd);
            try
            {
                adapter.Fill(dt);
            }
            catch (InvalidOperationException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            catch (SqlException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(err);
                }
                return null;
            }
            catch (Exception err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();
                
                if (this._connection.mTransaction == null&&this._connection.mCon!=null)
                    this._connection.mCon.Close();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"成功获取DataTable。执行SQL语句：{0}，数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return dt;
        }

        /// <summary>
        /// 针对Oracle数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private DataTable OracleGetDataTable(string sql)
        {
            DataTable dt = new DataTable();
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            DbDataAdapter adapter = NewDataAdapter(cmd);

            try
            {
                adapter.Fill(dt);
            }
            catch (InvalidOperationException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            catch (OracleException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(err);
                }
                return null;
            }
            catch (Exception err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();
               
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"成功获取DataTable。执行SQL语句：{0}，数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return dt;
        }
        #endregion

        #region 参数方式返回DataTable对象
        /// <summary>
        /// 通过指定的SQL查询语句，返回相应的DataTable对象结果集；如果失败，返回null;
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        internal DataSet GetDataTableP(string sql, string[] ParamNames, object[] ParamValues)
        {
            DataSet ds = null;
            try
            {
                OpenConn();
                switch (this._connection.ConnectionType)
                {
                    case DBType.SQLServer:
                        ds = SqlGetDataTableP(sql, ParamNames, ParamValues);
                        break;
                    case DBType.Oracle:
                        ds = OracleGetDataTableP(sql, ParamNames, ParamValues);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return ds;
        }

        /// <summary>
        /// 针对SQLServer数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private DataSet SqlGetDataTableP(string sql, string[] ParamNames, object[] ParamValues)
        {
            DataSet ds = new DataSet();
           
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;

            for (int i = 0; i < ParamNames.Length; i++)
            {
                SqlParameter op = new SqlParameter(ParamNames[i], ParamValues[i]);
                cmd.Parameters.Add(op);
            }

            DbDataAdapter adapter = NewDataAdapter(cmd);
            try
            {
                adapter.Fill(ds);
            }
            catch (InvalidOperationException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            catch (SqlException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(err);
                }
                return null;
            }
            catch (Exception err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();
                 
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"成功获取DataTable。执行SQL语句：{0}，数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return ds;
        }

        /// <summary>
        /// 针对Oracle数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private DataSet OracleGetDataTableP(string sql, string[] ParamNames, object[] ParamValues)
        {
            string sql1 = sql;
            if (ParamNames.Length != ParamValues.Length)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"传入的参数名称数组长度和参数对应值数据长度不一致。数据库连接：{0}；待执行SQL：{1}", this._connection.ConnectionString, sql));
                }
                return null;
            }

            DataSet ds = new DataSet();
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
           

            for (int i = 0; i < ParamNames.Length; i++)
            {
                OracleParameter op = new OracleParameter(ParamNames[i], ParamValues[i]);                
                if (ParamValues[i] is System.String)
                {
                    int length = ParamValues[i].ToString().Length;
                    if (length > CLOB_MIN_LENGTH && length < CLOB_MAX_LENGTH)
                        op.OracleType = OracleType.Clob;

                    sql1 = sql1.Replace(":" + ParamNames[i].ToString(), "'" + ParamValues[i].ToString() + "'");
                }
                if (ParamValues[i] is System.DateTime)
                    sql1 = sql1.Replace(":" + ParamNames[i].ToString(), "to_date(" + ParamValues[i].ToString() + ",'yyyy-MM-dd hh24:mi:ss')");

                cmd.Parameters.Add(op);
            }
            DbDataAdapter adapter = NewDataAdapter(cmd);
            
            try
            {
                adapter.Fill(ds);
            }
            catch (InvalidOperationException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            catch (OracleException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(err);
                }
                return null;
            }
            catch (Exception err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"成功获取DataTable。执行SQL语句：{0}，数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return ds;
        }
        #endregion

        #region 查询并返回DataSet对象
        /// <summary>
        /// 通过指定的SQL查询语句，返回相应的DataSet对象结果集
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        internal DataSet GetDataSet(string sql)
        {
            DataSet ds = null;
            try
            {
                OpenConn();
                switch (this._connection.ConnectionType)
                {
                    case DBType.SQLServer:
                        ds = SqlGetDataSet(sql);
                        break;
                    case DBType.Oracle:
                        ds = OracleGetDataSet(sql);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return ds;
        }

        /// <summary>
        /// 针对SQLServer数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private DataSet SqlGetDataSet(string sql)
        {
            DataSet ds = new DataSet();
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            DbDataAdapter adapter = NewDataAdapter(cmd);
            try
            {
                adapter.Fill(ds);
            }
            catch (InvalidOperationException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            catch (SqlException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(err);
                }
                return null;
            }
            catch (Exception err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();
                 
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"成功获取DataSet。执行SQL语句：{0}，数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return ds;
        }

        /// <summary>
        /// 针对Oracle数据库
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private DataSet OracleGetDataSet(string sql)
        {
            DataSet ds = new DataSet();
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            DbDataAdapter adapter = NewDataAdapter(cmd);
            try
            {
                adapter.Fill(ds);
            }
            catch (InvalidOperationException err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            catch (OracleException ex)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", ex.Message, this._connection.ConnectionString, sql));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(ex);
                }
                return null;
            }
            catch (Exception err)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", err.Message, this._connection.ConnectionString, sql));
                }
                return null;
            }
            finally
            {
                adapter.Dispose();
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"成功获取DataSet。执行SQL语句：{0}，数据库连接字符串：{1}", sql, this._connection.ConnectionString));
            }
            return ds;
        }
        #endregion

        #region 执行存储过程 有返回参数的
        public Hashtable RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            Hashtable result = new Hashtable();
            try
            {
                OpenConn();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        result = RunOracleProcedure(storedProcName, parameters);
                        break;
                    case DBType.SQLServer:
                        result = RunSqlProcedure(storedProcName, parameters);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return result;
        }

        internal Hashtable RunSqlProcedure(String procedureName, IDataParameter[] parameters)
        {
            Hashtable result = new Hashtable();


            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            

            if (parameters != null)
            {
                cmd.Parameters.AddRange(parameters);
            }

            try
            {
                cmd.ExecuteNonQuery();

                foreach (SqlParameter param in cmd.Parameters)
                {　　　　　　　　　　　// 这里把输出参数放到一个 HashTable 里面，方便取出   
                    if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput || param.Direction == ParameterDirection.ReturnValue)
                    {
                        result.Add(param.ParameterName, param.Value);
                    }
                }


            }
            catch (SqlException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return null;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                return null;
            }
            finally
            {
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", procedureName, this._connection.ConnectionString));
            }
            return result;
        }
        internal Hashtable RunOracleProcedure(String procedureName, IDataParameter[] parameters)
        {
            Hashtable result = new Hashtable();

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            if (parameters != null)
            {
                cmd.Parameters.AddRange(parameters);
            }

            try
            {
                cmd.ExecuteNonQuery();

                foreach (OracleParameter param in cmd.Parameters)
                {　　　　　　　　　　　// 这里把输出参数放到一个 HashTable 里面，方便取出   
                    if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput || param.Direction == ParameterDirection.ReturnValue)
                    {
                        result.Add(param.ParameterName, param.Value);
                    }
                }


            }
            catch (OracleException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return null;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                return null;
            }
            finally
            {
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", procedureName, this._connection.ConnectionString));
            }
            return result;
        }
        #endregion

        #region 执行存储过程 返回DataTable
        public DataTable GetTbByProcedure(string storedProcName, IDataParameter[] parameters)
        {
            DataTable result = new DataTable();
            try
            {
                OpenConn();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        result = GetTbByOrcProcedure(storedProcName, parameters);
                        break;
                    case DBType.SQLServer:
                        result = GetTbBySqlProcedure(storedProcName, parameters);
                        break;
                }
            }
            finally
            {
                CloseConn();
            }
            return result;
        }
        public DataTable GetTbByOrcProcedure(string procedureName, IDataParameter[] parameters)
        {

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            try
            {
                foreach (OracleParameter parameter in parameters)
                {
                    cmd.Parameters.Add(parameter);
                }
                DbDataAdapter adapter = NewDataAdapter(cmd);
                DataTable dt = new DataTable();
                adapter.Fill(dt);
                cmd.Dispose();
                return dt;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                return null;
            }
            finally
            {
                cmd.Dispose();
                 
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", procedureName, this._connection.ConnectionString));
            }
            return null;
        }
        public DataTable GetTbBySqlProcedure(string procedureName, IDataParameter[] parameters)
        {

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            try
            {
                foreach (SqlParameter parameter in parameters)
                {
                    cmd.Parameters.Add(parameter);
                }
                DbDataAdapter adapter = NewDataAdapter(cmd);
                DataTable dt = new DataTable();
                adapter.Fill(dt);
                cmd.Dispose();
                return dt;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                return null;
            }
            finally
            {
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", procedureName, this._connection.ConnectionString));
            }
            return null;
        }
        #endregion

        #region 执行存储过程 返回执行是否成功
        public bool ExecuteProcedure(string storedProcName, IDataParameter[] parameters)
        {
            bool result = false;
            try
            {
                OpenConn();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        result = ExecuteOrcProcedure(storedProcName, parameters);
                        break;
                    case DBType.SQLServer:
                        result = ExecuteSqlProcedure(storedProcName, parameters);
                        break;
                }
            }
            finally
            {
                if (this._connection.mCon != null)
                    this._connection.mCon.Close();
            }

            return result;
        }
        internal bool ExecuteOrcProcedure(string procedureName, IDataParameter[] parameters)
        {

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            try
            {
                foreach (OracleParameter parameter in parameters)
                {
                    cmd.Parameters.Add(parameter);
                }
                DbDataAdapter adapter = NewDataAdapter(cmd);
                cmd.ExecuteNonQuery();
                cmd.Dispose();
                return false;
                
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                return false;
            }
            finally
            {
                cmd.Dispose();                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", procedureName, this._connection.ConnectionString));
            }
            return false;
        }
        internal bool ExecuteSqlProcedure(string procedureName, IDataParameter[] parameters)
        {

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandText = procedureName;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = this.CommandTimeout;
            cmd.Transaction = this._connection.mTransaction;
            cmd.CommandTimeout = this.CommandTimeout;
            try
            {
                foreach (SqlParameter parameter in parameters)
                {
                    cmd.Parameters.Add(parameter);
                }
                cmd.ExecuteNonQuery();
                cmd.Dispose();
                return true;
            }
            catch (Exception e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, procedureName));
                }
                return false;
            }
            finally
            {
                cmd.Dispose();
                 
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}", procedureName, this._connection.ConnectionString));
            }
            return false;
        }

        #endregion


        #region 执行事务语句
        internal bool ExecuteTrans(string[] sqls)
        {
            bool effectRows = false;
            try
            {
                OpenConn();
                this._connection.BeginTransaction();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        effectRows = OracleCommandExecuteTrans(sqls);
                        break;
                    case DBType.SQLServer:
                        effectRows = SqlCommandExecuteTrans(sqls);
                        break;
                }
                if (effectRows)
                    this._connection.Commit();
                else
                    this._connection.Rollback();
            }
            finally
            {
                if (this._connection.mCon != null)
                    this._connection.mCon.Close();
            }

            return effectRows;
        }

        internal bool ExecuteTrans(List<SqlContent> sqlContents)
        {
            bool effectRows = false;
            try
            {
                OpenConn();

                this._connection.BeginTransaction();
                switch (this._dbType)
                {
                    case DBType.Oracle:
                        effectRows = OracleCommandExecuteTrans(sqlContents);
                        break;
                    case DBType.SQLServer:
                        break;
                }
                if (effectRows)
                    this._connection.Commit();
                else
                    this._connection.Rollback();
            }
            finally
            {
                CloseConn();
            }
            return effectRows;

        }

        private bool SqlCommandExecuteTrans(string[] sqls)
        {

            bool effectRows = false;
            string sqlAll = "";
            foreach (string sql in sqls)
            {
                sqlAll += sql;

            }

            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.Transaction = this._connection.mTransaction;
            try
            {
                foreach (string sql in sqls)
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                } 
            
                effectRows = true;


            }
            catch (SqlException e)
            {
                if (OnException != null)
                {
                    OnException(string.Format(@"数据库连接错误。连接字符串：{0}；待执行SQL：{1}", this._connection.ConnectionString, sqlAll));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(e);
                }
                return false;
            }
            catch
            {
              

                if (OnException != null)
                {
                    OnException(string.Format(@"数据库连接错误。连接字符串：{0}；待执行SQL：{1}", this._connection.ConnectionString, sqlAll));
                }
                return false;
            }
            finally
            {
                cmd.Dispose();
               
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sqlAll, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }

        /// <summary>
        /// 执行事务
        /// </summary>
        /// <param name="sqls"></param>
        /// <returns></returns>
        private bool OracleCommandExecuteTrans(List<SqlContent> sqlContents)
        {

            bool effectRows = false;
            string sqlAll = "";
            foreach (SqlContent sqlContent in sqlContents)
            {
                string temp = string.Empty;
                for (int i = 0; i < sqlContent.ParamNames.Length; i++)
                {
                    temp += sqlContent.ParamNames[i] + "=" + sqlContent.ParamValues[i].ToString() + "?" + sqlContent.ParamValues[i].GetType() + "|";
                }
                sqlAll += "\n" + sqlContent.Sql + "|" + temp;
            }
            DbCommand cmd = this._connection.mCon.CreateCommand();
            try
            {
                foreach (SqlContent sqlContent in sqlContents)
                {
                    cmd.Parameters.Clear();
                    sqlContent.Check();
                    cmd.CommandText = sqlContent.Sql;
                    for (int i = 0; i < sqlContent.ParamNames.Length; i++)
                    {
                        cmd.Parameters.Add(new OracleParameter(sqlContent.ParamNames[i], sqlContent.ParamValues[i]));
                    }
                    cmd.ExecuteNonQuery();
                }

               
                effectRows = true;

            }
            catch (OracleException ex)
            {
              
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", ex.Message, this._connection.ConnectionString, sqlAll));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(ex);
                }
                return false;
            }
            catch (Exception e)
            {
                
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sqlAll));
                }
                return false;
            }
            finally
            {
                cmd.Dispose();
                
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sqlAll, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }

        private bool OracleCommandExecuteTrans(string[] sqls)
        {

            bool effectRows = false;
            string sqlAll = "";
            foreach (string sql in sqls)
            {
                sqlAll += sql;

            }           
            DbCommand cmd = this._connection.mCon.CreateCommand();
            cmd.CommandType = System.Data.CommandType.Text;
            try
            {
                foreach (string sql in sqls)
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                } 
            }
            catch (OracleException ex)
            {
                
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", ex.Message, this._connection.ConnectionString, sqlAll));
                }
                if (OnExceptionEx != null)
                {
                    OnExceptionEx(ex);
                }
                return false;
            }
            catch (Exception e)
            {
                
                if (OnException != null)
                {
                    OnException(string.Format(@"错误信息：{0}。连接字符串：{1}；待执行SQL：{2}", e.Message, this._connection.ConnectionString, sqlAll));
                }
                return false;
            }
            finally
            {
                cmd.Dispose();
                 
            }
            if (OnProgress != null)
            {
                OnProgress(string.Format(@"SQL语句执行成功。执行SQL语句：{0}；数据库连接字符串：{1}；受影响行数：{2}", sqlAll, this._connection.ConnectionString, effectRows));
            }
            return effectRows;
        }
        #endregion

        #region
        private void OpenConn()
        {
            if (this._connection.mCon != null && this._connection.mCon.State == ConnectionState.Closed)
                this._connection.mCon.Open();
        }
        private void CloseConn()
        {
            if (this._connection.mTransaction == null && this._connection.mCon != null)
                this._connection.mCon.Close();
        }
        #endregion 
    }
}

