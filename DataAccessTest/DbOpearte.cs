﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataAccess;
using System.Data;
using System.Data.OracleClient;

namespace DataAccessTest
{
    class DbOpearte
    {
        public DBInfo SourceDBInfo;
        DBAccess acc;
        public delegate void TaskExceptionHandler(string ExceptionMessage);
        public event TaskExceptionHandler OnTaskException;

        public DbOpearte(string dbtype)
        {
            //实例化数据库访问类
            SourceDBInfo = GetDBInfo(dbtype);
            acc = _GenearlSourDBAcc();
            acc.OnException += new DataAccess.ExceptionHandler(OnException);
        }

        public int SelectDt()
        {
            string sql = "select  *  from   hqftable1";
            DataTable dtCols = acc.GetDataTable(sql);
            return dtCols.Rows.Count;
        }
        public int SelectDataSet()
        {



            string sql = "select  *  from   hqftable1";

            DataSet dsCols = acc.GetDataSet(sql);

            return dsCols.Tables.Count;
        }
        public int Update()
        {


            string sql = "update   hqftable1 set name='123'";
            return acc.Execute(sql);
        }
        public int UpdateWithPara()
        {
            string paraString =":";
            if (SourceDBInfo.DBType == DBType.SQLServer)
                paraString = "@";
            string sql = "update   hqftable1 set name="+paraString+"name";

            string[] ParamNames = new string[1];
            ParamNames[0] = "name";
            object[] ParamValues = new object[1];
            ParamValues[0] = "234";
            return acc.Execute(sql, ParamNames, ParamValues);
        }
        public int RecordUpdate()
        {

            string sql = "select  *  from   hqftable1";
            DataTable dtCols = acc.GetDataTable(sql);

            DataAccess.RecordCollection rc = _GeneralRecords(dtCols, SourceDBInfo.DBType);
            return acc.ProcessRecord(rc, DataAccess.ExecuteMode.InsertOrUpdate);
        }

        public int RecordUpdate(bool isaddlog)
        {

            string sql = "select  Name,content,photo   from   hqftable1";
            DataTable dtCols = acc.GetDataTable(sql);

            DataAccess.RecordCollection rc = _GeneralRecords(dtCols, SourceDBInfo.DBType);
            
            return acc.ProcessRecord(rc[0], DataAccess.ExecuteMode.InsertOrUpdate,true);
        }


        public DataTable GetTbByProcedure()
        {

            OracleParameter[] paras = new OracleParameter[3];
            OracleParameter op = null;

            op = new OracleParameter();
            op.OracleType = OracleType.VarChar;
            op.ParameterName = "v_UID";
            op.Direction = ParameterDirection.Input;
            op.Value = "bobshang";
            paras[0] = op;

            op = new OracleParameter();
            op.OracleType = OracleType.VarChar;
            op.ParameterName = "v_PWD";
            op.Direction = ParameterDirection.Input;
            op.Value = "96E79218965EB72C92A549DD5A33112";
            paras[1] = op;

            op = new OracleParameter();
            op.OracleType = OracleType.Cursor;
            op.ParameterName = "v_Cursor";
            op.Direction = ParameterDirection.Output;
            paras[2] = op;

            DataTable dt = acc.GetTbByProcedure("UserLogin", paras);

            return dt;           
            
        }
        public bool ExecuteProcedure()
        {

            OracleParameter[] parameters ={ 
                new OracleParameter("P_ID", OracleType.Number,4),
                new OracleParameter("P_UserID", OracleType.VarChar,12),
                new OracleParameter("P_UserName", OracleType.VarChar,8),
                new OracleParameter("P_Password", OracleType.VarChar,32),
                new OracleParameter("P_IsDel", OracleType.Char,1)
                };
            parameters[0].Value = "62";
            parameters[1].Value = "wzm123";
            parameters[2].Value = "吴周明";
            parameters[3].Value = ""; 
            parameters[4].Value = "2";

            
           return acc.ExecuteProcedure("updateuser", parameters);

        }





        void OnException(string ExceptionMessage)
        {
            if (OnTaskException != null)
            {
                OnTaskException(ExceptionMessage);
            }
        }
        private DataAccess.RecordCollection _GeneralRecords(DataTable sourDt, DBType type)
        {
            DataAccess.RecordCollection rc = new DataAccess.RecordCollection();

            int index = 0;
            foreach (DataRow dr in sourDt.Rows)
            {
                DataAccess.Record rec = _GeneralRecord(dr, type);
                rec.Index = index;
                rc.Add(rec);
                index++;
            }
            return rc;
        }
        private DataAccess.Record _GeneralRecord(DataRow dataRow, DBType type)
        {
            DataAccess.Record record = new DataAccess.Record("hqftable2");
            record.emLgdbId = "123";
            record.TableId = "1222";
            foreach (DataColumn dc in dataRow.Table.Columns)
            {
                record.AddField(dc.ColumnName
                                , dataRow[dc]
                                , _SetFieldType(dc)
                                , IsKey(dc.ColumnName));

            }
            record.dbType = type;

            return record;
        }
        public bool IsKey(string colunmnName)
        {
            if (colunmnName.ToUpper() == "NAME")
                return true;
            else
                return false;
        }
        /// <summary>
        /// 设定字段类型（ORACLE操作数据库需要更多额外的字段数据类型信息）
        /// </summary>
        /// <param name="dataColumn"></param>
        /// <returns></returns>
        private DataAccess.FieldType _SetFieldType(DataColumn dataColumn)
        {
            if (dataColumn.DataType.ToString().Equals(@"System.DateTime"))
            {
                return DataAccess.FieldType.DateTime;
            }
            else
            {
                return DataAccess.FieldType.Normal;
            }
        }

        /// <summary>
        /// 取得源数据库的访问入口类
        /// </summary>
        /// <returns></returns>
        private DBAccess _GenearlSourDBAcc()
        {
            DBAccess _sAcc = null;
            if (!string.IsNullOrEmpty(this.SourceDBInfo.Connstring))
            {
                _sAcc = new DBAccess(this.SourceDBInfo.Connstring, this.SourceDBInfo.DBType);
            }
            else
            {
                _sAcc = new DBAccess(this.SourceDBInfo.DataSource
                                    , this.SourceDBInfo.DBName
                                    , this.SourceDBInfo.UserID
                                    , this.SourceDBInfo.Password
                                    , this.SourceDBInfo.DBType);
            }
            return _sAcc;
        }

        private static DBInfo GetDBInfo(string type)
        {

            DBInfo dbInfo = new DBInfo();

            if (type == "Oracle")
            {
                dbInfo.Connstring = "";
                dbInfo.DataSource = "";
                dbInfo.DBName = "inputdbtest";
                dbInfo.UserID = "inputadmin";
                dbInfo.Password = "pa55w0rd";
                dbInfo.DBType = DBType.Oracle;
            }
            else
            {

                dbInfo.Connstring = "";
                dbInfo.DataSource = ".";
                dbInfo.DBName = "test";
                dbInfo.UserID = "sa";
                dbInfo.Password = "sa";
                dbInfo.DBType = DBType.SQLServer;
            }

            return dbInfo;
        }
    }

    /// <summary>
    /// 数据库信息
    /// </summary>
    public struct DBInfo
    {
        /// <summary>
        /// 用户名
        /// </summary>
        public string UserID;
        /// <summary>
        /// 密码
        /// </summary>
        public string Password;
        /// <summary>
        /// 数据库名称
        /// </summary>
        public string DBName;
        /// <summary>
        /// SQL数据库DataSource属性值
        /// </summary>
        public string DataSource;
        /// <summary>
        /// 连接字符串
        /// </summary>
        public string Connstring;
        /// <summary>
        /// 数据库类型
        /// </summary>
        public DataAccess.DBType DBType;
    }
}

