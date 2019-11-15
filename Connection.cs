using System;
using System.Collections.Generic;

using System.Text;
using System.Data.SqlClient;
using System.Data.OracleClient;
using System.Xml;
using System.Data.Common;

namespace DataAccess
{
    class Connection
    {
        #region 字段、属性
        /// <summary>
        /// 数据库连接类别；Oracle、SQL Server
        /// </summary>
        public DBType ConnectionType { get; set; }

        /// <summary>
        /// 连接字符串的DataSource对象，提供数据库所在IP地址
        /// </summary>
        public string DataSource { get; set; }

        /// <summary>
        /// 连接字符串的Database数据库对象，提供数据库名
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// 连接字符串的UserID对象,提供对数据库访问的用户名
        /// </summary>
        public string UserID { get; set; }

        /// <summary>
        /// 连接字符串的Password对象,提供对数据库访问的密码
        /// </summary>
        public string Password { get; set; }

        private int _connectTimeout = 15;
        /// <summary>
        /// 连接超时设置
        /// </summary>
        public int ConnectTimeout
        {
            get
            {
                return _connectTimeout;
            }
            set
            {
                _connectTimeout = value;
            }
        }

        private string _connectionString = string.Empty;
        public string ConnectionString
        {
            get
            {
                if (_connectionString == string.Empty)
                {
                    return GetConnectionString();
                }
                return _connectionString;
            }
            set
            {
                _connectionString = value;
            }
        }


        public DbConnection mCon = null;
        public DbTransaction mTransaction = null;

        #endregion

        #region 构造函数
        public Connection(string DataSource,string Database,string UserID,string Password,DBType ConnectionType)
        {
            this.ConnectionType = ConnectionType;
            this.DataSource = DataSource;
            this.Database = Database;
            this.UserID = UserID;
            this.Password = Password;

            
           if (ConnectionType == DBType.SQLServer)
            {
                mCon = new SqlConnection(ConnectionString);
            }
            else  if (ConnectionType == DBType.Oracle)
            {
                mCon = new OracleConnection(ConnectionString);
            }          
            else
            {
                string err = string.Format("Connection():不支持的数据库类型({0})", ConnectionType);
                throw new Exception(err);
            }
            mCon.Open();

        }
        public Connection(string ConnectionString, DBType ConnectionType)
        {
            this._connectionString = ConnectionString;
            this.ConnectionType = ConnectionType;

            if (ConnectionType == DBType.SQLServer)
            {
                mCon = new SqlConnection(_connectionString);
            }
            else  if (ConnectionType == DBType.Oracle)
            {
                mCon = new OracleConnection(_connectionString);
            }          
            else
            {
                string err = string.Format("Connection():不支持的数据库类型({0})", ConnectionType);
                throw new Exception(err);
            }
            mCon.Open();
           
        }
        #region 事务
        public void BeginTransaction()
        {
            this.mTransaction = this.mCon.BeginTransaction();
        }
        public void Commit()
        {
            if (this.mTransaction == null) return;
            this.mTransaction.Commit();
            this.mTransaction = null;
        }
        public void Rollback()
        {
            if (this.mTransaction == null) return;
            this.mTransaction.Rollback();
            this.mTransaction = null;
        }
        #endregion
        /// <summary>
        /// 为反序列化新增的构造函数
        /// </summary>
        private Connection()
        { }
        #endregion

        #region 根据Connection对象属性，创建连接字符串
        private string GetConnectionString()
        {
            string connectionString = string.Empty;
            switch (this.ConnectionType)
            {
                case DBType.Oracle:
                    connectionString = string.Format(@"Data Source={0};User Id={1};Password={2};", this.Database, this.UserID, this.Password);
                    break;
                case DBType.SQLServer:
                    connectionString = string.Format(@"Password={0};Persist Security Info=True;User ID={1};Initial Catalog={2};Data Source={3}", this.Password, this.UserID, this.Database, this.DataSource);
                    break;
            }
            return connectionString;
        }
        #endregion

        #region 测试数据库连接
        /// <summary>
        /// 测试数据库连接
        /// </summary>
        /// <returns>成功或失败</returns>
        public bool Connect()
        {
            bool bReturn = false;
            if (this.ConnectionType == DBType.Oracle)
            {
                OracleConnection conn = null;
                try
                {
                    conn = new OracleConnection(this._connectionString);
                    conn.Open();
                    bReturn = true;
                }
                catch
                {
                    bReturn = false;
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            if (this.ConnectionType == DBType.SQLServer)
            {
                SqlConnection conn = null;
                try
                {
                    conn = new SqlConnection(this._connectionString);
                    conn.Open();
                    bReturn = true;
                }
                catch
                {
                    bReturn = false;
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            return bReturn;
        }
        #endregion

        #region 序列化&反序列化

        /// <summary>
        /// 序列化方法
        /// </summary>
        /// <returns></returns>
        public XmlElement Serialization(XmlDocument _Record)
        {
            XmlElement _Connection = _Record.CreateElement("Connection");

            XmlAttribute xaConnectionType = _Record.CreateAttribute("ConnectionType");
            xaConnectionType.InnerText = this.ConnectionType.ToString();
            _Connection.Attributes.Append(xaConnectionType);

            XmlAttribute xaDataSource = _Record.CreateAttribute("DataSource");
            xaDataSource.InnerText = this.DataSource.ToString();
            _Connection.Attributes.Append(xaDataSource);

            XmlAttribute xaDatabase = _Record.CreateAttribute("Database");
            xaDatabase.InnerText = this.Database.ToString();
            _Connection.Attributes.Append(xaDatabase);

            XmlAttribute xaUserID = _Record.CreateAttribute("UserID");
            xaUserID.InnerText = this.UserID.ToString();
            _Connection.Attributes.Append(xaUserID);

            XmlAttribute xaPassword = _Record.CreateAttribute("Password");
            xaPassword.InnerText = this.Password.ToString();
            _Connection.Attributes.Append(xaPassword);

            XmlAttribute xaConnectTimeout = _Record.CreateAttribute("ConnectTimeout");
            xaConnectTimeout.InnerText = this.ConnectTimeout.ToString();
            _Connection.Attributes.Append(xaConnectTimeout);

            XmlAttribute xaConnectionString = _Record.CreateAttribute("ConnectionString");
            xaConnectionString.InnerText = this.ConnectionString.ToString();
            _Connection.Attributes.Append(xaConnectionString);

            return _Connection;
        }

        /// <summary>
        /// 反序列化方法
        /// </summary>
        /// <param name="_Record">XmlElement对象</param>
        public static Connection Deserialize(XmlElement _Connection)
        {

            try
            {
                Connection tempConnection = new Connection();

                XmlAttribute xaConnectionType = _Connection.Attributes["ConnectionType"];
                if (xaConnectionType != null)
                    tempConnection.ConnectionType = (DBType)Enum.Parse(typeof(DBType), xaConnectionType.InnerText);

                XmlAttribute xaDataSource = _Connection.Attributes["DataSource"];
                if (xaDataSource != null)
                    tempConnection.DataSource = xaDataSource.InnerText;

                XmlAttribute xaDatabase = _Connection.Attributes["Database"];
                if (xaDatabase != null)
                    tempConnection.Database = xaDatabase.InnerText;

                XmlAttribute xaUserID = _Connection.Attributes["UserID"];
                if (xaUserID != null)
                    tempConnection.UserID = xaUserID.InnerText;

                XmlAttribute xaPassword = _Connection.Attributes["Password"];
                if (xaPassword != null)
                    tempConnection.Password = xaPassword.InnerText;

                XmlAttribute xaConnectTimeout = _Connection.Attributes["ConnectTimeout"];
                if (xaConnectTimeout != null)
                {
                    int _ConnectTimeout = 30;
                    int.TryParse(xaConnectTimeout.InnerText, out _ConnectTimeout);
                    tempConnection.ConnectTimeout = _ConnectTimeout;
                }

                XmlAttribute xaConnectionString = _Connection.Attributes["ConnectionString"];
                if (xaConnectionString != null)
                    tempConnection.ConnectionString = xaConnectionString.InnerText;

                return tempConnection;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("数据库连接对象反序列化失败，错误信息：{0}", ex.Message));
            }

        }

        #endregion

    }
}