using System;
using System.Collections.Generic;

using System.Text;

namespace DataAccess
{
    public class SqlContent
    {
        string _sql;

        public string Sql
        {
            get { return _sql; }
            set { _sql = value; }
        }

        List<string> _paramNames;

        public string[] ParamNames
        {
            get { return _paramNames.ToArray(); }
        }

        List<object> _paramValues;

        public object[] ParamValues
        {
            get { return _paramValues.ToArray(); }
        }

        public SqlContent()
            : this(string.Empty)
        { }

        public SqlContent(string sql)
            : this(sql, new List<string>(), new List<object>())
        { }

        public SqlContent(string sql, List<string> paramNames, List<object> paramValues)
        {
            _sql = sql;
            _paramNames = paramNames;
            _paramValues = paramValues;
        }

        public void AddParam(string paramName, object paramValue)
        {
            if (string.IsNullOrEmpty(paramName))
            {
                throw new Exception("参数名错误");
            }

            _paramNames.Add(paramName);
            _paramValues.Add(paramValue);
        }

        public void Check()
        {
            if (string.IsNullOrEmpty(_sql))
            {
                throw new Exception("SQL语句错误");
            }

            if ((_paramNames != null && _paramValues == null) || (_paramValues != null && _paramNames == null))
            {
                throw new Exception("参数名和值不统一");
            }

            if (_paramNames != null && _paramValues != null && _paramNames.Count != _paramValues.Count)
            {
                throw new Exception("参数名和值不统一");
            }
        }

    }
}
