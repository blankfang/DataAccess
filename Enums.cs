using System;
using System.Collections.Generic;

using System.Text;

namespace DataAccess
{
    /// <summary>
    /// 数据库类型
    /// </summary>
    public enum DBType
    {
        SQLServer = 1,
        Oracle = 2
    }

    /// <summary>
    /// 数据记录操作模式
    /// </summary>
    public enum ExecuteMode
    {
        Insert = 0,
        Update = 1,
        InsertOrUpdate = 2,
        Delete = 3,
        Check = 4
    }

    /// <summary>
    /// 字段数据类型
    /// </summary>
    public enum FieldType
    {
        Normal=1,
        DateTime=2
    }
}
