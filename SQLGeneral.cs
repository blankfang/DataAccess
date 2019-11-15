using System;
using System.Collections.Generic;

using System.Text;

namespace DataAccess
{
    class SQLGeneral
    {
        public string General(Record record)
        {
            DBType dbType = record.Connection.ConnectionType;
            List<Field> keyFields = GetKeyFields(record);
            StringBuilder sbChkCondition = new StringBuilder();
            foreach (Field field in keyFields)
            {
                sbChkCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
            }
            if (sbChkCondition.Length >= 5)
                sbChkCondition = sbChkCondition.Remove(sbChkCondition.Length - 5, 5);

            return sbChkCondition.ToString();
        }
        public string GeneralOld(Record record)
        {
            DBType dbType = record.Connection.ConnectionType;
            List<Field> keyFields = GetKeyFields(record);
            StringBuilder sbChkCondition = new StringBuilder();
            foreach (Field field in keyFields)
            {
                sbChkCondition.Append(string.Format(@"{0}={1} and ", string.IsNullOrEmpty(field.oldName) ? field.Name : field.oldName, _formatFieldValue(field, dbType)));
            }
            if (sbChkCondition.Length >= 5)
                sbChkCondition = sbChkCondition.Remove(sbChkCondition.Length - 5, 5);

            return sbChkCondition.ToString();
        }
        public string GeneralOld(Record record,DBType dbType)
        {
            //DBType dbType = record.Connection.ConnectionType;
            List<Field> keyFields = GetKeyFields(record);
            StringBuilder sbChkCondition = new StringBuilder();
            foreach (Field field in keyFields)
            {
                sbChkCondition.Append(string.Format(@"{0}={1} and ", string.IsNullOrEmpty(field.oldName) ? field.Name : field.oldName, _formatFieldValue(field, dbType)));
            }
            if (sbChkCondition.Length >= 5)
                sbChkCondition = sbChkCondition.Remove(sbChkCondition.Length - 5, 5);

            return sbChkCondition.ToString();
        }

        /// <summary>
        /// 根据 Record内容、执行模式生成相应的执行语句
        /// </summary>
        /// <param name="record"></param>
        /// <param name="execMode"></param>
        /// <returns></returns>
        public string General(Record record, ExecuteMode execMode)
        {
            DBType dbType = record.Connection.ConnectionType;
            string sqlInsert = @"insert into {0}({1}) values({2})";
            string sqlUpdate = @"update {0} set {1} where {2}";
            string sqlDelete = @"delete from {0} where {1}";
            string sqlCheck = @"select count(*) from {0} where {1}";
            StringBuilder sbSQL = new StringBuilder();
            string sql = string.Empty;

            List<Field> keyFields = GetKeyFields(record);
            if (keyFields.Count == 0 && execMode != ExecuteMode.Insert)
            {
                throw new Exception(string.Format(@"表{0}没有设置主键字段", record.TableName));
            }

            FieldCollection fc = record.FieldCollection;
            switch (execMode)
            {
                case ExecuteMode.Insert:
                    StringBuilder sbFields = new StringBuilder();
                    StringBuilder sbValues = new StringBuilder();
                    foreach (Field field in fc)
                    {
                        //if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
                        //{
                        //    sbFields.Append(string.Format(@"{0},", field.Name));
                        //    sbValues.Append(string.Format(@"to_date('{0}','yyyy-mm-dd HH24:mi:ss'),", field.Value));
                        //}
                        //else
                        //{
                        //    sbFields.Append(string.Format(@"{0},", field.Name));
                        //    sbValues.Append(string.Format(@"'{0}',", field.Value));
                        //}
                        sbFields.Append(string.Format(@"{0},", field.Name));
                        sbValues.Append(string.Format(@"{0},", _formatFieldValue(field, dbType)));
                    }
                    sql = string.Format(sqlInsert, record.TableName, sbFields.ToString().TrimEnd(','), sbValues.ToString().TrimEnd(','));
                    break;
                case ExecuteMode.Update:
                    StringBuilder sbUpdate = new StringBuilder();
                    StringBuilder sbUpdateCondition = new StringBuilder();
                    foreach (Field field in fc)
                    {
                        if (!field.IsKey)
                        {
                            //if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
                            //{
                            //    sbUpdate.Append(string.Format(@"{0}=to_date('{1}','yyyy-mm-dd HH24:mi:ss'),", field.Name, field.Value));
                            //}
                            //else
                            //{
                            //    sbUpdate.Append(string.Format(@"{0}='{1}',", field.Name, field.Value));
                            //}

                            sbUpdate.Append(string.Format(@"{0}={1},", field.Name, _formatFieldValue(field, dbType)));
                        }
                        else
                        {
                            //if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
                            //{
                            //    sbUpdateCondition.Append(string.Format(@"{0}=to_date('{1}','yyyy-mm-dd HH24:mi:ss') and ", field.Name, field.Value));
                            //}
                            //else
                            //{
                            //    sbUpdateCondition.Append(string.Format(@"{0}='{1}' and ", field.Name, field.Value));
                            //}

                            sbUpdateCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
                        }
                    }
                    sbUpdateCondition = sbUpdateCondition.Remove(sbUpdateCondition.Length - 4, 4);
                    sql = string.Format(sqlUpdate, record.TableName, sbUpdate.ToString().TrimEnd(','), sbUpdateCondition.ToString());
                    break;
                case ExecuteMode.Delete:
                    StringBuilder sbDelCondition = new StringBuilder();
                    foreach (Field field in keyFields)
                    {
                        //if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
                        //{
                        //    sbDelCondition.Append(string.Format(@"{0}=to_date('{1}','yyyy-mm-dd HH24:mi:ss') and ", field.Name, field.Value));
                        //}
                        //else
                        //{
                        //    sbDelCondition.Append(string.Format(@"{0}='{1}' and ", field.Name, field.Value));
                        //}
                        sbDelCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
                    }
                    sbDelCondition = sbDelCondition.Remove(sbDelCondition.Length - 4, 4);
                    sql = string.Format(sqlDelete, record.TableName, sbDelCondition.ToString());
                    break;
                case ExecuteMode.Check:
                    StringBuilder sbChkCondition = new StringBuilder();
                    foreach (Field field in keyFields)
                    {
                        //if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
                        //{
                        //    sbChkCondition.Append(string.Format(@"{0}=to_date('{1}','yyyy-mm-dd HH24:mi:ss') and ", field.Name, field.Value));
                        //}
                        //else
                        //{
                        //    sbChkCondition.Append(string.Format(@"{0}='{1}' and ", field.Name, field.Value));
                        //}
                        sbChkCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
                    }
                    sbChkCondition = sbChkCondition.Remove(sbChkCondition.Length - 4, 4);
                    sql = string.Format(sqlCheck, record.TableName, sbChkCondition.ToString());
                    break;
            }

            return sql;
        }

        public string General(Record record, ExecuteMode execMode,out List<string> ParamNames,out List<object> ParamValues)
        {
            ParamNames = new List<string>();
            ParamValues = new List<object>();

            DBType dbType = record.Connection.ConnectionType;
            string sqlInsert = @"insert into {0}({1}) values({2})";
            string sqlUpdate = @"update {0} set {1} where {2}";
            string sqlDelete = @"delete from {0} where {1}";
            string sqlCheck = @"select count(*) from {0} where {1}";
            StringBuilder sbSQL = new StringBuilder();
            string sql = string.Empty;

            List<Field> keyFields = GetKeyFields(record);
            if (keyFields.Count == 0 && execMode != ExecuteMode.Insert)
            {
                throw new Exception(string.Format(@"表{0}没有设置主键字段", record.TableName));
            }

            FieldCollection fc = record.FieldCollection;
            switch (execMode)
            {
                case ExecuteMode.Insert:
                    StringBuilder sbFields = new StringBuilder();
                    StringBuilder sbValues = new StringBuilder();
                    foreach (Field field in fc)
                    {                        
                        sbFields.Append(string.Format(@"{0},", field.Name));
                        sbValues.Append("@"+field.Name+",");
                        
                        ParamNames.Add(field.Name);
                        ParamValues.Add(_formatFieldValue(field));
                        
                    }
                    sql = string.Format(sqlInsert, record.TableName, sbFields.ToString().TrimEnd(','), sbValues.ToString().TrimEnd(','));
                    break;
                case ExecuteMode.Update:
                    StringBuilder sbUpdate = new StringBuilder();
                    StringBuilder sbUpdateCondition = new StringBuilder();
                    foreach (Field field in fc)
                    {
                        if (!field.IsKey)
                        {
                            sbUpdate.Append(string.Format(@"{0}={1},", field.Name, "@"+field.Name));
                             
                            ParamNames.Add(field.Name);
                            ParamValues.Add(_formatFieldValue(field));

                            //sbUpdate.Append(string.Format(@"{0}={1},", field.Name, _formatFieldValue(field, dbType)));
                        }
                        else
                        {
                            sbUpdateCondition.Append(string.Format(@"{0}={1} and ", field.Name, "@" + field.Name));

                            ParamNames.Add(field.Name);
                            ParamValues.Add(_formatFieldValue(field));

                            //sbUpdateCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
                        }
                    }
                    sbUpdateCondition = sbUpdateCondition.Remove(sbUpdateCondition.Length - 4, 4);
                    sql = string.Format(sqlUpdate, record.TableName, sbUpdate.ToString().TrimEnd(','), sbUpdateCondition.ToString());
                    break;
                case ExecuteMode.Delete:
                    StringBuilder sbDelCondition = new StringBuilder();
                    foreach (Field field in keyFields)
                    {

                        sbDelCondition.Append(string.Format(@"{0}={1} and ", field.Name, "@"+field.Name));
                        
                        ParamNames.Add(field.Name);
                        ParamValues.Add(_formatFieldValue(field));

                        //sbDelCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
                    }
                    sbDelCondition = sbDelCondition.Remove(sbDelCondition.Length - 4, 4);
                    sql = string.Format(sqlDelete, record.TableName, sbDelCondition.ToString());
                    break;
                case ExecuteMode.Check:
                    StringBuilder sbChkCondition = new StringBuilder();
                    foreach (Field field in keyFields)
                    {
                        sbChkCondition.Append(string.Format(@"{0}={1} and ", field.Name, "@"+field.Name));

                        ParamNames.Add(field.Name);
                        ParamValues.Add(_formatFieldValue(field));
                        //sbChkCondition.Append(string.Format(@"{0}={1} and ", field.Name, _formatFieldValue(field, dbType)));
                    }
                    sbChkCondition = sbChkCondition.Remove(sbChkCondition.Length - 4, 4);
                    sql = string.Format(sqlCheck, record.TableName, sbChkCondition.ToString());
                    break;
            }

            return sql;
        }
        private string _formatParam(Field field,DBType dbType)
        {
            if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
            {
                return (field.Value == null || field.Value.ToString() == "") ? "null" : string.Format(@"to_date('{0}','yyyy-mm-dd HH24:mi:ss')", ":" + field.Name);
            }
            return "@" + field.Name;
        }
        private string _formatFieldValue(Field field, DBType dbType)
        {
            if (field.FieldType == FieldType.DateTime && dbType == DBType.Oracle)
            {
                return (field.Value == null ||field.Value.ToString() == "" ) ? "null" : string.Format(@"to_date('{0}','yyyy-mm-dd HH24:mi:ss')", field.Value);
            }
            return (field.Value == null ||field.Value.ToString() == "" ) ? "null" : string.Format(@"'{0}'", ((string)field.Value).Replace("'", "''"));
        }
        private object _formatFieldValue(Field field)
        {
            if (field.FieldType == FieldType.DateTime)
            {
                return  (field.Value == null||field.Value.ToString() == "") ? DBNull.Value : (object)Convert.ToDateTime(field.Value);
            }
            return field.Value;
        }
        /// <summary>
        /// 获得主键字段
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        private List<Field> GetKeyFields(Record record)
        {
            List<Field> keyFields = new List<Field>();
            FieldCollection fc = record.FieldCollection;
            foreach (Field field in fc)
            {
                if (field.IsKey)
                    keyFields.Add(field);
            }

            return keyFields;
        }
    }
}
