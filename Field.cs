using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.ComponentModel;

namespace DataAccess
{
    /// <summary>
    /// 数据字段
    /// </summary>
    public class Field
    {
        #region 字段、属性

        public bool IsNew = true;

        /// <summary>
        /// 字段名
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 旧字段名
        /// </summary>
        public string oldName { get; set; }

        /// <summary>
        /// 字段值
        /// </summary>
        private object _OldValue = null;
        public object OldValue
        {
            get
            {
                return _OldValue;
            }
        }
        private bool firstSetValue = true;
        private object _Value = null;
        public object Value
        {
            get
            {
                return _Value;
            }
            set
            {
                if (firstSetValue)
                {
                    _OldValue = value;
                    _Value = value;
                    firstSetValue = false;
                }
                else
                {
                    _Value = value;
                }
            }
        }

        private FieldType _fieldType = FieldType.Normal;
        /// <summary>
        /// 字段数据类型
        /// </summary>
        public FieldType FieldType
        {
            get
            {
                return _fieldType;
            }
            set
            {
                _fieldType = value;
            }
        }
        private bool _isKey = false;
        /// <summary>
        /// 是否主键。默认false;
        /// </summary>
        public bool IsKey
        {
            get
            {
                return _isKey;
            }
            set
            {
                _isKey = value;
            }
        }
        #endregion

        #region 构造
        public Field()
        {
        }

        public Field(string FieldName, string FieldValue, bool IsKey)
        {
            this.Name = FieldName;
            this.Value = FieldValue;
            this._isKey = IsKey;
            this._fieldType = DataAccess.FieldType.Normal;
        }

        public Field(string FieldName, string FieldValue)
        {
            this.Name = FieldName;
            this.Value = FieldValue;
            this._isKey = false;
            this._fieldType = DataAccess.FieldType.Normal;
        }

        public Field(string FieldName, string FieldValue, FieldType FieldType)
        {
            this.Name = FieldName;
            this.Value = FieldValue;
            this._isKey = false;
            this._fieldType = FieldType;
        }
        public Field(string FieldName, string FieldValue, FieldType FieldType, string oldFieldName)
        {
            this.Name = FieldName;
            this.Value = FieldValue;
            this._isKey = false;
            this._fieldType = FieldType;
            this.oldName = oldFieldName;
        }
        #endregion

        #region 序列化&反序列化

        /// <summary>
        /// 序列化方法
        /// </summary>
        /// <returns></returns>
        public XmlElement Serialization(XmlDocument _Record)
        {
            XmlElement _Field = _Record.CreateElement("Field");

            XmlAttribute xaName = _Record.CreateAttribute("Name");
            xaName.InnerText = this.Name;
            _Field.Attributes.Append(xaName);

            XmlAttribute xaValue = _Record.CreateAttribute("Value");
            xaValue.InnerText = this.Value.ToString();
            _Field.Attributes.Append(xaValue);

            XmlAttribute xaFieldType = _Record.CreateAttribute("FieldType");
            xaFieldType.InnerText = this.FieldType.ToString();
            _Field.Attributes.Append(xaFieldType);

            XmlAttribute xaIsKey = _Record.CreateAttribute("IsKey");
            xaIsKey.InnerText = this.IsKey.ToString();
            _Field.Attributes.Append(xaIsKey);

            return _Field;
        }

        /// <summary>
        /// 反序列化方法
        /// </summary>
        /// <param name="_Record">XmlElement对象</param>
        public static Field Deserialize(XmlElement _Field)
        {

            try
            {
                Field tempField = new Field();

                XmlAttribute xaName = _Field.Attributes["Name"];
                if (xaName != null)
                    tempField.Name = xaName.InnerText;

                XmlAttribute xaValue = _Field.Attributes["Value"];
                if (xaValue != null)
                    tempField.Value = xaValue.InnerText;

                XmlAttribute xaFieldType = _Field.Attributes["FieldType"];
                if (xaFieldType != null)
                    tempField.FieldType = (FieldType)Enum.Parse(typeof(FieldType), xaFieldType.InnerText);

                XmlAttribute xaIsKey = _Field.Attributes["IsKey"];
                if (xaIsKey != null)
                {
                    bool isKey = false;
                    bool.TryParse(xaIsKey.InnerText, out isKey);
                    tempField.IsKey = isKey;
                }

                return tempField;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("字段反序列化失败，错误信息：{0}", ex.Message));
            }

        }

        #endregion
    }
}
