using System;
using System.Collections.Generic;
using System.Collections;

using System.Text;
using System.Xml;

namespace DataAccess
{
    public class RecordCollection : IList<Record>
    {
        #region 字段、属性
        private readonly List<Record> _items = new List<Record>();
        #endregion

        #region 构造
        public RecordCollection()
        {
        }
        #endregion

        #region 索引方法
        public int this[Record record]
        {
            get
            {
                int index = GetRecordIndex(record);
                if (index == -1)
                    throw new ArgumentOutOfRangeException("record",
                                                          "Record \"" + record.TableName +
                                                          "\" was not found in the collection");
                return index;
            }
        }

        public int GetRecordIndex(Record record)
        {
            // TODO: should we rewrite this? what would be the key of a node?
            for (int i = 0; i < _items.Count; i++)
                if (record == _items[i])
                    return i;
            return -1;
        }

        #endregion

        #region IList<Record> 成员
        public int Count
        {
            get { return _items.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public Record this[int index]
        {
            get { return _items[index]; }
            set { _items[index] = value; }
        }

        public void Add(Record record)
        {
            _items.Add(record);
        }

        public void Clear()
        {
            _items.Clear();
        }

        public bool Contains(Record item)
        {
            return _items.Contains(item);
        }

        public void CopyTo(Record[] array, int arrayIndex)
        {
            _items.CopyTo(array, arrayIndex);
        }

        IEnumerator<Record> IEnumerable<Record>.GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        public int IndexOf(Record item)
        {
            return _items.IndexOf(item);
        }

        public void Insert(int index, Record record)
        {
            _items.Insert(index, record);
        }

        public bool Remove(Record item)
        {
            _items.Remove(item);
            return true;
        }

        public void RemoveAt(int index)
        {
            _items.RemoveAt(index);
        }
        #endregion

        #region 序列化&反序列化

        /// <summary>
        /// 对象序列化方法
        /// <code>
        /// XmlDocument doc=RecordCollection.Serialization();
        /// </code>
        /// </summary>
        /// <returns>XmlDocument</returns>
        public XmlDocument Serialization()
        {
            XmlDocument _Records = new XmlDocument();
            XmlElement xeRecords = _Records.CreateElement("Records");
            _Records.AppendChild(xeRecords);

            foreach (Record _Record in this._items)
            {
                XmlElement xeRecord = _Record.Serialization(_Records);
                xeRecords.AppendChild(xeRecord);
            }

            return _Records;
        }

        /// <summary>
        /// 反序列化对象
        /// <code>
        /// RecordCollection rc=RecordCollection.Deserialize( XmlDocument );
        /// </code>
        /// </summary>
        /// <param name="_Records">XmlDocument</param>
        /// <returns>RecordCollection</returns>
        public static RecordCollection Deserialize(XmlDocument _Records)
        {
            RecordCollection tempRecordCollection = new RecordCollection();
            tempRecordCollection.Clear();
            XmlElement xeRecords = _Records["Records"];
            foreach (XmlElement xeRecord in xeRecords.ChildNodes)
            {
                tempRecordCollection.Add(Record.Deserialize(xeRecord));
            }
            return tempRecordCollection;
        }

        /// <summary>
        /// 反序列化对象
        /// <code>
        /// RecordCollection rc=RecordCollection.Deserialize( string );
        /// </code>
        /// </summary>
        /// <param name="_Records">string</param>
        /// <returns>RecordCollection</returns>
        public static RecordCollection Deserialize(string _Records)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(_Records);
            return Deserialize(doc);
        }

        #endregion

    }
}
