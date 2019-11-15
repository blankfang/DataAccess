using System;
using System.Collections.Generic;
using System.Collections;

using System.Text;
using System.Xml;

namespace DataAccess
{
    /// <summary>
    /// 字段集合
    /// </summary>
    public class FieldCollection : IList<Field>
    {
        #region 字段、属性
        private readonly List<Field> _items = new List<Field>();
        #endregion

        #region 构造
        public FieldCollection()
        {
        }
        #endregion

        #region 索引方法
        public int this[Field field]
        {
            get
            {
                int index = GetFieldIndex(field);
                if (index == -1)
                    throw new ArgumentOutOfRangeException("field",
                                                          "Field \"" + field.Name +
                                                          "\" was not found in the collection");
                return index;
            }
        }

        public Field this[string fieldName]
        {
            get
            {
                fieldName = fieldName.ToLower();
                for (int i = 0; i < _items.Count; i++)
                    if (_items[i].Name.ToLower().Equals(fieldName))
                        return _items[i];

                return null;
            }
        }

        public int GetFieldIndex(Field field)
        {
            // TODO: should we rewrite this? what would be the key of a node?
            for (int i = 0; i < _items.Count; i++)
                if (field == _items[i])
                    return i;
            return -1;
        }

        #endregion

        #region IList<Field> 成员
        public int Count
        {
            get { return _items.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public Field this[int index]
        {
            get { return _items[index]; }
            set { _items[index] = value; }
        }

        public void Add(Field field)
        {
            _items.Add(field);
        }

        public void Clear()
        {
            _items.Clear();
        }

        public bool Contains(Field item)
        {
            return _items.Contains(item);
        }

        public void CopyTo(Field[] array, int arrayIndex)
        {
            _items.CopyTo(array, arrayIndex);
        }

        IEnumerator<Field> IEnumerable<Field>.GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        public int IndexOf(Field item)
        {
            return _items.IndexOf(item);
        }

        public void Insert(int index, Field field)
        {
            _items.Insert(index, field);
        }

        public bool Remove(Field item)
        {
            _items.Remove(item);
            return true;
        }

        public void RemoveAt(int index)
        {
            _items.RemoveAt(index);
        }
        #endregion
    }
}
