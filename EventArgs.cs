using System;
using System.Collections.Generic;

using System.Text;

namespace DataAccess
{
    public class EventArgs
    {
        public EventArgs()
        {
        }

        public string StackMessage { get; set; }
        public string ExceptionMessage { get; set; }
    }
}
