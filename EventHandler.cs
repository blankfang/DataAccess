using System;
using System.Collections.Generic;

using System.Text;
using System.Data.SqlClient;

namespace DataAccess
{
    public delegate void ExceptionHandler(string ExceptionMessage);
    public delegate void ExceptionExHandler(Exception Exception);
    public delegate void ExceptionXPHandler(object sender,DataAccess.EventArgs EventArgs);
    public delegate void MessageHandler(string Message);
    public delegate void RecordHandler(string record);
}
