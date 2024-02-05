using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Threading;
using System.Net;
using System.Xml;
using System.Web;
using System.IO;
using System.Configuration;
using MySql.Data.MySqlClient;

namespace bechtle_ugam
{
   public class My_sqlDB_Daily
    {
        public DataTable dt = null;
        public DataSet ds = null;

        public static string error = "";

        public static int introws;

        public string connection_String = " ";

        string conn = System.IO.File.ReadAllText(Application.StartupPath + @"\ConnectionStringDaily.txt");

        public DataSet executeSQL_dataset(string strQuery)
        {
        runAgain:
            MySqlConnection con = new MySqlConnection(conn);
            MySqlDataAdapter da = new MySqlDataAdapter();
            DataSet ds = new DataSet();
            try
            {
                con.Open();
                MySqlCommand cmd = new MySqlCommand();
                cmd.CommandText = strQuery;
                cmd.Connection = con;
                cmd.CommandTimeout = 10000;
                da.SelectCommand = cmd;
                da.Fill(ds);
                con.Close();

            }
            catch (Exception ex)
            {
                error = ex.Message;
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                if (error.Contains("Authentication to host '' for user '' using method 'mysql_native_password' failed with message: Access denied for user ''@'localhost' (using password: NO)") ||
                    error.Contains("Authentication to host 'localhost' for user 'root' using method 'mysql_native_password") ||
                    error.Contains("A connection attempt failed because the connected party did not properly respond after a period of time") ||
                    error.Contains("Unable to connect to any of the specified MySQL hosts") ||
                    error.Contains("Fatal error encountered during command execution."))
                {
                    //Messgae print::
                    Thread.Sleep(2000);
                    goto runAgain;
                }
                System.Diagnostics.Debugger.Break();
            }
            return ds;
        }

        public DataTable executeSQL_datatable(string strQuery)
        {
        runAgain:
            MySqlConnection con = new MySqlConnection(conn);
            MySqlDataAdapter da = new MySqlDataAdapter();
            DataTable dt = new DataTable();
            try
            {

                con.Open();
                MySqlCommand cmd = new MySqlCommand();
                cmd.CommandText = strQuery;
                cmd.Connection = con;
                cmd.CommandTimeout = 10000;
                da.SelectCommand = cmd;
                da.Fill(dt);
                con.Close();
            }
            catch (Exception ex)
            {
                error = ex.Message;
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                if (error.Contains("Authentication to host 'localhost' for user 'root' using method 'mysql_native_password") ||
           error.Contains("A connection attempt failed because the connected party did not properly respond after a period of time") ||
           error.Contains("Unable to connect to any of the specified MySQL hosts") ||
           error.Contains("Fatal error encountered during command execution."))
                {
                    Thread.Sleep(2000);
                    goto runAgain;
                }
                System.Diagnostics.Debugger.Break();
            }
            return dt;
        }

        public int executeDMLSQL(string strQuery)
        {
        runAgain_dml:
            introws = 0;
            error = "";
            MySqlConnection con = new MySqlConnection(conn);
            try
            {
                con.Open();
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = con;
                cmd.CommandText = strQuery;
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 10000;
                introws = cmd.ExecuteNonQuery();
                con.Close();
            }
            catch (Exception ex)
            {
                error = ex.Message;
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                if (error.Contains("Authentication to host 'localhost' for user 'root' using method 'mysql_native_password") ||
          error.Contains("A connection attempt failed because the connected party did not properly respond after a period of time") ||
          error.Contains("Unable to connect to any of the specified MySQL hosts") ||
          error.Contains("Fatal error encountered during command execution."))
                {
                    Thread.Sleep(2000);
                    goto runAgain_dml;
                }
            }
            return introws;
        }

    }
}
