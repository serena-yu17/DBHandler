﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Configuration;

namespace Livingstone.Library
{
    public struct FilterViewModel
    {
        public string returnCol { get; set; }
        public List<string> value { get; set; }
        public string type { get; set; }
        public string dataType { get; set; }
        public int allowNull { get; set; }
        public int complement { get; set; }
    }

    public static class DBHandler
    {
        static Dictionary<string, string> connStrings = new Dictionary<string, string>();

        public const string refreshCRMSQL2Cmd = @"
            BEGIN TRAN;
            truncate table tempUser;
            insert into tempUser
            (usr, pass, name, FirstName, LastName, [description],
	            ChaPas, PasNevExp, UpdatePrice, CreateDate, CreatedBy,
	            AccDis, DisDate, UserDef1, UserDef2, UserDef3, UserDef4,
	            UserDef5, UserDef6, UserDef7, UserDef8, UserDef9,
	            UserDef10, screen, email, Company, Office, Warehouse,
		            AutoLogIn, AutoLogOut, HomeComputer, passwd, ADUsr, 
		            AgentID, ForceAgentLogin, QuoteAuthEmail, QuoteConvertEmail)
            select * from openquery(crmau, 
            'select usr, pass, name, FirstName, LastName, [description],
	            ChaPas, PasNevExp, UpdatePrice, CreateDate, CreatedBy,
	            AccDis, DisDate, UserDef1, UserDef2, UserDef3, UserDef4,
	            UserDef5, UserDef6, UserDef7, UserDef8, UserDef9,
	            UserDef10, screen, email, Company, Office, Warehouse,
		            AutoLogIn, AutoLogOut, HomeComputer, passwd, ADUsr, 
		            AgentID, ForceAgentLogin, QuoteAuthEmail, QuoteConvertEmail 
            from [user] with (nolock)');
            COMMIT;
            ";

        static double lastRefresh = (new DateTime(1970, 1, 1)).Ticks;     //any dummy early time

        const int crmsqlInterval = 5;

        public static string getConnStr(string server)
        {
            if (connStrings.ContainsKey(server))
                return connStrings[server];
            else
            {
                var connStr = WebConfigurationManager.AppSettings[server];
                if (string.IsNullOrWhiteSpace(connStr))
                    connStr = ConfigurationManager.AppSettings[server];
                if (string.IsNullOrEmpty(connStr))
                    throw new DataException("The connection string is missing under the server: " + server);
                return connStr;
            }
        }

        public static void getDataList(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = header.Count - 1;
                            }
                        while (rd.Read())
                        {
                            List<string> row = new List<string>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(string.Empty);
                                else
                                    row.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
            }
        }

        public static void getDataListFromConnStr(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
    string sql, string connStr, Dictionary<string, object> parameters = null,
    Dictionary<string, Dictionary<bool, string>> boolStr = null,
    string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = header.Count - 1;
                            }
                        while (rd.Read())
                        {
                            List<string> row = new List<string>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(string.Empty);
                                else
                                    row.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
            }
        }

        public static void getDataList(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        rd.Read();
                        for (int col = 0; col < rd.FieldCount; col++)
                        {
                            if (rd.IsDBNull(col))
                                data.Add(string.Empty);
                            else
                                data.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                        }
                    }
                }
                con.Close();
            }
        }

        public static void getDataListFromConnStr(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        rd.Read();
                        for (int col = 0; col < rd.FieldCount; col++)
                        {
                            if (rd.IsDBNull(col))
                                data.Add(string.Empty);
                            else
                                data.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                        }
                    }
                }
                con.Close();
            }
        }

        public static async Task getDataListAsync(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        while (await rd.ReadAsync().ConfigureAwait(false))
                        {
                            List<string> row = new List<string>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(string.Empty);
                                else
                                    row.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
            }
        }

        public static async Task getDataListAsyncFromConnStr(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        while (await rd.ReadAsync().ConfigureAwait(false))
                        {
                            List<string> row = new List<string>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(string.Empty);
                                else
                                    row.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
            }
        }

        public static async Task getDataListAsync(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        await rd.ReadAsync().ConfigureAwait(false);

                        for (int col = 0; col < rd.FieldCount; col++)
                        {
                            if (rd.IsDBNull(col))
                                data.Add(string.Empty);
                            else
                                data.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                        }
                    }
                }
                con.Close();
            }
        }

        public static async Task getDataListAsyncFromConnStr(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm")
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        await rd.ReadAsync().ConfigureAwait(false);

                        for (int col = 0; col < rd.FieldCount; col++)
                        {
                            if (rd.IsDBNull(col))
                                data.Add(string.Empty);
                            else
                                data.Add(toString(rd.GetValue(col), rd.GetFieldType(col).Name, rd.GetName(col), boolStr, dateFormat, timeFormat));
                        }
                    }
                }
                con.Close();
            }
        }

        public static void getRawData(List<string> header, List<object> data, Dictionary<string, int> entries, List<string> types,
            string sql, string server, Dictionary<string, object> parameters = null)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        if (types != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                                types.Add(rd.GetFieldType(col).Name);
                        rd.Read();
                        for (int col = 0; col < rd.FieldCount; col++)
                        {
                            if (rd.IsDBNull(col))
                                data.Add(null);
                            else
                                data.Add(rd.GetValue(col));
                        }
                    }
                }
                con.Close();
            }
        }

        public static async Task getRawDataAsync(List<string> header, List<object> data, Dictionary<string, int> entries, List<string> types,
            string sql, string server, Dictionary<string, object> parameters = null)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        if (types != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                                types.Add(rd.GetFieldType(col).Name);

                        await rd.ReadAsync().ConfigureAwait(false);

                        for (int col = 0; col < rd.FieldCount; col++)
                        {
                            if (rd.IsDBNull(col))
                                data.Add(null);
                            else
                                data.Add(rd.GetValue(col));
                        }
                    }
                }
                con.Close();
            }
        }

        public static void getRawData(List<string> header, List<List<object>> data, Dictionary<string, int> entries, List<string> types,
        string sql, string server, Dictionary<string, object> parameters = null)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = header.Count - 1;
                            }
                        if (types != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                                types.Add(rd.GetFieldType(col).Name);
                        while (rd.Read())
                        {
                            List<object> row = new List<object>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(null);
                                else
                                    row.Add(rd.GetValue(col));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
            }
        }

        public static async Task getRawDataAsync(List<string> header, List<List<object>> data, Dictionary<string, int> entries, List<string> types,
         string sql, string server, Dictionary<string, object> parameters = null)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        if (header != null || entries != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                string newHeader = rd.GetName(col);
                                if (header != null)
                                    header.Add(newHeader);
                                if (entries != null)
                                    entries[newHeader] = col;
                            }
                        if (types != null)
                            for (int col = 0; col < rd.FieldCount; col++)
                                types.Add(rd.GetFieldType(col).Name);
                        while (await rd.ReadAsync().ConfigureAwait(false))
                        {
                            List<object> row = new List<object>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(null);
                                else
                                    row.Add(rd.GetValue(col));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
            }
        }

        public static string toString(object obj, string type, string header = null, Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm")
        {
            if (obj == null)
                return string.Empty;
            string fullTimeFormat = dateFormat + timeFormat;
            switch (type)
            {
                case "Boolean":
                    if (boolStr == null || !boolStr.ContainsKey(header))
                        return (bool)obj ? "true" : "false";
                    else
                        return boolStr[header][(bool)obj];
                case "DateTime":
                    if (header != null && header.ToLower().Contains("time"))
                        return ((DateTime)obj).ToString(fullTimeFormat);
                    else
                        return ((DateTime)obj).ToString(dateFormat);
                case "TimeSpan":
                    return ((TimeSpan)obj).ToString(@"hh\:mm");
                default:
                    return obj.ToString();
            }
        }

        public static Int32 toInt32(object obj, string type)
        {
            if (obj == null)
                return 0;
            if (type == "Decimal")
                return (Int32)Convert.ToDecimal(obj);
            else
                return Convert.ToInt32(obj);
        }

        public static bool toBool(object obj, string type)
        {
            if (obj == null)
                return false;
            if (type == "String")
                return (obj.ToString().ToLower().Trim() == "true" || obj.ToString().Trim() == "1");
            if (type == "Int32" || type == "UInt32" || type == "Int16" || type == "UInt16" || type == "Int64" || type == "UInt64")
                return (Int64)obj != 0;
            if (type == "Double" || type == "Single")
                return (Double)obj != 0;
            if (type == "Decimal")
                return (Decimal)obj != 0;
            return (bool)obj;
        }

        public static string getString(string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm")
        {
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        rd.Read();
                        string header = rd.GetName(0);
                        if (rd.IsDBNull(0))
                            return string.Empty;
                        else
                            switch (rd.GetFieldType(0).Name)
                            {
                                case "Boolean":
                                    if (boolStr == null || !boolStr.ContainsKey(header))
                                        return rd.GetBoolean(0) ? "true" : "false";
                                    else
                                        return boolStr[header][rd.GetBoolean(0)];
                                case "DateTime":
                                    if (header.ToLower().Contains("time"))
                                        return rd.GetDateTime(0).ToString(fullTimeFormat);
                                    else
                                        return rd.GetDateTime(0).ToString(dateFormat);
                                case "TimeSpan":
                                    return rd.GetTimeSpan(0).ToString(@"hh\:mm");
                                default:
                                    return rd.GetString(0);
                            }
                    }
                }
                con.Close();
            }
            return string.Empty;
        }

        public static async Task<string> getStringAsync(string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm")
        {
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        await rd.ReadAsync().ConfigureAwait(false);
                        string header = rd.GetName(0);
                        if (rd.IsDBNull(0))
                            return string.Empty;
                        else
                            switch (rd.GetFieldType(0).Name)
                            {
                                case "Boolean":
                                    if (boolStr == null || !boolStr.ContainsKey(header))
                                        return rd.GetBoolean(0) ? "true" : "false";
                                    else
                                        return boolStr[header][rd.GetBoolean(0)];
                                case "DateTime":
                                    if (header.ToLower().Contains("time"))
                                        return rd.GetDateTime(0).ToString(fullTimeFormat);
                                    else
                                        return rd.GetDateTime(0).ToString(dateFormat);
                                case "TimeSpan":
                                    return rd.GetTimeSpan(0).ToString(@"hh\:mm");
                                default:
                                    return rd.GetString(0);
                            }
                    }
                }
                con.Close();
            }
            return string.Empty;
        }

        public static Int32 getInt32(string sql, string server, Dictionary<string, object> parameters = null, Int32 defaultInt = 0)
        {
            Int32 res = defaultInt;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        rd.Read();
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = Convert.ToInt32(rd.GetValue(0));
                    }
                }
                con.Close();
            }
            return res;
        }

        public static async Task<Int32> getInt32Async(string sql, string server, Dictionary<string, object> parameters = null, Int32 defaultInt = 0)
        {
            Int32 res = defaultInt;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        await rd.ReadAsync().ConfigureAwait(false);
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = Convert.ToInt32(rd.GetValue(0));
                    }
                }
                con.Close();
            }
            return res;
        }

        public static double getDouble(string sql, string server, Dictionary<string, object> parameters = null, double defaultDouble = 0)
        {
            double res = defaultDouble;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        rd.Read();
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = Convert.ToDouble(rd.GetValue(0));
                    }
                }
                con.Close();
            }
            return res;
        }

        public static async Task<double> getDoubleAsync(string sql, string server, Dictionary<string, object> parameters = null, double defaultDouble = 0)
        {
            double res = defaultDouble;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        await rd.ReadAsync().ConfigureAwait(false);
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = Convert.ToDouble(rd.GetValue(0));
                    }
                }
                con.Close();
            }
            return res;
        }

        public static bool getBool(string sql, string server, Dictionary<string, object> parameters = null, bool defaultBool = false)
        {
            bool res = defaultBool;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        rd.Read();
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = Convert.ToBoolean(rd.GetValue(0));
                    }
                }
                con.Close();
            }
            return res;
        }

        public static async Task<bool> getBoolAsync(string sql, string server, Dictionary<string, object> parameters = null, bool defaultBool = false)
        {
            bool res = defaultBool;
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 6000000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        await rd.ReadAsync().ConfigureAwait(false);
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = Convert.ToBoolean(rd.GetValue(0));
                    }
                }
                con.Close();
            }
            return res;
        }

        public static void ExecuteNonQuery(string sql, string server, Dictionary<string, object> parameters = null)
        {
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
                ExecuteNonQuery(sql, con, parameters);
        }

        public static void ExecuteNonQueryFromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null)
        {
            using (SqlConnection con = new SqlConnection(connStr))
                ExecuteNonQuery(sql, con, parameters);
        }

        public static async Task ExecuteNonQueryAsync(string sql, string server, Dictionary<string, object> parameters = null)
        {
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
                await ExecuteNonQueryAsync(sql, con, parameters).ConfigureAwait(false);
        }

        public static async Task ExecuteNonQueryFromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null)
        {
            using (SqlConnection con = new SqlConnection(connStr))
                await ExecuteNonQueryAsync(sql, con, parameters).ConfigureAwait(false);
        }

        public static void ExecuteNonQuery(string sql, SqlConnection con, Dictionary<string, object> parameters = null)
        {
            con.Open();
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 600000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                com.ExecuteNonQuery();
            }
            con.Close();
        }

        public static async Task ExecuteNonQueryAsync(string sql, SqlConnection con, Dictionary<string, object> parameters = null)
        {
            await con.OpenAsync().ConfigureAwait(false);
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandTimeout = 600000;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await com.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            con.Close();
        }

        public static void bulkInsert(DataTable table, string server)
        {
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlBulkCopy bc = new SqlBulkCopy(con))
            {
                con.Open();
                bc.DestinationTableName = table.TableName;
                foreach (var col in table.Columns)
                    bc.ColumnMappings.Add(col.ToString(), col.ToString());
                bc.WriteToServer(table);
                con.Close();
            }
        }

        public static void bulkInsertFromConnStr(DataTable table, string connStr)
        {
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlBulkCopy bc = new SqlBulkCopy(con))
            {
                con.Open();
                bc.DestinationTableName = table.TableName;
                foreach (var col in table.Columns)
                    bc.ColumnMappings.Add(col.ToString(), col.ToString());
                bc.WriteToServer(table);
                con.Close();
            }
        }

        public static async Task bulkInsertAsync(DataTable table, string server)
        {
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
            using (SqlBulkCopy bc = new SqlBulkCopy(con))
            {
                con.Open();
                bc.DestinationTableName = table.TableName;
                foreach (var col in table.Columns)
                    bc.ColumnMappings.Add(col.ToString(), col.ToString());
                await bc.WriteToServerAsync(table);
                con.Close();
            }
        }

        public static async Task bulkInsertFromConnStrAsync(DataTable table, string connStr)
        {
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlBulkCopy bc = new SqlBulkCopy(con))
            {
                con.Open();
                bc.DestinationTableName = table.TableName;
                foreach (var col in table.Columns)
                    bc.ColumnMappings.Add(col.ToString(), col.ToString());
                await bc.WriteToServerAsync(table);
                con.Close();
            }
        }


        public static int countID(string id)
        {
            const string strCmd = "select count(*) from [user] where usr = @id";
            Dictionary<string, object> param = new Dictionary<string, object>()
            {
                {"@id", id}
            };
            return getInt32(strCmd, "NETCRMAU", param);
        }


        public static void hideColumn(List<string> header, List<string> data, Dictionary<string, int> entries, HashSet<string> colName)
        {
            List<string> tempHeader = new List<string>(header);
            List<string> tempData = new List<string>(data);
            entries.Clear();
            header.Clear();
            data.Clear();
            for (int i = 0; i < tempHeader.Count; i++)
                if (!colName.Contains(tempHeader[i]))
                {
                    header.Add(tempHeader[i]);
                    data.Add(tempData[i]);
                    entries[tempHeader[i]] = header.Count - 1;
                }
        }

        public static void hideColumn(List<string> header, List<List<string>> data, Dictionary<string, int> entries, HashSet<string> colName)
        {
            List<string> tempHeader = new List<string>(header);
            List<List<string>> tempData = new List<List<string>>(data);
            header.Clear();
            data.Clear();
            entries.Clear();
            for (int i = 0; i < tempHeader.Count; i++)
                if (!colName.Contains(tempHeader[i]))
                {
                    header.Add(tempHeader[i]);
                    entries[tempHeader[i]] = header.Count - 1;
                }
            for (int i = 0; i < tempData.Count; i++)
            {
                List<string> newRow = new List<string>();
                for (int j = 0; j < tempData[i].Count; j++)
                    if (!colName.Contains(tempHeader[j]))
                        newRow.Add(tempData[i][j]);
                data.Add(newRow);
            }
        }


        public static void addTimeSpan(List<string> header, List<string> data, Dictionary<string, int> entries,
            string dateFormat = "dd/MM/yyyy", string delimiter = "<br />")
        {
            if (!entries.ContainsKey("isFullDay"))
                return;
            int colFullDay = entries["isFullDay"];

            bool isFullDay = true;
            string str = data[colFullDay];
            if (str == "false")
                isFullDay = false;
            DateTime startTime = DateTime.ParseExact(data[entries["Start Time"]], dateFormat + " HH:mm", null);
            DateTime endTime = DateTime.ParseExact(data[entries["End Time"]], dateFormat + " HH:mm", null);
            if (isFullDay)
            {
                data[entries["Start Time"]] = startTime.Date.ToString(dateFormat);
                data[entries["End Time"]] = endTime.Date.ToString(dateFormat);
            }
            string timespan, hList;
            Holiday.holidayCount(out timespan, out hList, startTime, endTime, isFullDay, delimiter);
            if (!string.IsNullOrWhiteSpace(timespan))
            {
                data.Insert(entries["End Time"] + 1, timespan);
                header.Insert(entries["End Time"] + 1, "Working days");
                if (!string.IsNullOrWhiteSpace(hList))
                {
                    data.Insert(entries["End Time"] + 2, hList);
                    header.Insert(entries["End Time"] + 2, "Pub Holidays");
                }
            }
        }

        //reader: sql data reader. header: column headers. data: data rows. header and data will be appended with new data
        //entries: dictionary for quick finding header index
        //limit: string length limit for columns. startTime, endTime: helper variables
        public static void addTimeSpan(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string dateFormat = "dd/MM/yyyy", string delimiter = "<br />")
        {
            if (!entries.ContainsKey("isFullDay"))
                return;
            int colFullDay = entries["isFullDay"];

            for (int i = 0; i < data.Count; i++)
            {
                bool isFullDay = true;
                string str = data[i][colFullDay];
                DateTime startTime = DateTime.ParseExact(data[i][entries["Start Time"]], dateFormat + " HH:mm", null);
                DateTime endTime = DateTime.ParseExact(data[i][entries["End Time"]], dateFormat + " HH:mm", null);
                if (str == "Partial")
                    isFullDay = false;
                if (isFullDay)
                {
                    data[i][entries["Start Time"]] = startTime.Date.ToString(dateFormat);
                    data[i][entries["End Time"]] = endTime.Date.ToString(dateFormat);
                }
                string timespan, hList;
                Holiday.holidayCount(out timespan, out hList, startTime, endTime, isFullDay, delimiter);
                data[i].Insert(entries["End Time"] + 1, timespan);
                data[i].Insert(entries["End Time"] + 2, hList);
            }

            header.Insert(entries["End Time"] + 1, "Working days");
            header.Insert(entries["End Time"] + 2, "Pub Holidays");

            entries.Clear();
            for (int i = 0; i < header.Count; i++)
                entries[header[i]] = i;
        }

        public static void trimLength(List<string> header, List<List<string>> data, int limit, HashSet<string> except = null)
        {
            for (int i = 0; i < data.Count; i++)
                for (int j = 0; j < data[i].Count; j++)
                    if (data[i][j].Length > limit
                        && (except == null || !except.Contains(header[j]))
                        && !(data[i][j].Substring(0, 8) == "<a href="))
                        data[i][j] = data[i][j].Substring(0, limit) + "...";
        }

        public static void trimLength(List<string> header, List<string> data, int limit, HashSet<string> except = null)
        {
            for (int i = 0; i < data.Count; i++)
            {
                if (data[i].Length > limit
                    && (except == null || !except.Contains(header[i]))
                    && !(data[i].Substring(0, 8) == "<a href="))
                    data[i] = data[i].Substring(0, limit) + "...";
            }
        }

        //Apply the filters in List<FilterViewModel> and compile into SQL
        public static string applyFilter(string initialCmd, Dictionary<string, object> param, List<FilterViewModel> filter)
        {
            if (!initialCmd.Contains("where"))
                initialCmd += " where 1 = 1 ";
            int paramNo = 0;
            HashSet<string> allowedTypes = new HashSet<string>()
            {
                "option", "number", "date", "text", "checkbox", "select"
            };
            List<string> lstCmd = new List<string>() { initialCmd };
            for (int j = 0; j < filter.Count; j++)
            {
                bool conditionAdded = false;
                var item = filter[j];
                if (!allowedTypes.Contains(item.type))
                    continue;
                if (item.complement == 1)
                    lstCmd.Add("\n and not ((");
                else
                    lstCmd.Add("\n and ((");
                int quotePosition = lstCmd.Count - 1;
                switch (item.type)
                {
                    case "option":
                        lstCmd.Add(item.returnCol);
                        lstCmd.Add(" in (");
                        List<string> lstParam = new List<string>();
                        for (int i = 0; i < item.value.Count; i++)
                        {
                            string guid = "@param" + paramNo.ToString();
                            paramNo++;
                            lstParam.Add(guid);
                            param[guid] = item.value[i];
                            if (string.IsNullOrWhiteSpace(item.value[i]))
                                item.allowNull = 1;
                        }
                        lstCmd.Add(string.Join(", ", lstParam));
                        lstCmd.Add(")");
                        conditionAdded = true;
                        break;
                    case "number":
                    case "date":
                    case "text":
                        var value = new List<string>(item.value);
                        string guid1 = "@param" + paramNo.ToString();
                        paramNo++;
                        lstCmd.Add(item.returnCol);
                        lstCmd.Add(" >= ");
                        lstCmd.Add(guid1);
                        param[guid1] = item.value[0];
                        string guid2 = "@param" + paramNo.ToString();
                        paramNo++;
                        lstCmd.Add("\n and ");
                        lstCmd.Add(item.returnCol);
                        lstCmd.Add(" <= ");
                        lstCmd.Add(guid2);
                        param[guid2] = item.value[1];
                        if (string.IsNullOrEmpty(item.value[0]) || string.IsNullOrEmpty(item.value[1]))
                            item.allowNull = 1;
                        conditionAdded = true;
                        break;
                    case "checkbox":
                        string guid3 = "@param" + paramNo.ToString();
                        paramNo++;
                        lstCmd.Add(item.returnCol);
                        if (item.complement == 1)
                            lstCmd.Add(" != ");
                        else
                            lstCmd.Add(" = ");
                        lstCmd.Add(guid3);
                        param[guid3] = item.value[0];
                        if (string.IsNullOrEmpty(item.value[0]))
                            item.allowNull = 1;
                        conditionAdded = true;
                        break;
                    case "select":
                        short selectedVal;
                        if (string.IsNullOrEmpty(item.value[0]))
                            item.allowNull = 1;
                        else if (Int16.TryParse(item.value[0], out selectedVal) && selectedVal != 32767)
                            switch (item.dataType)
                            {
                                case "string":
                                    if (selectedVal == 1)
                                    {
                                        lstCmd.Add(item.returnCol);
                                        lstCmd.Add(" is not NULL and ");
                                        lstCmd.Add(item.returnCol);
                                        lstCmd.Add(" != '' ");
                                    }
                                    else
                                    {
                                        lstCmd.Add(item.returnCol);
                                        lstCmd.Add(" is NULL or ");
                                        lstCmd.Add(item.returnCol);
                                        lstCmd.Add(" = '' ");
                                    }
                                    conditionAdded = true;
                                    break;
                                case "bit":
                                    string guid4 = "@param" + paramNo.ToString();
                                    paramNo++;
                                    lstCmd.Add(item.returnCol);
                                    lstCmd.Add(" = ");
                                    lstCmd.Add(guid4);
                                    param[guid4] = selectedVal;
                                    conditionAdded = true;
                                    break;
                            }
                        break;
                }
                if (!conditionAdded)
                    lstCmd.RemoveAt(quotePosition);
                else
                {
                    lstCmd.Add(")");
                    if (item.allowNull == 1)
                    {
                        lstCmd.Add(" or ");
                        lstCmd.Add(item.returnCol);
                        lstCmd.Add(" is NULL ");
                    }
                    lstCmd.Add(")");
                }
            }
            return string.Join(" ", lstCmd);
        }


        public static void refreshCRMSQL2(ConcurrentDictionary<string, Func<object>> memKeys, bool noForce = true)
        {
            //5 hour timespan to auto refreshes
            if (noForce && DateTime.Now.Ticks - lastRefresh < crmsqlInterval * TimeSpan.TicksPerHour)
                return;
            ExecuteNonQuery(refreshCRMSQL2Cmd, "CRMSQL2");
            Interlocked.Exchange(ref lastRefresh, DateTime.Now.Ticks);
            if (memKeys != null && memKeys.Count != 0)
                CacheHandler.resetMemCache(memKeys);
        }
    }
}
