﻿using System;
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
        static Dictionary<string, int> connCount = new Dictionary<string, int>();
        static object counterLock = new object();

        private static void waitForConn(string connStr, int connectionLimit)
        {
            while (true)
            {
                //avoid connection pool overflow
                if (!connCount.ContainsKey(connStr) || connCount[connStr] < connectionLimit)
                    lock (counterLock)
                        if (!connCount.ContainsKey(connStr))
                        {
                            connCount[connStr] = 1;
                            break;
                        }
                        else if (connCount[connStr] < connectionLimit)
                        {
                            connCount[connStr]++;
                            break;
                        }
                Thread.Sleep(10);
            }
        }

        private static void finalizeConn(string connStr)
        {
            if (connCount.ContainsKey(connStr))
                lock (counterLock)
                    if (connCount.ContainsKey(connStr))
                        connCount[connStr]--;
        }

        private static async Task waitForConnAsync(string connStr, int connectionLimit)
        {
            while (true)
            {
                if (!connCount.ContainsKey(connStr) || connCount[connStr] < connectionLimit)
                    lock (counterLock)
                        if (!connCount.ContainsKey(connStr))
                        {
                            connCount[connStr] = 1;
                            break;
                        }
                        else if (connCount[connStr] < connectionLimit)
                        {
                            connCount[connStr]++;
                            break;
                        }
                await Task.Delay(10).ConfigureAwait(false);
            }
        }

        public static string getConnStr(string server)
        {
            string connStr = null;
            if (WebConfigurationManager.AppSettings != null && WebConfigurationManager.AppSettings[server] != null)
                connStr = WebConfigurationManager.AppSettings[server];
            if (string.IsNullOrEmpty(connStr) && ConfigurationManager.AppSettings != null && ConfigurationManager.AppSettings[server] != null)
                connStr = ConfigurationManager.AppSettings[server];
            if (string.IsNullOrEmpty(connStr) && WebConfigurationManager.ConnectionStrings != null && WebConfigurationManager.ConnectionStrings[server] != null)
                connStr = WebConfigurationManager.ConnectionStrings[server].ConnectionString;
            if (string.IsNullOrEmpty(connStr))
                throw new DataException("The connection string is missing under the server: " + server);
            return connStr;
        }

        public static void getDataList(DataTable table, string sql, string server,
            Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                getDataListFromConnStr(table, sql, connStr, parameters, commandType, connectionLimit, timeout, ct);
        }

        public static void getDataListFromConnStr(DataTable table, string sql, string connStr,
            Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            if (table == null)
                return;
            table.Clear();
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (CancellationTokenRegistration cr = ct.Register(() => com.Cancel()))
                using (SqlDataReader rd = com.ExecuteReader())
                    table.Load(rd);
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static async Task getDataListAsync(DataTable table, string sql, string server,
            Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                await getDataListFromConnStrAsync(table, sql, connStr, parameters,
                    commandType, connectionLimit, timeout, ct).ConfigureAwait(false);
        }

        public static async Task getDataListFromConnStrAsync(DataTable table, string sql, string connStr,
            Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            if (table == null)
                return;
            table.Clear();
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync(ct).ConfigureAwait(false))
                    table.Load(rd);
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static void getDataList(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                getDataListFromConnStr(header, data, entries, sql, connStr, parameters,
                    boolStr, dateFormat, timeFormat, commandType, connectionLimit, timeout, ct);
        }

        public static void getDataListFromConnStr(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
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
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (CancellationTokenRegistration cr = ct.Register(() => com.Cancel()))
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
                        while (rd.Read())
                        {
                            List<string> row = new List<string>();
                            for (int col = 0; col < rd.FieldCount; col++)
                            {
                                if (rd.IsDBNull(col))
                                    row.Add(string.Empty);
                                else
                                    row.Add(toString(rd.GetValue(col), rd.GetName(col), boolStr, dateFormat, timeFormat));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static void getDataList(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                getDataListFromConnStr(header, data, entries, sql, connStr, parameters,
                    boolStr, dateFormat, timeFormat, commandType, connectionLimit, timeout, ct);
        }

        public static void getDataListFromConnStr(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
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
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (CancellationTokenRegistration cr = ct.Register(() => com.Cancel()))
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
                                data.Add(toString(rd.GetValue(col), rd.GetName(col), boolStr, dateFormat, timeFormat));
                        }
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static async Task getDataListAsync(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                await getDataListFromConnStrAsync(header, data, entries, sql, connStr, parameters, boolStr, dateFormat, timeFormat,
                    commandType, connectionLimit, timeout, ct)
                    .ConfigureAwait(false);
        }

        public static async Task getDataListFromConnStrAsync(List<string> header, List<List<string>> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
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
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync(ct).ConfigureAwait(false))
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
                                    row.Add(toString(rd.GetValue(col), rd.GetName(col), boolStr, dateFormat, timeFormat));
                            }
                            data.Add(row);
                        }
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static async Task getDataListAsync(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                await getDataListFromConnStrAsync(header, data, entries, sql, connStr, parameters, boolStr, dateFormat, timeFormat,
                    commandType, connectionLimit, timeout, ct).ConfigureAwait(false);
        }

        public static async Task getDataListFromConnStrAsync(List<string> header, List<string> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null,
            string dateFormat = "dd/MM/yyyy", string timeFormat = " HH:mm",
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
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
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync(ct).ConfigureAwait(false))
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
                                data.Add(toString(rd.GetValue(col), rd.GetName(col), boolStr, dateFormat, timeFormat));
                        }
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static void getDataList(List<string> header, List<object> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                getDataListFromConnStr(header, data, entries, sql, connStr, parameters, commandType, connectionLimit, timeout, ct);
        }

        public static void getDataListFromConnStr(List<string> header, List<object> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (CancellationTokenRegistration cr = ct.Register(() => com.Cancel()))
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
                                data.Add(null);
                            else
                                data.Add(rd.GetValue(col));
                        }
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static async Task getDataListAsync(List<string> header, List<object> data, Dictionary<string, int> entries,
            string sql, string server, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                await getDataListFromConnStrAsync(header, data, entries, sql, connStr, parameters, 
                    commandType, connectionLimit, timeout, ct).ConfigureAwait(false);
        }

        public static async Task getDataListFromConnStrAsync(List<string> header, List<object> data, Dictionary<string, int> entries,
            string sql, string connStr, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            if (entries != null)
                entries.Clear();
            data.Clear();
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync(ct).ConfigureAwait(false))
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
                                data.Add(null);
                            else
                                data.Add(rd.GetValue(col));
                        }
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static void getDataList(List<string> header, List<List<object>> data, Dictionary<string, int> entries,
        string sql, string server, Dictionary<string, object> parameters = null,
        CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                getDataListFromConnStr(header, data, entries, sql, connStr, parameters, commandType, connectionLimit, timeout, ct);
        }

        public static void getDataListFromConnStr(List<string> header, List<List<object>> data, Dictionary<string, int> entries,
        string sql, string connStr, Dictionary<string, object> parameters = null,
        CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (CancellationTokenRegistration cr = ct.Register(() => com.Cancel()))
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
                finalizeConn(connStr);
            }
        }

        public static async Task getDataListAsync(List<string> header, List<List<object>> data, Dictionary<string, int> entries,
         string sql, string server, Dictionary<string, object> parameters = null,
         CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                await getDataListFromConnStrAsync(header, data, entries, sql, connStr, 
                    parameters, commandType, connectionLimit, timeout, ct).ConfigureAwait(false);
        }

        public static async Task getDataListFromConnStrAsync(List<string> header, List<List<object>> data, Dictionary<string, int> entries,
         string sql, string connStr, Dictionary<string, object> parameters = null,
         CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0, CancellationToken ct = default)
        {
            if (data == null)
                return;
            if (header != null)
                header.Clear();
            data.Clear();
            if (entries != null)
                entries.Clear();
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync(ct).ConfigureAwait(false))
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
                finalizeConn(connStr);
            }
        }

        public static UInt64 toUInt64(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(UInt64);
            switch (obj.GetType().Name)
            {
                case "String":
                    if (UInt64.TryParse((string)obj, out var val))
                        return val;
                    else return default(UInt64);
                case "Single":
                    return (UInt64)(Single)obj;
                case "Double":
                    return (UInt64)(double)obj;
                case "Int16":
                    return (UInt64)(Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int32":
                    return (UInt64)(Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int64":
                    return (UInt64)(Int64)obj;
                case "UInt64":
                    return (UInt64)obj;
                case "Decimal":
                    return (UInt64)(decimal)obj;
                default:
                    return (UInt64)obj;
            }
        }

        public static Int64 toInt64(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(Int64);
            switch (obj.GetType().Name)
            {
                case "String":
                    if (Int64.TryParse((string)obj, out var val))
                        return val;
                    else return default(Int64);
                case "Single":
                    return (Int64)(Single)obj;
                case "Double":
                    return (Int64)(double)obj;
                case "Int16":
                    return (Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int32":
                    return (Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int64":
                    return (Int64)obj;
                case "UInt64":
                    return (Int64)(UInt64)obj;
                case "Decimal":
                    return (Int64)(decimal)obj;
                default:
                    return (Int64)obj;
            }
        }

        public static float toFloat(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(float);
            switch (obj.GetType().Name)
            {
                case "String":
                    if (float.TryParse((string)obj, out var fVal))
                        return fVal;
                    else return default(float);
                case "Single":
                    return (Single)obj;
                case "Double":
                    return (float)(double)obj;
                case "Int16":
                    return (Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int32":
                    return (Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int64":
                    return (Int64)obj;
                case "UInt64":
                    return (UInt64)obj;
                case "Decimal":
                    return (float)(decimal)obj;
                default:
                    return (float)obj;
            }
        }

        public static char toChar(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(char);
            switch (obj.GetType().Name)
            {
                case "String":
                    var str = (string)obj;
                    if (str.Length != 0)
                        return str[0];
                    else
                        return default(char);
                default:
                    return (char)obj;
            }
        }

        public static DateTime? toDateTime(object obj, string format = "yyyy-MM-dd")
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    if (DateTime.TryParseExact((string)obj, format, System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.None, out var res))
                        return res;
                    else return null;
                case "Int64":
                    return new DateTime((Int64)obj);
                case "UInt64":
                    return new DateTime((long)(UInt64)obj);
                default:
                    return (DateTime?)obj;
            }
        }

        public static string toString(object obj, string header = null, Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm")
        {
            if (obj == null || obj == DBNull.Value)
                return string.Empty;
            string fullTimeFormat = dateFormat + timeFormat;
            switch (obj.GetType().Name)
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

        public static Int32 toInt32(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(Int32);
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (Int32)Convert.ToDecimal(obj);
                case "String":
                    if (Int32.TryParse(obj as string, out var res))
                        return res;
                    else
                        return default(Int32);
                case "Boolean":
                    return (bool)obj ? 1 : 0;
                default:
                    return Convert.ToInt32(obj);
            }
        }

        public static UInt32 toUInt32(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(UInt32);
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (UInt32)Convert.ToDecimal(obj);
                case "String":
                    if (UInt32.TryParse(obj as string, out var res))
                        return res;
                    else
                        return default(UInt32);
                case "Boolean":
                    return (bool)obj ? 1u : 0u;
                default:
                    return Convert.ToUInt32(obj);
            }
        }

        public static Int16 toInt16(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(Int16);
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (Int16)Convert.ToDecimal(obj);
                case "String":
                    if (Int16.TryParse(obj as string, out var res))
                        return res;
                    else
                        return default(Int16);
                case "Boolean":
                    return (bool)obj ? (Int16)1 : (Int16)0;
                default:
                    return Convert.ToInt16(obj);
            }
        }

        public static UInt16 toUInt16(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(UInt16);
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (UInt16)Convert.ToDecimal(obj);
                case "String":
                    if (UInt16.TryParse(obj as string, out var res))
                        return res;
                    else
                        return default(UInt16);
                case "Boolean":
                    return (bool)obj ? (UInt16)1 : (UInt16)0;
                default:
                    return Convert.ToUInt16(obj);
            }
        }

        public static decimal toDecimal(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return default(decimal);
            switch (obj.GetType().Name)
            {
                case "String":
                    if (decimal.TryParse(obj as string, out var res))
                        return res;
                    else
                        return default(decimal);
                case "Int32":
                    return (Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int16":
                    return (Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int64":
                    return (Int64)obj;
                case "UInt64":
                    return (UInt64)obj;
                case "Double":
                    return (decimal)(double)obj;
                case "Single":
                    return (decimal)(Single)obj;
                case "Boolean":
                    return (bool)obj ? 1 : 0;
                default:
                    return (decimal)obj;
            }
        }

        public static bool toBool(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return false;
            switch (obj.GetType().Name)
            {
                case "String":
                    return (obj.ToString().ToLower().Trim() == "true" || obj.ToString().Trim() == "1");
                case "Int32":
                    return (Int32)obj != 0;
                case "UInt32":
                    return (UInt32)obj != 0;
                case "Int16":
                    return (Int16)obj != 0;
                case "UInt16":
                    return (UInt16)obj != 0;
                case "Int64":
                    return (Int64)obj != 0;
                case "UInt64":
                    return (UInt64)obj != 0;
                case "Double":
                    return (Double)obj != 0;
                case "Single":
                    return (Single)obj != 0;
                case "Decimal":
                    return (Decimal)obj != 0;
                default:
                    return (bool)obj;
            }
        }

        public static UInt64? toUInt64Null(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    if (UInt64.TryParse((string)obj, out var val))
                        return val;
                    else return null;
                case "Single":
                    return (UInt64)(Single)obj;
                case "Double":
                    return (UInt64)(double)obj;
                case "Int16":
                    return (UInt64)(Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int32":
                    return (UInt64)(Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int64":
                    return (UInt64)(Int64)obj;
                case "UInt64":
                    return (UInt64)obj;
                case "Decimal":
                    return (UInt64)(decimal)obj;
                default:
                    return (UInt64)obj;
            }
        }

        public static Int64? toInt64Null(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    if (Int64.TryParse((string)obj, out var val))
                        return val;
                    else return null;
                case "Single":
                    return (Int64)(Single)obj;
                case "Double":
                    return (Int64)(double)obj;
                case "Int16":
                    return (Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int32":
                    return (Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int64":
                    return (Int64)obj;
                case "UInt64":
                    return (Int64)(UInt64)obj;
                case "Decimal":
                    return (Int64)(decimal)obj;
                default:
                    return (Int64)obj;
            }
        }

        public static float? toFloatNull(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    if (float.TryParse((string)obj, out var fVal))
                        return fVal;
                    else return null;
                case "Single":
                    return (Single)obj;
                case "Double":
                    return (float)(double)obj;
                case "Int16":
                    return (Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int32":
                    return (Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int64":
                    return (Int64)obj;
                case "UInt64":
                    return (UInt64)obj;
                case "Decimal":
                    return (float)(decimal)obj;
                default:
                    return (float)obj;
            }
        }

        public static char? toCharNull(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    var str = (string)obj;
                    if (str.Length != 0)
                        return str[0];
                    else
                        return default(char);
                default:
                    return null;
            }
        }

        public static Int32? toInt32Null(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (Int32)Convert.ToDecimal(obj);
                case "String":
                    if (Int32.TryParse(obj as string, out var res))
                        return res;
                    else
                        return null;
                case "Boolean":
                    return (bool)obj ? 1 : 0;
                default:
                    return Convert.ToInt32(obj);
            }
        }

        public static UInt32? toUInt32Null(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (UInt32)Convert.ToDecimal(obj);
                case "String":
                    if (UInt32.TryParse(obj as string, out var res))
                        return res;
                    else
                        return null;
                case "Boolean":
                    return (bool)obj ? 1u : 0u;
                default:
                    return Convert.ToUInt32(obj);
            }
        }

        public static Int16? toInt16Null(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (Int16)Convert.ToDecimal(obj);
                case "String":
                    if (Int16.TryParse(obj as string, out var res))
                        return res;
                    else
                        return null;
                case "Boolean":
                    return (bool)obj ? (Int16)1 : (Int16)0;
                default:
                    return Convert.ToInt16(obj);
            }
        }

        public static UInt16? toUInt16Null(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "Decimal":
                    return (UInt16)Convert.ToDecimal(obj);
                case "String":
                    if (UInt16.TryParse(obj as string, out var res))
                        return res;
                    else
                        return null;
                case "Boolean":
                    return (bool)obj ? (UInt16)1 : (UInt16)0;
                default:
                    return Convert.ToUInt16(obj);
            }
        }

        public static decimal? toDecimalNull(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    if (decimal.TryParse(obj as string, out var res))
                        return res;
                    else
                        return null;
                case "Int32":
                    return (Int32)obj;
                case "UInt32":
                    return (UInt32)obj;
                case "Int16":
                    return (Int16)obj;
                case "UInt16":
                    return (UInt16)obj;
                case "Int64":
                    return (Int64)obj;
                case "UInt64":
                    return (UInt64)obj;
                case "Double":
                    return (decimal)(double)obj;
                case "Single":
                    return (decimal)(Single)obj;
                case "Boolean":
                    return (bool)obj ? 1 : 0;
                default:
                    return (decimal)obj;
            }
        }

        public static bool? toBoolNull(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return null;
            switch (obj.GetType().Name)
            {
                case "String":
                    return (obj.ToString().ToLower().Trim() == "true" || obj.ToString().Trim() == "1");
                case "Int32":
                    return (Int32)obj != 0;
                case "UInt32":
                    return (UInt32)obj != 0;
                case "Int16":
                    return (Int16)obj != 0;
                case "UInt16":
                    return (UInt16)obj != 0;
                case "Int64":
                    return (Int64)obj != 0;
                case "UInt64":
                    return (UInt64)obj != 0;
                case "Double":
                    return (Double)obj != 0;
                case "Single":
                    return (Single)obj != 0;
                case "Decimal":
                    return (Decimal)obj != 0;
                default:
                    return (bool)obj;
            }
        }

        public static string getString(string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm", CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return getStringFromConnStr(sql, connStr, parameters, boolStr, dateFormat, timeFormat, commandType, connectionLimit, timeout);
            return string.Empty;
        }

        public static string getStringFromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm", CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
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
                finalizeConn(connStr);
            }
            return string.Empty;
        }

        public static async Task<string> getStringAsync(string sql, string server, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm", CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return await getStringFromConnStrAsync(sql, connStr, parameters, boolStr, dateFormat, timeFormat, commandType, connectionLimit, timeout)
                    .ConfigureAwait(false);
            return string.Empty;
        }

        public static async Task<string> getStringFromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null,
            Dictionary<string, Dictionary<bool, string>> boolStr = null, string dateFormat = "dd/MM/yyyy",
            string timeFormat = " HH:mm", CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string fullTimeFormat = dateFormat + timeFormat;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
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
                finalizeConn(connStr);
            }
            return string.Empty;
        }

        public static Int32 getInt32(string sql, string server, Dictionary<string, object> parameters = null,
            Int32 defaultInt = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return getInt32FromConnStr(sql, connStr, parameters, defaultInt, commandType, connectionLimit, timeout);
            return defaultInt;
        }

        public static Int32 getInt32FromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null,
            Int32 defaultInt = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            Int32 res = defaultInt;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        rd.Read();
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = toInt32(rd.GetValue(0));
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
            return res;
        }

        public static async Task<Int32> getInt32Async(string sql, string server, Dictionary<string, object> parameters = null,
            Int32 defaultInt = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return await getInt32FromConnStrAsync(sql, connStr, parameters, defaultInt, commandType, connectionLimit, timeout).ConfigureAwait(false);
            return defaultInt;
        }

        public static async Task<Int32> getInt32FromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null,
            Int32 defaultInt = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            Int32 res = defaultInt;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        await rd.ReadAsync().ConfigureAwait(false);
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = toInt32(rd.GetValue(0));
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
            return res;
        }

        public static decimal? getDecimal(string sql, string server, Dictionary<string, object> parameters = null,
            decimal? defaultDecimal = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return getDecimalFromConnStr(sql, connStr, parameters, defaultDecimal, commandType, connectionLimit, timeout);
            return defaultDecimal;
        }

        public static decimal? getDecimalFromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null,
            decimal? defaultDecimal = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            decimal? res = defaultDecimal;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
                con.Open();
                using (SqlDataReader rd = com.ExecuteReader())
                {
                    if (rd.HasRows)
                    {
                        rd.Read();
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = toDecimalNull(rd.GetValue(0));
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
            return res;
        }

        public static async Task<decimal?> getDecimalAsync(string sql, string server, Dictionary<string, object> parameters = null,
            decimal? defaultDecimal = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return await getDecimalFromConnStrAsync(sql, connStr, parameters, defaultDecimal, commandType, connectionLimit, timeout).ConfigureAwait(false);
            return defaultDecimal;
        }

        public static async Task<decimal?> getDecimalFromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null,
            decimal? defaultDecimal = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            decimal? res = defaultDecimal;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await con.OpenAsync().ConfigureAwait(false);
                using (SqlDataReader rd = await com.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (rd.HasRows)
                    {
                        await rd.ReadAsync().ConfigureAwait(false);
                        string header = rd.GetName(0);
                        if (!rd.IsDBNull(0))
                            res = toDecimalNull(rd.GetValue(0));
                    }
                }
                con.Close();
                finalizeConn(connStr);
            }
            return res;
        }

        public static double getDouble(string sql, string server, Dictionary<string, object> parameters = null,
            double defaultDouble = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return getDoubleFromConnStr(sql, connStr, parameters, defaultDouble, commandType, connectionLimit, timeout);
            return defaultDouble;
        }

        public static double getDoubleFromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null,
            double defaultDouble = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            double res = defaultDouble;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
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
                finalizeConn(connStr);
            }
            return res;
        }

        public static async Task<double> getDoubleAsync(string sql, string server, Dictionary<string, object> parameters = null,
            double defaultDouble = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return await getDoubleFromConnStrAsync(sql, connStr, parameters, defaultDouble, commandType, connectionLimit, timeout)
                    .ConfigureAwait(false);
            return defaultDouble;
        }

        public static async Task<double> getDoubleFromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null,
            double defaultDouble = 0, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            double res = defaultDouble;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
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
                finalizeConn(connStr);
            }
            return res;
        }

        public static bool getBool(string sql, string server, Dictionary<string, object> parameters = null,
            bool defaultBool = false, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return getBoolFromConnStr(sql, connStr, parameters, defaultBool, commandType, connectionLimit, timeout);
            return defaultBool;
        }

        public static bool getBoolFromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null,
            bool defaultBool = false, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            bool res = defaultBool;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                waitForConn(connStr, connectionLimit);
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
                finalizeConn(connStr);
            }
            return res;
        }

        public static async Task<bool> getBoolAsync(string sql, string server, Dictionary<string, object> parameters = null,
            bool defaultBool = false, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                return await getBoolFromConnStrAsync(sql, connStr, parameters, defaultBool, commandType, connectionLimit, timeout)
                    .ConfigureAwait(false);
            return defaultBool;
        }

        public static async Task<bool> getBoolFromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null,
            bool defaultBool = false, CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            bool res = defaultBool;
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
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
                finalizeConn(connStr);
            }
            return res;
        }

        public static void ExecuteNonQuery(string sql, string server, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
                ExecuteNonQuery(sql, con, parameters, commandType, connectionLimit, timeout);
        }

        public static void ExecuteNonQueryFromConnStr(string sql, string connStr, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            using (SqlConnection con = new SqlConnection(connStr))
                ExecuteNonQuery(sql, con, parameters, commandType, connectionLimit, timeout);
        }

        public static async Task ExecuteNonQueryAsync(string sql, string server, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            using (SqlConnection con = new SqlConnection(getConnStr(server)))
                await ExecuteNonQueryAsync(sql, con, parameters, commandType, connectionLimit, timeout).ConfigureAwait(false);
        }

        public static async Task ExecuteNonQueryFromConnStrAsync(string sql, string connStr, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            using (SqlConnection con = new SqlConnection(connStr))
                await ExecuteNonQueryAsync(sql, con, parameters, commandType, connectionLimit, timeout).ConfigureAwait(false);
        }

        public static void ExecuteNonQuery(string sql, SqlConnection con, Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            waitForConn(con.ConnectionString, connectionLimit);
            con.Open();
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                com.ExecuteNonQuery();
            }
            con.Close();
            finalizeConn(con.ConnectionString);
        }

        public static async Task ExecuteNonQueryAsync(string sql, SqlConnection con,
            Dictionary<string, object> parameters = null,
            CommandType commandType = CommandType.Text, int connectionLimit = 100, int timeout = 0)
        {
            await waitForConnAsync(con.ConnectionString, connectionLimit).ConfigureAwait(false);
            await con.OpenAsync().ConfigureAwait(false);
            using (SqlCommand com = new SqlCommand(sql, con))
            {
                com.CommandType = commandType;
                com.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var key in parameters)
                        com.Parameters.AddWithValue(key.Key, key.Value);
                await com.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            con.Close();
            finalizeConn(con.ConnectionString);
        }

        public static void bulkInsert(DataTable table, string server, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                bulkInsertFromConnStr(table, connStr, connectionLimit, timeout);
        }

        public static void bulkInsertFromConnStr(DataTable table, string connStr, int connectionLimit = 100, int timeout = 0)
        {
            using (SqlConnection con = new SqlConnection(connStr))
            using (SqlBulkCopy bc = new SqlBulkCopy(con))
            {
                bc.BulkCopyTimeout = timeout;
                waitForConn(connStr, connectionLimit);
                con.Open();
                bc.DestinationTableName = table.TableName;
                foreach (var col in table.Columns)
                    bc.ColumnMappings.Add(col.ToString(), col.ToString());
                bc.WriteToServer(table);
                con.Close();
                finalizeConn(connStr);
            }
        }

        public static async Task bulkInsertAsync(DataTable table, string server, int connectionLimit = 100, int timeout = 0)
        {
            string connStr = getConnStr(server);
            if (!string.IsNullOrEmpty(connStr))
                await bulkInsertFromConnStrAsync(table, connStr, connectionLimit, timeout).ConfigureAwait(false);
        }

        public static async Task bulkInsertFromConnStrAsync(DataTable table, string connStr, int connectionLimit = 100, int timeout = 0)
        {
            using (SqlConnection conn = new SqlConnection(connStr))
            using (SqlBulkCopy bc = new SqlBulkCopy(conn))
            {
                bc.BulkCopyTimeout = timeout;
                await waitForConnAsync(connStr, connectionLimit).ConfigureAwait(false);
                await conn.OpenAsync().ConfigureAwait(false);
                bc.DestinationTableName = table.TableName;
                foreach (var col in table.Columns)
                    bc.ColumnMappings.Add(col.ToString(), col.ToString());
                await bc.WriteToServerAsync(table).ConfigureAwait(false);
                conn.Close();
                finalizeConn(connStr);
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

        public static object prepareDBString(string input)
        {
            if (input == null)
                return DBNull.Value;
            return input.Trim();
        }

        public static object prepareLimitedDBStr(string input, int limit)
        {
            if (input == null)
                return DBNull.Value;
            if (input.Length > limit)
                return input.Substring(0, limit);
            return input.Trim();
        }

        static readonly HashSet<Type> numerics = new HashSet<Type>()
        {
            typeof(Int16),
            typeof(UInt16),
            typeof(Int32),
            typeof(UInt32),
            typeof(Int64),
            typeof(UInt64),
            typeof(Decimal),
            typeof(Single),
            typeof(Double)
        };

        public static bool isNumeric(Type type)
        {
            return numerics.Contains(type);
        }
    }

    public static class ErrorHandler
    {
        public static string getInfoString(Exception e, string delim = "\n")
        {
            List<string> arrMsg = new List<string>();
            if (e is AggregateException)
            {
                AggregateException iterator = e as AggregateException;
                arrMsg.Add(e.Message);
                if (iterator != null)
                    foreach (var innerEx in iterator.InnerExceptions)
                        arrMsg.Add(getInfoString(innerEx));
                return string.Join(delim, arrMsg);
            }
            else
            {
                Exception iterator = e;
                while (iterator != null)
                {
                    arrMsg.Add(iterator.Message);
                    iterator = iterator.InnerException;
                }
                return string.Join(delim + "Inner Exception:" + delim, arrMsg);
            }
        }
    }
}
