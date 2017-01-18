using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Configuration;
/// <summary>
/// 数据访问对象
/// </summary>
public class SQLServerDAO
{
    private static string ConnectionString = ConfigurationManager.ConnectionStrings["db"].ConnectionString;
    
    public SQLServerDAO()
	{
		//
		// TODO: 在此处添加构造函数逻辑
		//
	}
    /// <summary>
    /// 创建数据库连接
    /// </summary>
    /// <returns></returns>
    private static SqlConnection CreateSqlConnection()
    {
        using (SqlConnection conn = new SqlConnection(ConnectionString))
        {
            conn.Open();
            return conn;
        }
    }
    /// <summary>
    /// 返回SqlDbType类型
    /// </summary>
    /// <param name="TypeName">类型名称</param>
    /// <returns></returns>
    private static SqlDbType GetSqlDbType(string TypeName)
    {
        switch (TypeName.ToLower().Trim())
        {
            case "image":
                return SqlDbType.Image;
            case "text":
                return SqlDbType.Text;
            case "uniqueidentifier":
                return SqlDbType.UniqueIdentifier;
            case "tinyint":
                return SqlDbType.TinyInt;
            case "smallint":
                return SqlDbType.SmallInt;
            case "int":
                return SqlDbType.Int;
            case "smalldatetime":
                return SqlDbType.SmallDateTime;
            case "real":
                return SqlDbType.Real;
            case "money":
                return SqlDbType.Money;
            case "datetime":
                return SqlDbType.DateTime;
            case "float":
                return SqlDbType.Float;
            case "sql_variant":
                return SqlDbType.Variant;
            case "ntext":
                return SqlDbType.NText;
            case "bit":
                return SqlDbType.Bit;
            case "decimal":
                return SqlDbType.Decimal;
            case "numeric":
                return SqlDbType.Decimal;
            case "smallmoney":
                return SqlDbType.SmallMoney;
            case "bigint":
                return SqlDbType.BigInt;
            case "varbinary":
                return SqlDbType.VarBinary;
            case "varchar":
                return SqlDbType.VarChar;
            case "binary":
                return SqlDbType.Binary;
            case "char":
                return SqlDbType.Char;
            case "timestamp":
                return SqlDbType.Timestamp;
            case "nvarchar":
                return SqlDbType.NVarChar;
            case "nchar":
                return SqlDbType.NChar;
            case "xml":
                return SqlDbType.Xml;
            default:
                return SqlDbType.Variant;
        }
    }
    /// <summary>
    /// 创建数据库命令
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">数据库命令类型</param>
    /// <returns></returns>
    public static SqlCommand CreateSqlCommand(string sql, object parameters, CommandType cmdType)
    {

        using(SqlCommand cmd=new SqlCommand()){
            cmd.CommandTimeout = 60;
            cmd.CommandType = cmdType;
            cmd.Connection = CreateSqlConnection();
            cmd.CommandText=sql;
            cmd.Parameters.Clear();
            // 绑定返回值
            cmd.Parameters.Add("ReturnValue", SqlDbType.Variant).Direction = ParameterDirection.ReturnValue;
            if (parameters != null) {
                if (parameters is SqlParameter[]) {
                    cmd.Parameters.AddRange((SqlParameter[])parameters);
                }
                else if (parameters is Dictionary<string, object>)
                {

                }
                else {
                    throw new Exception("SqlCommand不支持该参数类型");
                }

            }
            
            return cmd;
        }
    }
    /// <summary>
    /// 执行SQL语句并返回受影响的行数
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static int ExecuteNonQuery(string sql, object parameters, bool IsUserTransaction=false)
    {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, CommandType.Text);

        try {
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    int ret = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return ret;
                }
                catch (Exception exp)
                {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else
            {
                return cmd.ExecuteNonQuery();
            }
        
        }
        finally {
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行查询
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(string sql, object parameters, CommandType cmdType, bool IsUserTransaction = false) {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        tran.Commit();
                        return sdr;
                    }
                }
                catch (Exception exp)
                {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else
            {
                return cmd.ExecuteReader();
            }
        }
        finally
        {
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行查询，并返回第一行第一列
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static object ExecuteScalar(string sql, object parameters, CommandType cmdType, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    object ret = cmd.ExecuteScalar();
                    tran.Commit();
                    return ret;
                }
                catch (Exception exp)
                {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else
            {
                return cmd.ExecuteScalar();
            }
        }
        finally
        {
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行查询
    /// </summary>
    /// <param name="StoredProcedureName">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static System.Xml.XmlReader ExecuteXmlReader(string sql, object parameters, CommandType cmdType, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    System.Xml.XmlReader xml = cmd.ExecuteXmlReader();
                    tran.Commit();
                    return xml;
                }
                catch (Exception exp) {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else {
                return cmd.ExecuteXmlReader();
            }
        }
        finally {
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行查询
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static DataTable ExecuteDataTable(string sql, object parameters, CommandType cmdType, bool IsUserTransaction = false) {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, cmdType);
        SqlDataAdapter sda=null;
        try
        {
            sda = new SqlDataAdapter(cmd);
            using (DataTable dt = new DataTable())
            {
                if (IsUserTransaction)
                {
                    SqlTransaction tran = cmd.Connection.BeginTransaction();
                    try
                    {
                        sda.Fill(dt);
                        tran.Commit();
                    }
                    catch (Exception exp)
                    {
                        tran.Rollback();
                        throw exp;
                    }finally {
                        tran.Dispose();
                    }
                }
                else
                {
                    sda.Fill(dt);
                }

                return dt;
            }
        }
        finally
        {
            sda.Dispose();
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行查询
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static DataSet ExecuteDataSet(string sql, object parameters, CommandType cmdType, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, cmdType);
        SqlDataAdapter sda = null;
        try
        {
            sda = new SqlDataAdapter(cmd);
            using (DataSet ds = new DataSet())
            {
                if (IsUserTransaction)
                {
                    SqlTransaction tran = cmd.Connection.BeginTransaction();
                    try
                    {
                        sda.Fill(ds);
                        tran.Commit();
                    }
                    catch (Exception exp)
                    {
                        tran.Rollback();
                        throw exp;
                    }
                    finally {
                        tran.Dispose();
                    }
                }
                else
                {
                    sda.Fill(ds);
                }
                return ds;
            }
        }
        finally
        {
            sda.Dispose();
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    
    /// <summary>
    /// 获取ReturnData<T>数据
    /// </summary>
    /// <typeparam name="T">数据类型</typeparam>
    /// <param name="cmd">数据库命令对象</param>
    /// <param name="data">数据</param>
    /// <returns></returns>
    public static ReturnData<T> GetReturnData<T>(SqlCommand cmd,T data) {
        try
        {
            ReturnData<T> rd = new ReturnData<T>();
            rd.Data = data;
            foreach (SqlParameter sparam in cmd.Parameters)
            {
                if (sparam.Direction == ParameterDirection.Input || sparam.Direction == ParameterDirection.InputOutput)
                {
                    continue;
                }
                if (sparam.Direction == ParameterDirection.Output)
                {
                    rd.OutputParameters.Add(sparam.ParameterName, sparam.Value);
                    continue;
                }
                if (sparam.Direction == ParameterDirection.ReturnValue)
                {
                    rd.ReturnValue = sparam.Value;
                    continue;
                }
            }

            if (cmd.Parameters.Count == 0) {
                rd = null;
            }

            return rd;
        }
        catch (Exception exp) {
            throw new Exception("获取ReturnData<T>数据异常：" + exp.Message);
        }
    }
    
    /// <summary>
    /// 执行存储过程
    /// </summary>
    /// <param name="StoredProcedureName">存储过程名称</param>
    /// <param name="parameters">参数</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static ReturnData<SqlDataReader> ExecuteReader(string StoredProcedureName, object parameters, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(StoredProcedureName, parameters, CommandType.StoredProcedure);
        SqlDataReader sdr = new SqlDataReader();
        try
        {
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    sdr = cmd.ExecuteReader();
                    tran.Commit();
                }
                catch (Exception exp)
                {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else
            {
                sdr= cmd.ExecuteReader();
            }

            return GetReturnData<SqlDataReader>(cmd, sdr);
            
        }
        finally
        {
            sdr.Dispose();
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行存储过程，并返回第一行第一列
    /// </summary>
    /// <param name="StoredProcedureName">存储过程名称</param>
    /// <param name="parameters">参数</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static ReturnData<object> ExecuteScalar(string StoredProcedureName, object parameters, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(StoredProcedureName, parameters, CommandType.StoredProcedure);
        try
        {
            object ret;
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    ret = cmd.ExecuteScalar();
                    tran.Commit();
                }
                catch (Exception exp)
                {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else
            {
                ret= cmd.ExecuteScalar();
            }
            return GetReturnData<object>(cmd, ret);
        }
        finally
        {
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行存储过程
    /// </summary>
    /// <param name="StoredProcedureName">存储过程名称</param>
    /// <param name="parameters">参数</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static ReturnData<System.Xml.XmlReader> ExecuteXmlReader(string StoredProcedureName, object parameters, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(StoredProcedureName, parameters, CommandType.StoredProcedure);
        try
        {
            System.Xml.XmlReader xml;
            if (IsUserTransaction)
            {
                SqlTransaction tran = cmd.Connection.BeginTransaction();
                try
                {
                    xml = cmd.ExecuteXmlReader();
                    tran.Commit();
                }
                catch (Exception exp)
                {
                    tran.Rollback();
                    throw exp;
                }
                finally
                {
                    tran.Dispose();
                }
            }
            else
            {
                xml= cmd.ExecuteXmlReader();
            }
            return GetReturnData<System.Xml.XmlReader>(cmd, xml);
        }
        finally
        {
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    
    /// <summary>
    /// 执行存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static ReturnData<DataSet> ExecuteDataSet(string sql, object parameters, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, CommandType.StoredProcedure);
        SqlDataAdapter sda = null;
        try
        {
            sda = new SqlDataAdapter(cmd);
            using (DataSet ds = new DataSet())
            {
                if (IsUserTransaction)
                {
                    SqlTransaction tran = cmd.Connection.BeginTransaction();
                    try
                    {
                        sda.Fill(ds);
                        tran.Commit();
                    }
                    catch (Exception exp)
                    {
                        tran.Rollback();
                        throw exp;
                    }
                    finally
                    {
                        tran.Dispose();
                    }
                }
                else
                {
                    sda.Fill(ds);
                }

                return GetReturnData<DataSet>(cmd, ds);

            }
        }
        finally
        {
            sda.Dispose();
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }
    /// <summary>
    /// 执行存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="IsUserTransaction">使用事务</param>
    /// <returns></returns>
    public static ReturnData<DataTable> ExecuteDataTable(string sql, object parameters, bool IsUserTransaction = false)
    {
        SqlCommand cmd = CreateSqlCommand(sql, parameters, CommandType.StoredProcedure);
        SqlDataAdapter sda = null;
        try
        {
            sda = new SqlDataAdapter(cmd);
            using (DataTable dt = new DataTable())
            {
                if (IsUserTransaction)
                {
                    SqlTransaction tran = cmd.Connection.BeginTransaction();
                    try
                    {
                        sda.Fill(dt);
                        tran.Commit();
                    }
                    catch (Exception exp)
                    {
                        tran.Rollback();
                        throw exp;
                    }
                    finally
                    {
                        tran.Dispose();
                    }
                }
                else
                {
                    sda.Fill(dt);
                }
                return GetReturnData<DataTable>(cmd, dt);
                
            }
        }
        finally
        {
            sda.Dispose();
            cmd.Connection.Close();
            cmd.Dispose();
        }
    }

    /// <summary>
    /// 数据表记录转化为实体类对象
    /// </summary>
    /// <typeparam name="T">泛型</typeparam>
    /// <param name="dr">SqlDataReader</param>
    /// <returns></returns>
    public static T ToORM<T>(SqlDataReader dr) where T : new()
    {
        T entity = new T();

        // 获得此模型的公共属性 
        FieldInfo[] fields = entity.GetType().GetFields();

        for (int i = 0; i < dr.FieldCount; i++)
        {
            string FieldName = dr.GetName(i);
            object value = dr[i];

            if (value == DBNull.Value) continue;

            FieldInfo field = entity.GetType().GetField(FieldName);
            if (field == null) continue;

            field.SetValue(entity, value);
        }

        return entity;
    }









}
/// <summary>
/// 执行数据库命令返回数据类
/// </summary>
/// <typeparam name="T">泛型</typeparam>
[Serializable]
public class ReturnData<T>{
    /// <summary>
    /// 输出参数
    /// </summary>
    public Dictionary<string, object> OutputParameters;
    /// <summary>
    /// 返回值
    /// </summary>
    public object ReturnValue;
    /// <summary>
    /// 返回的数据
    /// </summary>
    public T Data;

}
