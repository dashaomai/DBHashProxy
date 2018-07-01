using System;
using System.Data.SQLite;

namespace DBHashProxy.DBUtils
{
    /// <summary>
    /// Db hash proxy.
    /// 通过 Hash 的方式对输入查询键进行散列查询的工具类，把超大量的数据，分散到若干个离散的 SQLite 实例当中。
    /// </summary>
    public class DBProxy : IDisposable
    {
        // 所有数据库文件所在路径
        private string m_path;
        // 数据库拆分的数量
        private int m_dbCount;

        // 所有保持打开的数据库连接
        private SQLiteConnection[] m_connections;
        // 所有保持开启的数据库命令对象
        private SQLiteCommand[] m_commands;

        public DBProxy(string path, int dbCount)
        {
            m_path = path;
            m_dbCount = dbCount;

            // 初始化所有的 SQLite 连接
            m_connections = new SQLiteConnection[m_dbCount];
            m_commands = new SQLiteCommand[m_dbCount];

            for (int i = 0; i < m_dbCount; i++) {
                var file = m_path + "/" + i + ".db";
                var info = new SQLiteConnectionStringBuilder();
                info.DataSource = file;
                var conn = new SQLiteConnection(info.ToString());
                conn.Open();

                var command = conn.CreateCommand();
                // 创建表结构
                command.CommandText = "CREATE TABLE IF NOT EXISTS rate (kv1 INTEGER NOT NULL, kv2 INTEGER NOT NULL, val INTEGER NOT NULL, CONSTRAINT pk_key_pairs PRIMARY KEY(kv1 ASC, kv2 ASC))";
                command.ExecuteNonQuery();

                m_connections[i] = conn;
                m_commands[i] = command;
            }
        }

        /// <summary>
        /// 更新数据库中对应的数值。
        /// 
        /// 如果数据库中没有对应的记录，则创建新记录。
        /// 如果数据库中已有对应的记录，则将本次数值与已有记录的数值相加，并存储于数据库当中
        /// </summary>
        /// <param name="kv1">第一主键</param>
        /// <param name="kv2">第二主键</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        public bool Update(string kv1, string kv2, int value) {
            var connInfo = GetConnectionInfo(kv1, kv2);

            var conn = m_connections[connInfo.Index];
            var command = m_commands[connInfo.Index];

            using (var transaction = conn.BeginTransaction())
            {
                int result;

                // 检查记录是否存在，然后决定采用更新还是插入操作
                command.CommandText = String.Format("SELECT COUNT(*) FROM rate WHERE kv1={0} AND kv2={1} LIMIT 1", connInfo.HashCode1, connInfo.HashCode2);
                bool isExists = ExecuteIntScalar(command) > 0;

                if (isExists) {
                    command.CommandText = String.Format("UPDATE rate SET val=val+{2} WHERE kv1={0} AND kv2={1} LIMIT 1", connInfo.HashCode1, connInfo.HashCode2, value);
                } else {
                    command.CommandText = String.Format("INSERT OR IGNORE INTO rate (kv1, kv2, val) VALUES({0}, {1}, {2})", connInfo.HashCode1, connInfo.HashCode2, value);
                }
                result = command.ExecuteNonQuery();

                transaction.Commit();

                return 1 == result;
            }
        }

        /// <summary>
        /// 查询数据库中对应记录的数值
        /// </summary>
        /// <param name="kv1">第一主键</param>
        /// <param name="kv2">第二主键</param>
        /// <returns></returns>
        public int Query(string kv1, string kv2) {
            var connInfo = GetConnectionInfo(kv1, kv2);
            
            var command = m_commands[connInfo.Index];
            command.CommandText = String.Format(
                "SELECT val FROM rate WHERE kv1={0} AND kv2={1}",
                connInfo.HashCode1, connInfo.HashCode2
            );

            return ExecuteIntScalar(command);
        }

        public void Dispose() {
            for (int i = 0; i < m_dbCount; i++) {
                m_commands[i].Dispose();
                m_commands[i] = null;

                m_connections[i].Dispose();
                m_connections[i] = null;
            }
        }

        private ConnectionInfo GetConnectionInfo(string kv1, string kv2)
        {
            return new ConnectionInfo(kv1, kv2, m_dbCount);
        }

        private int ExecuteIntScalar(SQLiteCommand command)
        {
            var result = command.ExecuteScalar();

            return (null != result ? int.Parse(result.ToString()) : 0);
        }




        private class ConnectionInfo
        {
            private int m_hashCode1;
            private int m_hashCode2;
            private int m_index;

            public ConnectionInfo(string kv1, string kv2, int dbCount)
            {
                this.m_hashCode1 = kv1.GetHashCode();
                this.m_hashCode2 = kv2.GetHashCode();
                this.m_index = Math.Abs(this.m_hashCode1 % dbCount);
            }

            public int HashCode1 { get => m_hashCode1; }
            public int HashCode2 { get => m_hashCode2; }
            public int Index { get => m_index; }
        }
    }
}
