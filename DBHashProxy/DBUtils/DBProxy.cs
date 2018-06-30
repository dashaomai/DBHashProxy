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
        private string m_path;
        private int m_dbCount;
        private SQLiteConnection[] m_connections;

        public DBProxy(string path, int dbCount)
        {
            m_path = path;
            m_dbCount = dbCount;

            // 初始化所有的 SQLite 连接
            m_connections = new SQLiteConnection[m_dbCount];
            for (int i = 0; i < m_dbCount; i++) {
                var file = m_path + "/" + i + ".db";
                var conn = new SQLiteConnection("Data Source=" + file);
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // 创建表结构
                    command.CommandText = "CREATE TABLE IF NOT EXISTS rate (kv1 INTEGER NOT NULL, kv2 INTEGER NOT NULL, val INTEGER NOT NULL, CONSTRAINT pk_key_pairs PRIMARY KEY(kv1 ASC, kv2 ASC))";
                    command.ExecuteNonQuery();
                }

                m_connections[i] = conn;
            }
        }

        public bool Update(string kv1, string kv2, int value) {
            var hc1 = kv1.GetHashCode();
            var hc2 = kv2.GetHashCode();

            var index = Math.Abs(hc1 % m_dbCount);
            var conn = m_connections[index];
            using (var transaction = conn.BeginTransaction())
            {
                int result;
                using (var command = conn.CreateCommand())
                {
                    // 检查记录是否存在
                    command.CommandText = String.Format("SELECT COUNT(*) FROM rate WHERE kv1={0} AND kv2={1} LIMIT 1", hc1, hc2);
                    var count = command.ExecuteScalar();
                    bool isExists = (null != count) ? int.Parse(count.ToString()) > 0 : false;

                    if (isExists) {
                        command.CommandText = String.Format("UPDATE rate SET val=val+{2} WHERE kv1={0} AND kv2={1}", hc1, hc2, value);
                    } else {
                        command.CommandText = String.Format("INSERT OR IGNORE INTO rate (kv1, kv2, val) VALUES({0}, {1}, {2})", hc1, hc2, value);
                    }
                    result = command.ExecuteNonQuery();
                }

                transaction.Commit();

                return 1 == result;
            }
        }

        public int Query(string kv1, string kv2) {
            var hc1 = kv1.GetHashCode();
            var hc2 = kv2.GetHashCode();

            var index = Math.Abs(hc1 % m_dbCount);
            var conn = m_connections[index];
            using (var command = conn.CreateCommand())
            {
                command.CommandText = String.Format(
                    "SELECT val FROM rate WHERE kv1={0} AND kv2={1}",
                    hc1, hc2
                );

                var result = command.ExecuteScalar();

                return (null != result ? int.Parse(result.ToString()) : 0);
            }
        }

        public void Dispose() {
            for (int i = 0; i < m_dbCount; i++) {
                m_connections[i].Dispose();
                m_connections[i] = null;
            }
        }
    }
}
