using System;
using System.Text;
using DBHashProxy.DBUtils;

namespace DBHashProxy
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            string path = System.Environment.CurrentDirectory + "/test_db";
            int dbCount = 100;

            System.Console.WriteLine(String.Format("准备用：{0}, {1} 初始化数据库", path, dbCount));

            System.IO.Directory.CreateDirectory(path);
            using (var proxy = new DBProxy(path, dbCount))
            {
                Random random = new Random(DateTime.Now.GetHashCode());

                var t1 = DateTime.Now.Ticks;

                // 随机插入数据
                for (long i = 0, m = 2000000000L; i < m; i++)
                {
                    string kv1 = RandomStr(random);
                    string kv2 = RandomStr(random);
                    int value = random.Next(1000);

                    proxy.Update(kv1, kv2, value);
                }

                var t2 = DateTime.Now.Ticks;

                // 进行 1 万次查询
                for (int i = 0; i < 10000; i++) {
                    string kv1 = RandomStr(random);
                    string kv2 = RandomStr(random);
                    int value = proxy.Query(kv1, kv2);
                }

                var t3 = DateTime.Now.Ticks;

                var ts1 = new TimeSpan(t2 - t1);
                var ts2 = new TimeSpan(t3 - t2);
                var ts3 = new TimeSpan(t3 - t1);

                System.Console.WriteLine(String.Format("消耗时间：{0}, {1}, {2} （毫秒）", ts1.TotalMilliseconds, ts2.TotalMilliseconds, ts3.TotalMilliseconds));
            }
        }

        private static char[] m_chars = new char[] {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
        };

        /// <summary>
        /// 生成 16 位长度的随机字符串
        /// </summary>
        /// <returns>The string.</returns>
        /// <param name="random">Random.</param>
        private static string RandomStr(Random random)
        {
            var count = m_chars.Length;
            var builder = new StringBuilder();

            for (int i = 0; i < 16; i++) {
                var index = random.Next(count);
                builder.Append(m_chars[index]);
            }

            return builder.ToString();
        }
    }
}
