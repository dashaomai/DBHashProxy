using System;
using System.Text;
using DBHashProxy.DBUtils;

namespace DBHashProxy
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            // 定义文件数据库的存放目录，为软件运行相对目录下的 test_db 目录
            string path = Environment.CurrentDirectory + "/test_db";
            // 定义文件数据库的拆分数量
            int dbCount = 100;

            Console.WriteLine(String.Format("准备用：{0}, {1} 初始化数据库", path, dbCount));

            System.IO.Directory.CreateDirectory(path);

            // 几个计时变量，用于统计程序不同阶段的运行时间
            long t1, t2, t3;

            // 创建数据库哈希代理对象，用于后续的一系列操作
            using (var proxy = new DBProxy(path, dbCount))
            {
                Random random = new Random(DateTime.Now.Ticks.GetHashCode());

                t1 = DateTime.Now.Ticks;

                // 随机插入模拟数据（本想20亿条，后因运行时间太长，无奈改为20万条）
                for (long i = 0, m = 200000L; i < m; i++)
                {
                    string kv1 = RandomStr(random);
                    string kv2 = RandomStr(random);
                    int value = random.Next(1000);

                    proxy.Update(kv1, kv2, value);
                }

                t2 = DateTime.Now.Ticks;

                // 进行 1 万次查询
                for (int i = 0; i < 10000; i++) {
                    string kv1 = RandomStr(random);
                    string kv2 = RandomStr(random);
                    int value = proxy.Query(kv1, kv2);
                }

                t3 = DateTime.Now.Ticks;
            }

            // 统计并输出运行时间
            var ts1 = new TimeSpan(t2 - t1);
            var ts2 = new TimeSpan(t3 - t2);
            var ts3 = new TimeSpan(t3 - t1);

            Console.WriteLine(String.Format("消耗时间：{0}, {1}, {2} （毫秒）", ts1.TotalMilliseconds, ts2.TotalMilliseconds, ts3.TotalMilliseconds));
        }

        private static char[] m_chars = new char[] {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
        };

        private static int count = m_chars.Length;
        private static StringBuilder builder = new StringBuilder();

        /// <summary>
        /// 生成 16 位长度的随机字符串
        /// </summary>
        /// <returns>The string.</returns>
        /// <param name="random">Random.</param>
        private static string RandomStr(Random random)
        {
            builder.Clear();

            for (int i = 0; i < 16; i++) {
                var index = random.Next(count);
                builder.Append(m_chars[index]);
            }

            return builder.ToString();
        }
    }
}
