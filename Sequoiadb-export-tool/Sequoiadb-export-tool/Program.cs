using SequoiaDB;
using System.IO;

namespace Sequoiadb_export_tool
{
    class Program
    {
        static void Main(string[] args)
        {
            
            if (args.Length >= 2)
                doing(args);
            else
                usage();
            while (true)
            {
                System.Console.Read();
            }
        }
        static void doing(string[] args)
        {
            string outfolderpath = System.Environment.CurrentDirectory;
            
                outfolderpath = args[1];
            SequoiaDBHelper sh = new SequoiaDBHelper(args[0]);


            foreach (string c in sh.ListCollections())
            {
                FileStream fs = new FileStream(outfolderpath + "\\" + c + ".json", FileMode.Create);
                fs.WriteByte((byte)'[');
                int count = 0;
                DBCursor cursor = sh.findRow(
                     c.Substring(0, c.IndexOf('.')),
                     c.Substring(c.IndexOf('.') + 1),
                     new SequoiaDB.Bson.BsonDocument(),
                     new SequoiaDB.Bson.BsonDocument(),
                     new SequoiaDB.Bson.BsonDocument(),
                     new SequoiaDB.Bson.BsonDocument()
                     );
                if (cursor.Current() != null)
                {
                    byte[] bs = getbs(cursor.Current());
                    fs.Write(bs, 0, bs.Length);
                    count++;
                }
                while (cursor.Next() != null)
                {
                    fs.WriteByte((byte)',');
                    byte[] bs = getbs(cursor.Current());
                    fs.Write(bs, 0, bs.Length);
                    count++;
                }
                fs.WriteByte((byte)']');
               
                System.Console.WriteLine("write OK" + '\t'+ outfolderpath + "\\" + c + ".json" + "\n json count:"+ count + "\t" + fs.Length + " bytes");
                fs.Flush();
                fs.Close();
            }
        }
        static byte[] getbs(SequoiaDB.Bson.BsonDocument db)
        {
            return System.Text.Encoding.UTF8.GetBytes(db.ToString());
        }
        static void usage()
        {

            System.Console.Write( "usage:" +
                "Sequoiadb-export-tool.exe  112.12.123.12:50001   D://mySequoiadbPath");
        }
    }
}
