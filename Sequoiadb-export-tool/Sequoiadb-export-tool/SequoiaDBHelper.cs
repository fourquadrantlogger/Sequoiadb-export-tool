using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using SequoiaDB;
using SequoiaDB.Bson;
using System.Configuration;



namespace Sequoiadb_export_tool
{
    public class SequoiaDBHelper
    {
        public SequoiaDBHelper(string db_url)
        {
            Connect(db_url);

        }
        Sequoiadb sdb;

        /// <summary>
        /// 
        /// </summary>
        public Sequoiadb Sdb
        {
            get
            {
                return sdb;
            }
        }

        private bool Connect(string addr)
        {
            sdb = new Sequoiadb(addr);
            try
            {
                Sdb.Connect();
            }
            catch (BaseException e)
            {
                Console.Error.WriteLine("ErrorCode:{0}, ErrorType:{1}", e.ErrorCode, e.ErrorType);
                Console.Error.WriteLine(e.Message);
                return false;
            }
            catch (System.Exception e)
            {
                Console.Error.WriteLine(e.StackTrace);
                return false;
            }

            return true;
        }
        public List<string> ListCollectionSpaces()
        {
            DBCursor cursor = sdb.ListCollectionSpaces();
            List<string> result = new List<string>();

            if (cursor.Current() != null) result.Add((string)(cursor.Current()["Name"]));
            while (cursor.Next() != null)
            {
                result.Add((string)(cursor.Current()["Name"]));
            }
            return result;
        }
        public List<string> ListCollections()
        {
            DBCursor cursor = sdb.ListCollections();
            List<string> result = new List<string>();

            if (cursor.Current() != null) result.Add((string)(cursor.Current()["Name"]));
            while (cursor.Next() != null)
            {
                result.Add((string)(cursor.Current()["Name"]));
            }
            return result;
        }
        private CollectionSpace GetCollectionSpace(string csName)
        {
            CollectionSpace cs;
            if (sdb.IsCollectionSpaceExist(csName))
            {
                cs = sdb.GetCollecitonSpace(csName);

                return cs;
            }
            else {
                cs = sdb.CreateCollectionSpace(csName);
                return cs;
            }
        } 

        private DBCollection GetCollection(string csName, string clName)
        {
            CollectionSpace cs = GetCollectionSpace(csName);
            DBCollection dbc;
            if (cs.IsCollectionExist(clName)) { 
                dbc = cs.GetCollection(clName);
                return dbc;
            }
            else {
                 dbc = cs.CreateCollection(clName);
                return dbc;
            }
        }
        private void CreateIndex(ref DBCollection dbc, string Indexname,BsonDocument key)
        {
            bool isUnique = true;
            bool isEnforced = true;
            dbc.CreateIndex(Indexname, key, isUnique, isEnforced);
        }
        public BsonValue insertRow(string CollectionSpaceName, string CollectionName, BsonDocument insertor)
        {
            DBCollection dbc=GetCollection(CollectionSpaceName, CollectionName);
            BsonValue  result=dbc.Insert(insertor);            
            return result;
        }

        public BsonValue insertRowWithClose(string CollectionSpaceName, string CollectionName, BsonDocument insertor)
        {
            DBCollection dbc = GetCollection(CollectionSpaceName, CollectionName);
            BsonValue result = dbc.Insert(insertor);
            sdb.Disconnect();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="CollectionSpaceName"></param>
        /// <param name="CollectionName"></param>
        /// <param name="updater">修改值</param>
        /// <param name="matcher">条件</param>
        /// <param name=""></param>
        public void updateRow(string CollectionSpaceName, string CollectionName, BsonDocument matcher, BsonDocument updater)
        {
            DBCollection dbc = GetCollection(CollectionSpaceName, CollectionName);
            DBQuery query = new DBQuery();

            BsonDocument modifier = new BsonDocument();
            modifier.Add("$set", updater);

            query.Matcher = matcher;
            query.Modifier = modifier;
            dbc.Update(query);
        }
        /// <summary>
        /// while (cursor.Next() != null)
        ///Console.WriteLine(cursor.Current());
        /// </summary>
        /// <param name="CollectionSpaceName"></param>
        /// <param name="CollectionName"></param>
        /// <param name="indexname"></param>
        /// <param name="query"></param>
        /// <param name="selector"></param>
        /// <param name="orderBy"></param>
        /// <param name=""></param>
        /// <param name="hint">访问索引（index）</param>
        public DBCursor findRow(string CollectionSpaceName, string CollectionName,BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
        {
            DBCollection dbc = GetCollection(CollectionSpaceName, CollectionName);
            DBCursor cursor = dbc.Query(query, selector, orderBy, hint, 0, -1);
            cursor.Current();
            return cursor;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="CollectionSpaceName"></param>
        /// <param name="CollectionName"></param>
        /// <param name="query">查询匹配规则（query，包含相应的查询条件）</param>
        /// <param name="selector">域选择（selector）</param>
        /// <param name="orderBy">排序规则（orderBy，增序或降序）</param>
        /// <param name="hint">制定访问计划（hint）</param>
        /// <returns></returns>
        public List<BsonDocument> findRowData(string CollectionSpaceName, string CollectionName, BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
        {
            DBCollection dbc = GetCollection(CollectionSpaceName, CollectionName);
            DBCursor cursor = dbc.Query(query, selector, orderBy, hint, 0, -1);
            
            List<BsonDocument> result = new List<BsonDocument>();
            if(cursor.Current()!=null) result.Add(cursor.Current());
            while (cursor.Next() != null)
            {
                result.Add(cursor.Current());
            }
            return result;
        }

        public int findRowCount(string CollectionSpaceName, string CollectionName, BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
        {
            int count = 0;
            DBCollection dbc = GetCollection(CollectionSpaceName, CollectionName);
            DBCursor cursor = dbc.Query(query, selector, orderBy, hint, 0, -1);
            if(cursor.Current()!= null)count++ ;
            while (cursor.Next() != null)
            {
                count++;
            }
            return count;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="CollectionSpaceName"></param>
        /// <param name="CollectionName"></param>
        /// <param name="matcher"></param>
        /// <param name="modifier"></param>
        /// <param name="hint"></param>
        public void upsertRow(string CollectionSpaceName, string CollectionName, BsonDocument matcher, BsonDocument updater, BsonDocument hint)
        {
            DBCollection dbc = GetCollection(CollectionSpaceName, CollectionName);
            BsonDocument modifier = new BsonDocument();
            modifier.Add("$set", updater);
            dbc.Upsert(matcher,modifier,hint);
        }

    }
}