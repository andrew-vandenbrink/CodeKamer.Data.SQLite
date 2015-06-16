using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace CodeKamer.Data.SQLite
{
    public class SQLiteException : Exception
    {
        public int ErrorCode
        {
            get;
            private set;
        }

        private string messsage;
        public override string Message
        {
            get
            {
                return this.messsage;
            }

        }

        public SQLiteException(string message, int errorCode)
        {
            this.messsage = message;
            this.ErrorCode = errorCode;
        }
    }

    public class SQLiteConnectionHandle : IDisposable
    {
        private IntPtr handle;

        public SQLiteConnectionHandle(IntPtr handle)
        {
            this.handle = handle;
        }

        ~SQLiteConnectionHandle()
        {
            Dispose();
        }

        public void Dispose()
        {
            if (this.handle != IntPtr.Zero)
            {
                SQLiteAPI.Close(handle);
                this.handle = IntPtr.Zero;
            }
        }
    }
    public enum SQLiteResultCode
    {
        SQLITE_ABORT = 4,
        SQLITE_AUTH = 23,
        SQLITE_BUSY = 5,
        SQLITE_CANTOPEN = 14,
        SQLITE_CONSTRAINT = 19,
        SQLITE_CORRUPT = 11,
        SQLITE_DONE = 101,
        SQLITE_EMPTY = 16,
        SQLITE_ERROR = 1,
        SQLITE_FORMAT = 24,
        SQLITE_FULL = 13,
        SQLITE_INTERNAL = 2,
        SQLITE_INTERRUPT = 9,
        SQLITE_IOERR = 10,
        SQLITE_LOCKED = 6,
        SQLITE_MISMATCH = 20,
        SQLITE_MISUSE = 21,
        SQLITE_NOLFS = 22,
        SQLITE_NOMEM = 7,
        SQLITE_NOTADB = 26,
        SQLITE_NOTFOUND = 12,
        SQLITE_NOTICE = 27,
        SQLITE_OK = 0,
        SQLITE_PERM = 3,
        SQLITE_PROTOCOL = 15,
        SQLITE_RANGE = 25,
        SQLITE_READONLY = 8,
        SQLITE_ROW = 100,
        SQLITE_SCHEMA = 17,
        SQLITE_TOOBIG = 18,
        SQLITE_WARNING = 28,

        //
        SQLITE_ABORT_ROLLBACK = 516,
        SQLITE_BUSY_RECOVERY = 261,
        SQLITE_BUSY_SNAPSHOT = 517,
        SQLITE_CANTOPEN_CONVPATH = 1038,
        SQLITE_CANTOPEN_FULLPATH = 782,
        SQLITE_CANTOPEN_ISDIR = 526,
        SQLITE_CANTOPEN_NOTEMPDIR = 270,
        SQLITE_CONSTRAINT_CHECK = 275,
        SQLITE_CONSTRAINT_COMMITHOOK = 531,
        SQLITE_CONSTRAINT_FOREIGNKEY = 787,
        SQLITE_CONSTRAINT_FUNCTION = 1043,
        SQLITE_CONSTRAINT_NOTNULL = 1299,
        SQLITE_CONSTRAINT_PRIMARYKEY = 1555,
        SQLITE_CONSTRAINT_ROWID = 2579,
        SQLITE_CONSTRAINT_TRIGGER = 1811,
        SQLITE_CONSTRAINT_UNIQUE = 2067,
        SQLITE_CONSTRAINT_VTAB = 2323,
        SQLITE_CORRUPT_VTAB = 267,
        SQLITE_IOERR_ACCESS = 3338,
        SQLITE_IOERR_BLOCKED = 2826,
        SQLITE_IOERR_CHECKRESERVEDLOCK = 3594,
        SQLITE_IOERR_CLOSE = 4106,
        SQLITE_IOERR_CONVPATH = 6666,
        SQLITE_IOERR_DELETE = 2570,
        SQLITE_IOERR_DELETE_NOENT = 5898,
        SQLITE_IOERR_DIR_CLOSE = 4362,
        SQLITE_IOERR_DIR_FSYNC = 1290,
        SQLITE_IOERR_FSTAT = 1802,
        SQLITE_IOERR_FSYNC = 1034,
        SQLITE_IOERR_GETTEMPPATH = 6410,
        SQLITE_IOERR_LOCK = 3850,
        SQLITE_IOERR_MMAP = 6154,
        SQLITE_IOERR_NOMEM = 3082,
        SQLITE_IOERR_RDLOCK = 2314,
        SQLITE_IOERR_READ = 266,
        SQLITE_IOERR_SEEK = 5642,
        SQLITE_IOERR_SHMLOCK = 5130,
        SQLITE_IOERR_SHMMAP = 5386,
        SQLITE_IOERR_SHMOPEN = 4618,
        SQLITE_IOERR_SHMSIZE = 4874,
        SQLITE_IOERR_SHORT_READ = 522,
        SQLITE_IOERR_TRUNCATE = 1546,
        SQLITE_IOERR_UNLOCK = 2058,
        SQLITE_IOERR_WRITE = 778,
        SQLITE_LOCKED_SHAREDCACHE = 262,
        SQLITE_NOTICE_RECOVER_ROLLBACK = 539,
        SQLITE_NOTICE_RECOVER_WAL = 283,
        SQLITE_READONLY_CANTLOCK = 520,
        SQLITE_READONLY_DBMOVED = 1032,
        SQLITE_READONLY_RECOVERY = 264,
        SQLITE_READONLY_ROLLBACK = 776,
        SQLITE_WARNING_AUTOINDEX = 284
    }
    public enum SQLiteDataType
    {
        SQLITE_INTEGER = 1,
        SQLITE_FLOAT = 2,
        SQLITE_TEXT = 3,
        SQLITE_BLOB = 4,
        SQLITE_NULL = 5
    }
    public enum SQLiteOpenFlag
    {
        SQLITE_OPEN_READONLY = 0x00000001,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_READWRITE = 0x00000002,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_CREATE = 0x00000004,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_DELETEONCLOSE = 0x00000008,  /* VFS only */
        SQLITE_OPEN_EXCLUSIVE = 0x00000010,  /* VFS only */
        SQLITE_OPEN_AUTOPROXY = 0x00000020,  /* VFS only */
        SQLITE_OPEN_URI = 0x00000040,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_MEMORY = 0x00000080,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_MAIN_DB = 0x00000100,  /* VFS only */
        SQLITE_OPEN_TEMP_DB = 0x00000200,  /* VFS only */
        SQLITE_OPEN_TRANSIENT_DB = 0x00000400,  /* VFS only */
        SQLITE_OPEN_MAIN_JOURNAL = 0x00000800,  /* VFS only */
        SQLITE_OPEN_TEMP_JOURNAL = 0x00001000,  /* VFS only */
        SQLITE_OPEN_SUBJOURNAL = 0x00002000,  /* VFS only */
        SQLITE_OPEN_MASTER_JOURNAL = 0x00004000,  /* VFS only */
        SQLITE_OPEN_NOMUTEX = 0x00008000,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_FULLMUTEX = 0x00010000,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_SHAREDCACHE = 0x00020000,  /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_PRIVATECACHE = 0x00040000, /* Ok for sqlite3_open_v2() */
        SQLITE_OPEN_WAL = 0x00080000  /* VFS only */
    }
    public class SQLiteAPI
    {
        [DllImport("sqlite3", EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_open_v2(
  string filename,   /* Database filename (UTF-8) */
  out IntPtr handle,         /* OUT: SQLite db handle */
  int flags,              /* Flags */
  string zVfs        /* Name of VFS module to use */
);
        private static IntPtr Open(string connectionString, int flag = 2, string vfs = null)
        {
            IntPtr handle = IntPtr.Zero;
            var interopResult = sqlite3_open_v2(connectionString, out handle, flag, vfs);
            if (interopResult != (int)SQLiteResultCode.SQLITE_OK)
            {
                throw new SQLiteException(ErrorMessage(handle), interopResult);
            }
            return handle;
        }

        public static SQLiteConnectionHandle OpenConnection(string connectionString, int flag = 2, string vfs = null)
        {
            return new SQLiteConnectionHandle(Open(connectionString, flag, vfs));
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_enable_shared_cache", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_enable_shared_cache(int trueFalse);
        public static int EnableSharedCache()
        {
            return sqlite3_enable_shared_cache(1);
        }
        public static int DisableSharedCache()
        {
            return sqlite3_enable_shared_cache(0);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_prepare_v2", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_prepare_v2(
  IntPtr handle,            /* Database handle */
  string zSql,       /* SQL statement, UTF-8 encoded */
  int nByte,              /* Maximum length of zSql in bytes. */
  out IntPtr statementHandle,  /* OUT: Statement handle */
  out IntPtr pzTail     /* OUT: Pointer to unused portion of zSql */
);
        private static IntPtr Prepare(IntPtr handle, string query)
        {
            IntPtr statementHandle = IntPtr.Zero;
            IntPtr tail = IntPtr.Zero;

            var result = sqlite3_prepare_v2(handle, query, -1, out statementHandle, out tail);
            return statementHandle;
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_step", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_step(IntPtr statementHandle);
        private static int Step(IntPtr statementHandle)
        {
            return sqlite3_step(statementHandle);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_count", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_column_count(IntPtr statementHandle);
        private static int ColumnCount(IntPtr statementHandle)
        {
            return sqlite3_column_count(statementHandle);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_name", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr sqlite3_column_name(IntPtr statementHandle, int iCol);
        private static string ColumnName(IntPtr statementHandle, int columnIndex)
        {
            return PtrToStringUTF8(sqlite3_column_name(statementHandle, columnIndex));
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_type", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_column_type(IntPtr statementHandle, int iCol);
        private static int ColumnType(IntPtr statementHandle, int columnIndex)
        {
            return sqlite3_column_type(statementHandle, columnIndex);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_text", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr sqlite3_column_text(IntPtr statementHandle, int iCol);
        private static string ColumnText(IntPtr statementHandle, int columnIndex)
        {
            return PtrToStringUTF8(sqlite3_column_text(statementHandle, columnIndex));
        }
        private static string PtrToStringUTF8(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                return null;
            }

            var i = 0;
            while (Marshal.ReadByte(ptr, i) != 0)
            {
                i++;
            }

            var bytes = new byte[i];
            Marshal.Copy(ptr, bytes, 0, i);

            return Encoding.UTF8.GetString(bytes, 0, i);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_int", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_column_int(IntPtr statementHandle, int iCol);
        private static int ColumnInt(IntPtr statementHandle, int columnIndex)
        {
            return sqlite3_column_int(statementHandle, columnIndex);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_double", CallingConvention = CallingConvention.Cdecl)]
        internal static extern double sqlite3_column_double(IntPtr statementHandle, int iCol);
        private static double ColumnDouble(IntPtr statementHandle, int columnIndex)
        {
            return sqlite3_column_double(statementHandle, columnIndex);
        }


        [DllImport("sqlite3", EntryPoint = "sqlite3_finalize", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_finalize(IntPtr statementHandle);
        private static int Finalize(IntPtr statementHandle)
        {
            return sqlite3_finalize(statementHandle);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_close_v2", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int sqlite3_close_v2(IntPtr handle);
        internal static int Close(IntPtr handle)
        {
            return sqlite3_close_v2(handle);
        }

        [DllImport("sqlite3", EntryPoint = "sqlite3_errmsg", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr sqlite3_errmsg(IntPtr handle);
        private static string ErrorMessage(IntPtr handle)
        {
            return PtrToStringUTF8(sqlite3_errmsg(handle));
        }

        public static Tuple<string, int> InMemorySharedConnectionString()
        {
            return Tuple.Create("file::memory:?cache=shared", (int)(SQLiteOpenFlag.SQLITE_OPEN_URI | SQLiteOpenFlag.SQLITE_OPEN_READWRITE | SQLiteOpenFlag.SQLITE_OPEN_CREATE | SQLiteOpenFlag.SQLITE_OPEN_MEMORY | SQLiteOpenFlag.SQLITE_OPEN_SHAREDCACHE));
        }

        private static Task<T> Connection<T>(string connectionString, int flag, Func<IntPtr, T> transformConnection)
        {
            return Task.Run<T>(() =>
            {
                T result = default(T);

                IntPtr dbHandle = IntPtr.Zero;
                try
                {
                    dbHandle = Open(connectionString, flag);

                    result = transformConnection(dbHandle);
                }
                finally
                {
                    Close(dbHandle);
                }

                return result;
            });
        }

        public static Task<int> ExecuteNonQueryAsync(string connectionString, params string[] queries)
        {
            return ExecuteNonQueryAsync(connectionString, (int)(SQLiteOpenFlag.SQLITE_OPEN_READWRITE | SQLiteOpenFlag.SQLITE_OPEN_CREATE), queries);
        }

        public static Task<int> ExecuteNonQueryAsync(string connectionString, int flag, params string[] queries)
        {
            return Connection<int>(connectionString, flag, (dbHandle) =>
              {
                  foreach (var query in queries)
                  {
                      IntPtr statementHandle = Prepare(dbHandle, query);
                      try
                      {
                          int stepResult = Step(statementHandle);
                          if (stepResult != (int)SQLiteResultCode.SQLITE_DONE)
                          {
                              throw new SQLiteException(ErrorMessage(dbHandle), stepResult);
                          }
                      }
                      finally
                      {
                          Finalize(statementHandle);
                      }
                  }
                  return (int)SQLiteResultCode.SQLITE_OK;
              });
        }

        public static Task<List<object[]>> ReadAsync(string connectionString, string query, bool includeHeader = true)
        {
            return ReadAsync(connectionString, (int)(SQLiteOpenFlag.SQLITE_OPEN_READWRITE | SQLiteOpenFlag.SQLITE_OPEN_CREATE), query);
        }

        public static Task<List<object[]>> ReadAsync(string connectionString, int flag, string query, bool includeHeader = true)
        {
            return Connection<List<object[]>>(connectionString, flag, (dbHandle) =>
            {
                List<object[]> result = new List<object[]>();
                IntPtr statementHandle = Prepare(dbHandle, query);
                try
                {
                    var columnCount = ColumnCount(statementHandle);

                    int stepResult = Step(statementHandle);

                    Dictionary<int, int> columnsType = new Dictionary<int, int>();
                    if (stepResult == (int)SQLiteResultCode.SQLITE_ROW)
                    {
                        for (int iColumn = 0; iColumn < columnCount; iColumn++)
                        {
                            columnsType.Add(iColumn, ColumnType(statementHandle, iColumn));
                        }
                    }
                    else
                    {
                        throw new SQLiteException(ErrorMessage(dbHandle), stepResult);
                    }

                    object[] data;
                    if (includeHeader)
                    {
                        data = new object[columnCount];
                        for (int iColumn = 0; iColumn < columnCount; iColumn++)
                        {
                            data[iColumn] = ColumnName(statementHandle, iColumn);
                        }
                        result.Add(data);
                    }
                    while (stepResult != (int)SQLiteResultCode.SQLITE_DONE)
                    {
                        switch (stepResult)
                        {
                            case (int)SQLiteResultCode.SQLITE_ROW:
                                data = new object[columnCount];
                                for (int iColumn = 0; iColumn < columnCount; iColumn++)
                                {
                                    object value;
                                    switch (columnsType[iColumn])
                                    {
                                        case (int)SQLiteDataType.SQLITE_TEXT:
                                            value = ColumnText(statementHandle, iColumn);
                                            break;
                                        case (int)SQLiteDataType.SQLITE_INTEGER:
                                            value = ColumnInt(statementHandle, iColumn);
                                            break;
                                        case (int)SQLiteDataType.SQLITE_FLOAT:
                                            value = ColumnDouble(statementHandle, iColumn);
                                            break;
                                        default:
                                            value = null;
                                            break;
                                    }
                                    data[iColumn] = value;
                                }
                                result.Add(data);
                                break;
                        }

                        stepResult = Step(statementHandle);
                    }
                }
                finally
                {
                    Finalize(statementHandle);
                }
                return result;
            });
        }
    }
}
