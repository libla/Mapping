using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Mapping
{
	public class MySQL : DataSource
	{
		private static int total = 0;

		private readonly string host;
		private readonly int port;
		private readonly string username;
		private readonly string password;
		private readonly string database;

		public MySQL(string host, int port, string username, string password, string database) : this(
			host, port, username, password, database, null) { }

		public MySQL(
			string host, int port, string username, string password, string database,
			Func<Type, string, string> mapping) : base(mapping)
		{
			Interlocked.Increment(ref total);
			this.host = host;
			this.port = port;
			this.username = username;
			this.password = password;
			this.database = database;
		}

		protected override void Dispose(bool disposing)
		{
			if (Interlocked.Decrement(ref total) == 0)
				MySqlConnection.ClearAllPools();
		}

		protected override async Task<DbConnection> CreateConnection()
		{
			MySqlConnection connection = new MySqlConnection(string.Format(
																"Data Source = {0};Port = {1};Username = {2};Password = \"{3}\";CharSet = utf8mb4;",
																host, port, username, password.Replace("\"", "\"\"")));
			if (Token == null)
				await connection.OpenAsync();
			else
				await connection.OpenAsync(Token.Value);
			TimeSpan timezone = TimeZoneInfo.Local.BaseUtcOffset;
			string sql = string.Format("SET time_zone = '{1}:{2:00}';CREATE DATABASE IF NOT EXISTS {0};USE {0};", database, (timezone.Hours >= 0 ? "+" : "") + timezone.Hours.ToString(), Math.Abs(timezone.Minutes));
			DataSource.Command command;
			using (Parameters parameters = CreateParameters())
				command = CreateCommand(sql, parameters);
			if (Token.HasValue)
				await command.Execute(connection, Token.Value);
			else
				await command.Execute(connection);
			return connection;
		}

		protected override void CloseConnection(DbConnection connection)
		{
			MySqlConnection mysql = connection as MySqlConnection;
			if (mysql != null)
				mysql.CloseAsync();
			else
				connection.Close();
		}

		protected override Parameters CreateParameters()
		{
			return new Command();
		}

		protected override DataSource.Command CreateCommand(string sql, Parameters parameters)
		{
			Command command = (Command)parameters;
			command.Retain();
			command.command.CommandText = sql;
			return command;
		}

		protected override async Task<bool> IsTableExist(DbConnection connection, DataTable.Define define)
		{
			MySqlCommand exist = new MySqlCommand(string.Format(
													"SELECT count(TABLE_NAME) as result FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}';",
													database, NameMapping(define)), (MySqlConnection)connection);
			try
			{
				using (var reader = Token == null
					? await exist.ExecuteReaderAsync()
					: await exist.ExecuteReaderAsync(Token.Value))
				{
					if (Token == null ? await reader.ReadAsync() : await reader.ReadAsync(Token.Value))
						return reader.GetInt32(0) != 0;
					return false;
				}
			}
			finally
			{
				exist.Dispose();
			}
		}

		protected override async Task BeginTransaction(DbConnection connection, Isolation level)
		{
			string str;
			switch (level)
			{
			case Isolation.Repeatable:
				str = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;BEGIN;";
				break;
			case Isolation.Lock:
				str = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;BEGIN;";
				break;
			default:
				str = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;BEGIN;";
				break;
			}
			MySqlCommand command = new MySqlCommand(str, (MySqlConnection)connection);
			try
			{
				if (Token == null)
					await command.ExecuteNonQueryAsync();
				else
					await command.ExecuteNonQueryAsync(Token.Value);
			}
			finally
			{
				command.Dispose();
			}
		}

		protected override async Task CommitTransaction(DbConnection connection)
		{
			MySqlCommand command = new MySqlCommand("COMMIT;", (MySqlConnection)connection);
			try
			{
				if (Token == null)
					await command.ExecuteNonQueryAsync();
				else
					await command.ExecuteNonQueryAsync(Token.Value);
			}
			finally
			{
				command.Dispose();
			}
		}

		protected override async Task RollbackTransaction(DbConnection connection)
		{
			MySqlCommand command = new MySqlCommand("ROLLBACK;", (MySqlConnection)connection);
			try
			{
				if (Token == null)
					await command.ExecuteNonQueryAsync();
				else
					await command.ExecuteNonQueryAsync(Token.Value);
			}
			finally
			{
				command.Dispose();
			}
		}

		protected override void LimitCount(StringBuilder builder, uint count)
		{
			builder.Append(" LIMIT ");
			builder.Append(count);
		}

		protected override void InsertOrUpdate(
			StringBuilder builder, Parameters parameters, DataTable.Define define, DataTable.Reader reader)
		{
			using (Buffer columns = Acquire(), values = Acquire())
			{
				int index = 0;
				for (int i = 0; i < define.Columns.Count; ++i)
				{
					DataTable.Column column = define.Columns[i];
					switch (column.type)
					{
					case DataTable.Type.Bool:
						{
							bool? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.Int:
						{
							int? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.UInt:
						{
							uint? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.Long:
						{
							long? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.ULong:
						{
							ulong? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.Double:
						{
							double? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.String:
						{
							string result = null;
							if (!reader.Get(i, ref result) || result == null)
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result, values.buffer);
						}
						break;
					case DataTable.Type.DateTime:
						{
							DateTime? result = null;
							if (!reader.Get(i, ref result))
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
								values.buffer.Append("NULL");
							else
								ToString(result.Value, values.buffer);
						}
						break;
					case DataTable.Type.Bytes:
						{
							byte[] result = null;
							if (!reader.Get(i, ref result) || result == null)
								continue;
							if (index != 0)
								values.buffer.Append(", ");
							if (result == null)
							{
								values.buffer.Append("NULL");
							}
							else
							{
								int count = parameters.Count;
								values.buffer.Append(Placeholder(count));
								parameters.Add(count, result);
							}
						}
						break;
					}
					if (index != 0)
						columns.buffer.Append(", ");
					columns.buffer.Append(EscapeColumn(column));
					++index;
				}
				builder.AppendFormat("REPLACE INTO {0} ({1}) VALUES ({2});", EscapeTable(NameMapping(define)),
									columns.buffer.ToString(), values.buffer.ToString());
			}
		}

		protected override string ColumnType(DataTable.Column column)
		{
			string notnull = column.key ? "" : (column.notnull ? " NOT NULL" : " NULL");
			switch (column.type)
			{
			case DataTable.Type.Bool:
				return "tinyint(1)" + notnull;
			case DataTable.Type.Int:
				return "int(32)" + notnull;
			case DataTable.Type.UInt:
				return "int(32) UNSIGNED" + notnull;
			case DataTable.Type.Long:
				return "bigint" + notnull;
			case DataTable.Type.ULong:
				return "bigint UNSIGNED" + notnull;
			case DataTable.Type.Double:
				return "double" + notnull;
			case DataTable.Type.String:
				return (column.key || column.index ? "varchar(255) CHARACTER SET latin1" : "longtext CHARACTER SET utf8mb4") + notnull;
			case DataTable.Type.DateTime:
				return "timestamp" + notnull + " DEFAULT CURRENT_TIMESTAMP";
			case DataTable.Type.Bytes:
				return (column.key || column.index ? "varbinary(255)" : "longblob") + notnull;
			default:
				throw new ArgumentOutOfRangeException(nameof(column));
			}
		}

		protected override string EscapeTable(string s)
		{
			return '`' + s + '`';
		}

		protected override string EscapeColumn(DataTable.Column column)
		{
			return '`' + column.name + '`';
		}

		protected override string Placeholder(int index)
		{
			return "?p" + index;
		}

		protected override void ToString(bool b, StringBuilder builder)
		{
			builder.Append(b ? "1" : "0");
		}

		protected override void ToString(int i, StringBuilder builder)
		{
			builder.Append(i);
		}

		protected override void ToString(uint u, StringBuilder builder)
		{
			builder.Append(u);
		}

		protected override void ToString(long l, StringBuilder builder)
		{
			builder.Append(l);
		}

		protected override void ToString(ulong u, StringBuilder builder)
		{
			builder.Append(u);
		}

		protected override void ToString(double d, StringBuilder builder)
		{
			builder.Append(d);
		}

		private static readonly char[] escapes = {'\\', '\''};

		protected override void ToString(string s, StringBuilder builder)
		{
			builder.Append('\'');
			int start = 0;
			while (true)
			{
				int index = s.IndexOfAny(escapes, start);
				if (index < 0)
					index = s.Length;
				builder.Append(s, start, index - start);
				if (index == s.Length)
					break;
				builder.Append('\\');
				builder.Append(s[index]);
				start = index + 1;
			}
			builder.Append('\'');
		}

		protected override void ToString(DateTime d, StringBuilder builder)
		{
			if (d.Kind == DateTimeKind.Utc)
				d = d.ToLocalTime();
			builder.AppendFormat("'{0:yyyy-MM-dd HH:mm:ss}.{1:000}'", d, d.Millisecond);
		}

		protected override uint? ReadUInt(DbDataReader reader, int index)
		{
			if (reader.IsDBNull(index))
				return null;
			return ((MySqlDataReader)reader).GetUInt32(index);
		}

		protected override ulong? ReadULong(DbDataReader reader, int index)
		{
			if (reader.IsDBNull(index))
				return null;
			return ((MySqlDataReader)reader).GetUInt64(index);
		}

		protected override DateTime? ReadDateTime(DbDataReader reader, int index)
		{
			if (reader.IsDBNull(index))
				return null;
			DateTime datetime = ((MySqlDataReader)reader).GetDateTime(index);
			if (datetime.Kind == DateTimeKind.Utc)
				return datetime.ToLocalTime();
			else if (datetime.Kind == DateTimeKind.Local)
				return datetime;
			else
				return datetime.ToUniversalTime().ToLocalTime();
		}

		private new class Command : Parameters, DataSource.Command
		{
			public readonly MySqlCommand command;
			private int refcount;

			public Command()
			{
				command = new MySqlCommand();
				refcount = 1;
			}

			public void Dispose()
			{
				if (Interlocked.Decrement(ref refcount) == 0)
					command.Dispose();
			}

			public void Retain()
			{
				Interlocked.Increment(ref refcount);
			}

			public void Add(int index, bool b)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.Int32,
					Value = b ? 1 : 0,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, int i)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.Int32,
					Value = i,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, uint u)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.UInt32,
					Value = u,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, long l)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.Int64,
					Value = l,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, ulong u)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.UInt64,
					Value = u,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, double d)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.Double,
					Value = d,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, string s)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.String,
					Value = s,
				};
				command.Parameters["p" + index] = parameter;
			}

			public void Add(int index, byte[] bytes)
			{
				MySqlParameter parameter = new MySqlParameter {
					DbType = DbType.Binary,
					Value = bytes,
				};
				command.Parameters["p" + index] = parameter;
			}

			public int Count
			{
				get { return command.Parameters.Count; }
			}

			public async Task Execute(DbConnection connection)
			{
				try
				{
					command.Connection = (MySqlConnection)connection;
					await command.ExecuteNonQueryAsync();
				}
				catch (MySqlException e)
				{
					if (e.Number == (int)MySqlErrorCode.LockDeadlock)
						throw new LockException(e);
					throw;
				}
			}

			public async Task Execute(DbConnection connection, CancellationToken token)
			{
				try
				{
					command.Connection = (MySqlConnection)connection;
					await command.ExecuteNonQueryAsync(token);
				}
				catch (MySqlException e)
				{
					if (e.Number == (int)MySqlErrorCode.LockDeadlock)
						throw new LockException(e);
					throw;
				}
			}

			public async Task<DbDataReader> Query(DbConnection connection)
			{
				try
				{
					command.Connection = (MySqlConnection)connection;
					return await command.ExecuteReaderAsync();
				}
				catch (MySqlException e)
				{
					switch (e.Number)
					{
					case (int)MySqlErrorCode.LockDeadlock:
						throw new LockException(e);
					case (int)MySqlErrorCode.LockWaitTimeout:
						throw new TimeoutException(e.Message, e);
					default:
						throw;
					}
				}
			}

			public async Task<DbDataReader> Query(DbConnection connection, CancellationToken token)
			{
				try
				{
					command.Connection = (MySqlConnection)connection;
					return await command.ExecuteReaderAsync(token);
				}
				catch (MySqlException e)
				{
					if (e.Number == (int)MySqlErrorCode.LockDeadlock)
						throw new LockException(e);
					throw;
				}
			}
		}
	}
}