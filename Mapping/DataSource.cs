using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace System
{
	public sealed class LockException : Exception
	{
		internal LockException(DbException exception) : base(exception.Message) { }
	}
}

namespace Mapping
{
	public abstract class DataSource : IDisposable
	{
		public enum Isolation
		{
			Default,
			Repeatable,
			Lock,
		}

		public sealed class Buffer : IDisposable
		{
			private readonly Pool pool;
			public readonly StringBuilder buffer;

			public Buffer(Pool pool)
			{
				this.pool = pool;
				this.buffer = new StringBuilder();
			}

			public void Dispose()
			{
				buffer.Clear();
				pool.Release(this);
			}

			public interface Pool
			{
				Buffer Acquire();
				void Release(Buffer buffer);
			}
		}

		public class Reader<T> : IDisposable where T : new()
		{
			private readonly DataTable.Define define;
			private readonly DataSource source;
			private readonly DbConnection connection;
			private readonly Action<DbConnection> close;
			private readonly DbDataReader reader;
			private readonly DataTable.Writer writer;
			private T value;

			public Reader(DataTable.Define define, DataSource source, DbConnection connection, Action<DbConnection> close, DbDataReader reader)
			{
				this.define = define;
				this.source = source;
				this.connection = connection;
				this.close = close;
				this.reader = reader;
				this.writer = DataTable.WriteOf<T>.Create(value =>
				{
					this.value = value;
					return true;
				});
			}

			public T Value
			{
				get { return value; }
			}

			public async Task<bool> Next()
			{
				if (source.Token == null ? !await reader.ReadAsync() : !await reader.ReadAsync(source.Token.Value))
					return false;
				writer.Prepare();
				for (int i = 0; i < reader.FieldCount; ++i)
				{
					int index;
					if (define.Names.TryGetValue(reader.GetName(i), out index))
					{
						DataTable.Column column = define.Columns[index];
						switch (column.type)
						{
						case DataTable.Type.Bool:
							writer.Set(index, source.ReadBoolean(reader, i));
							break;
						case DataTable.Type.Int:
							writer.Set(index, source.ReadInt(reader, i));
							break;
						case DataTable.Type.UInt:
							writer.Set(index, source.ReadUInt(reader, i));
							break;
						case DataTable.Type.Long:
							writer.Set(index, source.ReadLong(reader, i));
							break;
						case DataTable.Type.ULong:
							writer.Set(index, source.ReadULong(reader, i));
							break;
						case DataTable.Type.Double:
							writer.Set(index, source.ReadDouble(reader, i));
							break;
						case DataTable.Type.String:
							writer.Set(index, source.ReadString(reader, i));
							break;
						case DataTable.Type.DateTime:
							writer.Set(index, source.ReadDateTime(reader, i));
							break;
						case DataTable.Type.Bytes:
							writer.Set(index, source.ReadBytes(reader, i));
							break;
						}
					}
				}
				return writer.Commit();
			}

			public void Dispose()
			{
				reader.Dispose();
				close(connection);
			}
		}

		public class Transaction : IDisposable
		{
			private readonly DataSource _datasource;
			private readonly DbConnection _connection;
			private readonly Func<Task<DbConnection>> _create;
			private int _commit;

			private static readonly Action<DbConnection> _close;

			static Transaction()
			{
				_close = _ => { };
			}

			public Transaction(DataSource datasource, DbConnection connection)
			{
				_datasource = datasource;
				_connection = connection;
				var task = AsyncTaskMethodBuilder<DbConnection>.Create();
				task.SetResult(connection);
				_create = () => task.Task;
				_commit = 0;
			}

			public async void Dispose()
			{
				if (Interlocked.CompareExchange(ref _commit, 1, 0) == 0)
					await _datasource.RollbackTransaction(_connection);
				_datasource.CloseConnection(_connection);
			}

			public async Task Commit()
			{
				if (Interlocked.CompareExchange(ref _commit, 1, 0) == 0)
					await _datasource.CommitTransaction(_connection);
			}

			public async Task Save<T>(T value)
			{
				Command command = _datasource.Save(DataTable.DefineOf<T>.Define, DataTable.ReadOf<T>.Create(value));
				using (command)
				{
					await _datasource.Execute(command, _create, _close);
				}
			}

			public async Task Delete<T>(Expression<Func<T, bool>> predicate)
			{
				Command command = _datasource.Delete(DataTable.DefineOf<T>.Define, predicate);
				using (command)
				{
					await _datasource.Execute(command, _create, _close);
				}
			}

			public async Task<Reader<T>> Search<T>(Expression<Func<T, bool>> predicate) where T : new()
			{
				DataTable.Define define = DataTable.DefineOf<T>.Define;
				Command command = _datasource.Search(define, predicate, null);
				using (command)
				{
					return await _datasource.Query<T>(define, command, _create, _close);
				}
			}

			public async Task<Reader<T>> Search<T>(Expression<Func<T, bool>> predicate, uint count) where T : new()
			{
				DataTable.Define define = DataTable.DefineOf<T>.Define;
				Command command = _datasource.Search(define, predicate, count);
				using (command)
				{
					return await _datasource.Query<T>(define, command, _create, _close);
				}
			}

			public async Task Update<T>(Expression<Func<T, T>> expr, Expression<Func<T, bool>> predicate)
				where T : new()
			{
				Command command = _datasource.Update(DataTable.DefineOf<T>.Define, expr, predicate);
				using (command)
				{
					await _datasource.Execute(command, _create, _close);
				}
			}
		}

		protected interface Parameters : IDisposable
		{
			void Add(int index, bool b);
			void Add(int index, int i);
			void Add(int index, uint u);
			void Add(int index, long l);
			void Add(int index, ulong u);
			void Add(int index, double d);
			void Add(int index, string s);
			void Add(int index, byte[] bytes);
			int Count { get; }
		}

		protected interface Command : IDisposable
		{
			Task Execute(DbConnection connection);
			Task Execute(DbConnection connection, CancellationToken token);
			Task<DbDataReader> Query(DbConnection connection);
			Task<DbDataReader> Query(DbConnection connection, CancellationToken token);
		}

		public Buffer.Pool Pool;
		public CancellationToken? Token;

		private static readonly HashSet<MethodInfo> commits;

		static DataSource()
		{
			Type type = typeof(DataSource);
			commits = new HashSet<MethodInfo> {
				type.GetMethod("Save"),
				type.GetMethod("Update"),
				type.GetMethod("Delete"),
			};
		}

		protected DataSource(Func<Type, string, string> mapping)
		{
			_create = CreateConnection;
			_close = CloseConnection;
			_mapping = mapping;
			columnnamefactory = define =>
			{
				ColumnString str = new ColumnString();
				using (Buffer buffer = Acquire())
				{
					for (int i = 0; i < define.Columns.Count; ++i)
					{
						if (i != 0)
							buffer.buffer.Append(", ");
						buffer.buffer.Append(EscapeColumn(define.Columns[i].name));
					}
					str.Columns = buffer.buffer.ToString();
					buffer.buffer.Clear();
					if (define.Orders.Count != 0)
					{
						buffer.buffer.Append(" ORDER BY ");
						for (int i = 0; i < define.Orders.Count; ++i)
						{
							if (i != 0)
								buffer.buffer.Append(", ");
							DataTable.ColumnOrder order = define.Orders[i];
							buffer.buffer.AppendFormat("{0} {1}", EscapeColumn(define.Columns[order.index].name),
														order.order == DataTable.Order.DESC ? "DESC" : "ASC");
						}
					}
					str.Orders = buffer.buffer.ToString();
				}
				return str;
			};
		}

		~DataSource()
		{
			Dispose(false);
		}

		void IDisposable.Dispose()
		{
			Close();
		}

		public void Close()
		{
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		public Task Initialize<T>()
		{
			return Initialize(DataTable.DefineOf<T>.Define);
		}

		public Task<Transaction> Begin()
		{
			return Begin(Isolation.Default);
		}

		public async Task<Transaction> Begin(Isolation level)
		{
			DbConnection connection = await CreateConnection();
			await BeginTransaction(connection, level);
			return new Transaction(this, connection);
		}

		public async Task Execute(string sql)
		{
			Command command;
			using (Parameters parameters = CreateParameters())
				command = CreateCommand(sql, parameters);
			using (command)
			{
				await Execute(command);
			}
		}

		public async Task<Reader<T>> Query<T>(string sql) where T : new()
		{
			Command command;
			using (Parameters parameters = CreateParameters())
				command = CreateCommand(sql, parameters);
			using (command)
			{
				return await Query<T>(DataTable.DefineOf<T>.Define, command);
			}
		}

		public async Task Save<T>(T value)
		{
			Command command = Save(DataTable.DefineOf<T>.Define, DataTable.ReadOf<T>.Create(value));
			using (command)
			{
				await Execute(command);
			}
		}

		public async Task Update<T>(Expression<Func<T, T>> expr, Expression<Func<T, bool>> predicate)
		{
			Command command = Update(DataTable.DefineOf<T>.Define, expr, predicate);
			using (command)
			{
				await Execute(command);
			}
		}

		public async Task Delete<T>(Expression<Func<T, bool>> predicate)
		{
			Command command = Delete(DataTable.DefineOf<T>.Define, predicate);
			using (command)
			{
				await Execute(command);
			}
		}

		public async Task<Reader<T>> Search<T>(Expression<Func<T, bool>> predicate) where T : new()
		{
			DataTable.Define define = DataTable.DefineOf<T>.Define;
			Command command = Search(define, predicate, null);
			using (command)
			{
				return await Query<T>(define, command);
			}
		}

		public async Task<Reader<T>> Search<T>(Expression<Func<T, bool>> predicate, uint count) where T : new()
		{
			DataTable.Define define = DataTable.DefineOf<T>.Define;
			Command command = Search(define, predicate, count);
			using (command)
			{
				return await Query<T>(define, command);
			}
		}

		public Task Commit(params Expression<Func<DataSource, Task>>[] tasks)
		{
			return Commit((IList<Expression<Func<DataSource, Task>>>)tasks);
		}

		public async Task Commit(IList<Expression<Func<DataSource, Task>>> tasks)
		{
			List<Command> commands = new List<Command>(tasks.Count);
			for (int i = 0; i < tasks.Count; ++i)
			{
				Expression<Func<DataSource, Task>> task = tasks[i];
				if (task.Body.NodeType != ExpressionType.Call)
					throw new ArgumentException();
				MethodCallExpression call = task.Body as MethodCallExpression;
				if (call == null || !call.Method.IsGenericMethod || !commits.Contains(call.Method.GetGenericMethodDefinition()))
					throw new ArgumentException();
				MethodInfo method = call.Method;
				Type type = method.GetGenericArguments()[0];
				DataTable.Define define = DataTable.Define.Get(type);
				switch (method.Name)
				{
				case "Save":
					{
						Delegate action = Expression.Lambda(typeof(Func<>).MakeGenericType(type), call.Arguments[0]).Compile();
						DataTable.Reader reader = DataTable.Define.CreateReader(type, action);
						commands.Add(Save(define, reader));
					}
					break;
				case "Update":
					{
						Expression expr = call.Arguments[0];
						if (expr.NodeType == ExpressionType.Quote)
							expr = ((UnaryExpression)expr).Operand;
						Expression predicate = call.Arguments[1];
						if (predicate.NodeType == ExpressionType.Quote)
							predicate = ((UnaryExpression)predicate).Operand;
						commands.Add(Update(define, (LambdaExpression)expr, (LambdaExpression)predicate));
					}
					break;
				case "Delete":
					{
						Expression predicate = call.Arguments[0];
						if (predicate.NodeType == ExpressionType.Quote)
							predicate = ((UnaryExpression)predicate).Operand;
						commands.Add(Delete(define, (LambdaExpression)predicate));
					}
					break;
				}
			}
			DbConnection connection = await CreateConnection();
			await BeginTransaction(connection, Isolation.Default);
			try
			{
				if (Token.HasValue)
				{
					CancellationToken token = Token.Value;
					for (int i = 0, j = commands.Count; i < j; ++i)
						await commands[i].Execute(connection, token);
				}
				else
				{
					for (int i = 0, j = commands.Count; i < j; ++i)
						await commands[i].Execute(connection);
				}
				await CommitTransaction(connection);
			}
			catch (Exception)
			{
				await RollbackTransaction(connection);
				throw;
			}
			finally
			{
				CloseConnection(connection);
			}
		}

		protected abstract void Dispose(bool disposing);

		protected Buffer Acquire()
		{
			return (Pool ?? defaultpool).Acquire();
		}

		public async Task Initialize(DataTable.Define define)
		{
			DbConnection connection = await CreateConnection();
			try
			{
				if (await IsTableExist(connection, define))
					return;
				await BeginTransaction(connection, Isolation.Default);
				using (Buffer buffer = Acquire())
				{
					string name = EscapeTable(NameMapping(define));
					buffer.buffer.AppendFormat("CREATE TABLE {0} (", name);
					for (int i = 0; i < define.Columns.Count; ++i)
					{
						if (i != 0)
							buffer.buffer.Append(", ");
						DataTable.Column column = define.Columns[i];
						buffer.buffer.AppendFormat("{0} {1}", EscapeColumn(column.name), ColumnType(column));
						if (column.type != DataTable.Type.String && column.type != DataTable.Type.Bytes)
							buffer.buffer.Append(" NOT NULL");
					}
					if (define.Keys.Count != 0)
					{
						buffer.buffer.Append(", PRIMARY KEY (");
						for (int i = 0; i < define.Keys.Count; ++i)
						{
							if (i != 0)
								buffer.buffer.Append(", ");
							DataTable.Column column = define.Columns[define.Keys[i]];
							buffer.buffer.Append(EscapeColumn(column.name));
						}
						buffer.buffer.Append(")");
					}
					buffer.buffer.Append(");");
					for (int i = 0; i < define.Indexs.Count; ++i)
					{
						buffer.buffer.AppendFormat("\nCREATE INDEX {0} ON {1} (",
													EscapeTable(NameMapping(define) + "#" + i.ToString()), name);
						IReadOnlyList<int> index = define.Indexs[i];
						for (int j = 0; j < index.Count; ++j)
						{
							if (j != 0)
								buffer.buffer.Append(", ");
							DataTable.Column column = define.Columns[index[j]];
							buffer.buffer.Append(EscapeColumn(column.name));
						}
						buffer.buffer.Append(");");
					}
					Command command;
					using (Parameters parameters = CreateParameters())
						command = CreateCommand(buffer.buffer.ToString(), parameters);
					using (command)
					{
						if (Token.HasValue)
							await command.Execute(connection, Token.Value);
						else
							await command.Execute(connection);
					}
				}
				await CommitTransaction(connection);
			}
			finally
			{
				CloseConnection(connection);
			}
		}

		private readonly Func<Type, string, string> _mapping;

		protected string NameMapping(DataTable.Define define)
		{
			return _mapping != null ? _mapping(define.Type, define.Name) : define.Name;
		}

		private readonly Func<Task<DbConnection>> _create;
		private readonly Action<DbConnection> _close;

		private Task Execute(Command command)
		{
			return Execute(command, _create, _close);
		}

		private async Task Execute(Command command, Func<Task<DbConnection>> create, Action<DbConnection> close)
		{
			DbConnection connection = await create();
			try
			{
				if (Token.HasValue)
					await command.Execute(connection, Token.Value);
				else
					await command.Execute(connection);
			}
			finally
			{
				close(connection);
			}
		}

		private Task<Reader<T>> Query<T>(DataTable.Define define, Command command) where T : new()
		{
			return Query<T>(define, command, _create, _close);
		}

		private async Task<Reader<T>> Query<T>(
			DataTable.Define define, Command command,
			Func<Task<DbConnection>> create, Action<DbConnection> close) where T : new()
		{
			DbConnection connection = await create();
			try
			{
				return new Reader<T>(define, this, connection, close, Token.HasValue
										? await command.Query(connection, Token.Value)
										: await command.Query(connection));
			}
			catch
			{
				close(connection);
				throw;
			}
		}

		private Command New(DataTable.Define define, DataTable.Reader reader)
		{
			Command command;
			using (Buffer buffer = Acquire())
			{
				using (Parameters parameters = CreateParameters())
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
									bool result = false;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.Int:
								{
									int result = 0;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.UInt:
								{
									uint result = 0;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.Long:
								{
									long result = 0;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.ULong:
								{
									ulong result = 0;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.Double:
								{
									double result = 0;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.String:
								{
									string result = null;
									if (!reader.Get(i, ref result) || result == null)
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.DateTime:
								{
									DateTime result = DateTime.UtcNow;
									if (!reader.Get(i, ref result))
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									ToString(result, values.buffer);
								}
								break;
							case DataTable.Type.Bytes:
								{
									byte[] result = null;
									if (!reader.Get(i, ref result) || result == null)
										continue;
									if (index != 0)
										values.buffer.Append(", ");
									int count = parameters.Count;
									values.buffer.Append(Placeholder(count));
									parameters.Add(count, result);
								}
								break;
							}
							if (index != 0)
								columns.buffer.Append(", ");
							columns.buffer.Append(EscapeColumn(column.name));
							++index;
						}
						buffer.buffer.AppendFormat("INSERT INTO {0} ({1}) VALUES ({2});", EscapeTable(NameMapping(define)),
													columns.buffer.ToString(), values.buffer.ToString());
					}
					command = CreateCommand(buffer.buffer.ToString(), parameters);
				}
			}
			return command;
		}

		private Command Set(DataTable.Define define, DataTable.Reader reader)
		{
			Command command;
			using (Buffer buffer = Acquire())
			{
				using (Parameters parameters = CreateParameters())
				{
					InsertOrUpdate(buffer.buffer, parameters, define, reader);
					command = CreateCommand(buffer.buffer.ToString(), parameters);
				}
			}
			return command;
		}

		private Command Save(DataTable.Define define, DataTable.Reader reader)
		{
			return define.Keys.Count == 0 ? New(define, reader) : Set(define, reader);
		}

		private Command Update(DataTable.Define define, LambdaExpression expr, LambdaExpression predicate)
		{
			Command command;
			using (Buffer buffer = Acquire())
			{
				using (Parameters parameters = CreateParameters())
				{
					buffer.buffer.AppendFormat("UPDATE {0} SET ", EscapeTable(NameMapping(define)));
					Set(define, expr, buffer.buffer, parameters);
					buffer.buffer.Append(' ');
					Where(define, predicate, buffer.buffer, parameters);
					buffer.buffer.Append(';');
					command = CreateCommand(buffer.buffer.ToString(), parameters);
				}
			}
			return command;
		}

		private Command Delete(DataTable.Define define, LambdaExpression predicate)
		{
			Command command;
			using (Buffer buffer = Acquire())
			{
				using (Parameters parameters = CreateParameters())
				{
					buffer.buffer.AppendFormat("DELETE FROM {0} ", EscapeTable(NameMapping(define)));
					Where(define, predicate, buffer.buffer, parameters);
					buffer.buffer.Append(';');
					command = CreateCommand(buffer.buffer.ToString(), parameters);
				}
			}
			return command;
		}

		private Command Search(DataTable.Define define, LambdaExpression predicate, uint? count)
		{
			Command command;
			using (Buffer buffer = Acquire())
			{
				using (Parameters parameters = CreateParameters())
				{
					ColumnString str = GetColumnString(define);
					buffer.buffer.AppendFormat("SELECT {0} FROM {1} ", str.Columns, EscapeTable(NameMapping(define)));
					Where(define, predicate, buffer.buffer, parameters);
					buffer.buffer.Append(str.Orders);
					if (count.HasValue)
						LimitCount(buffer.buffer, count.Value);
					buffer.buffer.Append(';');
					command = CreateCommand(buffer.buffer.ToString(), parameters);
				}
			}
			return command;
		}

		protected abstract Task<DbConnection> CreateConnection();
		protected abstract void CloseConnection(DbConnection connection);
		protected abstract Parameters CreateParameters();
		protected abstract Command CreateCommand(string sql, Parameters parameters);
		protected abstract Task<bool> IsTableExist(DbConnection connection, DataTable.Define define);
		protected abstract Task BeginTransaction(DbConnection connection, Isolation level);
		protected abstract Task CommitTransaction(DbConnection connection);
		protected abstract Task RollbackTransaction(DbConnection connection);
		protected abstract void LimitCount(StringBuilder builder, uint count);

		protected abstract void InsertOrUpdate(
			StringBuilder builder, Parameters parameters, DataTable.Define define, DataTable.Reader reader);

		protected abstract string ColumnType(DataTable.Column column);
		protected abstract string EscapeTable(string s);
		protected abstract string EscapeColumn(string s);
		protected abstract string Placeholder(int index);
		protected abstract void ToString(bool b, StringBuilder builder);
		protected abstract void ToString(int i, StringBuilder builder);
		protected abstract void ToString(uint u, StringBuilder builder);
		protected abstract void ToString(long l, StringBuilder builder);
		protected abstract void ToString(ulong u, StringBuilder builder);
		protected abstract void ToString(double d, StringBuilder builder);
		protected abstract void ToString(string s, StringBuilder builder);
		protected abstract void ToString(DateTime d, StringBuilder builder);

		protected virtual bool ReadBoolean(DbDataReader reader, int index)
		{
			return reader.GetBoolean(index);
		}

		protected virtual int ReadInt(DbDataReader reader, int index)
		{
			return reader.GetInt32(index);
		}

		protected virtual uint ReadUInt(DbDataReader reader, int index)
		{
			return (uint)reader.GetInt32(index);
		}

		protected virtual long ReadLong(DbDataReader reader, int index)
		{
			return reader.GetInt64(index);
		}

		protected virtual ulong ReadULong(DbDataReader reader, int index)
		{
			return (ulong)reader.GetInt64(index);
		}

		protected virtual double ReadDouble(DbDataReader reader, int index)
		{
			return reader.GetDouble(index);
		}

		protected virtual string ReadString(DbDataReader reader, int index)
		{
			if (reader.IsDBNull(index))
				return null;
			return reader.GetString(index);
		}

		protected virtual DateTime ReadDateTime(DbDataReader reader, int index)
		{
			return reader.GetDateTime(index);
		}

		protected virtual byte[] ReadBytes(DbDataReader reader, int index)
		{
			if (reader.IsDBNull(index))
				return null;
			long length = reader.GetBytes(index, 0, null, 0, 0);
			byte[] result = new byte[length];
			if (reader.GetBytes(index, 0, result, 0, result.Length) != length)
				throw new InvalidCastException();
			return result;
		}

		private void Set(
			DataTable.Define define, LambdaExpression expression, StringBuilder builder, Parameters parameters)
		{
			if (expression.Body.NodeType == ExpressionType.MemberInit)
			{
				MemberInitExpression init = (MemberInitExpression)expression.Body;
				if (init.Bindings.Count != 0)
				{
					for (int i = 0; i < init.Bindings.Count; ++i)
					{
						if (i != 0)
							builder.Append(", ");
						MemberAssignment member = (MemberAssignment)init.Bindings[i];
						builder.Append(EscapeColumn(define.Columns[define.Members[member.Member]].name));
						builder.Append(" = ");
						ToString(define, member.Expression, builder, parameters);
					}
					return;
				}
			}
			throw new ArgumentException();
		}

		private void Where(
			DataTable.Define define, LambdaExpression expression, StringBuilder builder, Parameters parameters)
		{
			builder.Append("WHERE ");
			ToString(define, expression.Body, builder, parameters);
		}

		private ColumnString GetColumnString(DataTable.Define define)
		{
			return columnnames.GetOrAdd(define, columnnamefactory);
		}

		private void ToString(
			DataTable.Define define, Expression expression, StringBuilder builder, Parameters parameters)
		{
			switch (expression.NodeType)
			{
			case ExpressionType.MemberAccess:
				{
					MemberExpression member = (MemberExpression)expression;
					if (member.Expression.NodeType == ExpressionType.Parameter)
					{
						builder.Append(EscapeColumn(define.Columns[define.Members[member.Member]].name));
						return;
					}
				}
				break;
			case ExpressionType.Conditional:
				{
					ConditionalExpression condition = (ConditionalExpression)expression;
					builder.Append("(");
					ToString(define, condition.Test, builder, parameters);
					builder.Append(" AND ");
					ToString(define, condition.IfTrue, builder, parameters);
					builder.Append(" OR NOT ");
					ToString(define, condition.Test, builder, parameters);
					builder.Append(" AND ");
					ToString(define, condition.IfFalse, builder, parameters);
					builder.Append(")");
				}
				return;
			case ExpressionType.Not:
				{
					UnaryExpression unary = (UnaryExpression)expression;
					builder.Append("NOT ");
					ToString(define, unary.Operand, builder, parameters);
				}
				return;
			case ExpressionType.Constant:
				{
					ConstantExpression constant = (ConstantExpression)expression;
					if (constant.Value == null)
					{
						builder.Append("NULL");
					}
					else
					{
						switch (Type.GetTypeCode(constant.Type))
						{
						case TypeCode.Boolean:
							ToString((bool)constant.Value, builder);
							break;
						case TypeCode.Byte:
							ToString((uint)(byte)constant.Value, builder);
							break;
						case TypeCode.SByte:
							ToString((sbyte)constant.Value, builder);
							break;
						case TypeCode.Int16:
							ToString((short)constant.Value, builder);
							break;
						case TypeCode.UInt16:
							ToString((uint)(ushort)constant.Value, builder);
							break;
						case TypeCode.Int32:
							ToString((int)constant.Value, builder);
							break;
						case TypeCode.UInt32:
							ToString((uint)constant.Value, builder);
							break;
						case TypeCode.Int64:
							ToString((long)constant.Value, builder);
							break;
						case TypeCode.UInt64:
							ToString((ulong)constant.Value, builder);
							break;
						case TypeCode.Single:
							ToString((float)constant.Value, builder);
							break;
						case TypeCode.Decimal:
							ToString((double)(decimal)constant.Value, builder);
							break;
						case TypeCode.Double:
							ToString((double)constant.Value, builder);
							break;
						case TypeCode.Char:
							ToString(new string((char)constant.Value, 1), builder);
							break;
						case TypeCode.String:
							ToString((string)constant.Value, builder);
							break;
						default:
							if (expression.Type != typeof(byte[]))
								throw new ArgumentException();
							byte[] bytes = (byte[])constant.Value;
							int index = parameters.Count;
							builder.Append(Placeholder(index));
							parameters.Add(index, bytes);
							break;
						}
					}
				}
				return;
			case ExpressionType.Call:
				{
					MethodCallExpression call = (MethodCallExpression)expression;
					if (call.Method.DeclaringType == typeof(Extensions))
					{
						string name = call.Method.Name;
						if (name == "Match")
						{
							builder.Append("(");
							ToString(define, call.Arguments[0], builder, parameters);
							builder.Append(" LIKE ");
							ToString(define, call.Arguments[1], builder, parameters);
							if (call.Arguments.Count == 3)
							{
								builder.Append(" ESCAPE ");
								ToString(define, call.Arguments[2], builder, parameters);
							}
							builder.Append(")");
							return;
						}
						if (name == "In")
						{
							builder.Append("(");
							ToString(define, call.Arguments[0], builder, parameters);
							builder.Append(" IN (");
							Type type = call.Arguments[1].Type;
							type = type.IsArray ? type.GetElementType() : type.GetGenericArguments()[0];
							switch (Type.GetTypeCode(type))
							{
							case TypeCode.Int32:
								{
									var list = ExecuteValue<IList<int>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										ToString(list[i], builder);
									}
								}
								break;
							case TypeCode.UInt32:
								{
									var list = ExecuteValue<IList<uint>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										ToString(list[i], builder);
									}
								}
								break;
							case TypeCode.Int64:
								{
									var list = ExecuteValue<IList<long>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										ToString(list[i], builder);
									}
								}
								break;
							case TypeCode.UInt64:
								{
									var list = ExecuteValue<IList<ulong>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										ToString(list[i], builder);
									}
								}
								break;
							case TypeCode.Double:
								{
									var list = ExecuteValue<IList<double>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										ToString(list[i], builder);
									}
								}
								break;
							case TypeCode.String:
								{
									var list = ExecuteValue<IList<string>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										if (list[i] == null)
											builder.Append("NULL");
										else
											ToString(list[i], builder);
									}
								}
								break;
							default:
								{
									var list = ExecuteValue<IList<byte[]>>(call.Arguments[1]);
									for (int i = 0; i < list.Count; ++i)
									{
										if (i != 0)
											builder.Append(", ");
										if (list[i] == null)
										{
											builder.Append("NULL");
										}
										else
										{
											int index = parameters.Count;
											builder.Append(Placeholder(index));
											parameters.Add(index, list[i]);
										}
									}
								}
								break;
							}
							builder.Append("))");
							return;
						}
					}
				}
				break;
			default:
				{
					BinaryExpression binary = expression as BinaryExpression;
					if (binary != null)
					{
						Expression left = binary.Left;
						Expression right = binary.Right;
						string op = GetOp(expression.NodeType, ref left, ref right);
						if (op != null)
						{
							builder.Append("(");
							ToString(define, left, builder, parameters);
							builder.Append(op);
							ToString(define, right, builder, parameters);
							builder.Append(")");
							return;
						}
					}
				}
				break;
			}
			switch (Type.GetTypeCode(expression.Type))
			{
			case TypeCode.Boolean:
				ToString(ExecuteValue<bool>(expression), builder);
				break;
			case TypeCode.Byte:
				ToString((uint)ExecuteValue<byte>(expression), builder);
				break;
			case TypeCode.SByte:
				ToString(ExecuteValue<sbyte>(expression), builder);
				break;
			case TypeCode.Int16:
				ToString(ExecuteValue<short>(expression), builder);
				break;
			case TypeCode.UInt16:
				ToString((uint)ExecuteValue<ushort>(expression), builder);
				break;
			case TypeCode.Int32:
				ToString(ExecuteValue<int>(expression), builder);
				break;
			case TypeCode.UInt32:
				ToString(ExecuteValue<uint>(expression), builder);
				break;
			case TypeCode.Int64:
				ToString(ExecuteValue<long>(expression), builder);
				break;
			case TypeCode.UInt64:
				ToString(ExecuteValue<ulong>(expression), builder);
				break;
			case TypeCode.Single:
				ToString(ExecuteValue<float>(expression), builder);
				break;
			case TypeCode.Decimal:
				ToString((double)ExecuteValue<decimal>(expression), builder);
				break;
			case TypeCode.Double:
				ToString(ExecuteValue<double>(expression), builder);
				break;
			case TypeCode.Char:
				ToString(new string(ExecuteValue<char>(expression), 1), builder);
				break;
			case TypeCode.String:
				{
					string s = ExecuteValue<string>(expression);
					if (s == null)
						builder.Append("NULL");
					else
						ToString(s, builder);
				}
				break;
			default:
				if (expression.Type != typeof(byte[]))
					throw new ArgumentException();
				byte[] bytes = ExecuteValue<byte[]>(expression);
				if (bytes == null)
				{
					builder.Append("NULL");
				}
				else
				{
					int index = parameters.Count;
					builder.Append(Placeholder(index));
					parameters.Add(index, bytes);
				}
				break;
			}
		}

		private static string GetOp(ExpressionType type, ref Expression left, ref Expression right)
		{
			switch (type)
			{
			case ExpressionType.AndAlso:
				return " AND ";
			case ExpressionType.OrElse:
				return " OR ";
			case ExpressionType.LessThan:
				return " < ";
			case ExpressionType.LessThanOrEqual:
				return " <= ";
			case ExpressionType.GreaterThan:
				return " > ";
			case ExpressionType.GreaterThanOrEqual:
				return " >= ";
			case ExpressionType.Add:
			case ExpressionType.AddChecked:
				return " + ";
			case ExpressionType.Subtract:
			case ExpressionType.SubtractChecked:
				return " - ";
			case ExpressionType.Multiply:
			case ExpressionType.MultiplyChecked:
				return " * ";
			case ExpressionType.Divide:
				return " / ";
			case ExpressionType.Modulo:
				return " % ";
			case ExpressionType.Equal:
				{
					bool isnull = false;
					if (left.NodeType == ExpressionType.Constant && ((ConstantExpression)left).Value == null)
					{
						var tmp = left;
						left = right;
						right = tmp;
						isnull = true;
					}
					else if (right.NodeType == ExpressionType.Constant && ((ConstantExpression)right).Value == null)
					{
						isnull = true;
					}
					return isnull ? " IS " : " = ";
				}
			case ExpressionType.NotEqual:
				{
					bool isnull = false;
					if (left.NodeType == ExpressionType.Constant && ((ConstantExpression)left).Value == null)
					{
						var tmp = left;
						left = right;
						right = tmp;
						isnull = true;
					}
					else if (right.NodeType == ExpressionType.Constant && ((ConstantExpression)right).Value == null)
					{
						isnull = true;
					}
					return isnull ? " IS NOT " : " <> ";
				}
			}
			return null;
		}

		private static T ExecuteValue<T>(Expression expression)
		{
			return Expression.Lambda<Func<T>>(expression).Compile()();
		}

		private static readonly Buffer.Pool defaultpool = new DefaultPool();

		private readonly ConcurrentDictionary<DataTable.Define, ColumnString> columnnames =
			new ConcurrentDictionary<DataTable.Define, ColumnString>();
		private readonly Func<DataTable.Define, ColumnString> columnnamefactory;

		private class DefaultPool : Buffer.Pool
		{
			private readonly ConcurrentQueue<Buffer> buffers = new ConcurrentQueue<Buffer>();

			public Buffer Acquire()
			{
				Buffer buffer;
				if (!buffers.TryDequeue(out buffer))
					buffer = new Buffer(this);
				return buffer;
			}

			public void Release(Buffer buffer)
			{
				buffers.Enqueue(buffer);
			}
		}

		private struct ColumnString
		{
			public string Columns;
			public string Orders;
		}
	}
}