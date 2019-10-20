using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Serialization.Formatters.Binary;
using SystemType = System.Type;

namespace Mapping
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
	public class DataTableAttribute : Attribute
	{
		public readonly string Name;

		public DataTableAttribute() : this(null) { }

		public DataTableAttribute(string name)
		{
			Name = name;
		}
	}

	public static class DataTable
	{
		public enum Type
		{
			NULL,
			Bool,
			Int,
			UInt,
			Long,
			ULong,
			Double,
			String,
			DateTime,
			Bytes,
		}

		public enum Order
		{
			NOT,
			ASC,
			DESC,
		}

		public struct Column
		{
			public string name;
			public Type type;
			public bool key;
			public bool index;
		}

		public struct ColumnOrder
		{
			public int index;
			public Order order;
		}

		[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
		public class ColumnAttribute : Attribute
		{
			public Type Type;
			public Order Order;
			public readonly string Name;

			public ColumnAttribute() : this(null) { }

			public ColumnAttribute(string name)
			{
				Type = Type.NULL;
				Order = Order.NOT;
				Name = name;
			}
		}

		[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
		public class KeyAttribute : ColumnAttribute
		{
			public KeyAttribute() : base(null) { }
			public KeyAttribute(string name) : base(name) { }
		}

		[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
		public class IndexAttribute : Attribute
		{
			public readonly uint Group;

			public IndexAttribute(uint group)
			{
				Group = group;
			}
		}

		public class Define
		{
			public readonly string Name;
			public readonly SystemType Type;
			public readonly IReadOnlyList<Column> Columns;
			public readonly IReadOnlyDictionary<string, int> Names;
			public readonly IReadOnlyDictionary<MemberInfo, int> Members;
			public readonly IReadOnlyList<int> Keys;
			public readonly IReadOnlyList<IReadOnlyList<int>> Indexs;
			public readonly IReadOnlyList<ColumnOrder> Orders;

			public Define(
				string name, SystemType type, IReadOnlyList<Column> columns, IReadOnlyDictionary<string, int> names,
				IReadOnlyDictionary<MemberInfo, int> members, IReadOnlyList<int> keys,
				IReadOnlyList<IReadOnlyList<int>> indexs, IReadOnlyList<ColumnOrder> orders)
			{
				Name = name;
				Type = type;
				Columns = columns;
				Names = names;
				Members = members;
				Keys = keys;
				Indexs = indexs;
				Orders = orders;
			}

			private static readonly ConcurrentDictionary<SystemType, Define> defines =
				new ConcurrentDictionary<SystemType, Define>();
			private static readonly Func<SystemType, Define> defineFactory = type =>
				typeof(DefineOf<>).MakeGenericType(type)
								.GetField("Define", BindingFlags.Static | BindingFlags.Public)
								.GetValue(null) as Define;

			private static readonly ConcurrentDictionary<SystemType, Func<Delegate, Reader>> readers =
				new ConcurrentDictionary<SystemType, Func<Delegate, Reader>>();
			private static readonly ConcurrentDictionary<SystemType, Func<Delegate, Writer>> writers =
				new ConcurrentDictionary<SystemType, Func<Delegate, Writer>>();
			private static readonly Func<SystemType, Func<Delegate, Reader>> readerFactory = type =>
				typeof(Read<>).MakeGenericType(type)
							.GetField("Factory", BindingFlags.Static | BindingFlags.Public)
							.GetValue(null) as Func<Delegate, Reader>;
			private static readonly Func<SystemType, Func<Delegate, Writer>> writerFactory = type =>
				typeof(Write<>).MakeGenericType(type)
							.GetField("Factory", BindingFlags.Static | BindingFlags.Public)
							.GetValue(null) as Func<Delegate, Writer>;

			public static Define Get(SystemType type)
			{
				return defines.GetOrAdd(type, defineFactory);
			}

			public static Reader CreateReader(SystemType type, Delegate action)
			{
				Func<Delegate, Reader> creator = readers.GetOrAdd(type, readerFactory);
				return creator(action);
			}

			public static Writer CreateWriter(SystemType type, Delegate action)
			{
				Func<Delegate, Writer> creator = writers.GetOrAdd(type, writerFactory);
				return creator(action);
			}

			private static class Read<T>
			{
				public static Func<Delegate, Reader> Factory = action => ReadOf<T>.Create(((Func<T>)action)());
			}

			private static class Write<T> where T : new()
			{
				public static Func<Delegate, Writer> Factory = action => WriteOf<T>.Create((Func<T, bool>)action);
			}
		}

		public abstract class Reader
		{
			public abstract bool Get(int index, ref bool b);
			public abstract bool Get(int index, ref int i);
			public abstract bool Get(int index, ref uint u);
			public abstract bool Get(int index, ref long l);
			public abstract bool Get(int index, ref ulong u);
			public abstract bool Get(int index, ref double d);
			public abstract bool Get(int index, ref string s);
			public abstract bool Get(int index, ref DateTime d);
			public abstract bool Get(int index, ref byte[] bytes);

			public abstract bool Get(string field, ref bool b);
			public abstract bool Get(string field, ref int i);
			public abstract bool Get(string field, ref uint u);
			public abstract bool Get(string field, ref long l);
			public abstract bool Get(string field, ref ulong u);
			public abstract bool Get(string field, ref double d);
			public abstract bool Get(string field, ref string s);
			public abstract bool Get(string field, ref DateTime d);
			public abstract bool Get(string field, ref byte[] bytes);
		}

		public abstract class Writer
		{
			public abstract bool Set(int index, bool b);
			public abstract bool Set(int index, int i);
			public abstract bool Set(int index, uint u);
			public abstract bool Set(int index, long l);
			public abstract bool Set(int index, ulong u);
			public abstract bool Set(int index, double d);
			public abstract bool Set(int index, string s);
			public abstract bool Set(int index, DateTime d);
			public abstract bool Set(int index, byte[] bytes);

			public abstract bool Set(string field, bool b);
			public abstract bool Set(string field, int i);
			public abstract bool Set(string field, uint u);
			public abstract bool Set(string field, long l);
			public abstract bool Set(string field, ulong u);
			public abstract bool Set(string field, double d);
			public abstract bool Set(string field, string s);
			public abstract bool Set(string field, DateTime d);
			public abstract bool Set(string field, byte[] bytes);

			public abstract void Prepare();
			public abstract bool Commit();
		}

		public static class DefineOf<T>
		{
			public static readonly Define Define;

			static DefineOf()
			{
				string tablename = "";
				List<Column> columns = new List<Column>();
				List<int> keys = new List<int>();
				Dictionary<uint, List<int>> indexs = new Dictionary<uint, List<int>>();
				Dictionary<string, int> names = new Dictionary<string, int>();
				Dictionary<MemberInfo, int> members = new Dictionary<MemberInfo, int>();
				List<ColumnOrder> orders = new List<ColumnOrder>();
				DataTableAttribute table = typeof(T).GetCustomAttribute<DataTableAttribute>();
				if (table != null)
				{
					tablename = table.Name ?? typeof(T).Name;
					Action<MemberInfo> each = member =>
					{
						if ((member.MemberType & (MemberTypes.Field | MemberTypes.Property)) != 0)
						{
							ColumnAttribute column = member.GetCustomAttribute<ColumnAttribute>();
							if (column != null)
							{
								string columnname = column.Name ?? member.Name;
								Type columntype;
								if (column.Type != Type.NULL)
								{
									columntype = column.Type;
								}
								else
								{
									SystemType ctype = member.MemberType == MemberTypes.Field
										? ((FieldInfo)member).FieldType
										: ((PropertyInfo)member).PropertyType;
									switch (SystemType.GetTypeCode(ctype))
									{
									case TypeCode.Boolean:
										columntype = Type.Bool;
										break;
									case TypeCode.SByte:
									case TypeCode.Int16:
									case TypeCode.Int32:
										columntype = Type.Int;
										break;
									case TypeCode.Int64:
										columntype = Type.Long;
										break;
									case TypeCode.Byte:
									case TypeCode.UInt16:
									case TypeCode.UInt32:
										columntype = Type.UInt;
										break;
									case TypeCode.UInt64:
										columntype = Type.ULong;
										break;
									case TypeCode.Single:
									case TypeCode.Double:
									case TypeCode.Decimal:
										columntype = Type.Double;
										break;
									case TypeCode.Char:
									case TypeCode.String:
										columntype = Type.String;
										break;
									case TypeCode.DateTime:
										columntype = Type.DateTime;
										break;
									default:
										columntype = Type.Bytes;
										break;
									}
								}
								bool isKey = member.IsDefined(typeof(KeyAttribute));
								if (isKey)
									keys.Add(columns.Count);
								bool hasIndex = false;
								foreach (IndexAttribute attr in member.GetCustomAttributes<IndexAttribute>())
								{
									hasIndex = true;
									List<int> index;
									if (!indexs.TryGetValue(attr.Group, out index))
									{
										index = new List<int>();
										indexs.Add(attr.Group, index);
									}
									index.Add(columns.Count);
								}
								if (column.Order != Order.NOT)
									orders.Add(new ColumnOrder {index = columns.Count, order = column.Order});
								names.Add(columnname, columns.Count);
								const BindingFlags flags =
									BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
								members.Add(
									member.DeclaringType == typeof(T)
										? member
										: member.DeclaringType.GetMember(member.Name, flags)[0],
									columns.Count);
								columns.Add(new Column {
									name = columnname,
									type = columntype,
									key = isKey,
									index = hasIndex,
								});
							}
						}
					};
					foreach (MemberInfo member in typeof(T).GetMembers(BindingFlags.Instance | BindingFlags.Public))
						each(member);
					foreach (MemberInfo member in typeof(T).GetMembers(BindingFlags.Instance | BindingFlags.NonPublic))
						each(member);
				}

				uint[] groups = indexs.Keys.ToArray();
				Array.Sort(groups);
				IReadOnlyList<int>[] array = new IReadOnlyList<int>[groups.Length];
				for (int i = 0; i < groups.Length; ++i)
					array[i] = indexs[groups[i]].ToArray();
				Define = new Define(tablename, typeof(T), columns.ToArray(), names, members, keys.ToArray(), array,
									orders.ToArray());
			}
		}

		public static class ReadOf<T>
		{
			public static readonly Define Define;

			private static readonly Delegate[] Getters;

			private delegate V Getter<out V>(ref T t);

			static ReadOf()
			{
				Define = DefineOf<T>.Define;
				List<Delegate> getters = new List<Delegate>();
				foreach (var kv in Define.Members)
				{
					MemberInfo member = kv.Key;
					Column column = Define.Columns[kv.Value];
					switch (column.type)
					{
					case Type.Bool:
						getters.Add(Create<bool>(member));
						break;
					case Type.Int:
						getters.Add(Create<int>(member));
						break;
					case Type.UInt:
						getters.Add(Create<uint>(member));
						break;
					case Type.Long:
						getters.Add(Create<long>(member));
						break;
					case Type.ULong:
						getters.Add(Create<ulong>(member));
						break;
					case Type.Double:
						getters.Add(Create<double>(member));
						break;
					case Type.String:
						getters.Add(Create<string>(member));
						break;
					case Type.DateTime:
						getters.Add(Create<DateTime>(member));
						break;
					default:
						getters.Add(Create<byte[]>(member));
						break;
					}
				}
				Getters = getters.ToArray();
			}

			private static Delegate Create<V>(MemberInfo member)
			{
				if (member.MemberType == MemberTypes.Field)
				{
					FieldInfo field = (FieldInfo)member;
					if (typeof(V) == typeof(byte[]) && typeof(byte[]) != field.FieldType)
						return CreateFieldSerialize(field);
					return CreateField<V>(field);
				}
				PropertyInfo property = (PropertyInfo)member;
				if (typeof(V) == typeof(byte[]) && typeof(byte[]) != property.PropertyType)
					return CreatePropertySerialize(property);
				return CreateProperty<V>(property);
			}

			#region 创建属性访问器
			private static Getter<V> CreateProperty<V>(PropertyInfo property)
			{
				if (property.GetGetMethod(true) == null)
					return null;
				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Get_{0}.{1}", property.DeclaringType.FullName, property.Name),
					typeof(V), new SystemType[] {typeof(T).MakeByRefType()}, property.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				PropertyAccess access = new PropertyAccess {Property = property};
				EmitGet(il, access, typeof(T), typeof(V));
				return (Getter<V>)dm.CreateDelegate(typeof(Getter<V>));
			}

			private static Getter<byte[]> CreatePropertySerialize(PropertyInfo property)
			{
				if (property.GetGetMethod(true) == null)
					return null;
				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Get_{0}.{1}", property.DeclaringType.FullName, property.Name),
					typeof(byte[]), new SystemType[] {typeof(T).MakeByRefType()}, property.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				PropertyAccess access = new PropertyAccess {Property = property};
				EmitSerialize(il, access, typeof(T));
				return (Getter<byte[]>)dm.CreateDelegate(typeof(Getter<byte[]>));
			}
			#endregion

			#region 创建字段访问器
			private static Getter<V> CreateField<V>(FieldInfo field)
			{
				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Get_{0}.{1}", field.DeclaringType.FullName, field.Name),
					typeof(V), new SystemType[] {typeof(T).MakeByRefType()}, field.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				FieldAccess access = new FieldAccess {Field = field};
				EmitGet(il, access, typeof(T), typeof(V));
				return (Getter<V>)dm.CreateDelegate(typeof(Getter<V>));
			}

			private static Getter<byte[]> CreateFieldSerialize(FieldInfo field)
			{
				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Get_{0}.{1}", field.DeclaringType.FullName, field.Name),
					typeof(byte[]), new SystemType[] {typeof(T).MakeByRefType()}, field.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				FieldAccess access = new FieldAccess {Field = field};
				EmitSerialize(il, access, typeof(T));
				return (Getter<byte[]>)dm.CreateDelegate(typeof(Getter<byte[]>));
			}
			#endregion

			#region 类型读接口
			private class ReaderType : Reader
			{
				public T value;

				public override bool Get(int index, ref bool b)
				{
					Getter<bool> func = Getters[index] as Getter<bool>;
					if (func == null)
						return false;
					b = func(ref value);
					return true;
				}

				public override bool Get(int index, ref int i)
				{
					Getter<int> func = Getters[index] as Getter<int>;
					if (func == null)
						return false;
					i = func(ref value);
					return true;
				}

				public override bool Get(int index, ref uint u)
				{
					Getter<uint> func = Getters[index] as Getter<uint>;
					if (func == null)
						return false;
					u = func(ref value);
					return true;
				}

				public override bool Get(int index, ref long l)
				{
					Getter<long> func = Getters[index] as Getter<long>;
					if (func == null)
						return false;
					l = func(ref value);
					return true;
				}

				public override bool Get(int index, ref ulong u)
				{
					Getter<ulong> func = Getters[index] as Getter<ulong>;
					if (func == null)
						return false;
					u = func(ref value);
					return true;
				}

				public override bool Get(int index, ref double d)
				{
					Getter<double> func = Getters[index] as Getter<double>;
					if (func == null)
						return false;
					d = func(ref value);
					return true;
				}

				public override bool Get(int index, ref string s)
				{
					Getter<string> func = Getters[index] as Getter<string>;
					if (func == null)
						return false;
					s = func(ref value);
					return true;
				}

				public override bool Get(int index, ref DateTime d)
				{
					Getter<DateTime> func = Getters[index] as Getter<DateTime>;
					if (func == null)
						return false;
					d = func(ref value);
					return true;
				}

				public override bool Get(int index, ref byte[] bytes)
				{
					Getter<byte[]> func = Getters[index] as Getter<byte[]>;
					if (func == null)
						return false;
					bytes = func(ref value);
					return true;
				}

				public override bool Get(string field, ref bool b)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref b);
				}

				public override bool Get(string field, ref int i)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref i);
				}

				public override bool Get(string field, ref uint u)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref u);
				}

				public override bool Get(string field, ref long l)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref l);
				}

				public override bool Get(string field, ref ulong u)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref u);
				}

				public override bool Get(string field, ref double d)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref d);
				}

				public override bool Get(string field, ref string s)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref s);
				}

				public override bool Get(string field, ref DateTime d)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref d);
				}

				public override bool Get(string field, ref byte[] bytes)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Get(index, ref bytes);
				}
			}
			#endregion

			public static Reader Create(T t)
			{
				return new ReaderType {value = t};
			}
		}

		public static class WriteOf<T> where T : new()
		{
			public static readonly Define Define;

			private static readonly Delegate[] Setters;

			private delegate void Setter<in V>(ref T t, V value);

			static WriteOf()
			{
				Define = DefineOf<T>.Define;
				List<Delegate> setters = new List<Delegate>();
				foreach (var kv in Define.Members)
				{
					MemberInfo member = kv.Key;
					Column column = Define.Columns[kv.Value];
					switch (column.type)
					{
					case Type.Bool:
						setters.Add(Create<bool>(member));
						break;
					case Type.Int:
						setters.Add(Create<int>(member));
						break;
					case Type.UInt:
						setters.Add(Create<uint>(member));
						break;
					case Type.Long:
						setters.Add(Create<long>(member));
						break;
					case Type.ULong:
						setters.Add(Create<ulong>(member));
						break;
					case Type.Double:
						setters.Add(Create<double>(member));
						break;
					case Type.String:
						setters.Add(Create<string>(member));
						break;
					case Type.DateTime:
						setters.Add(Create<DateTime>(member));
						break;
					default:
						setters.Add(Create<byte[]>(member));
						break;
					}
				}
				Setters = setters.ToArray();
			}

			private static Delegate Create<V>(MemberInfo member)
			{
				if (member.MemberType == MemberTypes.Field)
				{
					FieldInfo field = (FieldInfo)member;
					if (typeof(V) == typeof(byte[]) && typeof(byte[]) != field.FieldType)
						return CreateFieldDeserialize(field);
					return CreateField<V>(field);
				}
				PropertyInfo property = (PropertyInfo)member;
				if (typeof(V) == typeof(byte[]) && typeof(byte[]) != property.PropertyType)
					return CreatePropertyDeserialize(property);
				return CreateProperty<V>(property);
			}

			#region 创建属性访问器
			private static Setter<V> CreateProperty<V>(PropertyInfo property)
			{
				if (property.GetSetMethod(true) == null)
					return null;
				if (property.DeclaringType != typeof(T) && !property.DeclaringType.IsAssignableFrom(typeof(T)))
					throw new ArgumentException();

				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Set_{0}.{1}", property.DeclaringType.FullName, property.Name),
					typeof(void), new SystemType[] {typeof(T).MakeByRefType(), typeof(V)}, property.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				PropertyAccess access = new PropertyAccess {Property = property};
				EmitSet(il, access, typeof(T), typeof(V));
				return (Setter<V>)dm.CreateDelegate(typeof(Setter<V>));
			}

			private static Setter<byte[]> CreatePropertyDeserialize(PropertyInfo property)
			{
				if (property.GetSetMethod(true) == null)
					return null;
				if (property.DeclaringType != typeof(T) && !property.DeclaringType.IsAssignableFrom(typeof(T)))
					throw new ArgumentException();

				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Set_{0}.{1}", property.DeclaringType.FullName, property.Name),
					typeof(void), new SystemType[] {typeof(T).MakeByRefType(), typeof(byte[])}, property.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				PropertyAccess access = new PropertyAccess {Property = property};
				EmitDeserialize(il, access, typeof(T));
				return (Setter<byte[]>)dm.CreateDelegate(typeof(Setter<byte[]>));
			}
			#endregion

			#region 创建字段访问器
			private static Setter<V> CreateField<V>(FieldInfo field)
			{
				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Set_{0}.{1}", field.DeclaringType.FullName, field.Name),
					typeof(void), new SystemType[] {typeof(T).MakeByRefType(), typeof(V)}, field.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				FieldAccess access = new FieldAccess {Field = field};
				EmitSet(il, access, typeof(T), typeof(V));
				return (Setter<V>)dm.CreateDelegate(typeof(Setter<V>));
			}

			private static Setter<byte[]> CreateFieldDeserialize(FieldInfo field)
			{
				DynamicMethod dm = new DynamicMethod(
					string.Format("DataTable.Set_{0}.{1}", field.DeclaringType.FullName, field.Name),
					typeof(void), new SystemType[] {typeof(T).MakeByRefType(), typeof(byte[])}, field.DeclaringType, true);
				ILGenerator il = dm.GetILGenerator();
				FieldAccess access = new FieldAccess {Field = field};
				EmitDeserialize(il, access, typeof(T));
				return (Setter<byte[]>)dm.CreateDelegate(typeof(Setter<byte[]>));
			}
			#endregion

			#region 类型写接口
			private class WriterType : Writer
			{
				private T value;
				public Func<T, bool> action;

				public override bool Set(int index, bool b)
				{
					Setter<bool> func = Setters[index] as Setter<bool>;
					if (func == null)
						return false;
					func(ref value, b);
					return true;
				}

				public override bool Set(int index, int i)
				{
					Setter<int> func = Setters[index] as Setter<int>;
					if (func == null)
						return false;
					func(ref value, i);
					return true;
				}

				public override bool Set(int index, uint u)
				{
					Setter<uint> func = Setters[index] as Setter<uint>;
					if (func == null)
						return false;
					func(ref value, u);
					return true;
				}

				public override bool Set(int index, long l)
				{
					Setter<long> func = Setters[index] as Setter<long>;
					if (func == null)
						return false;
					func(ref value, l);
					return true;
				}

				public override bool Set(int index, ulong u)
				{
					Setter<ulong> func = Setters[index] as Setter<ulong>;
					if (func == null)
						return false;
					func(ref value, u);
					return true;
				}

				public override bool Set(int index, double d)
				{
					Setter<double> func = Setters[index] as Setter<double>;
					if (func == null)
						return false;
					func(ref value, d);
					return true;
				}

				public override bool Set(int index, string s)
				{
					Setter<string> func = Setters[index] as Setter<string>;
					if (func == null)
						return false;
					func(ref value, s);
					return true;
				}

				public override bool Set(int index, DateTime d)
				{
					Setter<DateTime> func = Setters[index] as Setter<DateTime>;
					if (func == null)
						return false;
					func(ref value, d);
					return true;
				}

				public override bool Set(int index, byte[] bytes)
				{
					Setter<byte[]> func = Setters[index] as Setter<byte[]>;
					if (func == null)
						return false;
					func(ref value, bytes);
					return true;
				}

				public override bool Set(string field, bool b)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, b);
				}

				public override bool Set(string field, int i)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, i);
				}

				public override bool Set(string field, uint u)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, u);
				}

				public override bool Set(string field, long l)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, l);
				}

				public override bool Set(string field, ulong u)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, u);
				}

				public override bool Set(string field, double d)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, d);
				}

				public override bool Set(string field, string s)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, s);
				}

				public override bool Set(string field, DateTime d)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, d);
				}

				public override bool Set(string field, byte[] bytes)
				{
					int index;
					return Define.Names.TryGetValue(field, out index) && Set(index, bytes);
				}

				public override void Prepare()
				{
					value = new T();
				}

				public override bool Commit()
				{
					return action(value);
				}
			}
			#endregion

			public static Writer Create(Func<T, bool> action)
			{
				return new WriterType {action = action};
			}
		}

		#region OpCode表
		private static readonly Dictionary<TypeCode, int> codes = new Dictionary<TypeCode, int> {
			{TypeCode.SByte, 0},
			{TypeCode.Byte, 1},
			{TypeCode.Int16, 2},
			{TypeCode.UInt16, 3},
			{TypeCode.Int32, 4},
			{TypeCode.UInt32, 5},
			{TypeCode.Int64, 6},
			{TypeCode.UInt64, 7},
			{TypeCode.Single, 8},
			{TypeCode.Double, 9},
		};

		private static readonly OpCode[,] converts = {
			/*    SByte            Byte             Int16           UInt16            Int32           UInt32            Int64           UInt64           Single           Double    */
/*SByte*/	{OpCodes.Nop,     OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1, OpCodes.Conv_I1},
/*Byte*/	{OpCodes.Conv_U1, OpCodes.Nop,     OpCodes.Conv_U1, OpCodes.Conv_U1, OpCodes.Conv_U1, OpCodes.Conv_U1, OpCodes.Conv_U1, OpCodes.Conv_U1, OpCodes.Conv_U1, OpCodes.Conv_U1},
/*Int16*/	{OpCodes.Nop,     OpCodes.Nop,     OpCodes.Nop,     OpCodes.Conv_I2, OpCodes.Conv_I2, OpCodes.Conv_I2, OpCodes.Conv_I2, OpCodes.Conv_I2, OpCodes.Conv_I2, OpCodes.Conv_I2},
/*UInt16*/	{OpCodes.Nop,     OpCodes.Nop,     OpCodes.Conv_U2, OpCodes.Nop,     OpCodes.Conv_U2, OpCodes.Conv_U2, OpCodes.Conv_U2, OpCodes.Conv_U2, OpCodes.Conv_U2, OpCodes.Conv_U2},
/*Int32*/	{OpCodes.Nop,     OpCodes.Nop,     OpCodes.Nop,     OpCodes.Nop,     OpCodes.Nop,     OpCodes.Conv_I4, OpCodes.Conv_I4, OpCodes.Conv_I4, OpCodes.Conv_I4, OpCodes.Conv_I4},
/*UInt32*/	{OpCodes.Nop,     OpCodes.Nop,     OpCodes.Nop,     OpCodes.Nop,     OpCodes.Conv_U4, OpCodes.Nop,     OpCodes.Conv_U4, OpCodes.Conv_U4, OpCodes.Conv_U4, OpCodes.Conv_U4},
/*Int64*/	{OpCodes.Conv_I8, OpCodes.Conv_I8, OpCodes.Conv_I8, OpCodes.Conv_I8, OpCodes.Conv_I8, OpCodes.Conv_I8, OpCodes.Nop,     OpCodes.Conv_I8, OpCodes.Conv_I8, OpCodes.Conv_I8},
/*UInt64*/	{OpCodes.Conv_U8, OpCodes.Conv_U8, OpCodes.Conv_U8, OpCodes.Conv_U8, OpCodes.Conv_U8, OpCodes.Conv_U8, OpCodes.Conv_U8, OpCodes.Nop,     OpCodes.Conv_U8, OpCodes.Conv_U8},
/*Single*/	{OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Conv_R4, OpCodes.Nop,     OpCodes.Conv_R4},
/*Double*/	{OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Conv_R8, OpCodes.Nop},
		};

		private static OpCode ConvertFrom(this TypeCode to, TypeCode from)
		{
			int index1;
			if (!codes.TryGetValue(to, out index1))
				return OpCodes.Throw;
			int index2;
			if (!codes.TryGetValue(from, out index2))
				return OpCodes.Throw;
			return converts[index1, index2];
		}
		#endregion

		#region 生成类型转换IL代码
		private static void EmitCast(this ILGenerator il, SystemType from, SystemType to)
		{
			TypeCode code = SystemType.GetTypeCode(to);
			if (code == TypeCode.Boolean)
			{
				switch (SystemType.GetTypeCode(from))
				{
				case TypeCode.UInt64:
				case TypeCode.Int64:
					il.Emit(OpCodes.Ldc_I4_0);
					il.Emit(OpCodes.Conv_I8);
					break;
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
					il.Emit(OpCodes.Ldc_I4_0);
					break;
				default:
					throw new ArgumentException();
				}
				il.Emit(OpCodes.Ceq);
				il.Emit(OpCodes.Ldc_I4_0);
				il.Emit(OpCodes.Ceq);
			}
			else
			{
				OpCode op = code.ConvertFrom(SystemType.GetTypeCode(from));
				if (op == OpCodes.Throw)
					throw new ArgumentException();
				if (op != OpCodes.Nop)
					il.Emit(op);
			}
		}
		#endregion

		#region 访问抽象接口
		private interface IAccess
		{
			SystemType DeclaringType { get; }
			SystemType AccessType { get; }
			void EmitSet(ILGenerator il);
			void EmitGet(ILGenerator il);
		}

		private struct PropertyAccess : IAccess
		{
			public PropertyInfo Property;

			public SystemType DeclaringType
			{
				get { return Property.DeclaringType; }
			}

			public SystemType AccessType
			{
				get { return Property.PropertyType; }
			}

			public void EmitSet(ILGenerator il)
			{
				MethodInfo method = Property.GetSetMethod(true);
				il.Emit(method.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, method);
			}

			public void EmitGet(ILGenerator il)
			{
				MethodInfo method = Property.GetGetMethod(true);
				il.Emit(method.IsVirtual ? OpCodes.Callvirt : OpCodes.Call, method);
			}
		}

		private struct FieldAccess : IAccess
		{
			public FieldInfo Field;

			public SystemType DeclaringType
			{
				get { return Field.DeclaringType; }
			}

			public SystemType AccessType
			{
				get { return Field.FieldType; }
			}

			public void EmitSet(ILGenerator il)
			{
				il.Emit(OpCodes.Stfld, Field);
			}

			public void EmitGet(ILGenerator il)
			{
				il.Emit(OpCodes.Ldfld, Field);
			}
		}
		#endregion

		#region 任意类型转换成二进制流
		private static ConcurrentQueue<FormatStream> formatstreams = new ConcurrentQueue<FormatStream>();

		private struct FormatStream
		{
			public BinaryFormatter formatter;
			public MemoryStream stream;
		}

		private static byte[] Serialize(object o)
		{
			FormatStream formatstream;
			if (!formatstreams.TryDequeue(out formatstream))
				formatstream = new FormatStream {formatter = new BinaryFormatter(), stream = new MemoryStream()};
			formatstream.formatter.Serialize(formatstream.stream, o);
			byte[] bytes = new byte[formatstream.stream.Length];
			formatstream.stream.Seek(0, SeekOrigin.Begin);
			int start = 0;
			while (start < bytes.Length)
				start += formatstream.stream.Read(bytes, start, bytes.Length - start);
			formatstream.stream.SetLength(0);
			formatstreams.Enqueue(formatstream);
			return bytes;
		}

		private static object Deserialize(byte[] bytes)
		{
			FormatStream formatstream;
			if (!formatstreams.TryDequeue(out formatstream))
				formatstream = new FormatStream {formatter = new BinaryFormatter(), stream = new MemoryStream()};
			formatstream.stream.Write(bytes, 0, bytes.Length);
			formatstream.stream.Seek(0, SeekOrigin.Begin);
			object o = formatstream.formatter.Deserialize(formatstream.stream);
			formatstream.stream.SetLength(0);
			formatstreams.Enqueue(formatstream);
			return o;
		}

		private static readonly MethodInfo SerializeMethod = typeof(DataTable).GetMethod("Serialize",
																						BindingFlags.Static |
																						BindingFlags.Public |
																						BindingFlags.NonPublic);
		private static readonly MethodInfo DeserializeMethod = typeof(DataTable).GetMethod("Deserialize",
																							BindingFlags.Static |
																							BindingFlags.Public |
																							BindingFlags.NonPublic);
		#endregion

		#region 实际生成IL代码
		private static void EmitSet<TAccess>(
			ILGenerator il, TAccess access, SystemType parameterType1, SystemType parameterType2)
			where TAccess : IAccess
		{
			il.Emit(OpCodes.Ldarg_0);
			if (access.DeclaringType.IsValueType)
			{
				if (!parameterType1.IsValueType)
					il.Emit(OpCodes.Unbox, access.DeclaringType);
			}
			else
			{
				il.Emit(OpCodes.Ldind_Ref);
			}
			il.Emit(OpCodes.Ldarg_1);
			if (access.AccessType != parameterType2)
				il.EmitCast(parameterType2, access.AccessType);
			access.EmitSet(il);
			il.Emit(OpCodes.Ret);
		}

		private static void EmitGet<TAccess>(
			ILGenerator il, TAccess access, SystemType parameterType1, SystemType parameterType2)
			where TAccess : IAccess
		{
			il.Emit(OpCodes.Ldarg_0);
			if (access.DeclaringType.IsValueType)
			{
				if (access.DeclaringType != parameterType1)
					il.Emit(OpCodes.Unbox, access.DeclaringType);
			}
			else
			{
				il.Emit(OpCodes.Ldind_Ref);
			}
			access.EmitGet(il);
			if (access.AccessType != parameterType2 && !parameterType2.IsAssignableFrom(access.AccessType))
				il.EmitCast(access.AccessType, parameterType2);
			else if (!parameterType2.IsValueType && access.AccessType.IsValueType)
				il.Emit(OpCodes.Box, access.AccessType);
			il.Emit(OpCodes.Ret);
		}

		private static void EmitDeserialize<TAccess>(ILGenerator il, TAccess access, SystemType parameterType)
			where TAccess : IAccess
		{
			il.Emit(OpCodes.Ldarg_0);
			if (access.DeclaringType.IsValueType)
			{
				if (!parameterType.IsValueType)
					il.Emit(OpCodes.Unbox, access.DeclaringType);
			}
			else
			{
				il.Emit(OpCodes.Ldind_Ref);
			}
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Call, DeserializeMethod);
			if (access.AccessType.IsValueType)
				il.Emit(OpCodes.Unbox, access.AccessType);
			else if (access.AccessType != typeof(object))
				il.Emit(OpCodes.Castclass, access.AccessType);
			access.EmitSet(il);
			il.Emit(OpCodes.Ret);
		}

		private static void EmitSerialize<TAccess>(ILGenerator il, TAccess access, SystemType parameterType)
			where TAccess : IAccess
		{
			il.Emit(OpCodes.Ldarg_0);
			if (access.DeclaringType.IsValueType)
			{
				if (access.DeclaringType != parameterType)
					il.Emit(OpCodes.Unbox, access.DeclaringType);
			}
			else
			{
				il.Emit(OpCodes.Ldind_Ref);
			}
			access.EmitGet(il);
			if (access.AccessType.IsValueType)
				il.Emit(OpCodes.Box, access.AccessType);
			il.Emit(OpCodes.Call, SerializeMethod);
			il.Emit(OpCodes.Ret);
		}
		#endregion
	}
}