using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BobbyTables
{
	internal enum RowState
	{
		Unchanged,
		Inserting,
		Updating,
		Deleting
	}

	internal class Row
	{
		public JObject Data;
		public RowState State;
	}

	internal class Table
	{
		public string Id { get; private set; }
		public Datastore Datastore { get; private set; }

		private Dictionary<string, Row> _rows;
		private List<JArray> _pendingChanges;
		private DatastoreManager _manager;

		internal delegate string IdGetter(object obj);
		internal delegate void IdSetter(object obj, string id);

		internal Table(DatastoreManager manager, Datastore store, string id)
		{
			Datastore = store;
			Id = id;
			_manager = manager;
			_rows = new Dictionary<string, Row>();
			_pendingChanges = new List<JArray>();
		}

		#region Internal housekeeping stuff

		internal void Load(JsonReader reader)
		{
			if (!reader.Read() || reader.TokenType != JsonToken.StartObject)
			{
				throw new DatastoreException("Error loading datastore snapshot. Expected StartObject but was " + reader.TokenType);
			}
			while (reader.Read() && reader.TokenType == JsonToken.PropertyName)
			{
				var id = (string)reader.Value;
				var row = new Row();
				_rows.Add(id, row);
				try
				{
					reader.Read();
					row.Data = JObject.ReadFrom(reader) as JObject;
				}
				catch (Exception ex)
				{
					throw new DatastoreException("Error loading datastore snapshot. Unable to load data for row " + id + ex.ToString());
				}
				if (row.Data == null)
				{
					throw new DatastoreException("Error loading datastore snapshot. Unable to load data for row " + id + " in table "+Id);
				}
			}
			if (reader.TokenType != JsonToken.EndObject)
			{
				throw new DatastoreException("Error loading datastore snapshot. Expected EndObject but was " + reader.TokenType);
			}
		}

		internal void Save(JsonWriter writer)
		{
			writer.WriteStartObject();
			foreach (var row in _rows)
			{
				writer.WritePropertyName(row.Key);
				row.Value.Data.WriteTo(writer);
			}
			writer.WriteEndObject();
		}

		internal bool HasPendingChanges
		{
			get { return _pendingChanges.Count > 0; }
		}

		internal List<JArray> PendingChanges
		{
			get { return _pendingChanges; }
		}

		internal void RevertPendingChanges()
		{
			foreach (var change in _pendingChanges)
			{
				Row row;
				var rowId = change[2].Value<string>();
				if (_rows.TryGetValue(rowId, out row))
				{
					row.State = RowState.Unchanged;
					if (row.Data == null)
					{
						_rows.Remove(rowId);
					}
				}
			}
			_pendingChanges.Clear();
		}

		internal void ApplyPendingChanges()
		{
			foreach (var change in _pendingChanges)
			{
				ApplyChange(change);
			}
			_pendingChanges.Clear();
		}

		internal void ApplyChange(JArray change)
		{
			var type = change[0].Value<string>();
			var rowId = change[2].Value<string>();

			switch (type)
			{
				case "I":
				{
					Row row;
					if (_rows.TryGetValue(rowId, out row))
					{
						if (row.Data != null) throw new ArgumentException("Cannot insert row " + rowId + " as it already exists and has a value");
					}
					else
					{
						row = new Row();
						_rows.Add(rowId, row);
					}
					if (change.Last.Type != JTokenType.Object)
					{
						throw new ArgumentException("Expected last property of insert insert row " + rowId + " to be Object, but was " + change.Last.Type);
					}
					else
					{
						row.Data = change.Last as JObject;
					}
					row.State = RowState.Unchanged;
				}
				break;
				case "D":
				{
					Row row;
					if (!_rows.TryGetValue(rowId, out row) || row.Data == null)
					{
						throw new ArgumentException("Cannot delete row "+rowId+" as it has already been deleted");
					}
					_rows.Remove(rowId);
				}
				break;
				case "U":
				{
					Row row;
					if (!_rows.TryGetValue(rowId, out row) || row.Data == null)
					{
						throw new ArgumentException("Cannot update row " + rowId + " as it does not exist");
					}
					ApplyUpdate(row, change);
					row.State = RowState.Unchanged;
				}
				break;
				default:
					throw new DatastoreException("Unknown Change type " + type);
			}
		}

		private void ApplyUpdate(Row row, JArray updates)
		{
			foreach (var child in updates.Last as JObject)
			{
				switch (child.Value[0].Value<string>())
				{
					case "P":
						row.Data[child.Key] = child.Value[1];
						break;
					case "D":
						row.Data.Remove(child.Key);
						break;
					case "LC":
						row.Data[child.Key] = new JArray();
						break;
					case "LP":
						row.Data[child.Key][child.Value[1].Value<int>()] = child.Value[2];
						break;
					case "LI":
						(row.Data[child.Key] as JArray).Insert(child.Value[1].Value<int>(), child.Value[2]);
						break;
					case "LD":
						(row.Data[child.Key] as JArray).RemoveAt(child.Value[1].Value<int>());
						break;
					case "LM":
						{
							int oldIndex = child.Value[1].Value<int>();
							var value = row.Data[child.Key][oldIndex];
							var array = row.Data[child.Key] as JArray;
							array.RemoveAt(oldIndex);
							array.Insert(child.Value[2].Value<int>(), value);
						}
						break;
					default:
						throw new ArgumentException("Unknown fieldop type " + child.Value[0].Value<string>());
				}
			}
		}

		internal void InsertInternal(string rowId, JObject insert)
		{
			_rows.Add(rowId, new Row { Data = insert, State = RowState.Unchanged });
		}

		#endregion

		#region public API

		public bool Insert(IdGetter idGetter,IdSetter idSetter, object insert)
		{
			string id = GetObjectId(insert, idGetter, idSetter);
			if (string.IsNullOrEmpty(id))
			{
				throw new ArgumentException("Object to be inserted must have a non null/empty Id");
			}

			Row row;
			if (_rows.TryGetValue(id, out row))
			{
				// can't insert something unless it doesn't exist
				// or has just been deleted
				if (row.State!=RowState.Deleting) return false;
			}
			else
			{
				row = new Row();
				_rows.Add(id, row);
			}
			row.State = RowState.Inserting;

			var change = new JArray();
			change.Add("I");
			change.Add(Id);
			change.Add(id);
			JObject data = new JObject();
			foreach (var field in insert.GetType().GetFields())
			{
				if (field.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0) 
				{
					object fieldValue = field.GetValue(insert);
					if (fieldValue != null)
					{
						data[field.Name] = SerializeValue(fieldValue);
					}
				}
			}
			foreach (var prop in insert.GetType().GetProperties())
			{
				if (prop.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0)
				{
					object propValue = prop.GetValue(insert, null);
					// we are only interested in read/writable fields
					if (prop.CanRead && prop.CanWrite && propValue != null)
					{
						data[prop.Name] = SerializeValue(propValue);
					}
				}
			}
			change.Add(data);

			_pendingChanges.Add(change);

			return true;
		}

		public bool Delete(string id)
		{
			Row row;
			if (_rows.TryGetValue(id, out row))
			{
				// can't delete something twice
				if (row.State == RowState.Deleting) return false;
				row.State = RowState.Deleting;

				var change = new JArray();
				change.Add("D");
				change.Add(Id);
				change.Add(id);

				_pendingChanges.Add(change);
				return true;
			}
			return false;
		}

		public T Get<T>(IdSetter idSetter,string id)
		  where T : class, new()
		{
			Row row;
			if (_rows.TryGetValue(id, out row) && row.Data != null)
			{
				T obj = new T();
				idSetter( obj, id);

				foreach (var field in obj.GetType().GetFields())
				{
					if (field.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0)
					{
						var data = row.Data[field.Name];
						if (data != null)
						{
							field.SetValue(obj, DeserializeValue(data, field.FieldType));
						}
					}
				}
				foreach (var prop in obj.GetType().GetProperties())
				{
					// we are only interested in read/writable fields
					if (prop.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0 && prop.CanRead && prop.CanWrite)
					{
						var data = row.Data[prop.Name];
						if (data != null)
						{
							prop.SetValue(obj, DeserializeValue(data, prop.PropertyType), null);
						}
					}
				}

				return obj;
			}
			return default(T);
		}

		public IEnumerable<T> GetAll<T>(IdSetter idSetter)
			where T : class, new()
		{
			foreach (var kvp in _rows)
			{
				yield return Get<T>(idSetter,kvp.Key);
			}
		}

		public bool Update(IdGetter idGetter, object update)
		{
			string id = GetObjectId(update, idGetter, null);
			if (string.IsNullOrEmpty(id))
			{
				throw new ArgumentException("Object to be updated must have a non null/empty Id");
			}

			Row row;
			if (_rows.TryGetValue(id, out row))
			{
				// can only update items that already exist and don't have any pending
				// modifications
				if (row.State != RowState.Unchanged) return false;
				row.State = RowState.Updating;

				var change = new JArray();
				change.Add("U");
				change.Add(Id);
				change.Add(id);
				change.Add(new JObject());

				foreach (var field in update.GetType().GetFields())
				{
					if (field.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0)
					{
						object fieldValue = field.GetValue(update);
						JToken value = fieldValue != null ? SerializeValue(fieldValue) : null;
						var operations = DetermineOperations(row.Data, field.Name, value);
						change = AddPendingOperations(field.Name, value, operations, change);
					}                
				}
				foreach (var prop in update.GetType().GetProperties())
				{
					// we are only interested in read/writable fields
					if (prop.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0 && prop.CanRead && prop.CanWrite)
					{
						object propValue = prop.GetValue(update, null);
						var value = propValue != null ? SerializeValue(propValue) : null;
						var operations = DetermineOperations(row.Data, prop.Name, value);
						change = AddPendingOperations(prop.Name, value, operations, change);
					}                  
				}

				if (change.Last.HasValues)
				{
					_pendingChanges.Add(change);
				}
				return true;
			}
			return false;
		}

		#endregion

		#region private member functions and helpers

		private JArray AddPendingOperations(string name, JToken value, List<JArray> operations, JArray change)
		{
			var dictionary = change.Last as JObject;

			foreach (var op in operations)
			{
				// this dictionary already has a change for this key
				// so we need to start a new change set
				if (dictionary[name] != null)
				{
					_pendingChanges.Add(change);
					var rowId = change[2].Value<string>();

					change = new JArray();
					change.Add("U");
					change.Add(Id);
					change.Add(rowId);
					dictionary = new JObject();
					change.Add(dictionary);
				}

				dictionary[name] = op;
			}
			return change;
		}

		private List<JArray> DetermineOperations(JObject originalData, string name, JToken value)
		{
			List<JArray> operations = new List<JArray>();
			JToken originalValue = originalData[name];
			if (value != null)
			{
				if (originalData[name] == null)
				{
					// the property doesn't currently exist, so its either a PUT or LIST_CREATE
					if (value.Type == JTokenType.Array )
					{
						// create an empty list property
						var op = new JArray();
						op.Add("LC");
						operations.Add(op);

						if ((value as JArray).Count != 0)
						{
							// create a list containing values
							JArray listValue = value as JArray;
							for (var i = 0; i < listValue.Count; ++i)
							{
								op = new JArray();
								op.Add("LP");
								op.Add(i);
								op.Add(listValue[i]);
								operations.Add(op);
							}
						}
					}
					else
					{
						var op = new JArray();
						op.Add("P");
						op.Add(value);
						operations.Add(op);
					}
				}
				else
				{
					// the property exists - now we want to see if its the same or not
					if (originalValue.Type == JTokenType.Array && value.Type == JTokenType.Array)
					{
						// for array values we want to try and be more efficient than just replacing
						// the entire list - so we'll try to identify just the elements that were
						// removed/updated/added to the list instead using a diff algorithm

						JArray original = originalValue as JArray;
						JArray update = value as JArray;

						// get the longest common subsequence between the two lists
						JArray lcs = ComputeLCS(original, update);

						int lcsIndex = 0;
						//anything present in the update but not in the LCS is a new addition
						for (int i = 0; i < update.Count; ++i)
						{
							if (lcsIndex >= lcs.Count || !SerializedValuesEqual(update[i], lcs[lcsIndex]))
							{
								var op = new JArray();
								op.Add("LI");
								op.Add(i);
								op.Add(update[i]);
								operations.Add(op);
							}
							else
							{
								++lcsIndex;
							}
						}

						lcsIndex = 0;
						//anything present in the original, but not in the LCS is a removal
						for (int i = 0; i < original.Count; ++i)
						{
							if (lcsIndex >= lcs.Count || !SerializedValuesEqual(original[i], lcs[lcsIndex]))
							{
								var op = new JArray();
								op.Add("LD");
								op.Add(i);
								operations.Add(op);
							}
							else
							{
								++lcsIndex;
							}
						}

						// apply operations in reverse order so the indexes don't get messed up as
						// the lists content changes
						operations.Sort((a, b) =>
						{
							var cmp = a[1].Value<int>().CompareTo(b[1].Value<int>()) * -1;
							if (cmp == 0)
							{
								// deletes should go ahead of inserts so we 
								// don't delete entries we have just added
								return a[0].Value<string>() == "LD" ? -1 : 1;
							}
							return cmp;
						});
					}
					else
					{
						// if we're not dealing with arrays, then we can just compare and put in the 
						// new value if necessary
						if (!SerializedValuesEqual(originalValue, value))
						{
							var op = new JArray();
							op.Add("P");
							op.Add(value);
							operations.Add(op);
						}
					}
				}
			}
			else if ( originalData[name] != null )
			{
				// value is null but it was not previously, It's a delete operation
				var op = new JArray();
				op.Add("D");
				operations.Add(op);
			}
			return operations;
		}

		private static JArray ComputeLCS(JArray a,JArray b)
		{
			var sequence = new JArray();
			if (a.Count==0 || b.Count==0)
				return sequence;

			int[,] num = new int[a.Count, b.Count];
			int maxlen = 0;
			int lastSubsBegin = 0;

			for (int i = 0; i < a.Count; i++)
			{
				for (int j = 0; j < b.Count; j++)
				{
					if (!SerializedValuesEqual(a[i], b[j]))
					{
						num[i, j] = 0;
					}
					else
					{
						if ((i == 0) || (j == 0))
							num[i, j] = 1;
						else
							num[i, j] = 1 + num[i - 1, j - 1];

						if (num[i, j] > maxlen)
						{
							maxlen = num[i, j];
							int thisSubsBegin = i - num[i, j] + 1;
							if (lastSubsBegin == thisSubsBegin)
							{
								sequence.Add(a[i]);
							}
							else
							{
								lastSubsBegin = thisSubsBegin;
								sequence.Clear();
								for (int k=0;k<(i+1)-lastSubsBegin;++k) {
									sequence.Add(a[lastSubsBegin+k]);
								}
							}
						}
					}
				}
			}
			return sequence;
		}

		private static string GetObjectId(object update,IdGetter idGetter, IdSetter idSetter)
		{
			string id = idGetter(update);
			if (idSetter!=null && string.IsNullOrEmpty(id))
			{
				id = GenerateId();
				idSetter(update, id);
			}
			return id;
		}

		private static string GenerateId()
		{
			// generate a guid based id and shorten it
			string encoded = Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Replace("/", "_").Replace("+", "-");
			return encoded.Substring(0, 22);
		}

		private static JToken SerializeValue(object value)
		{
			if (value == null)
			{
				throw new ArgumentException("Unable to serialize null value to Atom");
			}

			var type = value.GetType();

			// arrays (though byte arrays are excluded as they are considered
			// to a single value containing a byte stream )
			if (type.IsArray && type.GetElementType() != typeof(byte))
			{
				JArray array = new JArray();
				Array valueArray = value as Array;
				foreach (var element in valueArray)
				{
					array.Add(SerializeValue(element));
				}
				return array;
			}
			else if (type == typeof(bool))
			{
				return new JValue((bool)value);
			}
			else if (type == typeof(string))
			{
				return new JValue((string)value);
			}
			else if (type == typeof(float))
			{
				return new JValue((float)value);
			}
			else if (type == typeof(double))
			{
				return new JValue((double)value);
			}
			else if (type == typeof(Int16))
			{
				JObject obj = new JObject();
				obj["I"] = ((long)(Int16)value).ToString();
				return obj;
			}
			else if (type == typeof(UInt16))
			{
				JObject obj = new JObject();
				obj["I"] = ((long)(UInt16)value).ToString();
				return obj;
			}
			else if (type == typeof(int))
			{
				JObject obj = new JObject();
				obj["I"] = ((long)(int)value).ToString();
				return obj;
			}
			else if (type == typeof(uint))
			{
				JObject obj = new JObject();
				obj["I"] = ((long)(uint)value).ToString();
				return obj;
			}
			else if (type == typeof(long))
			{
				JObject obj = new JObject();
				obj["I"] = ((long)value).ToString();
				return obj;
			}
			else if (type == typeof(ulong))
			{
				JObject obj = new JObject();
				obj["I"] = ((long)(ulong)value).ToString();
				return obj;
			}
			else if (type == typeof(DateTime))
			{
				JObject obj = new JObject();
				obj["T"] = ((Int64)Math.Floor((((DateTime)value) - new DateTime(1970, 1, 1)).TotalMilliseconds)).ToString();
				return obj;
			}
			else if (type == typeof(byte[]))
			{
				JObject obj = new JObject();
				obj["B"] = Utils.ToDBase64((byte[])value);
				return obj;
			}
			else if (type.IsEnum)
			{
				JObject obj = new JObject();
				Enum.GetUnderlyingType(type);
				obj["I"] = ((long)(int)value).ToString();
				return obj;
			}
			else if (typeof(IList<byte>).IsAssignableFrom(type))
			{
				IList<byte> list = value as IList<byte>;
				byte[] array = new byte[list.Count];
				list.CopyTo(array, 0);

				JObject obj = new JObject();
				obj["B"] = Utils.ToDBase64(array);
				return obj;
			}
			else
			{
				foreach (Type interfaceType in type.GetInterfaces())
				{
					if (interfaceType.IsGenericType &&
						interfaceType.GetGenericTypeDefinition()
						== typeof(IList<>))
					{
						JArray array = new JArray();
						IList collection = (IList)value;
						foreach (var element in collection)
						{
							if (element == null)
							{
								throw new ArgumentException("Unable to serialize null value to Atom");
							}
							array.Add(SerializeValue(element));
						}
						return array;
					}
				}
			}

			throw new ArgumentException("Unable to serialize type " + type + " to Atom");
		}


		private static bool SerializedValuesEqual(JToken a, JToken b)
		{
			if (a.Type != b.Type) return false;

			switch (a.Type)
			{
				case JTokenType.Boolean:
				case JTokenType.Float:
				case JTokenType.Integer:
				case JTokenType.String:
					return a.Equals(b);
				case JTokenType.Array:
					var aArray = a as JArray;
					var bArray = b as JArray;
					if (aArray.Count != bArray.Count) return false;
					for (int i = 0; i < aArray.Count; ++i)
					{
						if (!SerializedValuesEqual(aArray[i], bArray[i])) return false;
					}
					return true;
				case JTokenType.Object:
					if (a["I"] != null)
					{
						return a["I"].Equals(b["I"]);
					}
					else if (a["T"] != null)
					{
						return a["T"].Equals(b["T"]);
					}
					else if (a["B"] != null)
					{
						return a["B"].Equals(b["B"]);
					}
					else
					{
						throw new ArgumentException("Unknown wrapped Atom type");
					}
				default:
					throw new ArgumentException("Unexpected Atom type " + a.Type);
			}
		}

		private static object DeserializeValue(JToken value, Type type)
		{
			if (value.Type == JTokenType.Boolean)
			{
				if (type != typeof(bool)) throw new ArgumentException("Unable to deserialize JSON type " + value.Type + " to type " + type);
				return value.Value<bool>();
			}
			else if (value.Type == JTokenType.String)
			{
				if (type != typeof(string)) throw new ArgumentException("Unable to deserialize JSON type " + value.Type + " to type " + type);
				return value.Value<string>();
			}
			else if (value.Type == JTokenType.Float || value.Type == JTokenType.Integer)
			{
				if (type == typeof(float))
				{
					return value.Value<float>();
				}
				else if (type == typeof(double))
				{
					return value.Value<double>();
				}
				throw new ArgumentException("Unable to deserialize JSON type " + value.Type + " to type " + type);
			}
			else if (value.Type == JTokenType.Object)
			{
				if (value["I"] != null)
				{
					//Integer values
					if (type.IsEnum)
					{
						return Enum.ToObject(type, value["I"].Value<ulong>());
					}
					if (type == typeof(int) || type == typeof(int?))
					{
						return value["I"].Value<int>();
					}
					else if (type == typeof(Int16) || type == typeof(Int16?))
					{
						return value["I"].Value<Int16>();
					}
					else if (type == typeof(UInt16) || type == typeof(UInt16?))
					{
						return value["I"].Value<UInt16>();
					}
					else if (type == typeof(int))
					{
						return value["I"].Value<int>();
					}
					else if (type == typeof(uint) || type == typeof(uint?))
					{
						return value["I"].Value<uint>();
					}
					else if (type == typeof(long) || type == typeof(long?))
					{
						return value["I"].Value<long>();
					}
					else if (type == typeof(ulong) || type == typeof(ulong?))
					{
						return value["I"].Value<ulong>();
					}
					throw new ArgumentException("Unable to deserialize Wrapped Integer to type " + type);
				}
				else if (value["T"] != null)
				{
					//DateTime values
					if (type == typeof(DateTime) || type == typeof(DateTime?))
					{
						return (new DateTime(1970, 1, 1)).AddMilliseconds(value["T"].Value<Int64>());
					}
					throw new ArgumentException("Unable to deserialize Wrapped Timestamp to type " + type);
				}
				else if (value["B"] != null)
				{
					//Byte array values
					if (type == typeof(byte[]))
					{
						return Utils.FromDBase64(value["B"].Value<string>());
					}
					else if (typeof(IList<byte>).IsAssignableFrom(type))
					{
						var array = Utils.FromDBase64(value["B"].Value<string>());
						IList collection = (IList)Activator.CreateInstance(type, new object[] { });
						foreach (var element in array) {
							collection.Add(element);
						}
						return collection;
					}
					throw new ArgumentException("Unable to deserialize Wrapped Byte array to type " + type);
				}
			}
			else if (value.Type == JTokenType.Array)
			{
				JArray array = value as JArray;
				if (type.IsArray)
				{
					object[] typedArray = (object[])Activator.CreateInstance(type, new object[] { array.Count });
					for (int i = 0; i < array.Count; ++i)
					{
						typedArray[i] = DeserializeValue(array[i], type.GetElementType());
					}
					return typedArray;
				}
				else
				{
					foreach (Type interfaceType in type.GetInterfaces())
					{
						if (interfaceType.IsGenericType &&
							interfaceType.GetGenericTypeDefinition()
							== typeof(IList<>))
						{
							Type itemType = interfaceType.GetGenericArguments()[0];
							IList collection = (IList)Activator.CreateInstance(type, new object[] { });
							foreach (var element in array)
							{
								collection.Add(DeserializeValue(element, itemType));
							}
							return collection;
						}
					}
				}
				throw new ArgumentException("Unable to deserialize Array to type " + type);
			}
			throw new ArgumentException("Unable to deserialize JSON type " + value.Type);
		}

		#endregion
	}

	/// <summary>
	/// A table of objects indexed by a string id
	/// </summary>
	/// <typeparam name="T">The type of objects stored in this table</typeparam>
	public class Table<T> : IEnumerable<T> 
		where T : class, new()
	{
		/// <summary>
		/// The id of this table
		/// </summary>
		public string Id
		{
			get
			{
				return _table.Id;
			}
		}

		/// <summary>
		/// Used to get the id for a tables record object type
		/// </summary>
		/// <param name="obj">a record object in a table</param>
		/// <returns></returns>
		public delegate string IdGetter(T obj);

		/// <summary>
		/// Used to set the id for a record object in a table
		/// </summary>
		/// <param name="obj">a record object in a table</param>
		/// <param name="id">the id to set for this object</param>
		public delegate void IdSetter(T obj, string id);

		/// <summary>
		/// Get the datastore this table belongs to
		/// </summary>
		public Datastore Datastore
		{
			get
			{
				return _table.Datastore;
			}
		}

		private Table _table;

		internal Table(Table table)
		{
			_table = table;
		}

		/// <summary>
		/// Insert an object into a table row. The objects public fields or properties should only
		/// contain primitive types or lists of primitive types in order to be serialized successfully
		/// </summary>
		/// <param name="idGetter">A function which returns the id value for the object to be inserted. This value cannot be null or empty</param>
		/// <param name="insert">The object to insert</param>
		/// <returns>True if the object is inserted, False if a row with this id already exists</returns>
		public bool Insert(IdGetter idGetter, T insert)
		{
			return _table.Insert(obj => idGetter(obj as T), null, insert);
		}

		/// <summary>
		/// Insert an object into a table row. The objects public fields or properties should only
		/// contain primitive types or lists of primitive types in order to be serialized successfully
		/// </summary>
		/// <param name="id">The row id to insert the object into</param>
		/// <param name="insert">The object to insert</param>
		/// <returns>True if the object is inserted, False if a row with this id already exists</returns>
		public bool Insert(string id, T insert)
		{
			return _table.Insert(obj => id,null, insert);
		}

		/// <summary>
		/// Insert an object with a public Id field/property into a table row. The objects public fields or properties should only
		/// contain primitive types or lists of primitive types in order to be serialized successfully. If the public Id field is
		/// empty or null, it will be set to an auto generated Id string.
		/// </summary>
		/// <param name="insert">The object to insert</param>
		/// <returns>True if the object is inserted, False if a row with this id already exists</returns>
		public bool Insert(T insert)
		{
			return _table.Insert(GetId,SetId,insert);
		}

		/// <summary>
		/// Update an object in a table row. The objects public fields or properties should only
		/// contain primitive types or lists of primitive types in order to be serialized successfully
		/// </summary>
		/// <param name="update">The object to update</param>
		/// <param name="idGetter">A function that returns the id value for the object being updated. This value cannot be null or empty</param>
		/// <returns>True if the object is updated, False if a row with this id does not exist</returns>
		public bool Update(IdGetter idGetter, T update)
		{
			return _table.Update(obj => idGetter(obj as T), update);
		}

		/// <summary>
		/// Update an object in a table row. The objects public fields or properties should only
		/// contain primitive types or lists of primitive types in order to be serialized successfully
		/// </summary>
		/// <param name="update">The object to update</param>
		/// <param name="id">The row id which the object should update</param>
		/// <returns>True if the object is updated, False if a row with this id does not exist</returns>
		public bool Update(string id, T update)
		{
			return _table.Update(obj=>id, update);
		}

		/// <summary>
		/// Update an object with a public Id field/property in a table row. The objects public fields or properties should only
		/// contain primitive types or lists of primitive types in order to be serialized successfully
		/// </summary>
		/// <param name="update">The object to update</param>
		/// <returns>True if the object is updated, False if a row with this id does not exist</returns>
		public bool Update(T update)
		{
			return _table.Update(GetId,update);
		}

		private static string GetId(object update)
		{
			FieldInfo fieldInfo = update.GetType().GetField("Id");
			if (fieldInfo != null && fieldInfo.FieldType == typeof(string))
			{
				return fieldInfo.GetValue(update) as string;
			}
			else
			{
				PropertyInfo propInfo = update.GetType().GetProperty("Id");
				if (propInfo != null && propInfo.PropertyType == typeof(string) && propInfo.CanRead && propInfo.CanWrite)
				{
					return propInfo.GetValue(update, null) as string;
				}
			}

			throw new ArgumentException("Object does not have a public string Id field or property");
		}

		private static void SetId(object update, string id)
		{
			FieldInfo fieldInfo = update.GetType().GetField("Id");
			if (fieldInfo != null && fieldInfo.FieldType == typeof(string))
			{
				fieldInfo.SetValue(update, id);
				return;
			}
			else
			{
				PropertyInfo propInfo = update.GetType().GetProperty("Id");
				if (propInfo != null && propInfo.PropertyType == typeof(string) && propInfo.CanRead && propInfo.CanWrite)
				{
					propInfo.SetValue(update, id, null);
					return;
				}
			}

			throw new ArgumentException("Object does not have a public string Id field or property");
		}

		/// <summary>
		/// Delete a row from the table
		/// </summary>
		/// <param name="id">The row id to remove from the table</param>
		/// <returns>True if the row was removed, False if the row did not exist</returns>
		public bool Delete(string id)
		{
			return _table.Delete(id);
		}

		/// <summary>
		/// Get a row from the table and convert it into the specified object type. Will fail
		/// if the object cannot be created from the supplied row data
		/// </summary>
		/// <param name="id">The row id where the data should be retrieved from</param>
		/// <returns>The constructed row object</returns>
		public T Get(string id)
		{
			return _table.Get<T>(SetId,id);
		}

		/// <summary>
		/// Get a row from the table and convert it into the specified object type. Will fail
		/// if the object cannot be created from the supplied row data
		/// </summary>
		/// <param name="idSetter">A function that will be called on the object to populate the objects id field</param>
		/// <param name="id">The row id where the data should be retrieved from</param>
		/// <returns>The constructed row object</returns>
		public T Get(IdSetter idSetter,string id)
		{
			return _table.Get<T>((obj, value) => idSetter(obj as T, value), id);
		}

		#region IEnumerable<T> Members

		/// <summary>
		/// Gets an enumerator containing all the objects in this table
		/// </summary>
		/// <returns></returns>
		public IEnumerator<T> GetEnumerator()
		{
			return _table.GetAll<T>(SetId).GetEnumerator();
		}

		/// <summary>
		/// Gets an enumerator containing all the objects in this table
		/// </summary>
		/// <param name="idSetter">A function that will be called on all objects to populate the objects id field</param>
		/// <returns></returns>
		public IEnumerator<T> GetEnumerator(IdSetter idSetter)
		{
			return _table.GetAll<T>((obj, value) => idSetter(obj as T,value)).GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return _table.GetAll<T>(SetId).GetEnumerator();
		}

		#endregion
	}
}
