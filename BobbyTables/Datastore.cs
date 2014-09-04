using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
#if NET45 || NET40 || PORTABLE
using System.Threading.Tasks;
#endif

namespace BobbyTables
{
	/// <summary>
	/// A callback delegate for the async datastore pull operation
	/// </summary>
	public delegate void PullDelegate();

	/// <summary>
	/// A datastore which contains tables containing object data
	/// </summary>
    public class Datastore
    {
		/// <summary>
		/// The Dropbox Id of this datastore
		/// </summary>
		public string Id { get; private set; }

		/// <summary>
		/// The Dropbox Handle for this datastore
		/// </summary>
		public string Handle { get; private set; }

		/// <summary>
		/// The Dropbox revision number for this datastore
		/// </summary>
		public long Rev { get; private set; }

		/// <summary>
		/// The title for this datastore Dropbox as defined by its Dropbox metadata
		/// </summary>
		public string Title { get; set; }

		/// <summary>
		/// The last modified date for this datastore Dropbox as defined by its Dropbox metadata
		/// </summary>
		public DateTime Modified { get; set; }

		private Dictionary<string, Table> _tables;
		private DatastoreManager _manager;

		internal Datastore(DatastoreManager manager,string id,string handle)
		{
			Id = id;
			Handle = handle;
			Rev = 0;
			_manager = manager;
			_tables = new Dictionary<string, Table>();
		}

		internal Datastore(DatastoreManager manager, TextReader reader)
		{
			_manager = manager;
			_tables = new Dictionary<string, Table>();

			using (var jsonReader = new JsonTextReader(reader))
			{
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.StartObject)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected StartObject but was " + jsonReader.TokenType);
				}
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.PropertyName)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected PropertyName 'id' but was " + jsonReader.TokenType);
				}
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.String)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected String but was " + jsonReader.TokenType);
				}
				Id = (string)jsonReader.Value;
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.PropertyName)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected PropertyName 'handle' but was " + jsonReader.TokenType);
				}
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.String)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected String but was " + jsonReader.TokenType);
				}
				Handle = (string)jsonReader.Value;
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.PropertyName)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected PropertyName 'rev' but was " + jsonReader.TokenType);
				}
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.Integer)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected Integer but was " + jsonReader.TokenType);
				}
				Rev = (long)jsonReader.Value;
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.PropertyName)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected PropertyName 'tables' but was " + jsonReader.TokenType);
				}
				if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.StartObject)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected StartObject but was " + jsonReader.TokenType);
				}
				while (jsonReader.Read() && jsonReader.TokenType==JsonToken.PropertyName)
				{
					var table = new Table(_manager,this, (string)jsonReader.Value);
					_tables.Add(table.Id, table);

					table.Load(jsonReader);
				}
				if (jsonReader.TokenType != JsonToken.EndObject)
				{
					throw new DatastoreException("Error loading datastore snapshot. Expected EndObject but was " + jsonReader.TokenType);
				}
			}
		}

		/// <summary>
		/// Save the database snapshot. Pending changes will not be saved
		/// </summary>
		/// <param name="writer">A writer to save the snapshot to</param>
		public void Save(TextWriter writer)
		{
			using (var jsonWriter = new JsonTextWriter(writer))
			{
				jsonWriter.WriteStartObject();
				jsonWriter.WritePropertyName("id");
				jsonWriter.WriteValue(Id);
				jsonWriter.WritePropertyName("handle");
				jsonWriter.WriteValue(Handle);
				jsonWriter.WritePropertyName("rev");
				jsonWriter.WriteValue(Rev);
				jsonWriter.WritePropertyName("tables");
				jsonWriter.WriteStartObject();
				foreach (var table in _tables)
				{
					jsonWriter.WritePropertyName(table.Key);
					table.Value.Save(jsonWriter);
				}
				jsonWriter.WriteEndObject();
				jsonWriter.WriteEndObject();
			}
		}


		/// <summary>
		/// Revert all pending local changes
		/// </summary>
		public void Revert()
		{
			foreach (var table in _tables)
			{
				table.Value.RevertPendingChanges();
			}
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Pull all pending deltas asynchronously (or get a new snapshot if this database hasn't been loaded before) and apply them.
		/// Will throw a DatastoreException if this method is called when any local changes are pending
		/// </summary>
		public async Task PullAsync()
		{
			foreach (var table in _tables)
			{
				if (table.Value.HasPendingChanges)
				{
					throw new DatastoreException("Unable to pull in remote changes to Datastore with id " + Id + " as it has local changes pending");
				}
			}

			if (Rev == 0)
			{
				IApiRequest request = GetSnapshotRequest();
				ApiResponse response = await request.GetResponseAsync();
				GetSnapshotResponse(response);
			}
			else
			{
				IApiRequest request = GetDeltasRequest();
				ApiResponse response = await request.GetResponseAsync();
				GetDeltasResponse(response);
			}
		}
#endif

		/// <summary>
		/// Pull all pending deltas asynchronously (or get a new snapshot if this database hasn't been loaded before) and apply them.
		/// Will throw a DatastoreException if this method is called when any local changes are pending
		/// </summary>
		public void PullAsync(PullDelegate success, Action<Exception> failure)
		{
			foreach (var table in _tables)
			{
				if (table.Value.HasPendingChanges)
				{
					failure(new DatastoreException("Unable to pull in remote changes to Datastore with id " + Id + " as it has local changes pending"));
					return;
				}
			}

			if (Rev == 0)
			{
				IApiRequest request = GetSnapshotRequest();
				request.GetResponseAsync(response =>
				{
					try
					{
						GetSnapshotResponse(response);
						success();
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}
			else
			{
				IApiRequest request = GetDeltasRequest();
				request.GetResponseAsync(response =>
				{
					try
					{
						GetDeltasResponse(response);
						success();
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}
		}

#if !PORTABLE
		/// <summary>
		/// Pull all pending deltas (or get a new snapshot if this database hasn't been loaded before) and apply them.
		/// Will throw a DatastoreException if this method is called when any local changes are pending
		/// </summary>
		public void Pull()
		{
			foreach (var table in _tables)
			{
				if (table.Value.HasPendingChanges)
				{
					throw new DatastoreException("Unable to pull in remote changes to Datastore with id " + Id + " as it has local changes pending");
				}
			}

			if (Rev == 0)
			{
				IApiRequest request = GetSnapshotRequest();
				ApiResponse response = request.GetResponse();
				GetSnapshotResponse(response);
			}
			else
			{
				IApiRequest request = GetDeltasRequest();
				ApiResponse response = request.GetResponse();
				GetDeltasResponse(response);
			}
		}
#endif

		private IApiRequest GetSnapshotRequest()
		{
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "get_snapshot", _manager.ApiToken);
			request.AddParam("handle", Handle);
			return request;
		}


		private void GetSnapshotResponse(ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call get_snapshot returned status code " + response.StatusCode);
			}

			if (response.Body["notfound"] != null)
			{
				throw new DatastoreException("Datastore with id " + Id + " not found");
			}

			Rev = response.Body["rev"].Value<int>();
			var rows = response.Body["rows"] as JArray;
			foreach (var row in rows)
			{
				var tid = row["tid"].Value<string>();
				var rowid = row["rowid"].Value<string>();

				Table table;
				if (!_tables.TryGetValue(tid, out table))
				{
					table = new Table(_manager, this, tid);
					_tables.Add(tid, table);
				}
				table.InsertInternal(rowid, row["data"] as JObject);
			}
		}

		private IApiRequest GetDeltasRequest()
		{
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "get_deltas", _manager.ApiToken);
			request.AddParam("handle", Handle);
			request.AddParam("rev", Rev.ToString());
			return request;
		}

		private void GetDeltasResponse(ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call get_deltas returned status code " + response.StatusCode);
			}

			if (response.Body["notfound"] != null)
			{
				throw new DatastoreException("Datastore " + Handle + " with rev " + Rev + " not found");
			}

			ApplyChanges(response.Body);
		}

		internal void ApplyChanges(JToken response)
		{
			var deltas = response["deltas"] as JArray;
			if (deltas == null) return;

			foreach (var delta in deltas)
			{
				var rev = delta["rev"].Value<int>();
				if (rev < Rev) continue;
				Rev = rev + 1;
				var changes = delta["changes"] as JArray;
				foreach (JArray change in changes)
				{
					var tid = change[1].Value<string>();
					var rowid = change[2].Value<string>();

					Table table;
					if (!_tables.TryGetValue(tid, out table))
					{
						table = new Table(_manager, this, tid);
						_tables.Add(tid, table);
					}
					table.ApplyChange(change);
				}
			}
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Wait for changes to this datastore from the server and apply them when(if) they arrive.
		/// This method will throw a DatastoreException if any local changes are pending
		/// </summary>
		/// <returns>True if changes were applied before the wait time elapsed</returns>
		public async Task<bool> AwaitPullAsync()
		{
			IApiRequest request = AwaitPullRequest();
			ApiResponse response = await request.GetResponseAsync();
			return AwaitPullResponse(response);
		}
#endif

		/// <summary>
		/// Wait for changes to this datastore from the server and apply them when(if) they arrive.
		/// This method will throw a DatastoreException if any local changes are pending
		/// </summary>
		/// <returns>True if changes were applied before the wait time elapsed</returns>
		public void AwaitPullAsync(Action<bool> success,Action<Exception> failure)
		{
			try
			{
				IApiRequest request = AwaitPullRequest();
				request.GetResponseAsync(response =>
				{
					try
					{
						success(AwaitPullResponse(response));
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}
			catch (Exception ex)
			{
				failure(ex);
			}
		}

#if !PORTABLE
		/// <summary>
		/// Wait for changes to this datastore from the server and apply them when(if) they arrive.
		/// This method will throw a DatastoreException if any local changes are pending
		/// </summary>
		/// <returns>True if changes were applied before the wait time elapsed</returns>
		public bool AwaitPull()
		{
			IApiRequest request = AwaitPullRequest();
			ApiResponse response = request.GetResponse();
			return AwaitPullResponse(response);
		}
#endif

		private IApiRequest AwaitPullRequest()
		{
			foreach (var table in _tables)
			{
				if (table.Value.HasPendingChanges)
				{
					throw new DatastoreException("Unable to pull in remote changes to Datastore with id " + Id + " as it has local changes pending");
				}
			}

			JObject args = new JObject();
			args["cursors"] = new JObject();
			args["cursors"][Handle] = Rev;

			IApiRequest request = ApiRequestFactory.Current.CreateRequest("GET", "await?get_deltas=" + Uri.EscapeDataString(args.ToString(Formatting.None)), _manager.ApiToken);

			return request;
		}

		private bool AwaitPullResponse(ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("await returned status code " + response.StatusCode);
			}

			var getDeltas = response.Body["get_deltas"];
			if (getDeltas != null)
			{
				var result = getDeltas["deltas"][Handle];

				if (result["notfoundresult"] != null)
				{
					throw new DatastoreException("Datastore " + Handle + " not found, or was deleted");
				}
				ApplyChanges(result);
				return true;
			}
			return false;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Push all pending changes to dropbox and apply them to the local datastore copy if the commit succeeds
		/// </summary>
		/// <returns>True if the changes are accepted without any errors or conflicts</returns>
		public async Task<bool> PushAsync()
		{
			IApiRequest request = PushRequest();
			ApiResponse response = await request.GetResponseAsync();
			return PushResponse(response);
		}
#endif

		/// <summary>
		/// Push all pending changes to dropbox and apply them to the local datastore copy if the commit succeeds
		/// </summary>
		/// <returns>True if the changes are accepted without any errors or conflicts</returns>
		public void PushAsync(Action<bool> success,Action<Exception> failure)
		{
			IApiRequest request = PushRequest();
			request.GetResponseAsync(response =>
			{
				try
				{
					success(PushResponse(response));
				}
				catch (Exception ex)
				{
					failure(ex);
				}
			});
		}

#if !PORTABLE
		/// <summary>
		/// Push all pending changes to dropbox and apply them to the local datastore copy if the commit succeeds
		/// </summary>
		/// <returns>True if the changes are accepted without any errors or conflicts</returns>
		public bool Push()
		{
			IApiRequest request = PushRequest();
			ApiResponse response = request.GetResponse();
			return PushResponse(response);
		}
#endif

		private IApiRequest PushRequest()
		{
			JArray args = new JArray();
			foreach (var table in _tables)
			{
				foreach (var change in table.Value.PendingChanges)
				{
					args.Add(change);
				}
			}

			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "put_delta", _manager.ApiToken);
			request.AddParam("handle", Handle);
			request.AddParam("rev", Rev.ToString());
			request.AddParam("changes", args.ToString(Formatting.None));
			return request;
		}


		private bool PushResponse(ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call put_delta returned status code " + response.StatusCode);
			}

			if (response.Body["notfoundresult"] != null)
			{
				throw new DatastoreException("Datastore " + Handle + " not found");
			}

			if (response.Body["conflict"] != null)
			{
				return false;
			}

			Rev = response.Body["rev"].Value<int>();

			foreach (var table in _tables)
			{
				table.Value.ApplyPendingChanges();
			}
			return true;
		}

		/// <summary>
		/// Create a new transaction. Any actions performed in the transaction will be committed atomically, and
		/// if the commit fails the datastore will be updated, and the actions will be retried until they succeed
		/// or the maximum number of retries is exceeded
		/// </summary>
		/// <param name="actions">The datastore operations to perform in the transaction. Note only synchronous operation
		/// should be performed within the actions delegate</param>
		/// <returns></returns>
		public Transaction Transaction(TransactionDelegate actions)
		{
			return new Transaction(this,actions);
		}

		/// <summary>
		/// Gets a strongly typed table wrapper
		/// </summary>
		/// <typeparam name="T">The type of objects stored in the rows of this table</typeparam>
		/// <param name="id">The dsid of the table</param>
		/// <returns>A strongly typed table wrapper</returns>
		public Table<T> GetTable<T>(string id) where T: class, new()
		{
			Table table;
			if (!_tables.TryGetValue(id, out table))
			{
				table = new Table(_manager,this,id);
				_tables.Add(id,table);
			}
			return new Table<T>(table);
		}
    }
}
