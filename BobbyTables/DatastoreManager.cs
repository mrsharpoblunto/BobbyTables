using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
#if !PORTABLE
using System.Security.Cryptography;
#endif
using System.Text;
using System.Text.RegularExpressions;
#if NET45 || NET40 || PORTABLE
using System.Threading.Tasks;
#endif
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BobbyTables
{
	/// <summary>
	/// Used to define the options when querying a datastore
	/// </summary>
	public enum DatastoreQueryOptions
	{
		/// <summary>
		/// Use a locally cached result if available (if no local result is available, will fetch from Dropbox)
		/// </summary>
		UseCached,

		/// <summary>
		/// Fetch new results from Dropbox
		/// </summary>
		ForceRefresh
	}

	/// <summary>
	/// Manages listing, retrieving, adding and removing datastores from dropbox
	/// </summary>
	public class DatastoreManager
	{
		/// <summary>
		/// The OAuth token used to authenticate with Dropbox
		/// </summary>
		public string ApiToken { get; private set; }

		private Dictionary<string, Datastore> _datastores;
		private string _token;
		private Regex _keyRegex;

		/// <summary>
		/// Construct a new Datastore manager and use the supplied apiToken for all Dropbox API calls
		/// </summary>
		/// <param name="apiToken">The OAuth token used to authenticate with Dropbox</param>
		public DatastoreManager(string apiToken)
		{
			ApiToken = apiToken;
			_datastores = new Dictionary<string, Datastore>();
			_keyRegex = new Regex("[-_A-Za-z0-9]{32,1000}");
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Retrieve a datastore object from dropbox asynchronously
		/// </summary>
		/// <param name="id">The id of the datastore</param>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <returns></returns>
		public async Task<Datastore> GetAsync(string id, DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			Datastore store;
			if (!_datastores.TryGetValue(id, out store) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = GetRequest(id);
				var response = await request.GetResponseAsync();
				store = GetResponse(id, store, response);
			}
			return store;
		}
#endif

		/// <summary>
		/// Retrieve a datastore object from dropbox asynchronously
		/// </summary>
		/// <param name="id">The id of the datastore</param>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		/// <returns></returns>
		public void GetAsync(string id, Action<Datastore> success, Action<Exception> failure, DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			Datastore store;
			if (!_datastores.TryGetValue(id, out store) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = GetRequest(id);
				request.GetResponseAsync(response =>
				{
					try
					{
						store = GetResponse(id, store, response);
						success(store);
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}
			else
			{
				success(store);
			}
		}

#if !PORTABLE
		/// <summary>
		/// Retrieve a datastore object from dropbox
		/// </summary>
		/// <param name="id">The id of the datastore</param>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <returns></returns>
		public Datastore Get(string id, DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			Datastore store;
			if (!_datastores.TryGetValue(id, out store) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = GetRequest(id);
				var response = request.GetResponse();
				store = GetResponse(id, store, response);
			}
			return store;
		}
#endif

		private IApiRequest GetRequest(string id)
		{
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "get_datastore", ApiToken);
			request.AddParam("dsid", id);
			return request;
		}

		private Datastore GetResponse(string id, Datastore store, ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call list_datastores returned status code " + response.StatusCode);
			}

			if (response.Body["notfound"] != null)
			{
				if (store != null)
				{
					_datastores.Remove(id);
				}
				return null;
			}

			if (store == null)
			{
				store = new Datastore(this, id, response.Body["handle"].Value<string>());
				_datastores.Add(id, store);
			}
			else
			{
				store = new Datastore(this, id, response.Body["handle"].Value<string>());
				_datastores[id] = store;
			}
			return store;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// List all dropbox datastores asynchronously
		/// </summary>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <returns>A list of all Dropbox datastores</returns>
		public async Task<IEnumerable<Datastore>> ListAsync(DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			if (string.IsNullOrEmpty(_token) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = ListRequest();
				var response = await request.GetResponseAsync();
				ListResponse(response);
			}
			return _datastores.Values;
		}
#endif

		/// <summary>
		/// List all dropbox datastores asynchronously
		/// </summary>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		/// <returns></returns>
		public void ListAsync(Action<IEnumerable<Datastore>> success,Action<Exception> failure,DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			if (string.IsNullOrEmpty(_token) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = ListRequest();
				request.GetResponseAsync(response =>
				{
					try
					{
						ListResponse(response);
						success(_datastores.Values);
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}
			else
			{
				success(_datastores.Values);
			}
		}

#if !PORTABLE
		/// <summary>
		/// List all dropbox datastores
		/// </summary>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <returns>A list of all Dropbox datastores</returns>
		public IEnumerable<Datastore> List(DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			if (string.IsNullOrEmpty(_token) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = ListRequest();
				var response = request.GetResponse();
				ListResponse(response);
			}
			return _datastores.Values;
		}
#endif

		private IApiRequest ListRequest()
		{
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("GET", "list_datastores", ApiToken);
			return request;
		}

		private void ListResponse(ApiResponse response)
		{
			_datastores.Clear();
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call list_datastores returned status code " + response.StatusCode);
			}

			ListDatastores(response.Body);
		}

		private void ListDatastores(JObject response)
		{
			_token = response["token"].Value<string>();
			var dataStores = response["datastores"] as JArray;
			foreach (var datastore in dataStores)
			{
				Datastore store;
				string dsid = datastore["dsid"].Value<string>();
				string handle = datastore["handle"].Value<string>();
				if (!_datastores.TryGetValue(dsid, out store))
				{
					store = new Datastore(this, dsid, handle);
					_datastores.Add(dsid, store);
				}
				if (datastore["info"] != null)
				{
					var title = datastore["info"]["title"].Value<string>();
					if (!string.IsNullOrEmpty(title))
					{
						store.Title = title;
					}
					var mtime = datastore["info"]["mtime"];
					if (mtime != null)
					{
						store.Modified = (new DateTime(1970, 1, 1)).AddMilliseconds(mtime["T"].Value<Int64>());
					}
				}
			}
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Wait asynchronously for up to a minute while checking for changes to the datastore list.
		/// </summary>
		/// <returns>If a change occurs during this time, returns True</returns>
		public async Task<bool> AwaitListChangesAsync()
		{
			IApiRequest request = AwaitListChangesRequest();
			ApiResponse response = await request.GetResponseAsync();
			return AwaitListChangesResponse(response);
		}
#endif

		/// <summary>
		/// Wait asynchronously for up to a minute while checking for changes to the datastore list.
		/// </summary>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		/// <returns>If a change occurs during this time, returns True</returns>
		public void AwaitListChangesAsync(Action<bool> success,Action<Exception> failure)
		{
			IApiRequest request = AwaitListChangesRequest();
			request.GetResponseAsync(response =>
			{
				try
				{
					success(AwaitListChangesResponse(response));
				}
				catch (Exception ex)
				{
					failure(ex);
				}
			});
		}

#if !PORTABLE
		/// <summary>
		/// Wait for up to a minute while checking for changes to the datastore list.
		/// </summary>
		/// <returns>If a change occurs during this time, returns True</returns>
		public bool AwaitListChanges()
		{
			IApiRequest request = AwaitListChangesRequest();
			ApiResponse response = request.GetResponse();
			return AwaitListChangesResponse(response);
		}
#endif

		private IApiRequest AwaitListChangesRequest()
		{
			JObject args = new JObject();
			args["token"] = _token;
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("GET", "await?list_datastores=" + Uri.EscapeDataString(args.ToString(Formatting.None)), ApiToken);
			return request;
		}

		private bool AwaitListChangesResponse(ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call await returned status code " + response.StatusCode);
			}

			var listDatastores = response.Body["list_datastores"] as JObject;
			if (listDatastores != null)
			{
				ListDatastores(listDatastores);
				return true;
			}
			return false;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Wait for up to a minute asynchronously while checking for changes to any datastore. The list of datastores
		/// is defined by internally calling the <see cref="ListAsync(DatastoreQueryOptions)" /> method
		/// </summary>
		/// <param name="changed">Adds every datastore which had a change during the waiting time</param>
		/// <returns>True if any changes were detected</returns>
		public async Task<bool>  AwaitDatastoreChangesAsync(List<Datastore> changed)
		{
			var datastores = await ListAsync();
			IApiRequest request = AwaitDatastoreChangesRequest(datastores);

			ApiResponse response = await request.GetResponseAsync();
			bool result = AwaitDatastoreChangesResponse(changed, datastores, response);
			return result;
		}
#endif

		/// <summary>
		/// Wait for up to a minute asynchronously while checking for changes to any datastore. The list of datastores
		/// is defined by internally calling the <see cref="ListAsync(Action&lt;IEnumerable&lt;Datastore&gt;&gt;, Action&lt;Exception&gt;, DatastoreQueryOptions)" /> method
		/// </summary>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		public void AwaitDatastoreChangesAsync(Action<List<Datastore>> success,Action<Exception> failure)
		{
			ListAsync(list =>
			{
				IApiRequest request = AwaitDatastoreChangesRequest(list);
				request.GetResponseAsync(response =>
				{
					try
					{
						List<Datastore> datastores = new List<Datastore>();
						AwaitDatastoreChangesResponse(datastores, list, response);
						success(datastores);
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}, ex =>
			{
				failure(ex);
			});
		}

#if !PORTABLE
		/// <summary>
		/// Wait for up to a minute while checking for changes to any datastore. The list of datastores
		/// is defined by internally calling the <see cref="List" /> method
		/// </summary>
		/// <param name="changed">Adds every datastore which had a change during the waiting time</param>
		/// <returns>True if any changes were detected</returns>
		public bool AwaitDatastoreChanges(List<Datastore> changed)
		{
			var datastores = List();
			IApiRequest request = AwaitDatastoreChangesRequest(datastores);

			ApiResponse response = request.GetResponse();
			return AwaitDatastoreChangesResponse(changed, datastores, response);
		}
#endif

		private IApiRequest AwaitDatastoreChangesRequest(IEnumerable<Datastore> datastores)
		{
			JObject args = new JObject();
			args["cursors"] = new JObject();
			foreach (var store in datastores)
			{
				args["cursors"][store.Handle] = store.Rev;
			}

			IApiRequest request = ApiRequestFactory.Current.CreateRequest("GET", "await?get_deltas=" + Uri.EscapeDataString(args.ToString(Formatting.None)), ApiToken);
			return request;
		}

		private static bool AwaitDatastoreChangesResponse(List<Datastore> changed, IEnumerable<Datastore> datastores, ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("await returned status code " + response.StatusCode);
			}

			var getDeltas = response.Body["get_deltas"];
			if (getDeltas != null)
			{
				var allDeltas = getDeltas["deltas"];
				foreach (var store in datastores)
				{
					var delta = allDeltas[store.Handle];
					if (delta != null && delta["notfoundresult"] == null)
					{
						store.ApplyChanges(delta);
						changed.Add(store);
					}
				}
			}
			return changed.Count > 0;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Gets a datastore asynchronously if it already exists, otherwise it is created
		/// </summary>
		/// <param name="id">The Dropbox id of the datastore</param>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <returns>The existing or newly created datastore</returns>
		public async Task<Datastore> GetOrCreateAsync(string id, DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			Datastore store;
			if (!_datastores.TryGetValue(id, out store) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = GetOrCreateRequest(id);
				ApiResponse response = await request.GetResponseAsync();
				store = GetOrCreateResponse(id, store, response);
			}
			return store;
		}
#endif
		/// <summary>
		/// Gets a datastore asynchronously if it already exists, otherwise it is created
		/// </summary>
		/// <param name="id">The Dropbox id of the datastore</param>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		public void GetOrCreateAsync(string id, Action<Datastore> success,Action<Exception> failure,DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			Datastore store;
			if (!_datastores.TryGetValue(id, out store) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = GetOrCreateRequest(id);
				request.GetResponseAsync(response =>
				{
					try
					{
						store = GetOrCreateResponse(id, store, response);
						success(store);
					}
					catch (Exception ex)
					{
						failure(ex);
					}
				});
			}
			else
			{
				success(store);
			}
		}

#if !PORTABLE
		/// <summary>
		/// Gets a datastore if it already exists, otherwise it is created
		/// </summary>
		/// <param name="id">The Dropbox id of the datastore</param>
		/// <param name="options">Whether to retrieve a locally cached version, or to recheck with dropbox</param>
		/// <returns>The existing or newly created datastore</returns>
		public Datastore GetOrCreate(string id, DatastoreQueryOptions options = DatastoreQueryOptions.UseCached)
		{
			Datastore store;
			if (!_datastores.TryGetValue(id, out store) || options == DatastoreQueryOptions.ForceRefresh)
			{
				IApiRequest request = GetOrCreateRequest(id);
				ApiResponse response = request.GetResponse();
				store = GetOrCreateResponse(id, store, response);
			}
			return store;
		}
#endif

		private IApiRequest GetOrCreateRequest(string id)
		{
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "get_or_create_datastore", ApiToken);
			request.AddParam("dsid", id);
			return request;
		}

		private Datastore GetOrCreateResponse(string id, Datastore store, ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call get_or_create_datastore returned status code " + response.StatusCode);
			}

			if (store == null)
			{
				store = new Datastore(this, id, response.Body["handle"].Value<string>());
				_datastores.Add(id, store);
			}
			else
			{
				store = new Datastore(this, id, response.Body["handle"].Value<string>());
				_datastores[id] = store;
			}
			return store;
		}

#if PORTABLE
		/// <summary>
		/// Creates a new globally unique datastore
		/// </summary>
		/// <param name="key">Used to generate a hash that will comprise the created datastores dropbox id</param>
		/// <param name="id">A SHA256 hash of the key (see https://www.dropbox.com/developers/datastore/docs/http#create_datastore for details on how to generate the id from a key)</param>
		/// <returns>The datastore created</returns>
		public async Task<Datastore> Create(string key,string id)
		{
			if (!_keyRegex.IsMatch(key))
			{
				throw new DatastoreException("Key did not match [-_A-Za-z0-9]{32,1000}");
			}

			if (_datastores.ContainsKey(id))
			{
				throw new DatastoreException("Datastore with id "+id+" already exists");
			}

			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "create_datastore", ApiToken);
			request.AddParam("dsid", id);
			request.AddParam("key", key);

			ApiResponse response = await request.GetResponseAsync();
			return CreateResponse(key, id, response);
		}
#else
		/// <summary>
		/// Creates a new globally unique datastore
		/// </summary>
		/// <param name="key">Used to generate a hash that will comprise the created datastores dropbox id</param>
		/// <returns>A Tuple containing the id of the created datastore, and the datastore itself</returns>
		public KeyValuePair<string,Datastore> Create(string key)
		{
			string id;
			IApiRequest request = CreateRequest(key, out id);
			ApiResponse response = request.GetResponse();
			return new KeyValuePair<string, Datastore>(id, CreateResponse(key, id, response));
		}

		private IApiRequest CreateRequest(string key, out string id)
		{
			if (!_keyRegex.IsMatch(key))
			{
				throw new DatastoreException("Key did not match [-_A-Za-z0-9]{32,1000}");
			}

			using (var hash = SHA256.Create())
			{
				id = "." + Utils.ToDBase64(hash.ComputeHash(Encoding.UTF8.GetBytes(key)));
			}

			if (_datastores.ContainsKey(id))
			{
				throw new DatastoreException("Datastore with id "+id+" already exists");
			}

			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "create_datastore", ApiToken);
			request.AddParam("dsid", id);
			request.AddParam("key", key);
			return request;
		}

		/// <summary>
		/// Creates a new globally unique datastore asynchronously
		/// </summary>
		/// <param name="key">Used to generate a hash that will comprise the created datastores dropbox id</param>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		public void CreateAsync(string key,Action<KeyValuePair<string,Datastore>> success,Action<Exception> failure)
		{
			string id;
			try 
			{
				IApiRequest request = CreateRequest(key, out id);
				request.GetResponseAsync(response => {
					try 
					{
						success(new KeyValuePair<string, Datastore>(id, CreateResponse(key, id, response)));
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
#endif

#if NET45 || NET40
		/// <summary>
		/// Creates a new globally unique datastore asynchronously
		/// </summary>
		/// <param name="key">Used to generate a hash that will comprise the created datastores dropbox id</param>
		/// <returns>A Tuple containing the id of the created datastore, and the datastore itself</returns>
		public async Task<KeyValuePair<string,Datastore>> CreateAsync(string key)
		{
			string id;
			IApiRequest request = CreateRequest(key, out id);
			ApiResponse response = await request.GetResponseAsync();
			return new KeyValuePair<string, Datastore>(id, CreateResponse(key, id, response));
		}
#endif

		private Datastore CreateResponse(string key, string id, ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call create_datastore returned status code " + response.StatusCode);
			}

			if (response.Body["notfound"] != null)
			{
				throw new DatastoreException("Datastore with key " + key + " (id " + id + ") not found");
			}

			Datastore store = new Datastore(this, id, response.Body["handle"].Value<string>());
			_datastores.Add(id, store);
			return store;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Delete a datastore and all its tables and data asynchronously
		/// </summary>
		/// <param name="datastore">The datastore to delete</param>
		/// <returns>True if the deletion succeeds</returns>
		public async Task<bool> DeleteAsync(Datastore datastore)
		{
			IApiRequest request = DeleteRequest(datastore);
			ApiResponse response = await request.GetResponseAsync();
			return DeleteResponse(datastore, response);
		}
#endif
		/// <summary>
		/// Delete a datastore and all its tables and data asynchronously
		/// </summary>
		/// <param name="datastore">The datastore to delete</param>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		public void DeleteAsync(Datastore datastore, Action<bool> success,Action<Exception> failure)
		{
			IApiRequest request = DeleteRequest(datastore);
			request.GetResponseAsync(response =>
			{
				try
				{
					success(DeleteResponse(datastore, response));
				}
				catch (Exception ex)
				{
					failure(ex);
				}
			});
		}

#if !PORTABLE
		/// <summary>
		/// Delete a datastore and all its tables and data
		/// </summary>
		/// <param name="datastore">The datastore to delete</param>
		/// <returns>True if the deletion succeeds</returns>
		public bool Delete(Datastore datastore)
		{
			IApiRequest request = DeleteRequest(datastore);
			ApiResponse response = request.GetResponse();
			return DeleteResponse(datastore, response);
		}
#endif

		private IApiRequest DeleteRequest(Datastore datastore)
		{
			IApiRequest request = ApiRequestFactory.Current.CreateRequest("POST", "delete_datastore", ApiToken);
			request.AddParam("handle", datastore.Handle);
			return request;
		}

		private bool DeleteResponse(Datastore datastore, ApiResponse response)
		{
			if (response.StatusCode != 200)
			{
				throw new DatastoreException("Api call create_datastore returned status code " + response.StatusCode);
			}

			if (response.Body["notfound"] != null)
			{
				throw new DatastoreException("Datastore with id " + datastore.Id + " not found");
			}

			_datastores.Remove(datastore.Id);
			return response.Body["ok"]!=null;
		}

		/// <summary>
		/// Loads a datastore from a previous snapshot. If this datastore manager already has a datastore with
		/// the same Id, loading a snapshot will replace the existing datastore state
		/// </summary>
		/// <param name="reader">A reader containing the snapshot data</param>
		/// <returns>The loaded datastore</returns>
		public Datastore Load(TextReader reader)
		{
			Datastore store = new Datastore(this, reader);
			if (_datastores.ContainsKey(store.Id))
			{
				_datastores[store.Id] = store;
			}
			else
			{
				_datastores.Add(store.Id, store);
			}
			return store;
		}
	}
}
