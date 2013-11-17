using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;
#if NET45 || NET40 || PORTABLE
using System.Threading.Tasks;
#endif

namespace BobbyTables
{
	public class ApiResponse
	{
		internal ApiResponse() 
		{ 
		}

		internal ApiResponse(int statusCode,string response)
		{
			StatusCode = statusCode;
			Body = JObject.Parse(response);
		}

		public int StatusCode { get; set; }
		public JObject Body { get; set; }
	}

	public interface IApiRequest
	{
		IApiRequest AddParam(string name, string value);
#if !PORTABLE
		ApiResponse GetResponse();
#endif
#if NET45 || NET40 || PORTABLE
		Task<ApiResponse> GetResponseAsync();
#endif
		void GetResponseAsync(Action<ApiResponse> completed);
	}

	public interface IApiRequestFactory
	{
		IApiRequest CreateRequest(string method,string url,string apiToken);
	}

	internal class ApiRequestFactory : IApiRequestFactory
	{
		static ApiRequestFactory()
		{
			Current = new ApiRequestFactory();
		}

		public static IApiRequestFactory Current { get; set; }

		public IApiRequest CreateRequest(string method, string url, string apiToken)
		{
			return new ApiRequest(method, url,apiToken);
		}
	}

	internal class ApiRequest: IApiRequest
	{
		public const string ApiBase = "https://api.dropbox.com/1/datastores/";

		private HttpWebRequest _request;
		private StringBuilder _params;

		public ApiRequest(string method, string url,string apiToken)
		{
			_params = new StringBuilder();
			_request = (HttpWebRequest)HttpWebRequest.Create(ApiBase+url);
			_request.Accept = "application/json, text/javascript";
			_request.Method = method;
			_request.Headers["Authorization"] = "Bearer " + apiToken;
		}

		public IApiRequest AddHeader(string name, string value)
		{
			_request.Headers[name] = value;
			return this;
		}

		public IApiRequest AddParam(string name,string value)
		{
			if (_params.Length > 0) _params.Append('&');
			_params.Append(name);
			_params.Append('=');
			_params.Append(Uri.EscapeDataString(value));
			return this;
		}

#if !PORTABLE
		public ApiResponse GetResponse()
		{
			ApiResponse result = new ApiResponse();

			try
			{
				if (_request.Method != "GET")
				{
					_request.ContentType = "application/x-www-form-urlencoded; charset=UTF-8";
					var requestStream = _request.GetRequestStream();
					var bytes = Encoding.UTF8.GetBytes(_params.ToString());
					requestStream.Write(bytes, 0, bytes.Length);
					requestStream.Close();
				}

				var response = (HttpWebResponse)_request.GetResponse();
				result.StatusCode = (int)response.StatusCode;
				using (var reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
				{
					result.Body = JObject.Parse(reader.ReadToEnd());
				}
			}
			catch (WebException ex)
			{
				var response = (HttpWebResponse)ex.Response;
				result.StatusCode = (int)response.StatusCode;

				if (response.GetResponseStream() != null)
				{
					using (var reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
					{
						result.Body = JObject.Parse(reader.ReadToEnd());
					}
				}
			}

			return result;
		}
#endif

#if NET45 || NET40 || PORTABLE
		public async Task<ApiResponse> GetResponseAsync()
		{
			ApiResponse result = new ApiResponse();

			try
			{
				if (_request.Method != "GET")
				{
					_request.ContentType = "application/x-www-form-urlencoded; charset=UTF-8";
					var bytes = Encoding.UTF8.GetBytes(_params.ToString());
					using (var requestStream = await _request.GetRequestStreamAsync()) {
						await requestStream.WriteAsync(bytes, 0, bytes.Length);
						await requestStream.FlushAsync();
					}
				}

				HttpWebResponse response = (HttpWebResponse)await _request.GetResponseAsync();
				result.StatusCode = (int)response.StatusCode;
				using (var reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
				{
					result.Body = JObject.Parse(reader.ReadToEnd());
				}
			}
			catch (WebException ex)
			{
				var response = (HttpWebResponse)ex.Response;
				result.StatusCode = (int)response.StatusCode;

				if (response.GetResponseStream() != null)
				{
					using (var reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
					{
						result.Body = JObject.Parse(reader.ReadToEnd());
					}
				}
			}

			return result;
		}
#endif

		public void GetResponseAsync(Action<ApiResponse> completed)
		{
			if (_request.Method != "GET")
			{
				_request.ContentType = "application/x-www-form-urlencoded; charset=UTF-8";

				_request.BeginGetRequestStream(new AsyncCallback(asyncResult=>{
					Stream postStream = _request.EndGetRequestStream(asyncResult);

					var bytes = Encoding.UTF8.GetBytes(_params.ToString());
					postStream.Write(bytes,0,bytes.Length);
					postStream.Flush();

					_request.BeginGetResponse(new AsyncCallback(GetResponseCallback),completed);
				}),null);
			}
			else 
			{
				_request.BeginGetResponse(new AsyncCallback(GetResponseCallback),completed);
			}
		}

		private void GetResponseCallback(IAsyncResult asyncResult)
		{
			ApiResponse result = new ApiResponse();
			Action<ApiResponse> completed = (Action<ApiResponse>)asyncResult.AsyncState;
			try
			{
				HttpWebResponse response = (HttpWebResponse)_request.EndGetResponse(asyncResult);
				result.StatusCode = (int)response.StatusCode;
				using (var reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
				{
					result.Body = JObject.Parse(reader.ReadToEnd());
				}
			}
			catch (WebException ex)
			{
				var response = (HttpWebResponse)ex.Response;
				result.StatusCode = (int)response.StatusCode;

				if (response.GetResponseStream() != null)
				{
					using (var reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
					{
						result.Body = JObject.Parse(reader.ReadToEnd());
					}
				}
			}
			catch (Exception)
			{ 
			}
			completed(result);
		}

	}
}
