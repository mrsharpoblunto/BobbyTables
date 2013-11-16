using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace BobbyTables.Tests
{
	[TestFixture]
    public class DatastoreManagerTests
    {
		private Mock<IApiRequestFactory> RequestFactory { get; set; }
		private DatastoreManager Manager { get; set; }

		[SetUp]
		public void Setup()
		{
			Manager = new DatastoreManager("abcd");

			var factory = new Mock<IApiRequestFactory>();
			ApiRequestFactory.Current = factory.Object;
			RequestFactory = factory;
		}

		[Test]
		public void GetOrCreateDatastore()
		{
			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1, ""created"": false}"));
			mockRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));
			
			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockRequest.Object);

			var db = Manager.GetOrCreate("test");

			Assert.AreEqual("yyyy", db.Handle);
			//rev is always initialized to 0 to force the client
			//to do a refresh before any changes will be pushed
			Assert.AreEqual(0, db.Rev);

			mockRequest.Verify(req => req.AddParam(It.IsIn<string>(new[] { "dsid" }), It.IsIn<string>(new[] { "test" })), Times.Exactly(1));

			// now we'll change the remote copy to see if specifying the UseCached parameter works as expected
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""zzzz"", ""rev"": 1, ""created"": false}"));

			db = Manager.GetOrCreate("test", DatastoreQueryOptions.UseCached);

			//should recieve the cached copy
			Assert.AreEqual("yyyy", db.Handle);

			db = Manager.GetOrCreate("test", DatastoreQueryOptions.ForceRefresh);

			//should have got the new copy
			Assert.AreEqual("zzzz", db.Handle);
		}

		[Test]
		public void GetDatastore()
		{
			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1}"));
			mockRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));
			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_datastore", Manager.ApiToken))
				.Returns(mockRequest.Object);

			var db = Manager.Get("test");

			Assert.AreEqual("yyyy", db.Handle);
			// rev is always initialized to 0 to force the client
			// to do a refresh before any changes will be pushed
			Assert.AreEqual(0, db.Rev);

			mockRequest.Verify(req => req.AddParam(It.IsIn<string>(new[] { "dsid" }), It.IsIn<string>(new[] { "test" })), Times.Exactly(1));

			// now we'll change the remote copy to see if specifying the UseCached parameter works as expected
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""zzzz"", ""rev"": 1}"));

			db = Manager.Get("test",DatastoreQueryOptions.UseCached);

			//should recieve the cached copy
			Assert.AreEqual("yyyy", db.Handle);

			db = Manager.Get("test", DatastoreQueryOptions.ForceRefresh);

			//should have got the new copy
			Assert.AreEqual("zzzz", db.Handle);
		}

		[Test]
		public void CreateDatastore()
		{
			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1, ""created"": true}"));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "create_datastore", Manager.ApiToken))
				.Returns(mockRequest.Object);

			var result = Manager.Create("test123456789012345678901234567890123456789012345678901");
			var db = result.Value;

			Assert.AreEqual("yyyy", db.Handle);
			// rev is always initialized to 0 to force the client
			// to do a refresh before any changes will be pushed
			Assert.AreEqual(0, db.Rev);
			Assert.AreEqual(".NiSM2WWVGz-nlpNk5kEmCHhQ313Q_lek40C_4b0jSkY", result.Key);

			mockRequest.Verify(req => req.AddParam("dsid", ".NiSM2WWVGz-nlpNk5kEmCHhQ313Q_lek40C_4b0jSkY"), Times.Exactly(1));
			mockRequest.Verify(req => req.AddParam("key", "test123456789012345678901234567890123456789012345678901"), Times.Exactly(1));
		}

		[Test]
		public void ListDatastores()
		{
			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""datastores"": [{""handle"": ""xxxx"", ""rev"": 0, ""dsid"": ""db1""}, {""handle"": ""yyyy"", ""rev"": 23, ""dsid"": ""db2""}], ""token"": ""zzzz""}"));

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "list_datastores", Manager.ApiToken))
				.Returns(mockRequest.Object);

			var list = Manager.List();

			Assert.AreEqual(2, list.Count());

			Assert.AreEqual(0,list.First().Rev);
			Assert.AreEqual("xxxx",list.First().Handle);
			Assert.AreEqual("db1", list.First().Id);
			Assert.AreEqual(0, list.Last().Rev);
			Assert.AreEqual("yyyy", list.Last().Handle);
			Assert.AreEqual("db2", list.Last().Id);

			// now we'll change the remote copy to see if specifying the UseCached parameter works as expected
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""datastores"": [{""handle"": ""yyyy"", ""rev"": 23, ""dsid"": ""db2""}], ""token"": ""zzzz""}"));

			list = Manager.List(DatastoreQueryOptions.UseCached);
			Assert.AreEqual(2, list.Count());

			list = Manager.List(DatastoreQueryOptions.ForceRefresh);
			Assert.AreEqual(1, list.Count());
		}

		[Test]
		public void DeleteDatastore()
		{
			var mockListRequest = new Mock<IApiRequest>();
			mockListRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""datastores"": [{""handle"": ""xxxx"", ""rev"": 0, ""dsid"": ""db1""}, {""handle"": ""yyyy"", ""rev"": 23, ""dsid"": ""db2""}], ""token"": ""zzzz""}"));

			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""ok"": ""Deleted datastore with handle: u'xxxx'""}"));

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "list_datastores", Manager.ApiToken))
				.Returns(mockListRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "delete_datastore", Manager.ApiToken))
				.Returns(mockRequest.Object);

			var list = Manager.List();

			Assert.AreEqual(2, list.Count());

			//now lets delete one of the datastores.
			Manager.Delete(list.First());

			//now only one should be left
			list = Manager.List();
			Assert.AreEqual(1, list.Count());
			Assert.AreEqual(0, list.First().Rev);
			Assert.AreEqual("yyyy", list.First().Handle);
			Assert.AreEqual("db2", list.First().Id);

			//did we pass the correct params to delete?
			mockRequest.Verify(req => req.AddParam("handle", "xxxx"), Times.Exactly(1));
		}

		[Test]
		public void AwaitListChanges()
		{
			var mockListRequest = new Mock<IApiRequest>();
			mockListRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""datastores"": [], ""token"": ""yyyy""}"));

			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{
  ""list_datastores"": {
    ""datastores"": [
      {
        ""handle"": ""xxxx"",
        ""rev"": 0,
        ""dsid"": ""db1""
      },
      {
        ""handle"": ""yyyy"",
        ""rev"": 0,
        ""dsid"": ""db2""
      }
    ],
    ""token"": ""zzzz""
  }
}"));
			JObject args = new JObject();
			args["token"] = "yyyy";

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "await?list_datastores=" + Uri.EscapeDataString(args.ToString(Formatting.None)), Manager.ApiToken))
				.Returns(mockRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "list_datastores", Manager.ApiToken))
				.Returns(mockListRequest.Object);

			var list = Manager.List();
			Assert.AreEqual(0, list.Count());

			Assert.IsTrue(Manager.AwaitListChanges());
			
			list = Manager.List();
			Assert.AreEqual(0, list.First().Rev);
			Assert.AreEqual("xxxx", list.First().Handle);
			Assert.AreEqual("db1", list.First().Id);
			Assert.AreEqual(0, list.Last().Rev);
			Assert.AreEqual("yyyy", list.Last().Handle);
			Assert.AreEqual("db2", list.Last().Id);
		}
    }
}
