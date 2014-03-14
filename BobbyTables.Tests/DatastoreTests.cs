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
	public enum TestEnum
	{
		First,
		Second,
		Third
	}

	public class TestObject
	{
		public TestObject()
		{
			_privateFieldsIgnored = 1;
		}
		public string Id;

		[Ignore]
		public string IgnoreThisField;

		private int _privateFieldsIgnored;

		public int ReadonlyPropertiesIgnored
		{
			get { return _privateFieldsIgnored; }
		}

		public string MethodsIgnored()
		{
			return "Should not get serialized";
		}

		public TestEnum EnumValue { get; set; }
		public double DoubleValue;
		public Single SingleValue;
		public float FloatValue { get; set; }
		public int IntValue { get; set; }
		public Int16 Int16Value { get; set; }
		public Int32 Int32Value { get; set; }
		public uint UIntValue { get; set; }
		public UInt16 UInt16Value { get; set; }
		public UInt32 UInt32Value { get; set; }
		public Int64 Int64Value { get; set; }
		public UInt64 UInt64Value { get; set; }
		public long LongValue { get; set; }
		public ulong ULongValue { get; set; }
		public string StringValue;
		public DateTime TimeValue;
		public List<byte> ByteList = new List<byte>();
		public byte[] ByteArray;
		public List<string> StringList = new List<string>();
		public List<int> IntList = new List<int>();
	}

	public class NoIdObject: Record
	{
		public string Description;
	}

	public class CustomObject
	{

	}

	public class UnserializableObject
	{
		public CustomObject Child = new CustomObject();
	}

	[TestFixture]
	public class DatastoreTests
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
		public void InsertWithIdGetter()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [], ""rev"": 1}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 2}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			var db = Manager.GetOrCreate("default");
			db.Pull();
			var table = db.GetTable<NoIdObject>("test_objects");

			var obj = new NoIdObject { Description = "hello", Id="world" };

			// use the description field for the objects id.
			Assert.IsTrue(table.Insert(i=>i.Description, obj));

			Assert.IsTrue(db.Push());

			string expectedRequest = (@"[
  [
    ""I"",
    ""test_objects"",
    ""hello"",
    {
      ""Description"": ""hello"",
      ""Id"": ""world""
    }
  ]
]").Replace(" ", string.Empty).Replace("\r\n", string.Empty);

			// check that we pushed the correct values to dropbox
			mockPushRequest.Verify(req => req.AddParam("handle", "yyyy"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("rev", "1"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("changes", expectedRequest), Times.Exactly(1));
		}

		[Test]
		public void InsertWithAutoIdGeneration()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [], ""rev"": 1}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 2}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			var db = Manager.GetOrCreate("default");
			db.Pull();
			var table = db.GetTable<NoIdObject>("test_objects");

			var obj = new NoIdObject { Description = "hello" };

			// no id specified
			Assert.IsTrue(string.IsNullOrEmpty(obj.Id));

			Assert.IsTrue(table.Insert(obj));

			// after insertion the id has been set
			Assert.IsFalse(string.IsNullOrEmpty(obj.Id));

			Assert.IsTrue(db.Push());

			string expectedRequest = ( @"[
  [
    ""I"",
    ""test_objects"",
    """+obj.Id+ @""",
    {
      ""Description"": ""hello"",
      ""Id"": """ + obj.Id + @"""
    }
  ]
]" ).Replace(" ", string.Empty).Replace("\r\n", string.Empty);

			// check that we pushed the correct values to dropbox
			mockPushRequest.Verify(req => req.AddParam("handle", "yyyy"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("rev", "1"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("changes", expectedRequest), Times.Exactly(1));
		}

		[Test]
		public void InsertDeleteObjects()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [], ""rev"": 1}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 2}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			var db = Manager.GetOrCreate("default");
			db.Pull();

			var table = db.GetTable<TestObject>("test_objects");

			var obj = new TestObject{
				Id = "1",
				EnumValue = TestEnum.Second,
				DoubleValue = 1.0,
				SingleValue = 2.0F,
				FloatValue = 3.0F,
				IntValue = 1,
				Int16Value = 2,
				Int32Value = 3,
				UIntValue = 4,
				UInt16Value = 5,
				UInt32Value = 6,
				Int64Value = 7,
				UInt64Value = 8,
				LongValue = 9,
				ULongValue = 10,
				StringValue = "hello",
				TimeValue = new DateTime(1985,5,28,0,0,0,0)
			};
			obj.ByteList = new List<byte> { 0, 1, 255 };
			obj.ByteArray = new byte[] { 255, 1, 0 };
			obj.StringList = new List<string> { "hello", "world"};
			obj.IntList = new List<int> { 1,2,3 };

			// insert the changes and push them to dropbox
			Assert.IsTrue(table.Insert(obj));
			Assert.IsFalse(table.Insert(obj));
			Assert.IsTrue(db.Push());

			// check that we pushed the correct values to dropbox
			mockPushRequest.Verify(req => req.AddParam("handle", "yyyy"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("rev", "1"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("changes", @"[
  [
    ""I"",
    ""test_objects"",
    ""1"",
    {
      ""Id"": ""1"",
      ""DoubleValue"": 1.0,
      ""SingleValue"": 2.0,
      ""StringValue"": ""hello"",
      ""TimeValue"": {
        ""T"": ""486086400000""
      },
      ""ByteList"": {
        ""B"": ""AAH_""
      },
      ""ByteArray"": {
        ""B"": ""_wEA""
      },
      ""StringList"": [
        ""hello"",
        ""world""
      ],
      ""IntList"": [
        {
          ""I"": ""1""
        },
        {
          ""I"": ""2""
        },
        {
          ""I"": ""3""
        }
      ],
      ""EnumValue"":{
        ""I"":""1""
      },
      ""FloatValue"": 3.0,
      ""IntValue"": {
        ""I"": ""1""
      },
      ""Int16Value"": {
        ""I"": ""2""
      },
      ""Int32Value"": {
        ""I"": ""3""
      },
      ""UIntValue"": {
        ""I"": ""4""
      },
      ""UInt16Value"": {
        ""I"": ""5""
      },
      ""UInt32Value"": {
        ""I"": ""6""
      },
      ""Int64Value"": {
        ""I"": ""7""
      },
      ""UInt64Value"": {
        ""I"": ""8""
      },
      ""LongValue"": {
        ""I"": ""9""
      },
      ""ULongValue"": {
        ""I"": ""10""
      }
    }
  ]
]".Replace(" ", string.Empty).Replace("\r\n", string.Empty)), Times.Exactly(1));

			// after we push the changes can we  retrieve the changes
			var found = from t in table where t.Int16Value == 2 select t;
			Assert.AreEqual(1, found.Count());

			// also check that we can search by the id
			var foundAgain = table.Get("1");
			Assert.IsNotNull(foundAgain);

			//verify the retrieved object is the same as the original
			Assert.AreEqual(obj.Id,foundAgain.Id);
			Assert.AreEqual(obj.EnumValue, TestEnum.Second);
			Assert.AreEqual(obj.DoubleValue,foundAgain.DoubleValue);
			Assert.AreEqual(obj.SingleValue,foundAgain.SingleValue);
			Assert.AreEqual(obj.FloatValue,foundAgain.FloatValue);
			Assert.AreEqual(obj.IntValue,foundAgain.IntValue);
			Assert.AreEqual(obj.Int16Value,foundAgain.Int16Value);
			Assert.AreEqual(obj.Int32Value,foundAgain.Int32Value);
			Assert.AreEqual(obj.UIntValue,foundAgain.UIntValue);
			Assert.AreEqual(obj.UInt16Value,foundAgain.UInt16Value);
			Assert.AreEqual(obj.UInt32Value,foundAgain.UInt32Value);
			Assert.AreEqual(obj.Int64Value,foundAgain.Int64Value);
			Assert.AreEqual(obj.UInt64Value,foundAgain.UInt64Value);
			Assert.AreEqual(obj.LongValue,foundAgain.LongValue);
			Assert.AreEqual(obj.ULongValue,foundAgain.ULongValue);
			Assert.AreEqual(obj.StringValue,foundAgain.StringValue);
			Assert.AreEqual(obj.TimeValue, foundAgain.TimeValue);
			Assert.AreEqual(obj.ByteList.Count, foundAgain.ByteList.Count);
			for (int i=0;i<obj.ByteList.Count;++i) {
				Assert.AreEqual(obj.ByteList[i],foundAgain.ByteList[i]);
			}
			Assert.AreEqual(obj.ByteArray.Length, foundAgain.ByteArray.Length);
			for (int i = 0; i < obj.ByteArray.Length; ++i)
			{
				Assert.AreEqual(obj.ByteArray[i], foundAgain.ByteArray[i]);
			}
			Assert.AreEqual(obj.StringList.Count, foundAgain.StringList.Count);
			for (int i = 0; i < obj.StringList.Count; ++i)
			{
				Assert.AreEqual(obj.StringList[i], foundAgain.StringList[i]);
			}
			Assert.AreEqual(obj.IntList.Count, foundAgain.IntList.Count);
			for (int i = 0; i < obj.IntList.Count; ++i)
			{
				Assert.AreEqual(obj.IntList[i], foundAgain.IntList[i]);
			}

			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 3}"));
			mockPushRequest.ResetCalls();

			//now delete the object
			Assert.IsTrue(table.Delete("1"));
			Assert.IsFalse(table.Delete("1"));
			Assert.IsTrue(db.Push());

			//check that the right stuff got sent to dropbox
			mockPushRequest.Verify(req => req.AddParam("handle", "yyyy"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("rev", "2"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("changes", @"[
  [
    ""D"",
    ""test_objects"",
    ""1""
  ]
]".Replace(" ", string.Empty).Replace("\r\n", string.Empty)), Times.Exactly(1));


			// now the object should be missing
			foundAgain = table.Get("1");
			Assert.IsNull(foundAgain);
		}

		[Test]
		public void PullAndUpdateObjects()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 28, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [{""tid"": ""test_objects"", ""data"": {""ByteArray"": {""B"": ""_wEA""},""EnumValue"": {""I"":""1""},""UInt32Value"": {""I"": ""6""}, ""FloatValue"": 3.0, ""ByteList"": {""B"": ""AAH_""}, ""TimeValue"": {""T"": ""486086400000""}, ""LongValue"": {""I"": ""9""}, ""Int32Value"": {""I"": ""3""}, ""DoubleValue"": 1.0, ""IntList"": [{""I"": ""1""}, {""I"": ""2""}, {""I"": ""3""}], ""IntValue"": {""I"": ""1""}, ""Int16Value"": {""I"": ""2""}, ""UIntValue"": {""I"": ""4""}, ""UInt16Value"": {""I"": ""5""}, ""ULongValue"": {""I"": ""10""}, ""StringValue"": ""hello"", ""Int64Value"": {""I"": ""7""}, ""Id"": ""1"", ""SingleValue"": 2.0, ""StringList"": [""hello"", ""world""], ""UInt64Value"": {""I"": ""8""}}, ""rowid"": ""1""}], ""rev"": 28}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 29}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			var db = Manager.GetOrCreate("default");
			db.Pull();

			var table = db.GetTable<TestObject>("test_objects");

			// after we push the updates can we  retrieve the changes
			var obj = table.Get("1");
			Assert.IsNotNull(obj);

			//lets make some changes to an existing object
			obj.TimeValue = new DateTime(1985, 5, 29, 0, 0, 0, 0);
			obj.EnumValue = TestEnum.Third;
			obj.ByteList = new List<byte> { 0, 1, 255, 1 };
			obj.ByteArray = new byte[] { 255, 1, 0, 255 };
			obj.StringList.Insert(1, "there");
			obj.IntList.RemoveAt(2);
			obj.IntList.Add(4);
			obj.IntList.Add(5);

			Assert.IsTrue(table.Update(obj));
			Assert.IsFalse(table.Update(obj));
			Assert.IsTrue(db.Push());

			// also check that we can search by the id
			var foundAgain = table.Get("1");
			Assert.IsNotNull(foundAgain);

			//verify the retrieved object is the same as the original
			Assert.AreEqual(obj.Id, foundAgain.Id);
			Assert.AreEqual(obj.DoubleValue, foundAgain.DoubleValue);
			Assert.AreEqual(obj.SingleValue, foundAgain.SingleValue);
			Assert.AreEqual(obj.FloatValue, foundAgain.FloatValue);
			Assert.AreEqual(obj.IntValue, foundAgain.IntValue);
			Assert.AreEqual(obj.Int16Value, foundAgain.Int16Value);
			Assert.AreEqual(obj.Int32Value, foundAgain.Int32Value);
			Assert.AreEqual(obj.UIntValue, foundAgain.UIntValue);
			Assert.AreEqual(obj.UInt16Value, foundAgain.UInt16Value);
			Assert.AreEqual(obj.UInt32Value, foundAgain.UInt32Value);
			Assert.AreEqual(obj.Int64Value, foundAgain.Int64Value);
			Assert.AreEqual(obj.UInt64Value, foundAgain.UInt64Value);
			Assert.AreEqual(obj.LongValue, foundAgain.LongValue);
			Assert.AreEqual(obj.ULongValue, foundAgain.ULongValue);
			Assert.AreEqual(obj.StringValue, foundAgain.StringValue);
			Assert.AreEqual(obj.TimeValue, foundAgain.TimeValue);
			Assert.AreEqual(obj.ByteList.Count, foundAgain.ByteList.Count);
			for (int i = 0; i < obj.ByteList.Count; ++i)
			{
				Assert.AreEqual(obj.ByteList[i], foundAgain.ByteList[i]);
			}
			Assert.AreEqual(obj.ByteArray.Length, foundAgain.ByteArray.Length);
			for (int i = 0; i < obj.ByteArray.Length; ++i)
			{
				Assert.AreEqual(obj.ByteArray[i], foundAgain.ByteArray[i]);
			}
			Assert.AreEqual(obj.StringList.Count, foundAgain.StringList.Count);
			for (int i = 0; i < obj.StringList.Count; ++i)
			{
				Assert.AreEqual(obj.StringList[i], foundAgain.StringList[i]);
			}
			Assert.AreEqual(obj.IntList.Count, foundAgain.IntList.Count);
			for (int i = 0; i < obj.IntList.Count; ++i)
			{
				Assert.AreEqual(obj.IntList[i], foundAgain.IntList[i]);
			}

			// check that we pushed the correct change delta to dropbox
			mockPushRequest.Verify(req => req.AddParam("handle", "yyyy"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("rev", "28"), Times.Exactly(1));
			mockPushRequest.Verify(req => req.AddParam("changes",
@"[
	[""U"", ""test_objects"", ""1"", {
        ""TimeValue"": [""P"", {
            ""T"": ""486172800000""
        }],
        ""ByteList"": [""P"", {
            ""B"": ""AAH_AQ""
        }],
        ""ByteArray"": [""P"", {
            ""B"": ""_wEA_w""
        }],
        ""StringList"": [""LI"", 2, ""world""]
    }],
    [""U"", ""test_objects"", ""1"", {
        ""StringList"": [""LD"", 1]
    }],
    [""U"", ""test_objects"", ""1"", {
        ""StringList"": [""LI"", 1, ""there""],
        ""IntList"": [""LI"", 3, {
            ""I"": ""5""
        }]
    }],
    [""U"", ""test_objects"", ""1"", {
        ""IntList"": [""LD"", 2]
    }],
    [""U"", ""test_objects"", ""1"", {
        ""IntList"": [""LI"", 2, {
            ""I"": ""4""
        }],
		""EnumValue"":[""P"", {
            ""I"":""2""
        }]
    }]
]".Replace(" ", string.Empty).Replace("\t", string.Empty).Replace("\r\n", string.Empty)), Times.Exactly(1));
		}

		[Test]
		public void Pull_WhenRemoteDoesNotHaveIdColumn_MapRowIdToId()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 28, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [{""tid"": ""test_objects"", ""data"": {""ByteArray"": {""B"": ""_wEA""},""EnumValue"": {""I"":""1""},""UInt32Value"": {""I"": ""6""}, ""FloatValue"": 3.0, ""ByteList"": {""B"": ""AAH_""}, ""TimeValue"": {""T"": ""486086400000""}, ""LongValue"": {""I"": ""9""}, ""Int32Value"": {""I"": ""3""}, ""DoubleValue"": 1.0, ""IntList"": [{""I"": ""1""}, {""I"": ""2""}, {""I"": ""3""}], ""IntValue"": {""I"": ""1""}, ""Int16Value"": {""I"": ""2""}, ""UIntValue"": {""I"": ""4""}, ""UInt16Value"": {""I"": ""5""}, ""ULongValue"": {""I"": ""10""}, ""StringValue"": ""hello"", ""Int64Value"": {""I"": ""7""}, ""SingleValue"": 2.0, ""StringList"": [""hello"", ""world""], ""UInt64Value"": {""I"": ""8""}}, ""rowid"": ""1""}], ""rev"": 28}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			var db = Manager.GetOrCreate("default");
			db.Pull();

			var table = db.GetTable<TestObject>("test_objects");

			// use the implicit id setter which will populate any public id properties or fields
			var item = table.Get("1");
			Assert.IsNotNull(item);
			Assert.AreEqual("1", item.Id);

			//also check that if an explicit setter is set, that it is used as well
			item = table.Get((t,value)=>t.Id = value + "2","1");
			Assert.IsNotNull(item);
			Assert.AreEqual("12", item.Id);
		}

		[Test]
		public void AwaitPull()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 0, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockAwaitPullRequest = new Mock<IApiRequest>();
			mockAwaitPullRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{
  ""get_deltas"": {
    ""deltas"": {
      ""yyyy"": {
        ""deltas"": [
          {
            ""nonce"": """",
            ""changes"": [
              [
                ""I"",
                ""test_objects"",
                ""1"",
                {
                  ""ByteArray"": {
                    ""B"": ""_wEA""
                  },
                  ""EnumValue"": {
                    ""I"":""1""
                  },
                  ""UInt32Value"": {
                    ""I"": ""6""
                  },
                  ""FloatValue"": 3,
                  ""LongValue"": {
                    ""I"": ""9""
                  },
                  ""ByteList"": {
                    ""B"": ""AAH_""
                  },
                  ""TimeValue"": {
                    ""T"": ""486086400000""
                  },
                  ""Int32Value"": {
                    ""I"": ""3""
                  },
                  ""DoubleValue"": 1,
                  ""StringList"": [
                    ""hello"",
                    ""world""
                  ],
                  ""ULongValue"": {
                    ""I"": ""10""
                  },
                  ""IntValue"": {
                    ""I"": ""1""
                  },
                  ""Int16Value"": {
                    ""I"": ""2""
                  },
                  ""UIntValue"": {
                    ""I"": ""4""
                  },
                  ""UInt16Value"": {
                    ""I"": ""5""
                  },
                  ""IntList"": [
                    {
                      ""I"": ""1""
                    },
                    {
                      ""I"": ""2""
                    },
                    {
                      ""I"": ""3""
                    }
                  ],
                  ""StringValue"": ""hello"",
                  ""Int64Value"": {
                    ""I"": ""7""
                  },
                  ""SingleValue"": 2,
                  ""Id"": ""1"",
                  ""UInt64Value"": {
                    ""I"": ""8""
                  }
                }
              ]
            ],
            ""rev"": 1
          }
        ]
      }
    }
  }
}"));
			mockAwaitPullRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			JObject args = new JObject();
			args["cursors"] = new JObject();
			args["cursors"]["yyyy"] = 0;

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "await?get_deltas=" + Uri.EscapeDataString(args.ToString(Formatting.None)), Manager.ApiToken))
				.Returns(mockAwaitPullRequest.Object);

			var db = Manager.GetOrCreate("default");
			Assert.IsTrue(db.AwaitPull());

			var table = db.GetTable<TestObject>("test_objects");

			// ensure that the database was populated by the await call
			var foundAgain = table.Get("1");
			Assert.IsNotNull(foundAgain);

			//verify the retrieved object is the same as the original
			Assert.AreEqual("1", foundAgain.Id);
			Assert.AreEqual(1.0, foundAgain.DoubleValue);
			Assert.AreEqual("hello", foundAgain.StringValue);
		}

		[Test]
		public void AwaitDatastoreChanges()
		{
			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""datastores"": [{""handle"": ""yyyy"", ""rev"": 0, ""dsid"": ""default""}], ""token"": ""zzzz""}"));

			var mockAwaitPullRequest = new Mock<IApiRequest>();
			mockAwaitPullRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{
  ""get_deltas"": {
    ""deltas"": {
      ""yyyy"": {
        ""deltas"": [
          {
            ""nonce"": """",
            ""changes"": [
              [
                ""I"",
                ""test_objects"",
                ""1"",
                {
                  ""ByteArray"": {
                    ""B"": ""_wEA""
                  },
                  ""EnumValue"": {
                    ""I"":""1""
                  },
                  ""UInt32Value"": {
                    ""I"": ""6""
                  },
                  ""FloatValue"": 3,
                  ""LongValue"": {
                    ""I"": ""9""
                  },
                  ""ByteList"": {
                    ""B"": ""AAH_""
                  },
                  ""TimeValue"": {
                    ""T"": ""486086400000""
                  },
                  ""Int32Value"": {
                    ""I"": ""3""
                  },
                  ""DoubleValue"": 1,
                  ""StringList"": [
                    ""hello"",
                    ""world""
                  ],
                  ""ULongValue"": {
                    ""I"": ""10""
                  },
                  ""IntValue"": {
                    ""I"": ""1""
                  },
                  ""Int16Value"": {
                    ""I"": ""2""
                  },
                  ""UIntValue"": {
                    ""I"": ""4""
                  },
                  ""UInt16Value"": {
                    ""I"": ""5""
                  },
                  ""IntList"": [
                    {
                      ""I"": ""1""
                    },
                    {
                      ""I"": ""2""
                    },
                    {
                      ""I"": ""3""
                    }
                  ],
                  ""StringValue"": ""hello"",
                  ""Int64Value"": {
                    ""I"": ""7""
                  },
                  ""SingleValue"": 2,
                  ""Id"": ""1"",
                  ""UInt64Value"": {
                    ""I"": ""8""
                  }
                }
              ]
            ],
            ""rev"": 1
          }
        ]
      }
    }
  }
}"));
			mockAwaitPullRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "list_datastores", Manager.ApiToken))
				.Returns(mockRequest.Object);

			JObject args = new JObject();
			args["cursors"] = new JObject();
			args["cursors"]["yyyy"] = 0;

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "await?get_deltas=" + Uri.EscapeDataString(args.ToString(Formatting.None)), Manager.ApiToken))
				.Returns(mockAwaitPullRequest.Object);

			List<Datastore> stores = new List<Datastore>();
			Assert.IsTrue(Manager.AwaitDatastoreChanges(stores));
			Assert.AreEqual(1,stores.Count);

			var table = stores[0].GetTable<TestObject>("test_objects");

			// ensure that the database was populated by the await call
			var foundAgain = table.Get("1");
			Assert.IsNotNull(foundAgain);

			//verify the retrieved object is the same as the original
			Assert.AreEqual("1", foundAgain.Id);
			Assert.AreEqual(1.0, foundAgain.DoubleValue);
			Assert.AreEqual("hello", foundAgain.StringValue);
		}

		[Test]
		public void AwaitDatastoreChanges_WhenLocalDatastoreDoesNotHaveDeltas()
		{
			var mockRequest = new Mock<IApiRequest>();
			mockRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""datastores"": [{""handle"": ""yyyy"", ""rev"": 0, ""dsid"": ""default""}, {""handle"": ""no-delta"", ""rev"": 0, ""dsid"": ""no-delta""}], ""token"": ""zzzz""}"));

			var mockAwaitPullRequest = new Mock<IApiRequest>();
			mockAwaitPullRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{
  ""get_deltas"": {
    ""deltas"": {
      ""yyyy"": {
        ""deltas"": [
          {
            ""nonce"": """",
            ""changes"": [
              [
                ""I"",
                ""test_objects"",
                ""1"",
                {
                  ""ByteArray"": {
                    ""B"": ""_wEA""
                  },
                  ""EnumValue"": {
                    ""I"":""1""
                  },
                  ""UInt32Value"": {
                    ""I"": ""6""
                  },
                  ""FloatValue"": 3,
                  ""LongValue"": {
                    ""I"": ""9""
                  },
                  ""ByteList"": {
                    ""B"": ""AAH_""
                  },
                  ""TimeValue"": {
                    ""T"": ""486086400000""
                  },
                  ""Int32Value"": {
                    ""I"": ""3""
                  },
                  ""DoubleValue"": 1,
                  ""StringList"": [
                    ""hello"",
                    ""world""
                  ],
                  ""ULongValue"": {
                    ""I"": ""10""
                  },
                  ""IntValue"": {
                    ""I"": ""1""
                  },
                  ""Int16Value"": {
                    ""I"": ""2""
                  },
                  ""UIntValue"": {
                    ""I"": ""4""
                  },
                  ""UInt16Value"": {
                    ""I"": ""5""
                  },
                  ""IntList"": [
                    {
                      ""I"": ""1""
                    },
                    {
                      ""I"": ""2""
                    },
                    {
                      ""I"": ""3""
                    }
                  ],
                  ""StringValue"": ""hello"",
                  ""Int64Value"": {
                    ""I"": ""7""
                  },
                  ""SingleValue"": 2,
                  ""Id"": ""1"",
                  ""UInt64Value"": {
                    ""I"": ""8""
                  }
                }
              ]
            ],
            ""rev"": 1
          }
        ]
      }
    }
  }
}"));
			mockAwaitPullRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "list_datastores", Manager.ApiToken))
				.Returns(mockRequest.Object);

			JObject args = new JObject();
			args["cursors"] = new JObject();
			args["cursors"]["yyyy"] = 0;
			args["cursors"]["no-delta"] = 0;

			RequestFactory
				.Setup(api => api.CreateRequest("GET", "await?get_deltas=" + Uri.EscapeDataString(args.ToString(Formatting.None)), Manager.ApiToken))
				.Returns(mockAwaitPullRequest.Object);

			List<Datastore> stores = new List<Datastore>();
			Assert.IsTrue(Manager.AwaitDatastoreChanges(stores));
			Assert.AreEqual(1, stores.Count);

			var table = stores[0].GetTable<TestObject>("test_objects");

			// ensure that the database was populated by the await call
			var foundAgain = table.Get("1");
			Assert.IsNotNull(foundAgain);

			//verify the retrieved object is the same as the original
			Assert.AreEqual("1", foundAgain.Id);
			Assert.AreEqual(1.0, foundAgain.DoubleValue);
			Assert.AreEqual("hello", foundAgain.StringValue);
		}


		[Test]
		public void ChangesNotPushedWhenConflictOccurs()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 3, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""conflict"": ""Conflict: delta is for db rev 0, but actual db rev is 3""}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [], ""rev"": 3}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			var db = Manager.GetOrCreate("default");

			var table = db.GetTable<TestObject>("test_objects");

			var obj = new TestObject
			{
				Id = "1",
				TimeValue = DateTime.Now,
				StringValue = "Hello"
			};
			obj.ByteList = new List<byte> { };
			obj.ByteArray = new byte[] {};
			obj.StringList = new List<string> {};
			obj.IntList = new List<int> {};

			// insert the changes and push them to dropbox
			Assert.IsTrue(table.Insert(obj));

			// the push should fail due to a conflict
			Assert.IsFalse(db.Push());

			// the persisted state of the table should remain unchanged
			Assert.IsNull(table.Get("1"));

			// trying to pull to update should fail as there
			// are still local pending changes
			Assert.Catch(typeof(DatastoreException), () =>
			{
				db.Pull();
			});

			db.Revert();

			// after reverting pending changes, the pull should succeed
			db.Pull();
			Assert.AreEqual(3, db.Rev);
		}

		[Test]
		public void TransactionRetryWithConflict()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 3, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""conflict"": ""Conflict: delta is for db rev 0, but actual db rev is 3""}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [], ""rev"": 3}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			var db = Manager.GetOrCreate("default");

			var table = db.GetTable<TestObject>("test_objects");

			var obj = new TestObject
			{
				Id = "1",
				TimeValue = DateTime.Now,
				StringValue = "Hello"
			};
			obj.ByteList = new List<byte> { };
			obj.ByteArray = new byte[] { };
			obj.StringList = new List<string> { };
			obj.IntList = new List<int> { };

			int attempts = 0;
			var transaction = db.Transaction(() =>
			{
				Assert.IsTrue(table.Insert(obj));
				if (attempts++ == 1)
				{
					// ensure the mock push request succeeds the second time
					mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 4}"));
				}
			});

			Assert.IsTrue(transaction.Push());

			// check that the transaction had to retry due to the initial conflict
			Assert.AreEqual(2,attempts);
			// check that the second push attempt went through and got a valid response
			Assert.AreEqual(4, db.Rev);
		}

		[Test]
		public void SaveAndLoad()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 28, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [{""tid"": ""test_objects"", ""data"": {""ByteArray"": {""B"": ""_wEA""}, ""UInt32Value"": {""I"": ""6""}, ""FloatValue"": 3.0, ""ByteList"": {""B"": ""AAH_""}, ""TimeValue"": {""T"": ""486086400000""}, ""LongValue"": {""I"": ""9""}, ""Int32Value"": {""I"": ""3""}, ""DoubleValue"": 1.0, ""IntList"": [{""I"": ""1""}, {""I"": ""2""}, {""I"": ""3""}], ""IntValue"": {""I"": ""1""}, ""Int16Value"": {""I"": ""2""}, ""UIntValue"": {""I"": ""4""}, ""UInt16Value"": {""I"": ""5""}, ""ULongValue"": {""I"": ""10""}, ""StringValue"": ""hello"", ""Int64Value"": {""I"": ""7""}, ""Id"": ""1"", ""SingleValue"": 2.0, ""StringList"": [""hello"", ""world""], ""UInt64Value"": {""I"": ""8""}}, ""rowid"": ""1""}], ""rev"": 28}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			// initialize the database from the remote state
			var db = Manager.GetOrCreate("default");
			db.Pull();

			// save the database state
			StringBuilder saved = new StringBuilder();
			using (StringWriter writer = new StringWriter(saved))
			{
				db.Save(writer);
			}

			string expected = @"{""id"":""default"",""handle"":""yyyy"",""rev"":28,""tables"":{""test_objects"":{""1"":{""ByteArray"":{""B"":""_wEA""},""UInt32Value"":{""I"":""6""},""FloatValue"":3.0,""ByteList"":{""B"":""AAH_""},""TimeValue"":{""T"":""486086400000""},""LongValue"":{""I"":""9""},""Int32Value"":{""I"":""3""},""DoubleValue"":1.0,""IntList"":[{""I"":""1""},{""I"":""2""},{""I"":""3""}],""IntValue"":{""I"":""1""},""Int16Value"":{""I"":""2""},""UIntValue"":{""I"":""4""},""UInt16Value"":{""I"":""5""},""ULongValue"":{""I"":""10""},""StringValue"":""hello"",""Int64Value"":{""I"":""7""},""Id"":""1"",""SingleValue"":2.0,""StringList"":[""hello"",""world""],""UInt64Value"":{""I"":""8""}}}}}";
			string actual = saved.ToString();
			Assert.AreEqual(expected, actual);

			// load up the saved content
			using (StringReader reader = new StringReader(actual)) {
				db = Manager.Load(reader);
			}

			// grab a stored object from the database and ensure it is unchanged.
			var table = db.GetTable<TestObject>("test_objects");
			// the table should still only have one entry
			Assert.AreEqual(1, table.Count());
			var obj = table.Get("1");
			Assert.IsNotNull(obj);
		}


		[Test]
		public void SerializationErrors()
		{
			var mockGetRequest = new Mock<IApiRequest>();
			mockGetRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""handle"": ""yyyy"", ""rev"": 1, ""created"": false}"));
			mockGetRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockSnapshotRequest = new Mock<IApiRequest>();
			mockSnapshotRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rows"": [], ""rev"": 1}"));
			mockSnapshotRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			var mockPushRequest = new Mock<IApiRequest>();
			mockPushRequest.Setup(req => req.GetResponse()).Returns(new ApiResponse(200, @"{""rev"": 2}"));
			mockPushRequest.Setup(req => req.AddParam(It.IsAny<string>(), It.IsAny<string>()));

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_or_create_datastore", Manager.ApiToken))
				.Returns(mockGetRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "get_snapshot", Manager.ApiToken))
				.Returns(mockSnapshotRequest.Object);

			RequestFactory
				.Setup(api => api.CreateRequest("POST", "put_delta", Manager.ApiToken))
				.Returns(mockPushRequest.Object);

			var db = Manager.GetOrCreate("default");
			db.Pull();

			var table = db.GetTable<TestObject>("test_objects");

			var obj = new TestObject
			{
				Id = "1",
			};

			// can't insert objects which have null field values
			// as dropbox can't store null values
			Assert.Catch(typeof(ArgumentException) ,()=>
			{
				table.Insert(obj);
			});

			var table1 = db.GetTable<UnserializableObject>("invalid_objects");

			// can't insert objects which fields containing types
			// which cannot be stored by the dropbox datastore - this
			// includes any custom classes or non primitive types that
			// are not lists of primitive types
			Assert.Catch(typeof(ArgumentException), () =>
			{
				table1.Insert("2", new UnserializableObject());
			});
		}
	}
}
