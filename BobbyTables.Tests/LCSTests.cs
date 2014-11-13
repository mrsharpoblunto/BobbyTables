using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace BobbyTables.Tests
{
	[TestFixture]
	public class LCSTests
	{
		[Test]
		public void TestSequence()
		{
			JArray a = new JArray();
			a.Add("X");
			a.Add("M");
			a.Add("J");
			a.Add("Y");
			a.Add("A");
			a.Add("U");
			a.Add("Z");
			JArray b = new JArray();
			b.Add("M");
			b.Add("Z");
			b.Add("J");
			b.Add("A");
			b.Add("W");
			b.Add("X");
			b.Add("U");

			JArray lcs = Utils.ComputeLCS(a, b, (v1, v2) => v1.ToString() == v2.ToString());
			Assert.AreEqual(4, lcs.Count);
		}
	}
}
