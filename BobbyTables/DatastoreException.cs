using System;
using System.Collections.Generic;
using System.Text;

namespace BobbyTables
{
	public class DatastoreException : Exception
	{
		public DatastoreException(string message)
			: base(message)
		{
		}
	}
}
