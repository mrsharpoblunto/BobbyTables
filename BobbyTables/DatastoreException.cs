using System;
using System.Collections.Generic;
using System.Text;

namespace BobbyTables
{
	/// <summary>
	/// Used when errors occur when calling various API methods
	/// </summary>
	public class DatastoreException : Exception
	{
		/// <summary>
		/// Default constructor for a new Datastore exception
		/// </summary>
		/// <param name="message"></param>
		public DatastoreException(string message)
			: base(message)
		{
		}
	}
}
