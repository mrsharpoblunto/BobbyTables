using System;
using System.Collections.Generic;
using System.Text;

namespace BobbyTables
{
	/// <summary>
	/// Provides a public id field so that classes inheriting from this can automatically
	/// be persisted and retrieved. Inheriting from this class is optional - there are a number
	/// of ways to provide object id's (see the insert and update methods of 
	/// <seealso cref="BobbyTables.Table&lt;T&gt;"/> for more information)
	/// </summary>
	public class Record
	{
		/// <summary>
		/// Id used to uniquely identify the record in a dropbox datastore table
		/// </summary>
		public string Id { get; set; }
	}
}
