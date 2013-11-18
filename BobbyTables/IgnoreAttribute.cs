using System;
using System.Collections.Generic;
using System.Text;

namespace BobbyTables
{
	/// <summary>
	/// Putting this attribute on a field or property will result in it being ignored when the object is
	/// being serialized or deserialized by BobbyTables
	/// </summary>
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property,AllowMultiple=false,Inherited=true)]
	public class IgnoreAttribute: Attribute
	{
	}
}
