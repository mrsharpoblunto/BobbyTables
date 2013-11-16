using System;
using System.Collections.Generic;
using System.Text;

namespace BobbyTables
{
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property,AllowMultiple=false,Inherited=true)]
	public class IgnoreAttribute: Attribute
	{
	}
}
