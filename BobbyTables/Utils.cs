using System;
using System.Collections.Generic;
using System.Text;

namespace BobbyTables
{
	internal static class Utils
	{
		public static string ToDBase64(byte[] bytes) {
			return Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').Replace("=", string.Empty);
		}

		public static byte[] FromDBase64(string b64)
		{
			b64 = b64.Replace('-', '+').Replace('_', '/');
			if (b64.Length % 4 != 0)
				b64 += ("===").Substring(0, 4 - (b64.Length % 4));
			return Convert.FromBase64String(b64);
		}
	}
}
