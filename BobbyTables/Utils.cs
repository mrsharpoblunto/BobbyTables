using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;

namespace BobbyTables
{
	internal delegate bool JTokenEqualityComparer(JToken a, JToken b);

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

		public static JArray ComputeLCS(JArray a, JArray b, JTokenEqualityComparer equalityFunc)
		{
			var sequence = new JArray();
			if (a.Count==0 || b.Count==0)
				return sequence;

			int[,] num = new int[a.Count, b.Count];

			for (int i = 0; i < a.Count; i++)
			{
				for (int j = 0; j < b.Count; j++)
				{
					if (equalityFunc(a[i], b[j])) 
					{
						if (i == 0 || j == 0)
						{
							num[i, j] = 1;
						} 
						else 
						{
							num[i, j] = 1 + num[i - 1, j - 1];
						}
					} 
					else 
					{
						if (i == 0 && j == 0)
						{
							num[i, j] = 0;
						}
						else if ((i == 0) && !(j == 0))
						{
							num[i, j] = Math.Max(0, num[i, j - 1]);
						}
						else if (!(i == 0) && (j == 0))
						{
							num[i, j] = Math.Max(num[i - 1, j], 0);
						}
						else if (!(i == 0) && !(j == 0))
						{
							num[i, j] = Math.Max(num[i - 1, j], num[i, j - 1]);
						}
					}
				}
			}

			Backtrack(sequence, num, equalityFunc, a, b, a.Count-1, b.Count-1);
			return sequence;
		}

		private static void Backtrack(JArray lcs,int[,] num, JTokenEqualityComparer equalityFunc, JArray a, JArray b, int i, int j)
		{
			if (equalityFunc(a[i], b[j]))
			{
				if (i > 0 && j > 0)
				{
					Backtrack(lcs, num, equalityFunc, a, b, i - 1, j - 1);
				}
				lcs.Add(a[i]);
			}
			else
			{
				if (j > 0 && (i == 0 || num[i, j - 1] >= num[i - 1, j]))
				{
					Backtrack(lcs, num, equalityFunc, a, b, i, j - 1);
				}
				else if (i > 0 && (j == 0 || num[i, j - 1] < num[i - 1, j]))
				{
					Backtrack(lcs, num, equalityFunc, a, b, i-1, j);
				}
			}
		}
	}
}
