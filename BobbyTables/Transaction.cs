using System;
using System.Collections.Generic;
using System.Text;
#if NET45 || NET40 || PORTABLE
using System.Threading.Tasks;
#endif

namespace BobbyTables
{
	public delegate void TransactionDelegate();

	public class Transaction
	{
		private Datastore _store;
		private int _retries;
		TransactionDelegate _actions;

		internal Transaction(Datastore store, TransactionDelegate actions, int retries)
		{
			_store = store;
			_actions = actions;
			_retries = retries;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Attempts to push all actions in the transaction to dropbox asynchronously
		/// </summary>
		/// <returns>True if the actions in the transaction were pushed to dropbox</returns>
		public async Task<bool> PushAsync()
		{
			int count = 0;
			while (count <= _retries)
			{
				_actions();

				if (!await _store.PushAsync())
				{
					_store.Revert();
					await _store.PullAsync();
					++count;
				}
				else
				{
					return true;
				}
			}
			return false;
		}
#endif

		internal class PushAsyncContext 
		{
			public int Count;
			public Action<bool> Success;
			public Action<Exception> Failure;
		};

		/// <summary>
		/// Attempts to push all actions in the transaction to dropbox asynchronously
		/// </summary>
		/// <returns>True if the actions in the transaction were pushed to dropbox</returns>
		public void PushAsync(Action<bool> success, Action<Exception> failure)
		{
			PushAsyncContext context = new PushAsyncContext
			{
				Count = 0,
				Success = success,
				Failure = failure
			};

			PushAsyncInternal(context);
		}

		private void PushAsyncInternal(PushAsyncContext context)
		{
			if (context.Count > _retries)
			{
				context.Success(false);
				return;
			}

			try
			{
				_actions();
			}
			catch (Exception ex)
			{
				context.Failure(ex);
				return;
			}

			_store.PushAsync(pushResult =>
			{
				if (!pushResult)
				{
					_store.Revert();
					_store.PullAsync(() =>
					{
						++context.Count;
						PushAsyncInternal(context);
					}, ex =>
					{
						context.Failure(ex);
					});
				}
				else
				{
					context.Success(true);
				}
			}, ex =>
			{
				context.Failure(ex);
			});
		}

#if !PORTABLE
		/// <summary>
		/// Attempts to push all actions in the transaction to dropbox
		/// </summary>
		/// <returns>True if the actions in the transaction were pushed to dropbox</returns>
		public bool Push()
		{
			int count = 0;
			while (count <= _retries)
			{
				_actions();

				if (!_store.Push())
				{
					_store.Revert();
					_store.Pull();
					++count;
				}
				else
				{
					return true;
				}
			}
			return false;
		}
#endif
	}
}
