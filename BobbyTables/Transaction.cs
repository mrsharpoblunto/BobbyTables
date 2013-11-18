using System;
using System.Collections.Generic;
using System.Text;
#if NET45 || NET40 || PORTABLE
using System.Threading.Tasks;
#endif

namespace BobbyTables
{
	/// <summary>
	/// A delegate used to contain all the operations that will occur in a given transaction
	/// </summary>
	public delegate void TransactionDelegate();

	/// <summary>
	/// Used to handle submitting serveral operations in a single atomic operation and handling
	/// any possible conflicts by retrying those operations if necessary
	/// </summary>
	public class Transaction
	{
		private Datastore _store;
		TransactionDelegate _actions;

		internal Transaction(Datastore store, TransactionDelegate actions)
		{
			_store = store;
			_actions = actions;
		}

#if NET45 || NET40 || PORTABLE
		/// <summary>
		/// Attempts to push all actions in the transaction to dropbox asynchronously
		/// </summary>
		/// <returns>True if the actions in the transaction were pushed to dropbox</returns>
		public async Task<bool> PushAsync(int retries = 1)
		{
			int count = 0;
			while (count <= retries)
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
			public int Count,Retries;
			public Action<bool> Success;
			public Action<Exception> Failure;
		};

		/// <summary>
		/// Attempts to push all actions in the transaction to dropbox asynchronously
		/// </summary>
		/// <param name="retries">How many times to retry committing the actions before giving up</param>
		/// <param name="success">Callback if the method is successful</param>
		/// <param name="failure">Callback if the method fails</param>
		/// <returns>True if the actions in the transaction were pushed to dropbox</returns>
		public void PushAsync(Action<bool> success, Action<Exception> failure, int retries = 1)
		{
			PushAsyncContext context = new PushAsyncContext
			{
				Count = 0,
				Retries = retries,
				Success = success,
				Failure = failure
			};

			PushAsyncInternal(context);
		}

		private void PushAsyncInternal(PushAsyncContext context)
		{
			if (context.Count > context.Retries)
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
		/// <param name="retries">How many times to retry committing the actions before giving up</param>
		/// <returns>True if the actions in the transaction were pushed to dropbox</returns>
		public bool Push(int retries = 1)
		{
			int count = 0;
			while (count <= retries)
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
