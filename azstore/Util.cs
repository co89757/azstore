using System;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.WindowsAzure.Storage;

namespace azstore{

    [AttributeUsage(AttributeTargets.Parameter)]
    public class NotNullAttribute : Attribute{

    }

    public static class Util{

        public static T Syncify<T>(Func<Task<T>> fn ) => Task.Factory.StartNew(fn)
                                                            .Unwrap()
                                                            .GetAwaiter()
                                                            .GetResult();

        public static void Syncify(Func<Task> fn) => Task.Factory.StartNew(fn)
                                                            .Unwrap()
                                                            .GetAwaiter()
                                                            .GetResult();
        public static void EnsureNonNull<T>(T arg, string argname) {
            if (null == arg)
            {
                throw new ArgumentNullException(argname);
            }
        }

        public static void Ensure<T>(T arg, Predicate<T> validator, string errorMessage){
            EnsureNonNull(errorMessage, "error message");
            if (! validator(arg))
            {
                throw new ArgumentException(errorMessage);
            }
        }

        public static void Retry(Action action, int maxRetry, int delayMs, Predicate<Exception> retryWhen ){
            for (int i = 0; i < maxRetry; i++)
            {
              try
              {
                  action();
                  return;
              }
              catch (System.Exception e) when (retryWhen(e))
              {
                  Thread.Sleep(delayMs * (i+1) );
              }
            }
            throw new TimeoutException($"action {action.Method.Name} fails after {maxRetry} retries");
        }

        public static void Retry<TEx>(Action action, int maxRetry, int delayMs ) where TEx : System.Exception {
            for (int i = 0; i < maxRetry; i++)
            {
              try
              {
                  action();
                  return;
              }
              catch (TEx e)  
              {
                  Thread.Sleep(delayMs * (i+1) );
                  if (i == maxRetry)
                  {
                      throw new TimeoutException($"action {action.Method.Name} fails after {maxRetry} retries", e);
                  }
              }
            }            
        }

        /// <summary>
        /// Performs operation on a given Azure object. If the operation cannot be completed because of
        /// a conflict on the server, it is repeated every 500 ms up to 15 minutes.
        /// After 15 minutes an exception is thrown.
        /// </summary>
        public static void PerformAzureOperationWithTimeout<T>(Action<T> operation, T azureObject)
        {
            for (int i = 0; ; i++)
            {
                try
                {
                    operation(azureObject);
                    break;
                }
                catch (StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)System.Net.HttpStatusCode.Conflict)
                    {
                        if (i == 0)
                        {
                            
                        }
                        else if (i >= (15 * 60 * 1000 / 500)) // 15 minutes
                        {                             
                            throw;
                        }

                        // this object is being deleted, wait and try again
                        System.Threading.Thread.Sleep(500);
                    }
                    else
                    {
                        // there is other unknown problem
                        throw;
                    }
                }
            }
        }

         
    }
}