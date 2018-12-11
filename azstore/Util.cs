using System;
using System.Threading.Tasks;
using System.Threading;
namespace azstore{

    [AttributeUsage(AttributeTargets.Parameter)]
    public class NotNullAttribute : Attribute{

    }

    public static class Util{

        public static T Syncify<T>(this Task<T> wrapee){
            try
            {
                wrapee.Wait();
                return wrapee.Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException ;
            }
        }

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
    }
}