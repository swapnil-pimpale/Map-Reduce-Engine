/**
 * ThreadCompletionListeners: Interface to listen for thread completions
 */


public interface ThreadCompletionListener {
		void notifyOfMapperThreadCompletion(MapRunner mapRunner);
		void notifyOfReducerThreadCompletion(ReduceRunner reduceRunner);
}
