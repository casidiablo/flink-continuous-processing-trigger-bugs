/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package flink.bug;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * This will make the trigger (ContinuousProcessingTimeTrigger) lose its state when the windows are merged. Hence,
 * it will never trigger again.
 */
public class Bug2 {
  public static void main(String[] args) throws Exception {
    LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setParallelism(1);

    env.addSource(new FakeSource())
        .keyBy(new KeySelector<String, String>() {
          @Override
          public String getKey(String value) throws Exception {
            return String.valueOf(value.charAt(0));
          }
        })
        .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
        .trigger(new SafeContinuousProcessingTimeTrigger(ContinuousProcessingTimeTrigger.of(Time.seconds(3))))
        .reduce(new ReduceFunction<String>() {
          @Override
          public String reduce(String value1, String value2) throws Exception {
            return value1;
          }
        })
        .print();

    env.execute();
  }

  public static class SafeContinuousProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {

    private final ContinuousProcessingTimeTrigger<W> delegate;

    public SafeContinuousProcessingTimeTrigger(ContinuousProcessingTimeTrigger<W> delegate) {
      this.delegate = delegate;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
      return delegate.onElement(element, timestamp, window, ctx);
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
      return delegate.onProcessingTime(time, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
      return delegate.onEventTime(time, window, ctx);
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
      try {
        delegate.clear(window, ctx);
      } catch (Exception e) {
        // Failed to clear... let's see what happens...
      }
    }

    @Override
    public boolean canMerge() {
      return delegate.canMerge();
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
      delegate.onMerge(window, ctx);
    }
  }
}
