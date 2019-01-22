package flink.bug;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

public class FakeSource implements SourceFunction<String> {

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    ctx.collect(UUID.randomUUID().toString());
    Thread.sleep(2000L);

    while (true) {
      ctx.collect(UUID.randomUUID().toString());
      Thread.sleep(300L);
    }
  }

  @Override
  public void cancel() {
  }
}
